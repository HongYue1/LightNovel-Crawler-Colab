"""LightNovel Downloader - a Flask web app that wraps lightnovel-crawler
(lncrawl 4.10.0) as a library for Google Colab.

Why a web app instead of a notebook widget:
  * a real full-page UI you can scroll comfortably
  * background worker threads update shared state freely and the browser polls
    over HTTP, so progress is always live (no Colab main-thread trait limits)
  * finished files download straight through the browser (Flask send_file)

Features:
  * per-download thread count (blank = the source's own default)
  * multiple novels downloading at the same time
  * chapter-granular pause / resume
  * chapter ranges (all / first N / last N / a-b), each file has ONLY its range
  * EPUB + TXT output
  * per-file direct download + one-click upload to GoFile

Launch (inside Colab):  import app; app.main()
"""

from __future__ import annotations

import logging
import os
import shutil
import socket
import threading
import time
import traceback
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from urllib.parse import urlparse

from flask import Flask, jsonify, request, send_file, Response


# --------------------------------------------------------------------------- #
# Environment helpers
# --------------------------------------------------------------------------- #
def _in_colab():
    try:
        import google.colab  # noqa: F401
        return True
    except Exception:
        return False


def _patch_lncrawl_for_root():
    """Colab runs as root and its matplotlib inline backend makes lncrawl think
    a real display exists, so it never goes headless and never disables Chrome's
    sandbox -> Chrome crashes as root. Force headless/no-sandbox by telling
    lncrawl there is no display."""
    try:
        from lncrawl.utils.platforms import Platform
        Platform.has_display = False
    except Exception:
        traceback.print_exc()


def _human_size(num):
    try:
        num = float(num)
    except (TypeError, ValueError):
        return ""
    for unit in ("B", "KB", "MB", "GB"):
        if num < 1024 or unit == "GB":
            return ("%.0f %s" if unit == "B" else "%.1f %s") % (num, unit)
        num /= 1024
    return ""


def _file_size(path):
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def _fmt_enum(fmt):
    OutputFormat = None
    for mod in ("lncrawl.enums", "lncrawl.dao", "lncrawl.models"):
        try:
            OutputFormat = __import__(mod, fromlist=["OutputFormat"]).OutputFormat
            break
        except Exception:
            continue
    if OutputFormat is None:
        return fmt
    alias = {"txt": "text"}
    return getattr(OutputFormat, alias.get(fmt, fmt), fmt)


def _upload_gofile(path):
    try:
        from gofilepy import GofileClient
    except Exception as exc:
        return None, "gofilepy-api is not installed (%s)" % exc
    try:
        client = GofileClient()
        with open(path, "rb") as handle:
            uploaded = client.upload(file=handle)
        link = (
            getattr(uploaded, "page_link", None)
            or getattr(uploaded, "download_page", None)
            or getattr(uploaded, "page", None)
        )
        if not link:
            return None, "GoFile upload returned no link"
        return link, None
    except Exception as exc:
        return None, "GoFile upload failed: %s" % exc


# --------------------------------------------------------------------------- #
# One download job
# --------------------------------------------------------------------------- #
class Job:
    ACTIVE = ("queued", "fetching_meta", "downloading", "binding")

    def __init__(self, manager, opts):
        self.manager = manager
        self.id = uuid.uuid4().hex[:10]
        self.url = (opts.get("url") or "").strip()
        self.opts = {
            "threads": int(opts.get("threads") or 0),
            "range_mode": opts.get("range_mode") or "all",
            "range_n": int(opts.get("range_n") or 0),
            "range_a": int(opts.get("range_a") or 0),
            "range_b": int(opts.get("range_b") or 0),
            "formats": [f for f in (opts.get("formats") or ["epub"]) if f],
        }
        if not self.opts["formats"]:
            self.opts["formats"] = ["epub"]

        self.status = "queued"
        self.title = None
        self.domain = urlparse(self.url).netloc or self.url
        self.total = 0
        self.done = 0
        self.failed = 0
        self.range_label = "All"
        self.threads_effective = None
        self.error = None
        self.results = []

        self.novel = None
        self.crawler = None
        self._selected_ids = []
        self._done_ids = set()
        self._failed_ids = set()
        self.pending = []

        self._pause = threading.Event()
        self._cancel = threading.Event()

    def _fail(self, exc):
        self.error = str(exc) or exc.__class__.__name__
        traceback.print_exc()
        self.status = "failed"

    def run(self):
        try:
            if self.novel is None:
                self.status = "fetching_meta"
                self._prepare()
            if self._cancel.is_set():
                self.status = "canceled"
                return
            self.status = "downloading"
            finished = self._download_phase()
            if self._cancel.is_set():
                self.status = "canceled"
                return
            if not finished:
                self.status = "paused"
                return
            self._images_phase()
            if self._cancel.is_set():
                self.status = "canceled"
                return
            self.status = "binding"
            self._bind_phase()
            if not self.results and not self.error:
                self.error = "No output files were produced."
            self.status = "failed" if (self.error and not self.results) else "done"
        except Exception as exc:
            self._fail(exc)

    def _prepare(self):
        ctx = self.manager.ctx
        crawler = ctx.sources.init_crawler(self.url)
        self.crawler = crawler

        if self.opts["threads"] > 0:
            try:
                crawler.taskman.init_executor(workers=self.opts["threads"])
            except Exception:
                pass
            self.threads_effective = self.opts["threads"]
        else:
            self.threads_effective = getattr(crawler, "workers", None) or 4

        # Fresh start: lncrawl persists every downloaded chapter per novel and
        # its binder bundles ALL of them. Remove any prior copy of this novel so
        # each download's file contains ONLY the range selected for this job.
        try:
            existing = ctx.novels.find_by_url(self.url)
            if existing is not None:
                ctx.novels.delete(existing.id)
        except Exception:
            traceback.print_exc()

        novel = ctx.crawler.fetch_novel(self.manager.user.id, self.url, custom=crawler)
        self.novel = novel
        self.title = getattr(novel, "title", None) or self.url

        all_ids = list(ctx.chapters.list_ids(novel_id=novel.id))
        self._selected_ids, self.range_label = self._select(all_ids)
        self.total = len(self._selected_ids)
        self.pending = list(self._selected_ids)

    def _select(self, all_ids):
        mode = self.opts["range_mode"]
        count = len(all_ids)
        if mode == "first":
            n = self.opts["range_n"] or count
            return all_ids[:n], "First %d" % min(n, count)
        if mode == "last":
            n = self.opts["range_n"] or count
            return all_ids[-n:], "Last %d" % min(n, count)
        if mode == "range":
            a = max(1, self.opts["range_a"] or 1)
            b = self.opts["range_b"] or count
            b = min(b, count)
            return all_ids[a - 1:b], "%d\u2013%d" % (a, b)
        return list(all_ids), "All"

    def _download_phase(self):
        ctx = self.manager.ctx
        executor = self.crawler.taskman
        max_inflight = max(1, self.threads_effective or 4)
        pending = list(self.pending)
        i = 0
        inflight = {}

        while i < len(pending) or inflight:
            if self._cancel.is_set():
                self._abort_inflight(inflight)
                break
            while (
                not self._pause.is_set()
                and not self._cancel.is_set()
                and len(inflight) < max_inflight
                and i < len(pending)
            ):
                cid = pending[i]
                i += 1
                fut = executor.submit_task(
                    ctx.crawler.fetch_chapter,
                    self.manager.user.id,
                    cid,
                    custom=self.crawler,
                )
                inflight[fut] = cid

            if not inflight:
                if self._pause.is_set():
                    self.pending = [c for c in self._selected_ids if c not in self._done_ids]
                    return False
                break

            done, _ = wait(list(inflight.keys()), timeout=0.4, return_when=FIRST_COMPLETED)
            for fut in done:
                cid = inflight.pop(fut)
                try:
                    fut.result()
                    self._done_ids.add(cid)
                    self.done = len(self._done_ids)
                except Exception:
                    self._failed_ids.add(cid)
                    self.failed = len(self._failed_ids)

        self.pending = [c for c in self._selected_ids if c not in self._done_ids]
        if self._cancel.is_set() or self._pause.is_set():
            return False
        return True

    def _abort_inflight(self, inflight):
        try:
            self.crawler.scraper.signal.set()
        except Exception:
            pass
        if inflight:
            wait(list(inflight.keys()))
        try:
            self.crawler.scraper.signal.clear()
        except Exception:
            pass

    def _images_phase(self):
        ctx = self.manager.ctx
        try:
            image_ids = []
            for cid in list(self._done_ids):
                try:
                    image_ids.extend(list(ctx.images.list_ids(chapter_id=cid)))
                except Exception:
                    pass
            for iid in image_ids:
                if self._cancel.is_set():
                    return
                try:
                    self.crawler.taskman.submit_task(
                        ctx.crawler.fetch_image,
                        self.manager.user.id,
                        iid,
                        custom=self.crawler,
                    ).result()
                except Exception:
                    pass
        except Exception:
            pass

    def _bind_phase(self):
        ctx = self.manager.ctx
        formats = self.opts["formats"]
        order = (["epub"] if "epub" in formats else []) + [f for f in formats if f != "epub"]
        epub_artifact = None
        self.results = []
        for fmt in order:
            try:
                artifact = ctx.binder.make_artifact(
                    self.novel.id,
                    self.title,
                    format=_fmt_enum(fmt),
                    user_id=self.manager.user.id,
                    epub=epub_artifact,
                )
                if fmt == "epub":
                    epub_artifact = artifact
                if getattr(artifact, "is_available", True):
                    path = str(ctx.files.resolve(artifact.output_file))
                    size = getattr(artifact, "file_size", None) or _file_size(path)
                    self.results.append({
                        "fmt": fmt,
                        "path": self._stage(path),
                        "size": _human_size(size),
                        "link": None,
                        "uploading": False,
                        "error": None,
                    })
            except Exception as exc:
                self.error = "Failed to build %s: %s" % (fmt.upper(), exc)

    def _stage(self, path):
        try:
            outdir = "/content/LightNovel_Downloads" if _in_colab() else os.path.join(os.getcwd(), "downloads")
            os.makedirs(outdir, exist_ok=True)
            dest = os.path.join(outdir, os.path.basename(path))
            shutil.copy2(path, dest)
            return dest
        except Exception:
            return path

    def upload_gofile(self, fmt):
        target = next((r for r in self.results if r["fmt"] == fmt), None)
        if not target or target.get("uploading") or target.get("link"):
            return
        target["uploading"] = True
        target["error"] = None
        self.manager.io_pool.submit(self._do_upload, target)

    def _do_upload(self, target):
        link, err = _upload_gofile(target["path"])
        target["link"] = link
        target["error"] = err
        target["uploading"] = False

    def path_for(self, fmt):
        target = next((r for r in self.results if r["fmt"] == fmt), None)
        return target["path"] if target else None

    def has_upload_inflight(self):
        return any(r.get("uploading") for r in self.results)

    def is_active(self):
        return self.status in self.ACTIVE or self.has_upload_inflight()

    def is_finished(self):
        return self.status in ("done", "failed", "canceled") and not self.has_upload_inflight()

    def pause(self):
        if self.status == "downloading":
            self._pause.set()

    def resume(self):
        if self.status == "paused":
            self._pause.clear()
            self.manager.pool.submit(self.run)

    def cancel(self):
        self._cancel.set()
        self._pause.clear()
        try:
            self.crawler.scraper.signal.set()
        except Exception:
            pass

    def retry(self):
        self._cancel.clear()
        self._pause.clear()
        self.error = None
        self.pending = [c for c in self._selected_ids if c not in self._done_ids]
        self.manager.pool.submit(self.run)

    def close(self):
        try:
            if self.crawler is not None:
                self.crawler.close()
        except Exception:
            pass

    def to_dict(self):
        return {
            "id": self.id,
            "url": self.url,
            "title": self.title or self.url,
            "domain": self.domain,
            "status": self.status,
            "total": self.total,
            "done": self.done,
            "failed": self.failed,
            "threads": self.opts["threads"] or None,
            "threads_effective": self.threads_effective,
            "range_label": self.range_label,
            "formats": self.opts["formats"],
            "results": [
                {
                    "fmt": r["fmt"],
                    "size": r["size"],
                    "link": r["link"],
                    "uploading": bool(r.get("uploading")),
                    "error": r.get("error"),
                }
                for r in self.results
            ],
            "error": self.error,
            "chapter_count": getattr(self.novel, "chapter_count", None),
        }


# --------------------------------------------------------------------------- #
# Manager
# --------------------------------------------------------------------------- #
class JobManager:
    def __init__(self, max_parallel=3):
        self.pool = ThreadPoolExecutor(max_workers=max_parallel, thread_name_prefix="lnd-job")
        self.io_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="lnd-io")
        self.jobs = {}
        self.order = []
        self._lock = threading.Lock()
        self.ctx = None
        self.user = None
        self.env_error = None
        self._version = ""
        self._setup()

    def _setup(self):
        _patch_lncrawl_for_root()
        try:
            from lncrawl import __version__ as ver
            self._version = ver
        except Exception:
            self._version = ""
        try:
            from lncrawl.context import ctx
            ctx.setup()
            self.ctx = ctx
            self.user = ctx.users.get_admin()
        except Exception as exc:
            self.env_error = str(exc)
            traceback.print_exc()

    def env_line(self):
        if self.env_error:
            return "Setup error: %s" % self.env_error
        ver = ("lncrawl %s" % self._version) if self._version else "lncrawl"
        return "%s in Colab" % ver if _in_colab() else "%s (local)" % ver

    def snapshot(self):
        with self._lock:
            return [self.jobs[j].to_dict() for j in self.order if j in self.jobs]

    def add(self, opts):
        if self.ctx is None:
            return {"ok": False, "error": self.env_error or "engine not ready"}
        job = Job(self, opts)
        with self._lock:
            self.jobs[job.id] = job
            self.order.append(job.id)
        self.pool.submit(job.run)
        return {"ok": True, "id": job.id}

    def _get(self, job_id):
        return self.jobs.get(job_id)

    def pause(self, job_id):
        j = self._get(job_id)
        if j:
            j.pause()

    def resume(self, job_id):
        j = self._get(job_id)
        if j:
            j.resume()

    def cancel(self, job_id):
        j = self._get(job_id)
        if j:
            j.cancel()

    def retry(self, job_id):
        j = self._get(job_id)
        if j:
            j.retry()

    def gofile(self, job_id, fmt):
        j = self._get(job_id)
        if j:
            j.upload_gofile(fmt)

    def remove(self, job_id):
        with self._lock:
            job = self.jobs.pop(job_id, None)
            if job_id in self.order:
                self.order.remove(job_id)
        if job:
            job.cancel()
            self.io_pool.submit(job.close)

    def clear_finished(self):
        removed = 0
        with self._lock:
            for jid in list(self.order):
                job = self.jobs.get(jid)
                if job and job.is_finished():
                    self.jobs.pop(jid, None)
                    self.order.remove(jid)
                    self.io_pool.submit(job.close)
                    removed += 1
        return removed

    def path_for(self, job_id, fmt):
        j = self._get(job_id)
        return j.path_for(fmt) if j else None


# --------------------------------------------------------------------------- #
# Flask app
# --------------------------------------------------------------------------- #
def create_app(max_parallel=3):
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    app = Flask(__name__)
    manager = JobManager(max_parallel=max_parallel)
    app.config["LND_MANAGER"] = manager

    @app.get("/")
    def index():
        return Response(INDEX_HTML, mimetype="text/html")

    @app.get("/api/state")
    def state():
        return jsonify({"jobs": manager.snapshot(), "status_line": manager.env_line()})

    @app.post("/api/add")
    def add():
        data = request.get_json(force=True, silent=True) or {}
        return jsonify(manager.add(data))

    @app.post("/api/action")
    def action():
        data = request.get_json(force=True, silent=True) or {}
        cmd = data.get("cmd")
        jid = data.get("id")
        fmt = data.get("fmt")
        if cmd == "pause":
            manager.pause(jid)
        elif cmd == "resume":
            manager.resume(jid)
        elif cmd == "cancel":
            manager.cancel(jid)
        elif cmd == "retry":
            manager.retry(jid)
        elif cmd == "remove":
            manager.remove(jid)
        elif cmd == "gofile":
            manager.gofile(jid, fmt)
        elif cmd == "clear_finished":
            return jsonify({"ok": True, "removed": manager.clear_finished()})
        else:
            return jsonify({"ok": False, "error": "unknown command"}), 400
        return jsonify({"ok": True})

    @app.get("/api/download/<job_id>/<fmt>")
    def download(job_id, fmt):
        path = manager.path_for(job_id, fmt)
        if not path or not os.path.isfile(path):
            return "File not found", 404
        return send_file(path, as_attachment=True, download_name=os.path.basename(path))

    return app


def _free_port(preferred=8000):
    for port in [preferred, 8010, 8080, 8888, 7860, 5001]:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(("0.0.0.0", port))
            s.close()
            return port
        except OSError:
            continue
    return preferred


def _wait_up(port, tries=60):
    for _ in range(tries):
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=0.3)
            s.close()
            return True
        except OSError:
            time.sleep(0.2)
    return False


def _launch_card(url, env_line):
    return (
        '<div style="max-width:520px;margin:14px 0;padding:22px 24px;'
        'background:#202020;border:1px solid rgba(255,255,255,0.14);border-radius:14px;'
        'font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,sans-serif;'
        'color:#fff;line-height:1.5;">'
        '<div style="display:flex;align-items:center;gap:10px;font-size:18px;font-weight:700;'
        'letter-spacing:-0.01em;">'
        '<span style="width:10px;height:10px;border-radius:50%;background:#5e9fe8;'
        'box-shadow:0 0 0 4px rgba(94,159,232,0.18);"></span>LightNovel Downloader</div>'
        '<div style="color:rgba(255,255,255,0.45);font-size:12.5px;margin:6px 0 18px;">'
        + env_line + ' &middot; running</div>'
        '<a href="' + url + '" target="_blank" rel="noopener" '
        'style="display:inline-flex;align-items:center;gap:8px;background:#5e9fe8;color:#10243b;'
        'text-decoration:none;font-weight:650;font-size:14px;padding:11px 20px;border-radius:9px;">'
        'Open the app &rarr;</a>'
        '<div style="color:rgba(255,255,255,0.42);font-size:12px;margin-top:14px;">'
        'Opens in a new browser tab &middot; keep this browser tab open while you use it.</div>'
        '</div>'
    )


def _show_launch_ui(port, env_line):
    try:
        from IPython.display import display, HTML
    except Exception:
        print("LightNovel Downloader running at http://localhost:%d/" % port)
        return
    if _in_colab():
        proxied = None
        try:
            from google.colab.output import eval_js
            proxied = eval_js("google.colab.kernel.proxyPort(%d)" % port)
        except Exception:
            proxied = None
        if proxied:
            display(HTML(_launch_card(proxied, env_line)))
            return
        try:  # fallback: native link, silencing its deprecation warning
            import contextlib
            from google.colab import output
            with open(os.devnull, "w") as dn, contextlib.redirect_stdout(dn), contextlib.redirect_stderr(dn):
                output.serve_kernel_port_as_window(port, path="/", anchor_text="Open LightNovel Downloader")
            return
        except Exception:
            traceback.print_exc()
    display(HTML(_launch_card("http://localhost:%d/" % port, env_line)))


def main(port=None, max_parallel=3, open_window=True):
    """Start the server and show a clean, clickable launch card."""
    port = port or _free_port(8000)

    # silence Flask/werkzeug startup noise
    try:
        import flask.cli
        flask.cli.show_server_banner = lambda *a, **k: None
    except Exception:
        pass
    logging.getLogger("werkzeug").setLevel(logging.ERROR)

    app = create_app(max_parallel=max_parallel)

    def _run():
        app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False, debug=False)

    threading.Thread(target=_run, daemon=True).start()
    _wait_up(port)
    _show_launch_ui(port, app.config["LND_MANAGER"].env_line())
    return None


# --------------------------------------------------------------------------- #
# Front-end (single embedded HTML document)
# --------------------------------------------------------------------------- #
INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>LightNovel Downloader</title>
<style>
:root {
  --lncs-canvas: #191919;
  --lncs-surface: #202020;
  --lncs-raised: #262626;
  --lncs-hover: #383836;
  --lncs-text: #ffffff;
  --lncs-text-2: rgba(255, 255, 255, 0.65);
  --lncs-text-3: rgba(255, 255, 255, 0.42);
  --lncs-border: rgba(255, 255, 255, 0.14);
  --lncs-border-strong: rgba(255, 255, 255, 0.24);
  --lncs-blue: #5e9fe8;
  --lncs-blue-soft: rgba(94, 159, 232, 0.14);
  --lncs-green: #72bc8f;
  --lncs-green-soft: rgba(114, 188, 143, 0.14);
  --lncs-orange: #de9255;
  --lncs-orange-soft: rgba(222, 146, 85, 0.14);
  --lncs-red: #e97366;
  --lncs-red-soft: rgba(233, 115, 102, 0.14);
  --lncs-radius: 12px;
  --lncs-radius-sm: 8px;
  --lncs-font: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
}
* , *::before, *::after { box-sizing: border-box; }
html, body { margin: 0; padding: 0; }
body {
  background: var(--lncs-canvas);
  color: var(--lncs-text);
  font-family: var(--lncs-font);
  font-size: 14px;
  line-height: 1.5;
  min-height: 100vh;
  padding: 28px 18px 60px;
}
.lncs-root { max-width: 940px; margin: 0 auto; }

/* Header */
.lncs-header { display: flex; align-items: center; justify-content: space-between; gap: 16px; margin-bottom: 22px; flex-wrap: wrap; }
.lncs-title { font-size: 23px; font-weight: 700; letter-spacing: -0.01em; margin: 0; display: flex; align-items: center; gap: 11px; }
.lncs-title .lncs-dot { width: 10px; height: 10px; border-radius: 50%; background: var(--lncs-blue); box-shadow: 0 0 0 4px var(--lncs-blue-soft); }
.lncs-subtitle { color: var(--lncs-text-3); font-size: 12.5px; margin: 4px 0 0; }
.lncs-header-right { display: flex; align-items: center; gap: 14px; }
.lncs-status-line { color: var(--lncs-text-2); font-size: 12px; white-space: nowrap; }

/* Panel */
.lncs-panel { background: var(--lncs-surface); border: 1px solid var(--lncs-border); border-radius: var(--lncs-radius); padding: 18px; }

/* Form */
.lncs-field { display: flex; flex-direction: column; gap: 6px; }
.lncs-label { font-size: 11.5px; font-weight: 600; color: var(--lncs-text-2); text-transform: uppercase; letter-spacing: 0.05em; }
.lncs-url-row { display: flex; gap: 10px; align-items: stretch; }
.lncs-url-row .lncs-field { flex: 1; }
input.lncs-input, select.lncs-input {
  width: 100%; background: var(--lncs-raised); color: var(--lncs-text);
  border: 1px solid var(--lncs-border); border-radius: var(--lncs-radius-sm);
  padding: 10px 12px; font-size: 14px; font-family: var(--lncs-font); min-height: 42px;
  transition: border-color 120ms ease, box-shadow 120ms ease;
}
input.lncs-input::placeholder { color: var(--lncs-text-3); }
input.lncs-input:focus, select.lncs-input:focus, .lncs-btn:focus-visible, .lncs-check:focus-within {
  outline: none; border-color: var(--lncs-blue); box-shadow: 0 0 0 3px var(--lncs-blue-soft);
}
select.lncs-input { appearance: none; -webkit-appearance: none; background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='%23a0a0a0' stroke-width='2'><path d='M6 9l6 6 6-6'/></svg>"); background-repeat: no-repeat; background-position: right 12px center; padding-right: 32px; cursor: pointer; }
.lncs-controls { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 14px; margin-top: 14px; }
.lncs-range-extra { display: flex; gap: 8px; }
.lncs-range-extra input.lncs-input { min-width: 0; }
.lncs-checks { display: flex; gap: 8px; flex-wrap: wrap; }
.lncs-check { display: inline-flex; align-items: center; gap: 8px; background: var(--lncs-raised); border: 1px solid var(--lncs-border); border-radius: var(--lncs-radius-sm); padding: 9px 12px; cursor: pointer; font-size: 13px; user-select: none; min-height: 42px; }
.lncs-check input { accent-color: var(--lncs-blue); width: 16px; height: 16px; cursor: pointer; }
.lncs-check.is-on { border-color: var(--lncs-blue); background: var(--lncs-blue-soft); }

/* Buttons */
.lncs-btn svg { width: 15px; height: 15px; flex-shrink: 0; }
.lncs-tag svg { width: 13px; height: 13px; flex-shrink: 0; }
.lncs-btn { border: 1px solid var(--lncs-border-strong); background: var(--lncs-raised); color: var(--lncs-text); padding: 10px 16px; border-radius: var(--lncs-radius-sm); font-size: 13.5px; font-weight: 600; cursor: pointer; font-family: var(--lncs-font); min-height: 42px; display: inline-flex; align-items: center; justify-content: center; gap: 7px; transition: background 120ms ease, border-color 120ms ease, opacity 120ms ease; white-space: nowrap; text-decoration: none; }
.lncs-btn:hover { background: var(--lncs-hover); }
.lncs-btn:disabled { opacity: 0.45; cursor: not-allowed; }
.lncs-btn-primary { background: var(--lncs-blue); border-color: var(--lncs-blue); color: #10243b; }
.lncs-btn-primary:hover { background: #74afee; }
.lncs-btn-sm { padding: 7px 12px; min-height: 34px; font-size: 12.5px; }
.lncs-btn-ghost { background: transparent; }
.lncs-btn-danger { color: var(--lncs-red); border-color: var(--lncs-red-soft); }
.lncs-btn-danger:hover { background: var(--lncs-red-soft); }
.lncs-add-row { margin-top: 16px; display: flex; justify-content: flex-end; }

/* Section */
.lncs-section-title { font-size: 12px; font-weight: 700; color: var(--lncs-text-2); text-transform: uppercase; letter-spacing: 0.06em; margin: 26px 4px 12px; display: flex; align-items: center; gap: 8px; }
.lncs-count { background: var(--lncs-raised); color: var(--lncs-text-2); border-radius: 20px; padding: 1px 9px; font-size: 11px; font-weight: 600; }
.lncs-jobs { display: flex; flex-direction: column; gap: 12px; }

/* Job card */
.lncs-job { background: var(--lncs-surface); border: 1px solid var(--lncs-border); border-radius: var(--lncs-radius); padding: 16px; }
.lncs-job-top { display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; }
.lncs-job-name { font-size: 15px; font-weight: 650; margin: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 520px; }
.lncs-job-sub { color: var(--lncs-text-3); font-size: 12px; margin: 3px 0 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 520px; }

/* Status pill */
.lncs-pill { display: inline-flex; align-items: center; gap: 6px; font-size: 11.5px; font-weight: 700; padding: 4px 10px; border-radius: 20px; white-space: nowrap; letter-spacing: 0.02em; }
.lncs-pill svg { width: 12px; height: 12px; }
.lncs-pill.s-queued { background: var(--lncs-hover); color: var(--lncs-text-2); }
.lncs-pill.s-meta, .lncs-pill.s-downloading, .lncs-pill.s-binding { background: var(--lncs-blue-soft); color: var(--lncs-blue); }
.lncs-pill.s-paused { background: var(--lncs-orange-soft); color: var(--lncs-orange); }
.lncs-pill.s-done { background: var(--lncs-green-soft); color: var(--lncs-green); }
.lncs-pill.s-failed { background: var(--lncs-red-soft); color: var(--lncs-red); }
.lncs-pill.s-canceled { background: var(--lncs-hover); color: var(--lncs-text-3); }
.lncs-spin { display: inline-flex; animation: lncs-spin 900ms linear infinite; }
@keyframes lncs-spin { to { transform: rotate(360deg); } }
@media (prefers-reduced-motion: reduce) { .lncs-spin { animation: none; } }

/* Progress */
.lncs-progress-wrap { margin-top: 14px; }
.lncs-progress-meta { display: flex; justify-content: space-between; font-size: 12px; color: var(--lncs-text-2); margin-bottom: 6px; }
.lncs-progress-meta b { color: var(--lncs-text); font-weight: 650; }
.lncs-bar { height: 8px; background: var(--lncs-raised); border-radius: 20px; overflow: hidden; }
.lncs-bar-fill { height: 100%; width: 0; background: var(--lncs-blue); border-radius: 20px; transition: width 240ms ease; }
.lncs-bar-fill.is-paused { background: var(--lncs-orange); }
.lncs-bar-fill.is-done { background: var(--lncs-green); }
.lncs-bar-fill.is-failed { background: var(--lncs-red); }
.lncs-bar-fill.is-indeterminate { width: 35% !important; animation: lncs-indet 1.2s ease-in-out infinite; }
@keyframes lncs-indet { 0% { margin-left: -35%; } 100% { margin-left: 100%; } }
@media (prefers-reduced-motion: reduce) { .lncs-bar-fill.is-indeterminate { animation: none; margin-left: 0; } }

/* Tags */
.lncs-tags { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 12px; }
.lncs-tag { font-size: 11px; color: var(--lncs-text-2); background: var(--lncs-raised); border: 1px solid var(--lncs-border); border-radius: 6px; padding: 3px 8px; display: inline-flex; align-items: center; gap: 5px; }
.lncs-tag b { color: var(--lncs-text); font-weight: 600; }

/* Actions + results */
.lncs-job-actions { display: flex; gap: 8px; margin-top: 14px; flex-wrap: wrap; }
.lncs-results { margin-top: 12px; padding-top: 12px; border-top: 1px solid var(--lncs-border); display: flex; flex-direction: column; gap: 10px; }
.lncs-result { display: flex; align-items: center; gap: 10px; font-size: 13px; flex-wrap: wrap; }
.lncs-result .lncs-fmt { font-weight: 700; font-size: 11px; text-transform: uppercase; background: var(--lncs-blue-soft); color: var(--lncs-blue); border-radius: 5px; padding: 2px 7px; letter-spacing: 0.04em; }
.lncs-result .lncs-size { color: var(--lncs-text-3); font-size: 12px; }
.lncs-result-actions { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; margin-left: auto; }
.lncs-result-err { flex-basis: 100%; color: var(--lncs-red); font-size: 12px; margin-top: 2px; }
.lncs-link { color: var(--lncs-blue); text-decoration: none; word-break: break-all; display: inline-flex; align-items: center; gap: 4px; }
.lncs-link:hover { text-decoration: underline; }
.lncs-link svg { width: 13px; height: 13px; flex-shrink: 0; }
.lncs-err { color: var(--lncs-red); font-size: 12.5px; margin-top: 12px; background: var(--lncs-red-soft); border: 1px solid var(--lncs-red-soft); border-radius: var(--lncs-radius-sm); padding: 9px 12px; display: flex; align-items: flex-start; gap: 8px; }
.lncs-err svg { width: 16px; height: 16px; flex-shrink: 0; margin-top: 1px; }

/* Empty */
.lncs-empty { text-align: center; padding: 44px 20px; color: var(--lncs-text-3); border: 1px dashed var(--lncs-border); border-radius: var(--lncs-radius); }
.lncs-empty svg { width: 34px; height: 34px; opacity: 0.5; margin-bottom: 10px; }
.lncs-empty p { margin: 0; font-size: 13.5px; }

@media (max-width: 560px) {
  body { padding: 16px 12px 40px; }
  .lncs-url-row { flex-direction: column; }
  .lncs-job-name, .lncs-job-sub { max-width: 210px; }
}
</style>
</head>
<body>
<div id="app" class="lncs-root"></div>
<script>
(function () {
  var ICON = {
    spin: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"><path d="M21 12a9 9 0 1 1-6.2-8.5"/></svg>',
    pause: '<svg viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="5" width="4" height="14" rx="1"/><rect x="14" y="5" width="4" height="14" rx="1"/></svg>',
    play: '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M7 5.5v13a1 1 0 0 0 1.5.87l11-6.5a1 1 0 0 0 0-1.74l-11-6.5A1 1 0 0 0 7 5.5z"/></svg>',
    stop: '<svg viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="6" width="12" height="12" rx="2"/></svg>',
    check: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M20 6L9 17l-5-5"/></svg>',
    alert: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 9v4M12 17h.01M10.3 3.9L1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z"/></svg>',
    trash: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 6h18M8 6V4a1 1 0 0 1 1-1h6a1 1 0 0 1 1 1v2M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/></svg>',
    down: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3v12M7 10l5 5 5-5M5 21h14"/></svg>',
    cloud: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 13v8M8.5 17.5 12 21l3.5-3.5"/><path d="M20 16.6A5 5 0 0 0 18 7h-1.3A8 8 0 1 0 4 15"/></svg>',
    link: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10 13a5 5 0 0 0 7 0l3-3a5 5 0 0 0-7-7l-1.5 1.5"/><path d="M14 11a5 5 0 0 0-7 0l-3 3a5 5 0 0 0 7 7l1.5-1.5"/></svg>',
    clock: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></svg>',
    book: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M4 5a2 2 0 0 1 2-2h13v16H6a2 2 0 0 0-2 2z"/><path d="M19 17H6a2 2 0 0 0-2 2"/></svg>'
  };
  var STATUS = {
    queued: { label: 'Queued', icon: ICON.clock, cls: 's-queued' },
    fetching_meta: { label: 'Reading novel', icon: ICON.spin, cls: 's-meta', spin: true },
    downloading: { label: 'Downloading', icon: ICON.spin, cls: 's-downloading', spin: true },
    paused: { label: 'Paused', icon: ICON.pause, cls: 's-paused' },
    binding: { label: 'Building files', icon: ICON.spin, cls: 's-binding', spin: true },
    done: { label: 'Complete', icon: ICON.check, cls: 's-done' },
    failed: { label: 'Failed', icon: ICON.alert, cls: 's-failed' },
    canceled: { label: 'Canceled', icon: ICON.stop, cls: 's-canceled' }
  };
  var ACTIVE = ['queued', 'fetching_meta', 'downloading', 'binding'];

  function esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }
  function jobActive(job) {
    if (ACTIVE.indexOf(job.status) !== -1) return true;
    return (job.results || []).some(function (r) { return r.uploading; });
  }
  function jobFinished(job) {
    return ['done', 'failed', 'canceled'].indexOf(job.status) !== -1 &&
      !(job.results || []).some(function (r) { return r.uploading; });
  }

  function api(path, body) {
    var opts = { method: body ? 'POST' : 'GET' };
    if (body) { opts.headers = { 'Content-Type': 'application/json' }; opts.body = JSON.stringify(body); }
    return fetch(path, opts).then(function (r) { return r.json(); });
  }

  var root = document.getElementById('app');
  root.innerHTML =
    '<div class="lncs-header">' +
      '<div><h1 class="lncs-title"><span class="lncs-dot"></span>LightNovel Downloader</h1>' +
      '<p class="lncs-subtitle">In-notebook downloader \u00b7 EPUB + TXT \u00b7 parallel, pausable</p></div>' +
      '<div class="lncs-header-right">' +
        '<span class="lncs-status-line" id="lncs-env"></span>' +
        '<button type="button" class="lncs-btn lncs-btn-sm lncs-btn-ghost" id="lncs-clear">' + ICON.trash + 'Clear finished</button>' +
      '</div>' +
    '</div>' +
    '<div class="lncs-panel">' +
      '<div class="lncs-url-row"><div class="lncs-field">' +
        '<label class="lncs-label" for="lncs-url">Novel page URL</label>' +
        '<input id="lncs-url" class="lncs-input" type="url" inputmode="url" placeholder="https://novelsite.com/novel/some-title" />' +
      '</div></div>' +
      '<div class="lncs-controls">' +
        '<div class="lncs-field"><label class="lncs-label" for="lncs-threads">Threads</label>' +
          '<input id="lncs-threads" class="lncs-input" type="number" min="1" max="32" placeholder="source default" /></div>' +
        '<div class="lncs-field"><label class="lncs-label" for="lncs-range">Chapters</label>' +
          '<select id="lncs-range" class="lncs-input">' +
            '<option value="all">All chapters</option>' +
            '<option value="first">First N</option>' +
            '<option value="last">Last N</option>' +
            '<option value="range">Range (a to b)</option>' +
          '</select></div>' +
        '<div class="lncs-field" id="lncs-extra" style="display:none"><label class="lncs-label">Amount</label>' +
          '<div class="lncs-range-extra">' +
            '<input id="lncs-n" class="lncs-input" type="number" min="1" placeholder="N" />' +
            '<input id="lncs-a" class="lncs-input" type="number" min="1" placeholder="from" style="display:none" />' +
            '<input id="lncs-b" class="lncs-input" type="number" min="1" placeholder="to" style="display:none" />' +
          '</div></div>' +
        '<div class="lncs-field"><label class="lncs-label">Formats</label>' +
          '<div class="lncs-checks">' +
            '<label class="lncs-check is-on"><input type="checkbox" value="epub" checked />EPUB</label>' +
            '<label class="lncs-check"><input type="checkbox" value="txt" />TXT</label>' +
          '</div></div>' +
      '</div>' +
      '<div class="lncs-add-row"><button type="button" class="lncs-btn lncs-btn-primary" id="lncs-add">' + ICON.down + 'Add to queue</button></div>' +
    '</div>' +
    '<div class="lncs-section-title">Downloads<span class="lncs-count" id="lncs-count">0</span></div>' +
    '<div class="lncs-jobs" id="lncs-jobs"></div>';

  var urlInput = document.getElementById('lncs-url');
  var threadsInput = document.getElementById('lncs-threads');
  var rangeSel = document.getElementById('lncs-range');
  var extra = document.getElementById('lncs-extra');
  var nInput = document.getElementById('lncs-n');
  var aInput = document.getElementById('lncs-a');
  var bInput = document.getElementById('lncs-b');
  var jobsWrap = document.getElementById('lncs-jobs');
  var countEl = document.getElementById('lncs-count');
  var envEl = document.getElementById('lncs-env');

  function syncRange() {
    var v = rangeSel.value;
    extra.style.display = v === 'all' ? 'none' : '';
    nInput.style.display = (v === 'first' || v === 'last') ? '' : 'none';
    aInput.style.display = v === 'range' ? '' : 'none';
    bInput.style.display = v === 'range' ? '' : 'none';
  }
  rangeSel.addEventListener('change', syncRange);
  syncRange();

  Array.prototype.forEach.call(document.querySelectorAll('.lncs-check input'), function (cb) {
    cb.addEventListener('change', function () {
      cb.closest('.lncs-check').classList.toggle('is-on', cb.checked);
    });
  });

  function doAdd() {
    var url = (urlInput.value || '').trim();
    if (!/^https?:\/\//i.test(url)) {
      urlInput.focus();
      urlInput.style.borderColor = 'var(--lncs-red)';
      setTimeout(function () { urlInput.style.borderColor = ''; }, 1200);
      return;
    }
    var formats = [];
    Array.prototype.forEach.call(document.querySelectorAll('.lncs-check input:checked'), function (c) { formats.push(c.value); });
    if (!formats.length) formats.push('epub');
    var threads = parseInt(threadsInput.value, 10);
    api('/api/add', {
      url: url,
      threads: (isFinite(threads) && threads > 0) ? threads : 0,
      range_mode: rangeSel.value,
      range_n: parseInt(nInput.value, 10) || 0,
      range_a: parseInt(aInput.value, 10) || 0,
      range_b: parseInt(bInput.value, 10) || 0,
      formats: formats
    }).then(function () { urlInput.value = ''; urlInput.focus(); bump(); });
  }
  document.getElementById('lncs-add').addEventListener('click', doAdd);
  urlInput.addEventListener('keydown', function (e) { if (e.key === 'Enter') doAdd(); });

  document.getElementById('lncs-clear').addEventListener('click', function () {
    if (window.confirm('Clear all finished downloads (complete, failed, canceled) from the list? Files already saved are not deleted.')) {
      api('/api/action', { cmd: 'clear_finished' }).then(bump);
    }
  });

  jobsWrap.addEventListener('click', function (e) {
    var btn = e.target.closest('button[data-act]');
    if (!btn) return;
    api('/api/action', { cmd: btn.getAttribute('data-act'), id: btn.getAttribute('data-id'), fmt: btn.getAttribute('data-fmt') }).then(bump);
  });

  function pct(job) { return job.total ? Math.min(100, Math.round((job.done / job.total) * 100)) : 0; }

  function resultRow(job, r) {
    var size = r.size ? '<span class="lncs-size">' + esc(r.size) + '</span>' : '';
    var dl = '<a class="lncs-btn lncs-btn-sm" href="/api/download/' + esc(job.id) + '/' + esc(r.fmt) + '">' + ICON.down + 'Download</a>';
    var go;
    if (r.link) {
      go = '<a class="lncs-link" href="' + esc(r.link) + '" target="_blank" rel="noopener">' + ICON.link + esc(r.link) + '</a>';
    } else if (r.uploading) {
      go = '<button type="button" class="lncs-btn lncs-btn-sm" disabled><span class="lncs-spin">' + ICON.spin + '</span>Uploading\u2026</button>';
    } else {
      go = '<button type="button" class="lncs-btn lncs-btn-sm" data-act="gofile" data-id="' + esc(job.id) + '" data-fmt="' + esc(r.fmt) + '">' + ICON.cloud + 'Upload to GoFile</button>';
    }
    var err = r.error ? '<div class="lncs-result-err">' + esc(r.error) + '</div>' : '';
    return '<div class="lncs-result"><span class="lncs-fmt">' + esc(r.fmt) + '</span>' + size +
      '<div class="lncs-result-actions">' + dl + go + '</div>' + err + '</div>';
  }

  function jobCard(job) {
    var st = STATUS[job.status] || STATUS.queued;
    var pillIcon = st.spin ? '<span class="lncs-spin">' + st.icon + '</span>' : st.icon;
    var busy = job.status === 'fetching_meta' || job.status === 'binding';
    var indet = busy && !job.total;
    var fillCls = '';
    if (job.status === 'paused') fillCls = 'is-paused';
    else if (job.status === 'done') fillCls = 'is-done';
    else if (job.status === 'failed') fillCls = 'is-failed';
    if (indet) fillCls += ' is-indeterminate';
    var p = job.status === 'done' ? 100 : pct(job);
    var countText = job.total
      ? '<b>' + job.done + '</b> / ' + job.total + ' chapters' + (job.failed ? ' \u00b7 ' + job.failed + ' failed' : '')
      : (busy ? esc(st.label) + '\u2026' : 'Waiting');

    var tags = '';
    tags += '<span class="lncs-tag">' + ICON.book + '<b>' + esc(job.range_label || 'All') + '</b></span>';
    tags += '<span class="lncs-tag">Threads <b>' + esc(job.threads_effective || job.threads || 'default') + '</b></span>';
    tags += '<span class="lncs-tag">' + esc((job.formats || []).join(' \u00b7 ').toUpperCase()) + '</span>';

    var acts = '';
    if (job.status === 'downloading') acts += '<button type="button" class="lncs-btn lncs-btn-sm" data-act="pause" data-id="' + esc(job.id) + '">' + ICON.pause + 'Pause</button>';
    if (job.status === 'paused') acts += '<button type="button" class="lncs-btn lncs-btn-sm lncs-btn-primary" data-act="resume" data-id="' + esc(job.id) + '">' + ICON.play + 'Resume</button>';
    if (ACTIVE.indexOf(job.status) !== -1 || job.status === 'paused') acts += '<button type="button" class="lncs-btn lncs-btn-sm lncs-btn-danger" data-act="cancel" data-id="' + esc(job.id) + '">' + ICON.stop + 'Cancel</button>';
    if (job.status === 'failed' || job.status === 'canceled') acts += '<button type="button" class="lncs-btn lncs-btn-sm" data-act="retry" data-id="' + esc(job.id) + '">' + ICON.play + 'Retry</button>';
    if (jobFinished(job)) acts += '<button type="button" class="lncs-btn lncs-btn-sm" data-act="remove" data-id="' + esc(job.id) + '">' + ICON.trash + 'Remove</button>';

    var results = (job.results && job.results.length)
      ? '<div class="lncs-results">' + job.results.map(function (r) { return resultRow(job, r); }).join('') + '</div>' : '';
    var errBox = job.error ? '<div class="lncs-err">' + ICON.alert + '<span>' + esc(job.error) + '</span></div>' : '';

    return '<div class="lncs-job">' +
      '<div class="lncs-job-top"><div style="min-width:0">' +
        '<h3 class="lncs-job-name">' + esc(job.title || job.url) + '</h3>' +
        '<p class="lncs-job-sub">' + esc(job.domain || job.url) + '</p></div>' +
        '<span class="lncs-pill ' + st.cls + '">' + pillIcon + esc(st.label) + '</span></div>' +
      '<div class="lncs-progress-wrap"><div class="lncs-progress-meta"><span>' + countText + '</span><span>' + (job.total ? p + '%' : '') + '</span></div>' +
        '<div class="lncs-bar"><div class="lncs-bar-fill ' + fillCls + '" style="width:' + p + '%"></div></div></div>' +
      '<div class="lncs-tags">' + tags + '</div>' + results + errBox +
      (acts ? '<div class="lncs-job-actions">' + acts + '</div>' : '') +
    '</div>';
  }

  function render(data) {
    envEl.textContent = data.status_line || '';
    var jobs = data.jobs || [];
    countEl.textContent = String(jobs.length);
    if (!jobs.length) {
      jobsWrap.innerHTML = '<div class="lncs-empty">' + ICON.book + '<p>No downloads yet. Paste a novel URL above to begin.</p></div>';
    } else {
      jobsWrap.innerHTML = jobs.map(jobCard).join('');
    }
    return jobs.some(jobActive);
  }

  var timer = null;
  var fast = false;
  function schedule(active) {
    var want = active ? 800 : 3000;
    if (timer && fast === active) return;
    fast = active;
    if (timer) clearInterval(timer);
    timer = setInterval(poll, want);
  }
  function poll() {
    api('/api/state').then(function (data) { schedule(render(data)); })
      .catch(function () {});
  }
  function bump() { schedule(true); poll(); }

  poll();
})();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    main()

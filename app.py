#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Combined exam archive: download, index, OCR, search, and serve.

Pipeline: download → index → OCR → backfill authors → rebuild FTS
Runs automatically once per day via background scheduler.
"""

import argparse
import csv
import hashlib
import io
import logging
import os
import re
import shlex
import shutil
import sqlite3
import sys
import threading
import time
import traceback
import unicodedata
import zipfile
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

from dotenv import load_dotenv
load_dotenv()

from flask import Flask, abort, jsonify, request, send_file, send_from_directory
from flask_cors import CORS
from werkzeug.exceptions import HTTPException
from werkzeug.middleware.proxy_fix import ProxyFix

# Optional heavy deps — degrade gracefully
try:
    import requests as _requests
    from bs4 import BeautifulSoup
    from requests.adapters import HTTPAdapter, Retry
except ImportError:
    _requests = None
    BeautifulSoup = None

try:
    import pikepdf
except ImportError:
    pikepdf = None

try:
    from pdfminer.high_level import extract_text as pdfminer_extract_text
    from pdfminer.pdfparser import PDFSyntaxError
except ImportError:
    pdfminer_extract_text = None
    PDFSyntaxError = Exception

try:
    import pypdfium2 as pdfium
except ImportError:
    pdfium = None

try:
    import numpy as np
    from PIL import Image
except ImportError:
    np = None
    Image = None

try:
    import cv2
except ImportError:
    cv2 = None

try:
    import pytesseract
    from pytesseract import TesseractError, TesseractNotFoundError
except ImportError:
    pytesseract = None
    TesseractError = RuntimeError
    TesseractNotFoundError = RuntimeError

try:
    from dateutil import parser as dateparser
except ImportError:
    dateparser = None

try:
    from tqdm import tqdm
except ImportError:
    # Minimal fallback so pipeline code still runs
    def tqdm(iterable=None, total=None, desc="", disable=False, **kw):
        return iterable if iterable is not None else range(0)

# =====================================================================
# Configuration & constants
# =====================================================================

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = os.environ.get("EXAM_DB") or str(BASE_DIR / "metadata.db")
DIST_DIR = BASE_DIR / "React" / "dist"
EXAMS_DIR = BASE_DIR / "exams"
CATALOG_CSV = BASE_DIR / "catalog.csv"
FTS_TABLE = "page_fts"

YEAR_MIN, YEAR_MAX = 1989, 2030
MAX_TOKENS = 60
REFILL_PER_SEC = 1.0

# Schedule: seconds between automatic pipeline runs (default 24h)
PIPELINE_INTERVAL = int(os.environ.get("PIPELINE_INTERVAL", str(24 * 3600)))

# Admin password (change this!)
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "Password")

# PDF data roots for serving files
DATA_ROOTS: list[Path] = []
for _env in ("EXAM_DATA_DIR", "EXAMS_ROOT", "DATA_ROOT"):
    _v = os.environ.get(_env)
    if _v:
        DATA_ROOTS.append(Path(_v))
DATA_ROOTS += [Path("/exams"), EXAMS_DIR, BASE_DIR, Path.cwd()]
DATA_ROOTS = [p.resolve() for p in DATA_ROOTS if p]

# Download source pages
SEED_PAGES = [
    # Skip "Home" — it duplicates all department links. Check each dept individually.
    ("Analytical Chemistry", "https://libguides.uark.edu/chbc-cumes/AnalyticalChemistry"),
    ("Biochemistry", "https://libguides.uark.edu/chbc-cumes/Biochemistry"),
    ("Inorganic Chemistry", "https://libguides.uark.edu/chbc-cumes/InorganicChemistry"),
    ("Organic Chemistry", "https://libguides.uark.edu/chbc-cumes/OrganicChemistry"),
    ("Physical Chemistry", "https://libguides.uark.edu/chbc-cumes/PhysicalChemistry"),
]

DEPT_MAP = {
    "analytical": "Analytical Chemistry",
    "inorganic": "Inorganic Chemistry",
    "organic": "Organic Chemistry",
    "biochem": "Biochemistry",
    "biochemistry": "Biochemistry",
    "physical": "Physical Chemistry",
    "p-chem": "Physical Chemistry",
    "pchem": "Physical Chemistry",
}

# Regexes
LD_HINT = re.compile(r"/ld\.php\?content_id=\d+", re.I)
YEAR_RE = re.compile(r"(19|20)\d{2}")
PDF_MAGIC = b"%PDF-"
EOF_RE = re.compile(rb"%%EOF\s*$", re.DOTALL)
RE_DATE_YYYYMMDD = re.compile(r"(?P<y>\d{4})[-_](?P<m>\d{2})[-_](?P<d>\d{2})")
RE_DATE_MDY = re.compile(
    r"(?P<month>January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+(?P<day>\d{1,2}),\s*(?P<year>\d{4})",
    re.IGNORECASE,
)
RE_CUME = re.compile(r"cume[\s_#-]*(?P<n>\d+)", re.IGNORECASE)
RE_SOLUTIONS = re.compile(r"solution", re.IGNORECASE)

# Text normalization
LIGATURES = {"ﬁ": "fi", "ﬂ": "fl", "ﬀ": "ff", "ﬃ": "ffi", "ﬄ": "ffl"}
GREEK = {"π": "pi", "μ": "mu", "α": "alpha", "β": "beta", "γ": "gamma", "δ": "delta"}

USER_AGENT = "CUMES-Downloader/2.0 (+personal-study; polite crawler)"

# =====================================================================
# Flask app setup
# =====================================================================

app = Flask(
    __name__,
    static_folder=str(DIST_DIR) if DIST_DIR.exists() else None,
    static_url_path="/" if DIST_DIR.exists() else None,
)
CORS(app, resources={r"/api/*": {"origins": ["https://exams.gvnb.org"]}})
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

# Logging
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_FILE = os.environ.get("LOG_FILE", str(BASE_DIR / "exams.log"))


def _setup_logging(flask_app: Flask):
    fmt = logging.Formatter("%(asctime)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S%z")
    for h in list(flask_app.logger.handlers):
        flask_app.logger.removeHandler(h)
    flask_app.logger.setLevel(LOG_LEVEL)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    sh.setLevel(LOG_LEVEL)
    flask_app.logger.addHandler(sh)
    try:
        fh = RotatingFileHandler(LOG_FILE, maxBytes=10_485_760, backupCount=5, encoding="utf-8")
        fh.setFormatter(fmt)
        fh.setLevel(LOG_LEVEL)
        flask_app.logger.addHandler(fh)
    except Exception as e:
        flask_app.logger.warning(f"file logging disabled: {e}")


_setup_logging(app)

# =====================================================================
# Shared helpers
# =====================================================================


def _ip() -> str:
    cf = request.headers.get("CF-Connecting-IP")
    if cf:
        return cf
    xff = request.headers.get("X-Forwarded-For")
    if xff:
        return xff.split(",")[0].strip()
    return request.remote_addr or "-"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"(\w)-\s*\n\s*(\w)", r"\1\2", s)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    for k, v in LIGATURES.items():
        s = s.replace(k, v)
    for k, v in GREEK.items():
        s = s.replace(k, f"{k} {v}")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if (ch == "\n" or not unicodedata.category(ch).startswith("C")))
    return s.strip()


def safe_piece(txt: str) -> str:
    txt = (txt or "").strip()
    return re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", txt) or "Unknown"


def dept_from_text(txt: str) -> Optional[str]:
    if not txt:
        return None
    low = txt.lower()
    for k, v in DEPT_MAP.items():
        if k in low:
            return v
    return None


def guess_year(*texts: str) -> str:
    for t in texts:
        if not t:
            continue
        m = YEAR_RE.search(t)
        if m:
            return m.group(0)
    return "unknown"


def month_to_term(month: int) -> str:
    if month in (1, 2, 3, 4):
        return "Spring"
    if month in (5, 6, 7, 8):
        return "Summer"
    if month in (9, 10, 11):
        return "Fall"
    return "Winter"


# =====================================================================
# Database helpers
# =====================================================================


def get_db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=5000;")
    con.execute("PRAGMA case_sensitive_like = OFF;")
    return con


def open_pipeline_db(db_path: Optional[str] = None) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path or DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA mmap_size=268435456;")
    conn.execute("PRAGMA busy_timeout=60000;")
    return conn


def ensure_schema(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS documents(
            id INTEGER PRIMARY KEY,
            path TEXT UNIQUE,
            filename TEXT,
            department TEXT,
            exam_date TEXT,
            year INTEGER,
            term TEXT,
            cume_number INTEGER,
            title TEXT,
            author TEXT,
            pages INTEGER,
            bytes INTEGER,
            sha256 TEXT UNIQUE,
            has_solutions INTEGER,
            is_scanned INTEGER,
            ocr_confidence REAL,
            date_source TEXT,
            added_at TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_docs_dept_year ON documents(department, year DESC, exam_date DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_docs_author ON documents(author)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_docs_cume ON documents(department, cume_number)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pages(
            doc_id INTEGER NOT NULL,
            page   INTEGER NOT NULL,
            text   TEXT,
            is_scanned INTEGER,
            ocr_confidence REAL,
            text_len INTEGER,
            PRIMARY KEY (doc_id, page)
        )
    """)
    conn.execute(f"""
        CREATE VIRTUAL TABLE IF NOT EXISTS {FTS_TABLE}
        USING fts5(doc_id UNINDEXED, page UNINDEXED, text, content='',
                   tokenize='unicode61 remove_diacritics 2')
    """)
    conn.commit()


def upsert_page(conn: sqlite3.Connection, doc_id: int, page_idx: int, text: str,
                is_scanned: int, ocr_conf: Optional[float]):
    tnorm = normalize_text(text)
    tlen = len(tnorm)
    conn.execute(
        "INSERT INTO pages(doc_id, page, text, is_scanned, ocr_confidence, text_len) "
        "VALUES(?,?,?,?,?,?) "
        "ON CONFLICT(doc_id, page) DO UPDATE SET "
        "text=excluded.text, is_scanned=excluded.is_scanned, "
        "ocr_confidence=excluded.ocr_confidence, text_len=excluded.text_len",
        (doc_id, page_idx, tnorm, is_scanned, ocr_conf, tlen),
    )
    conn.execute(f"DELETE FROM {FTS_TABLE} WHERE doc_id=? AND page=?", (doc_id, page_idx))
    conn.execute(f"INSERT INTO {FTS_TABLE}(doc_id, page, text) VALUES(?,?,?)", (doc_id, page_idx, tnorm))


# =====================================================================
# Section: Download exams from university site
# =====================================================================

def _make_download_session():
    if _requests is None:
        raise RuntimeError("requests library not installed")
    s = _requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/pdf;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    retries = Retry(
        total=6, connect=6, read=6, backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=["GET", "HEAD"],
        respect_retry_after_header=True,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s


def _iter_links_with_context(soup, base_url):
    current_heading = ""
    for el in soup.find_all(True):
        name = el.name.lower()
        if name in {"h1", "h2", "h3", "h4", "h5"}:
            txt = el.get_text(" ", strip=True)
            if txt:
                current_heading = txt
        if name == "a" and el.has_attr("href"):
            href = el["href"].strip()
            if not href:
                continue
            abs_url = urljoin(base_url, href)
            text = el.get_text(" ", strip=True)
            yield abs_url, text, current_heading


class _Candidate:
    __slots__ = ("url", "page_url", "page_dept", "link_text", "heading_text")

    def __init__(self, url, page_url, page_dept, link_text, heading_text):
        self.url = url
        self.page_url = page_url
        self.page_dept = page_dept
        self.link_text = link_text
        self.heading_text = heading_text


def _gather_candidates_by_page(session, delay=0.25, ld_only=True):
    """Return candidates grouped by department page: list of (page_dept, [candidates]).
    Links are in page order (newest first on LibGuides)."""
    if BeautifulSoup is None:
        print("[download] BeautifulSoup not installed, skipping crawl", flush=True)
        return []
    result = []
    seen = set()
    for page_dept, page_url in SEED_PAGES:
        time.sleep(delay)
        try:
            r = session.get(page_url, timeout=25)
            r.raise_for_status()
        except Exception as e:
            print(f"[download] FAILED to load page: {page_dept} -> {e}", flush=True)
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        page_cands = []
        for url, link_text, heading in _iter_links_with_context(soup, r.url):
            p = urlparse(url)
            if p.scheme not in ("http", "https"):
                continue
            if ld_only and not LD_HINT.search(url):
                continue
            if url in seen:
                continue
            seen.add(url)
            page_cands.append(_Candidate(url, page_url, page_dept, link_text, heading))
        print(f"[download] {page_dept}: found {len(page_cands)} PDF links", flush=True)
        if page_cands:
            result.append((page_dept, page_cands))
    return result


def _get_header_filename(headers):
    dispo = headers.get("Content-Disposition", "")
    m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', dispo, flags=re.I)
    return m.group(1).strip() if m else None


def _validate_pdf(path: Path) -> bool:
    try:
        with path.open("rb") as f:
            head = f.read(8)
            if not head.startswith(PDF_MAGIC):
                return False
            try:
                f.seek(-2048, os.SEEK_END)
            except OSError:
                f.seek(0)
            tail = f.read()
            if not EOF_RE.search(tail):
                return False
        return True
    except Exception:
        return False


def _extract_pdf_candidates_from_html(html: str, base_url: str) -> list:
    if BeautifulSoup is None:
        return []
    soup = BeautifulSoup(html, "html.parser")
    cand = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        txt = a.get_text(" ", strip=True).lower()
        if not href:
            continue
        absu = urljoin(base_url, href)
        if ".pdf" in href.lower() or "download" in href.lower() or "download" in txt:
            cand.append(absu)
    for tag in soup.find_all(["iframe", "embed", "object"]):
        src = tag.get("src") or tag.get("data")
        if src and ".pdf" in src.lower():
            cand.append(urljoin(base_url, src))
    for el in soup.find_all(attrs={"data-file-url": True}):
        u = el.get("data-file-url")
        if u and ".pdf" in u.lower():
            cand.append(urljoin(base_url, u))
    out, seen = [], set()
    for u in cand:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out


def _resolve_to_real_pdf(session, url):
    r0 = session.get(url, stream=True, allow_redirects=True, timeout=35)
    r0.raise_for_status()
    it = r0.iter_content(16384)
    first = next(it, b"")
    ctype = r0.headers.get("Content-Type", "").lower()

    if first.startswith(PDF_MAGIC) or "application/pdf" in ctype:
        def chain():
            if first:
                yield first
            for chunk in it:
                yield chunk
        r0.iter_content = lambda chunk_size: chain()
        return r0, r0.url, None

    html = first.decode("latin-1", errors="ignore") + r0.content.decode("latin-1", errors="ignore")
    cand_urls = _extract_pdf_candidates_from_html(html, r0.url)
    r0.close()

    for cand in cand_urls:
        try:
            headers = {"Referer": url}
            r1 = session.get(cand, stream=True, allow_redirects=True, timeout=35, headers=headers)
            r1.raise_for_status()
            it1 = r1.iter_content(16384)
            first1 = next(it1, b"")
            ctype1 = r1.headers.get("Content-Type", "").lower()
            if first1.startswith(PDF_MAGIC) or "application/pdf" in ctype1:
                def chain1():
                    if first1:
                        yield first1
                    for chunk in it1:
                        yield chunk
                r1.iter_content = lambda chunk_size: chain1()
                return r1, r1.url, url
            r1.close()
        except Exception:
            continue
    return None, None, None


def _download_one(session, c: _Candidate, delay: float, out_root: Path):
    time.sleep(delay)
    try:
        resp, final_url, used_ref = _resolve_to_real_pdf(session, c.url)
        if resp is None:
            return {"department": "", "year": "", "filename": "", "bytes": 0,
                    "sha256": "", "source_url": c.url, "ref_page": c.page_url, "status": "skip_not_pdf"}

        header_fname = _get_header_filename(resp.headers)
        path_fname = os.path.basename(urlparse(final_url).path) if final_url else "file.pdf"
        fname = header_fname or path_fname
        if not fname.lower().endswith(".pdf"):
            fname += ".pdf"
        fname = safe_piece(fname)

        dept = (dept_from_text(fname) or dept_from_text(c.link_text) or dept_from_text(c.heading_text)
                or (c.page_dept if c.page_dept.lower() != "home" else None)
                or dept_from_text(final_url or "") or "Unknown")
        year = guess_year(fname, c.link_text, c.heading_text, final_url or c.url)

        dest_dir = out_root / safe_piece(dept) / safe_piece(year)
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_file = dest_dir / fname

        if dest_file.exists():
            return {"department": dept, "year": year, "filename": str(dest_file),
                    "bytes": dest_file.stat().st_size, "sha256": sha256_file(dest_file),
                    "source_url": c.url, "ref_page": c.page_url, "status": "skipped_exists"}

        written = 0
        with open(dest_file, "wb") as out:
            for chunk in resp.iter_content(1024 * 64):
                if not chunk:
                    continue
                out.write(chunk)
                written += len(chunk)
        resp.close()

        if not _validate_pdf(dest_file):
            try:
                dest_file.unlink()
            except Exception:
                pass
            return {"department": dept, "year": year, "filename": "", "bytes": 0, "sha256": "",
                    "source_url": c.url, "ref_page": c.page_url, "status": "error:invalid_pdf"}

        digest = sha256_file(dest_file)
        return {"department": dept, "year": year, "filename": str(dest_file), "bytes": written,
                "sha256": digest, "source_url": c.url, "ref_page": c.page_url, "status": "downloaded"}

    except Exception as e:
        return {"department": "", "year": "", "filename": "", "bytes": 0, "sha256": "",
                "source_url": c.url, "ref_page": c.page_url, "status": f"error:{e}"}


def run_download(delay=0.25, ld_only=True) -> dict:
    """Download new exams using early-stop logic.

    For each department page, iterate links newest-first. As soon as we
    hit a file that already exists on disk, stop checking that section
    and move to the next department. This avoids re-checking all 1500+ files.
    """
    if _requests is None:
        print("[download] ERROR: requests library not installed", flush=True)
        return {"error": "requests library not installed", "downloaded": 0}
    EXAMS_DIR.mkdir(parents=True, exist_ok=True)
    session = _make_download_session()
    print("[download] Crawling seed pages for PDF links...", flush=True)
    pages = _gather_candidates_by_page(session, delay=delay, ld_only=ld_only)

    downloaded = checked = errors = 0
    rows = []

    for page_dept, candidates in pages:
        sect_new = 0
        for c in candidates:
            checked += 1
            row = _download_one(session, c, delay, EXAMS_DIR)
            rows.append(row)
            st = row["status"]

            if st.startswith("downloaded"):
                downloaded += 1
                sect_new += 1
                print(f"[download] NEW: {row['filename']}", flush=True)
            elif st == "skipped_exists":
                # Hit an existing file — everything after this is old, skip section
                print(f"[download] {page_dept}: found existing file after {sect_new} new, moving on", flush=True)
                break
            elif st.startswith("error"):
                errors += 1
                print(f"[download] ERROR: {c.link_text} -> {st}", flush=True)
                # Errors (e.g. not a PDF) don't mean we've hit old files, keep going
        else:
            # Exhausted all candidates without finding an existing file (all new, or all errors)
            print(f"[download] {page_dept}: checked all {len(candidates)} links, {sect_new} new", flush=True)

    # Append to catalog CSV
    if rows:
        write_header = not CATALOG_CSV.exists()
        with open(CATALOG_CSV, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=[
                "department", "year", "filename", "bytes", "sha256", "source_url", "ref_page", "status"
            ])
            if write_header:
                w.writeheader()
            for r in rows:
                w.writerow(r)

    print(f"[download] Done: {downloaded} new, {checked} checked, {errors} errors", flush=True)
    return {"downloaded": downloaded, "checked": checked, "errors": errors}


# =====================================================================
# Section: Index PDFs into documents table
# =====================================================================

def _safe_get_pdf_info(p: Path):
    pages = None
    title = None
    author = None
    info = {}
    if pikepdf is None:
        return pages, info, title, author
    try:
        with pikepdf.open(p) as pdf:
            pages = len(pdf.pages)
            info = {k[1:]: str(v) for k, v in pdf.docinfo.items()} if pdf.docinfo else {}
            title = info.get("Title") or None
            author = info.get("Author") or None
            try:
                md = pdf.open_metadata()
                xmp_creator = md.get("http://purl.org/dc/elements/1.1/creator")
                if xmp_creator:
                    if isinstance(xmp_creator, list):
                        author = author or ", ".join([str(x) for x in xmp_creator if x])
                    else:
                        author = author or str(xmp_creator)
                xmp_title = md.get("http://purl.org/dc/elements/1.1/title")
                if xmp_title:
                    if isinstance(xmp_title, dict) and "x-default" in xmp_title:
                        title = title or xmp_title["x-default"]
                    elif isinstance(xmp_title, str):
                        title = title or xmp_title
            except Exception:
                pass
    except Exception:
        pass
    return pages, info, title, author


def _extract_text_first_page(pdf_path: Path, max_chars: int = 5000) -> str:
    if pdfminer_extract_text is None:
        return ""
    try:
        txt = pdfminer_extract_text(str(pdf_path))
        return (txt[:max_chars] if txt else "") or ""
    except Exception:
        return ""


def _parse_from_filename(fname: str) -> dict:
    out = {"exam_date": None, "year": None, "cume_number": None,
           "department": None, "has_solutions": None}
    out["department"] = dept_from_text(fname)
    m = RE_DATE_YYYYMMDD.search(fname)
    if m:
        y, mo, d = m.group("y", "m", "d")
        out["exam_date"] = f"{y}-{mo}-{d}"
        out["year"] = int(y)
    m2 = RE_CUME.search(fname)
    if m2:
        try:
            out["cume_number"] = int(m2.group("n"))
        except Exception:
            pass
    out["has_solutions"] = 1 if RE_SOLUTIONS.search(fname) else 0
    return out


def _parse_dates_from_text(text: str) -> Optional[str]:
    if dateparser is None:
        return None
    m = RE_DATE_MDY.search(text or "")
    if m:
        try:
            dt = dateparser.parse(m.group(0), fuzzy=True)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    if text:
        try:
            dt = dateparser.parse(text[:600], fuzzy=True)
            if dt.year >= 1900:
                return dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    return None


def run_index(root: Optional[Path] = None) -> dict:
    """Index new PDFs under root into documents table. Skips already-indexed files."""
    root = root or EXAMS_DIR
    files = sorted(root.rglob("*.pdf"))
    if not files:
        print(f"[index] No PDFs found under {root}", flush=True)
        return {"processed": 0, "skipped": 0, "errors": 0, "message": f"No PDFs under {root}"}

    conn = open_pipeline_db()
    ensure_schema(conn)

    # Get paths already in the database so we can skip them
    known_paths = {r[0] for r in conn.execute("SELECT path FROM documents WHERE path IS NOT NULL").fetchall()}
    new_files = [f for f in files if str(f.resolve()) not in known_paths]

    print(f"[index] {len(files)} total PDFs, {len(known_paths)} already indexed, {len(new_files)} new to index", flush=True)
    if not new_files:
        print("[index] Nothing new to index.", flush=True)
        conn.close()
        return {"processed": 0, "skipped": len(known_paths), "errors": 0}

    processed = errors = 0

    for pdf in new_files:
        try:
            byte_size = pdf.stat().st_size
            sha = sha256_file(pdf)
            pages, info_dict, meta_title, meta_author = _safe_get_pdf_info(pdf)
            first_text = _extract_text_first_page(pdf)
            text_len = len(first_text.strip()) if first_text else 0
            is_scanned = 1 if text_len < 30 else 0

            finfo = _parse_from_filename(pdf.name)
            department = finfo["department"] or dept_from_text(str(pdf.parent)) or None
            exam_date = finfo["exam_date"]
            date_source = "filename" if exam_date else None
            if not exam_date:
                exam_date = _parse_dates_from_text(first_text or "")
                if exam_date:
                    date_source = "text"
            if not exam_date and info_dict and dateparser:
                for k in ("CreationDate", "ModDate"):
                    raw = info_dict.get(k)
                    if raw:
                        try:
                            dt = dateparser.parse(raw, fuzzy=True)
                            if dt.year >= 1900:
                                exam_date = dt.strftime("%Y-%m-%d")
                                date_source = "metadata"
                                break
                        except Exception:
                            pass

            year = None
            term = None
            if exam_date:
                year = int(exam_date[:4])
                try:
                    term = month_to_term(int(exam_date[5:7]))
                except Exception:
                    pass
            else:
                m = re.search(r"(19|20)\d{2}", str(pdf.parent))
                if m:
                    year = int(m.group(0))
                    date_source = "fallback"

            cume_number = finfo["cume_number"]
            if not cume_number and first_text:
                m = RE_CUME.search(first_text)
                if m:
                    try:
                        cume_number = int(m.group("n"))
                    except Exception:
                        pass

            title = meta_title or pdf.stem.replace("_", " ").strip()
            # Don't store author during indexing — the backfill_authors step
            # validates per-date uniqueness to filter out uploader names.
            author = None

            has_solutions = finfo["has_solutions"]
            if not has_solutions and first_text:
                has_solutions = 1 if RE_SOLUTIONS.search(first_text) else 0

            row = {
                "path": str(pdf.resolve()),
                "filename": pdf.name,
                "department": department,
                "exam_date": exam_date,
                "year": year,
                "term": term,
                "cume_number": cume_number,
                "title": title,
                "author": author,
                "pages": pages,
                "bytes": byte_size,
                "sha256": sha,
                "has_solutions": has_solutions,
                "is_scanned": is_scanned,
                "ocr_confidence": None,
                "date_source": date_source or None,
                "added_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            conn.execute("""
                INSERT INTO documents(path, filename, department, exam_date, year, term, cume_number,
                    title, author, pages, bytes, sha256, has_solutions, is_scanned, ocr_confidence,
                    date_source, added_at)
                VALUES (:path, :filename, :department, :exam_date, :year, :term, :cume_number,
                    :title, :author, :pages, :bytes, :sha256, :has_solutions, :is_scanned,
                    :ocr_confidence, :date_source, :added_at)
                ON CONFLICT(sha256) DO UPDATE SET
                    path=excluded.path, filename=excluded.filename, department=excluded.department,
                    exam_date=excluded.exam_date, year=excluded.year, term=excluded.term,
                    cume_number=excluded.cume_number, title=excluded.title, author=excluded.author,
                    pages=excluded.pages, bytes=excluded.bytes, has_solutions=excluded.has_solutions,
                    is_scanned=excluded.is_scanned, ocr_confidence=excluded.ocr_confidence,
                    date_source=excluded.date_source
            """, row)
            conn.commit()
            processed += 1
            if processed % 100 == 0:
                print(f"[index] Progress: {processed}/{len(new_files)} indexed", flush=True)
        except Exception as e:
            errors += 1
            app.logger.error(f"index error {pdf}: {e}")

    conn.close()
    print(f"[index] Done: {processed} new indexed, {errors} errors", flush=True)
    return {"processed": processed, "skipped": len(known_paths), "errors": errors}


# =====================================================================
# Section: OCR pipeline (all pages, multi-pass)
# =====================================================================

def _resolve_tesseract(cmd_arg: Optional[str] = None) -> Optional[str]:
    if cmd_arg:
        p = Path(cmd_arg)
        if p.exists():
            return str(p)
    found = shutil.which("tesseract")
    if found:
        return found
    win_default = Path(r"C:\Program Files\Tesseract-OCR\tesseract.exe")
    if win_default.exists():
        return str(win_default)
    return None


def _render_page_to_pil(pdf, page_index: int, dpi: int):
    page = pdf.get_page(page_index)
    try:
        pil = page.render(scale=dpi / 72).to_pil().convert("L")
        pil.info["dpi"] = (dpi, dpi)
        return pil
    finally:
        page.close()


def _downscale_if_needed(img, max_width: int):
    if max_width <= 0 or Image is None:
        return img
    w, h = img.size
    if w <= max_width:
        return img
    new_h = int(h * (max_width / float(w)))
    return img.resize((max_width, new_h), resample=Image.LANCZOS)


def _preprocess_pil_hq(img, do_unsharp: bool):
    if cv2 is None or np is None:
        return img
    arr = np.array(img)
    if arr.ndim == 3:
        arr = cv2.cvtColor(arr, cv2.COLOR_RGB2GRAY)
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    arr = clahe.apply(arr)
    arr = cv2.bilateralFilter(arr, 7, 50, 50)
    binar = cv2.adaptiveThreshold(arr, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 31, 5)
    coords = cv2.findNonZero(255 - binar)
    if coords is not None:
        angle = cv2.minAreaRect(coords)[-1]
        if angle < -45:
            angle = 90 + angle
        M = cv2.getRotationMatrix2D((binar.shape[1] // 2, binar.shape[0] // 2), angle, 1.0)
        binar = cv2.warpAffine(binar, M, (binar.shape[1], binar.shape[0]),
                               flags=cv2.INTER_CUBIC, borderMode=cv2.BORDER_REPLICATE)
    if do_unsharp:
        blur = cv2.GaussianBlur(binar, (0, 0), sigmaX=1.0, sigmaY=1.0)
        binar = cv2.addWeighted(binar, 1.5, blur, -0.5, 0)
    return Image.fromarray(binar)


def _extract_text_pdfium(pdf, page_index: int, max_chars: int = 50000) -> str:
    page = pdf.get_page(page_index)
    try:
        textpage = page.get_textpage()
        raw = textpage.get_text_range()
        textpage.close()
        return (raw[:max_chars] if raw else "") or ""
    finally:
        page.close()


def _tesseract_text(img, lang: str, oem: int, psm: int, tess_threads: int):
    if pytesseract is None:
        return "", None
    if tess_threads > 0:
        os.environ["OMP_THREAD_LIMIT"] = str(tess_threads)
        os.environ["OMP_NUM_THREADS"] = str(tess_threads)
    config = f"--oem {oem} --psm {psm}"
    try:
        data = pytesseract.image_to_data(img, lang=lang, config=config, output_type=pytesseract.Output.DICT)
    except Exception:
        return "", None
    words = [w for w in (data.get("text", []) or []) if w and w.strip()]
    confs = []
    for c in (data.get("conf", []) or []):
        s = str(c).strip()
        if s and s != "-1":
            try:
                confs.append(float(s))
            except Exception:
                pass
    txt = " ".join(words)
    avg = (sum(confs) / len(confs)) if confs else None
    return txt, avg


def _ocr_worker(args_tuple):
    """Process one document: extract native text + OCR for each page."""
    (doc_id, path_str, pages_to_skip, dpi, max_width, do_preprocess, do_unsharp,
     tesseract_cmd, lang, oem, psm_primary, psm_secondary, tess_threads,
     min_native_len, min_accept_len) = args_tuple

    if tesseract_cmd:
        try:
            import pytesseract as _pt
            _pt.pytesseract.tesseract_cmd = tesseract_cmd
        except Exception:
            pass

    rows = []
    pages_count = 0
    try:
        import pypdfium2 as _pdfium
        pdf = _pdfium.PdfDocument(path_str)
    except Exception as e:
        return (doc_id, path_str, rows, pages_count, f"open_pdfium: {e}")

    n = len(pdf)
    for i in range(n):
        if i in pages_to_skip:
            continue
        pages_count += 1

        try:
            native = _extract_text_pdfium(pdf, i)
        except Exception:
            native = ""
        native_norm = normalize_text(native)
        native_len = len(native_norm)

        need_ocr = native_len < min_native_len
        best_text, best_conf, best_is_scanned = native_norm, None, 0

        if need_ocr:
            try:
                pil = _render_page_to_pil(pdf, i, dpi=dpi)
                if max_width > 0:
                    pil = _downscale_if_needed(pil, max_width)
                if do_preprocess:
                    pil = _preprocess_pil_hq(pil, do_unsharp)
            except Exception:
                pil = None

            ocr_txt, ocr_conf = "", None
            if pil is not None:
                t1, c1 = _tesseract_text(pil, lang=lang, oem=oem, psm=psm_primary, tess_threads=tess_threads)
                ocr_txt, ocr_conf = t1, c1
                if len(normalize_text(ocr_txt)) < min_accept_len:
                    try:
                        if do_preprocess:
                            pil2 = _render_page_to_pil(pdf, i, dpi=dpi)
                            if max_width > 0:
                                pil2 = _downscale_if_needed(pil2, max_width)
                        else:
                            pil2 = _preprocess_pil_hq(pil, do_unsharp)
                    except Exception:
                        pil2 = None
                    if pil2 is not None:
                        t2, c2 = _tesseract_text(pil2, lang=lang, oem=oem, psm=psm_secondary, tess_threads=tess_threads)
                        if len(normalize_text(t2)) > len(normalize_text(ocr_txt)):
                            ocr_txt, ocr_conf = t2, c2

            olen = len(normalize_text(ocr_txt))
            if olen > native_len:
                best_text, best_is_scanned, best_conf = ocr_txt, 1, ocr_conf
            else:
                best_text, best_is_scanned, best_conf = native_norm, 0, None

        rows.append((i, best_text, best_is_scanned, best_conf))

    try:
        pdf.close()
    except Exception:
        pass
    return (doc_id, path_str, rows, pages_count, None)


def run_ocr(dpi=320, max_width=2200, cpu_workers=None, tesseract_cmd="",
            lang="eng+equ", min_native_len=80, min_accept_len=60) -> dict:
    """OCR all pages for all documents. Returns summary."""
    if pdfium is None:
        print("[ocr] ERROR: pypdfium2 not installed", flush=True)
        return {"error": "pypdfium2 not installed", "pages_indexed": 0}

    cpu_workers = cpu_workers or max(1, (os.cpu_count() or 4) - 2)
    tesseract_cmd = tesseract_cmd or _resolve_tesseract() or ""

    conn = open_pipeline_db()
    ensure_schema(conn)

    # Pick documents that have NO page rows with sufficient text yet.
    # A document is "done" when it has at least one page row with text_len > min_accept_len.
    # (We can't rely on d.pages because pikepdf may not be installed to populate it.)
    docs = conn.execute("""
        SELECT d.id, d.path, d.pages
        FROM documents d
        WHERE d.path IS NOT NULL
          AND d.id NOT IN (
              SELECT DISTINCT p.doc_id FROM pages p
              WHERE IFNULL(p.text_len, 0) > ?
          )
    """, (min_accept_len,)).fetchall()
    if not docs:
        total = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        print(f"[ocr] All {total} documents already have OCR'd text. Nothing to do.", flush=True)
        conn.close()
        return {"pages_indexed": 0, "errors": 0, "skipped": total}

    work = []
    for doc_id, path, _total_pages, _ocrd_pages in docs:
        if not path or not Path(path).exists():
            continue
        # Pages already indexed with enough text — pass to worker so it skips them
        good = {r[0] for r in conn.execute(
            "SELECT page FROM pages WHERE doc_id=? AND IFNULL(text_len,0) > ?",
            (doc_id, min_accept_len)
        ).fetchall()}
        work.append((
            doc_id, path, good, dpi, max_width,
            True, False, tesseract_cmd, lang, 1,
            6, 3, 1, min_native_len, min_accept_len
        ))

    all_docs = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
    print(f"[ocr] {len(work)} documents need OCR ({all_docs - len(work)} already done, {cpu_workers} workers)", flush=True)

    processed_pages = 0
    doc_errors = 0
    docs_done = 0

    with ProcessPoolExecutor(max_workers=cpu_workers) as ex:
        futures = [ex.submit(_ocr_worker, w) for w in work]
        for fut in as_completed(futures):
            docs_done += 1
            try:
                doc_id, path_str, rows, counted, err = fut.result()
            except Exception as e:
                doc_errors += 1
                app.logger.error(f"OCR future failed: {e}")
                continue
            if err:
                doc_errors += 1
                app.logger.error(f"OCR error {path_str}: {err}")
                continue
            if not rows:
                continue
            try:
                conn.execute("BEGIN IMMEDIATE")
                for (page_idx, text, is_scanned, conf) in rows:
                    upsert_page(conn, doc_id, page_idx, text, is_scanned, conf)
                conn.execute("COMMIT")
                processed_pages += len(rows)
                fname = os.path.basename(path_str)
                print(f"[ocr] ({docs_done}/{len(work)}) {fname}: {len(rows)} pages indexed ({counted} total)", flush=True)
            except Exception as e:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                doc_errors += 1
                app.logger.error(f"OCR write error doc_id={doc_id}: {e}")

    # Backfill d.pages from actual page rows for any docs still NULL
    try:
        conn.execute("""
            UPDATE documents SET pages = (
                SELECT COUNT(*) FROM pages p WHERE p.doc_id = documents.id
            ) WHERE pages IS NULL AND id IN (SELECT DISTINCT doc_id FROM pages)
        """)
        conn.commit()
    except Exception as e:
        app.logger.error(f"[ocr] pages backfill error: {e}")

    try:
        conn.execute("PRAGMA optimize;")
    except Exception:
        pass
    conn.close()
    print(f"[ocr] Done: {processed_pages} pages indexed from {docs_done} documents, {doc_errors} errors", flush=True)
    return {"pages_indexed": processed_pages, "errors": doc_errors}


# =====================================================================
# Section: Backfill authors from PDF metadata
# =====================================================================

def _extract_author_pypdf(pdf_path: str) -> str:
    try:
        from pypdf import PdfReader
        reader = PdfReader(pdf_path)
        md = getattr(reader, "metadata", None)
        author = None
        if md:
            author = getattr(md, "author", None) or (md.get("/Author") if hasattr(md, "get") else None)
        if author:
            author = str(author).strip().strip("\x00")
        return author if author else "Unknown"
    except Exception:
        return "Unknown"


def run_backfill_authors() -> dict:
    """Backfill author column from PDF metadata with same-date validation.

    PDF metadata "author" is often the person who compiled/uploaded the
    exams, not the real exam author.  To detect this we check: if every
    cume on the same exam_date has the *same* author string, that author
    is almost certainly the uploader — discard it.  Only keep the author
    when different sections on the same date have different authors
    (meaning the metadata really identifies who wrote each exam).
    """
    conn = open_pipeline_db()

    # Phase 1: extract raw author from every PDF that has a path
    rows = conn.execute(
        "SELECT id, path, exam_date FROM documents WHERE path IS NOT NULL"
    ).fetchall()
    print(f"[authors] Phase 1: extracting metadata from {len(rows)} documents...", flush=True)

    raw_authors: dict[int, str] = {}   # doc_id → raw author string
    by_date: dict[str, list[tuple[int, str]]] = defaultdict(list)  # date → [(id, raw_author)]
    missing = 0

    for doc_id, path, exam_date in rows:
        if not path or not os.path.isfile(path):
            missing += 1
            continue
        raw = _extract_author_pypdf(path)
        raw_authors[doc_id] = raw
        if exam_date:
            by_date[exam_date].append((doc_id, raw))

    # Phase 2: validate — only keep author when same-date cumes differ
    valid_ids: set[int] = set()
    for date, entries in by_date.items():
        real_authors = {a for _, a in entries if a and a != "Unknown"}
        if len(real_authors) > 1:
            # Different authors on same date → these are real
            valid_ids.update(doc_id for doc_id, a in entries if a and a != "Unknown")

    # Also keep authors for docs with no exam_date if they look real
    for doc_id, raw in raw_authors.items():
        if doc_id not in valid_ids and raw and raw != "Unknown":
            # Check if this doc has an exam_date — if not, we can't validate
            # so keep it tentatively (single docs can't be cross-checked)
            pass  # err on the side of not storing unverifiable authors

    # Phase 3: write to DB
    updated = 0
    for doc_id, raw in raw_authors.items():
        if doc_id in valid_ids:
            author = raw
        else:
            author = "Unknown"
        conn.execute("UPDATE documents SET author=? WHERE id=?", (author, doc_id))
        updated += 1

    conn.commit()
    conn.close()
    real_count = len(valid_ids)
    print(f"[authors] Done: {updated} processed, {real_count} with verified real authors, {missing} files missing", flush=True)
    return {"processed": updated, "real_authors": real_count, "missing": missing}


# =====================================================================
# Section: FTS rebuild
# =====================================================================

def run_fts_rebuild() -> dict:
    """Drop and rebuild FTS index from pages table."""
    print("[fts] Rebuilding full-text search index...", flush=True)
    conn = open_pipeline_db()
    try:
        conn.execute("BEGIN IMMEDIATE;")
        conn.execute(f"DROP TABLE IF EXISTS {FTS_TABLE};")
        conn.execute(f"""
            CREATE VIRTUAL TABLE {FTS_TABLE}
            USING fts5(
                doc_id UNINDEXED, page UNINDEXED, text, content='',
                tokenize='unicode61 remove_diacritics 2',
                prefix='2 3 4'
            );
        """)
        conn.execute(f"""
            INSERT INTO {FTS_TABLE}(doc_id, page, text)
            SELECT doc_id, page, IFNULL(text,'')
            FROM pages WHERE IFNULL(text,'') <> '';
        """)
        conn.execute("COMMIT;")
    except Exception as e:
        conn.execute("ROLLBACK;")
        conn.close()
        return {"error": str(e)}
    pf = conn.execute(f"SELECT COUNT(*) FROM {FTS_TABLE}").fetchone()[0]
    pg = conn.execute("SELECT COUNT(*) FROM pages WHERE IFNULL(text_len,0) > 0").fetchone()[0]
    try:
        conn.execute("PRAGMA optimize;")
    except Exception:
        pass
    conn.close()
    print(f"[fts] Done: {pf} FTS rows from {pg} pages with text", flush=True)
    return {"fts_rows": pf, "pages_with_text": pg}


# =====================================================================
# Section: Full pipeline + scheduler
# =====================================================================

_pipeline_lock = threading.Lock()
_pipeline_status = {
    "running": False,
    "step": "",
    "last_run": None,
    "last_result": None,
    "error": None,
    "next_run": None,
}


def run_full_pipeline() -> dict:
    """Run the complete pipeline: download → index → OCR → authors → FTS."""
    if not _pipeline_lock.acquire(blocking=False):
        return {"error": "Pipeline already running"}
    try:
        _pipeline_status["running"] = True
        _pipeline_status["error"] = None
        start = time.time()
        results = {}

        for step_name, step_fn in [
            ("download", run_download),
            ("index", run_index),
            ("ocr", run_ocr),
            ("authors", run_backfill_authors),
            ("fts", run_fts_rebuild),
        ]:
            _pipeline_status["step"] = step_name
            app.logger.info(f"Pipeline: {step_name}...")
            results[step_name] = step_fn()

        elapsed = round(time.time() - start, 1)
        results["elapsed_seconds"] = elapsed
        app.logger.info(f"Pipeline: complete in {elapsed}s")

        _pipeline_status["step"] = "done"
        _pipeline_status["last_run"] = datetime.now(timezone.utc).isoformat() + "Z"
        _pipeline_status["last_result"] = results
        return results
    except Exception as e:
        _pipeline_status["error"] = str(e)
        app.logger.error(f"Pipeline error: {e}")
        return {"error": str(e)}
    finally:
        _pipeline_status["running"] = False
        _pipeline_lock.release()


def _scheduler_loop():
    """Background thread that runs the pipeline on a schedule."""
    while True:
        next_time = datetime.now(timezone.utc) + timedelta(seconds=PIPELINE_INTERVAL)
        _pipeline_status["next_run"] = next_time.isoformat() + "Z"
        time.sleep(PIPELINE_INTERVAL)
        try:
            app.logger.info("Scheduled pipeline run starting...")
            run_full_pipeline()
        except Exception as e:
            app.logger.error(f"Scheduled pipeline error: {e}\n{traceback.format_exc()}")


def start_scheduler():
    t = threading.Thread(target=_scheduler_loop, daemon=True, name="pipeline-scheduler")
    t.start()
    app.logger.info(f"Scheduler started: pipeline runs every {PIPELINE_INTERVAL}s ({PIPELINE_INTERVAL/3600:.1f}h)")


# =====================================================================
# Flask error handler & rate limiting
# =====================================================================

@app.errorhandler(Exception)
def _unhandled(e):
    if isinstance(e, HTTPException):
        return e
    app.logger.error(f'ip={_ip()} error path="{request.path}"')
    return jsonify({"error": "internal"}), 500


_BUCKET: Dict[str, Dict[str, float]] = {}


def _allow(ip: str) -> bool:
    now = time.time()
    b = _BUCKET.get(ip, {"t": now, "tokens": MAX_TOKENS})
    elapsed = max(0.0, now - b["t"])
    b["tokens"] = min(MAX_TOKENS, b["tokens"] + elapsed * REFILL_PER_SEC)
    b["t"] = now
    if b["tokens"] >= 1.0:
        b["tokens"] -= 1.0
        _BUCKET[ip] = b
        return True
    _BUCKET[ip] = b
    return False


@app.before_request
def _rate_guard():
    if request.path.startswith("/api/"):
        if not _allow(request.remote_addr or "unknown"):
            return jsonify({"error": "rate_limited"}), 429


# =====================================================================
# Flask API: Health / Stats / Facets
# =====================================================================

@app.get("/api/health")
def api_health():
    return jsonify({"ok": True})


@app.get("/api/stats")
def api_stats():
    con = get_db()
    doc_count = con.execute("SELECT COUNT(*) FROM documents;").fetchone()[0]
    page_count = con.execute("SELECT COUNT(*) FROM pages;").fetchone()[0]
    try:
        fts_rows = con.execute(f"SELECT COUNT(*) FROM {FTS_TABLE};").fetchone()[0]
    except sqlite3.Error:
        fts_rows = 0
    return jsonify({"documents": doc_count, "pages": page_count, "page_fts_rows": fts_rows})


@app.get("/api/authors")
def api_authors():
    con = get_db()
    rows = con.execute("""
        SELECT author, COUNT(*) AS count FROM documents
        WHERE author IS NOT NULL AND author <> '' AND author <> 'Unknown'
        GROUP BY author ORDER BY author;
    """).fetchall()
    return jsonify([{"author": r["author"], "count": r["count"]} for r in rows])


@app.get("/api/departments")
def api_departments():
    con = get_db()
    rows = con.execute("""
        SELECT department, COUNT(*) AS count FROM documents
        GROUP BY department ORDER BY department;
    """).fetchall()
    return jsonify([{"department": r["department"], "count": r["count"]} for r in rows])


@app.get("/api/years")
def api_years():
    con = get_db()
    dept = request.args.get("dept")
    if dept:
        rows = con.execute("""
            SELECT year, COUNT(*) AS count FROM documents
            WHERE department = ? GROUP BY year ORDER BY year DESC;
        """, (dept,)).fetchall()
    else:
        rows = con.execute("""
            SELECT year, COUNT(*) AS count FROM documents
            GROUP BY year ORDER BY year DESC;
        """).fetchall()
    out = [{"year": int(max(YEAR_MIN, min(YEAR_MAX, r["year"]))), "count": r["count"]} for r in rows]
    return jsonify(out)


# =====================================================================
# Flask API: Browse / Search
# =====================================================================

def _clamp_year(v: Optional[int]) -> Optional[int]:
    if v is None:
        return None
    v = int(v)
    return max(YEAR_MIN, min(YEAR_MAX, v))


def _build_doc_filters(args: Dict[str, Any]) -> Tuple[str, list]:
    where, params = [], []
    if args.get("dept"):
        where.append("d.department = ?")
        params.append(args["dept"])
    y_min = _clamp_year(args.get("year_min"))
    y_max = _clamp_year(args.get("year_max"))
    if y_min is not None:
        where.append("d.year >= ?")
        params.append(y_min)
    if y_max is not None:
        where.append("d.year <= ?")
        params.append(y_max)
    if args.get("author"):
        where.append("d.author = ?")
        params.append(args["author"])
    return (" AND " + " AND ".join(where)) if where else "", params


def _tokenize(q: str) -> List[str]:
    if not q:
        return []
    return [t for t in shlex.split(q) if t.strip()]


def _fts_match_from_q(q: str, any_mode: bool, raw: bool) -> str:
    if not q:
        return ""
    if raw:
        return q
    parts = _tokenize(q)
    if not parts:
        return ""

    def term(tok: str) -> str:
        if tok.startswith('"') and tok.endswith('"') and len(tok) >= 2:
            return f'text:{tok}'
        if tok.endswith('*'):
            return f'text:{tok}'
        return f'text:{tok}*'

    return (" OR " if any_mode else " AND ").join(term(t) for t in parts)


@app.get("/api/browse")
def api_browse():
    con = get_db()
    dept = request.args.get("dept")
    author = request.args.get("author")
    year_min = request.args.get("year_min", type=int)
    year_max = request.args.get("year_max", type=int)
    limit = request.args.get("limit", default=20, type=int)
    offset = request.args.get("offset", default=0, type=int)
    where, params = _build_doc_filters({"dept": dept, "year_min": year_min, "year_max": year_max, "author": author})
    sql = f"""
        SELECT d.id, d.department, d.year, d.filename, d.path, d.author,
               (SELECT COUNT(*) FROM pages p3 WHERE p3.doc_id = d.id) AS total_pages
        FROM documents d WHERE 1=1 {where}
        ORDER BY d.year DESC, d.department, d.filename LIMIT ? OFFSET ?;
    """
    rows = con.execute(sql, params + [limit, offset]).fetchall()
    return jsonify([{**dict(r), "page": 1, "pages": int(r["total_pages"] or 0)} for r in rows])


def _search_docs_all(con, filters, limit, offset):
    where, params = _build_doc_filters(filters)
    sql = f"""
        SELECT d.id AS doc_id, d.department, d.year, d.filename, d.path, d.author,
               0 AS first_page0,
               (SELECT COUNT(*) FROM pages p3 WHERE p3.doc_id = d.id) AS total_pages,
               '' AS snippet
        FROM documents d WHERE 1=1 {where}
        ORDER BY d.year DESC, d.department, d.filename LIMIT ? OFFSET ?;
    """
    return con.execute(sql, params + [limit, offset]).fetchall()


def _search_docs_fts(con, match, filters, limit, offset):
    where_docs, params = _build_doc_filters(filters)
    params_core = [match] + params + [limit, offset]
    sql = f"""
        WITH matched_pages AS (
            SELECT p.doc_id, p.page, {FTS_TABLE}.rowid AS rnk
            FROM {FTS_TABLE}
            JOIN pages p ON p.rowid = {FTS_TABLE}.rowid
            JOIN documents d ON d.id = p.doc_id
            WHERE {FTS_TABLE} MATCH ? {where_docs}
        ),
        doc_hits AS (
            SELECT doc_id, MIN(rnk) AS best_rowid, MIN(page) AS first_page0
            FROM matched_pages GROUP BY doc_id
        )
        SELECT d.id AS doc_id, d.department, d.year, d.filename, d.path, d.author,
               dh.first_page0,
               (SELECT COUNT(*) FROM pages p3 WHERE p3.doc_id = d.id) AS total_pages,
               (SELECT substr(p2.text, 1, 240) FROM pages p2
                WHERE p2.doc_id = d.id AND p2.page = dh.first_page0) AS snippet
        FROM doc_hits dh JOIN documents d ON d.id = dh.doc_id
        ORDER BY d.year DESC, d.department, d.filename LIMIT ? OFFSET ?;
    """
    return con.execute(sql, params_core).fetchall()


def _like_predicate_from_q(q: str, any_mode: bool) -> Tuple[str, list]:
    tokens = _tokenize(q)
    if not tokens:
        return "", []
    parts, params = [], []
    for tok in tokens:
        if tok.startswith('"') and tok.endswith('"') and len(tok) >= 2:
            s = tok[1:-1]
            parts.append("p.text LIKE ?")
            params.append(f"%{s}%")
        else:
            if tok.endswith("*"):
                tok = tok[:-1]
                parts.append("p.text LIKE ?")
                params.append(f"{tok}%")
            else:
                parts.append("p.text LIKE ?")
                params.append(f"%{tok}%")
    joiner = " OR " if any_mode else " AND "
    return joiner.join(parts), params


def _search_docs_like(con, q, filters, limit, offset):
    where_docs, doc_params = _build_doc_filters(filters)
    like_expr, like_params = _like_predicate_from_q(q, any_mode=bool(filters.get("any")))
    if not like_expr:
        return []
    sql = f"""
        WITH doc_hits AS (
            SELECT d.id AS doc_id, MIN(p.page) AS first_page0
            FROM documents d JOIN pages p ON p.doc_id = d.id
            WHERE 1=1 {where_docs} AND ({like_expr})
            GROUP BY d.id
        )
        SELECT d.id AS doc_id, d.department, d.year, d.filename, d.path, d.author,
               dh.first_page0,
               (SELECT COUNT(*) FROM pages p3 WHERE p3.doc_id = d.id) AS total_pages,
               (SELECT substr(p2.text, 1, 240) FROM pages p2
                WHERE p2.doc_id = d.id AND p2.page = dh.first_page0) AS snippet
        FROM doc_hits dh JOIN documents d ON d.id = dh.doc_id
        ORDER BY d.year DESC, d.department, d.filename LIMIT ? OFFSET ?;
    """
    params = doc_params + like_params + [limit, offset]
    return con.execute(sql, params).fetchall()


@app.get("/api/search")
def api_search():
    con = get_db()
    q = request.args.get("q", "", type=str)
    any_mode = request.args.get("any", default=False, type=lambda v: str(v).lower() in ("1", "true", "yes", "on"))
    raw = request.args.get("raw", default=False, type=lambda v: str(v).lower() in ("1", "true", "yes", "on"))
    dept = request.args.get("dept")
    author = request.args.get("author")
    year_min = request.args.get("year_min", type=int)
    year_max = request.args.get("year_max", type=int)
    limit = request.args.get("limit", default=20, type=int)
    offset = request.args.get("offset", default=0, type=int)
    filters = {"dept": dept, "year_min": year_min, "year_max": year_max, "author": author, "any": any_mode}

    if not q.strip():
        rows = _search_docs_all(con, filters, limit, offset)
    else:
        rows = []
        match = _fts_match_from_q(q, any_mode=any_mode, raw=raw)
        if match:
            try:
                rows = _search_docs_fts(con, match, filters, limit, offset)
            except sqlite3.Error:
                rows = []
        if not rows:
            rows = _search_docs_like(con, q, filters, limit, offset)

    payload = [{
        "doc_id": r["doc_id"],
        "department": r["department"],
        "year": r["year"],
        "filename": r["filename"],
        "path": r["path"],
        "author": r["author"],
        "page": int((r["first_page0"] if "first_page0" in r.keys() else 0)) + 1,
        "pages": int(r["total_pages"] or 0),
        "snippet": r["snippet"] or ""
    } for r in rows]

    app.logger.info(
        f'ip={_ip()} search q="{(q or "")[:200]}" dept="{dept or ""}" '
        f'author="{author or ""}" years=[{year_min},{year_max}] hits={len(payload)}'
    )
    return jsonify(payload)


# =====================================================================
# Flask API: View / Download
# =====================================================================

def _resolve_doc_path_from_row(row) -> Optional[str]:
    raw = (row["path"] or "").strip()
    fname = (row["filename"] or "").strip()
    dept = (row["department"] or "").strip()
    year = str(row["year"] or "").strip()
    candidates: list[Path] = []
    if raw:
        p = Path(raw)
        candidates.append(p)
        candidates += [(root / raw) for root in DATA_ROOTS]
    if fname:
        for root in DATA_ROOTS:
            if dept and year:
                candidates.append(root / dept / year / fname)
            candidates.append(root / fname)
    for c in candidates:
        try:
            if c.is_file():
                return str(c.resolve())
        except Exception:
            continue
    return None


def _send_pdf_for_doc_id(con, doc_id: int):
    row = con.execute("""
        SELECT id, filename, path, department, year FROM documents WHERE id = ?;
    """, (doc_id,)).fetchone()
    if not row:
        abort(404)
    path = _resolve_doc_path_from_row(row)
    if not path:
        abort(404, description="File not found on disk for this document")
    app.logger.info(f'ip={_ip()} view doc_id={doc_id} path="{path}"')
    return send_file(path, as_attachment=False, download_name=row["filename"], mimetype="application/pdf")


@app.get("/api/view/<int:doc_id>")
def api_view_doc(doc_id: int):
    return _send_pdf_for_doc_id(get_db(), doc_id)


@app.get("/api/view")
def api_view_query():
    doc_id = request.args.get("doc_id", type=int)
    if not doc_id:
        abort(400, "Missing doc_id")
    return _send_pdf_for_doc_id(get_db(), doc_id)


@app.get("/api/download")
def api_download():
    doc_id = request.args.get("doc_id", type=int)
    if not doc_id:
        abort(400, "Missing doc_id")
    con = get_db()
    row = con.execute("SELECT id, filename, path, department, year FROM documents WHERE id = ?;", (doc_id,)).fetchone()
    if not row:
        abort(404)
    path = _resolve_doc_path_from_row(row)
    if not path:
        abort(404, description="File not found on disk for this document")
    app.logger.info(f'ip={_ip()} download doc_id={doc_id} path="{path}"')
    return send_file(path, as_attachment=True, download_name=row["filename"], mimetype="application/pdf")


@app.post("/api/download/bulk")
def api_download_bulk():
    data = request.get_json(silent=True) or {}
    doc_ids = data.get("doc_ids") or []
    if not isinstance(doc_ids, list) or not doc_ids:
        abort(400, "doc_ids required")
    con = get_db()
    qmarks = ",".join("?" for _ in doc_ids)
    rows = con.execute(f"""
        SELECT id, filename, path, department, year FROM documents WHERE id IN ({qmarks});
    """, doc_ids).fetchall()
    if not rows:
        abort(404)

    mem = io.BytesIO()
    files_added = missing = 0
    used_names: Dict[str, int] = {}
    with zipfile.ZipFile(mem, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for r in rows:
            path = _resolve_doc_path_from_row(r)
            if not path:
                missing += 1
                continue
            arcname = (r["filename"] or f"{r['id']}.pdf").strip() or f"{r['id']}.pdf"
            base = Path(arcname).stem
            ext = Path(arcname).suffix or ".pdf"
            if arcname in used_names:
                used_names[arcname] += 1
                arcname = f"{base} (id {r['id']}){ext}"
            else:
                used_names[arcname] = 1
            zf.write(path, arcname=arcname)
            files_added += 1

    if files_added == 0:
        abort(404, description="No files found for requested doc_ids")
    mem.seek(0)
    app.logger.info(f'ip={_ip()} bulk_download count={files_added} missing={missing}')
    return send_file(mem, as_attachment=True, download_name="exams.zip", mimetype="application/zip")


# =====================================================================
# Flask API: Admin / Pipeline
# =====================================================================

def _check_admin_auth():
    """Check Authorization header for admin password. Returns error response or None."""
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        token = auth[7:]
    else:
        token = auth
    if token != ADMIN_PASSWORD:
        return jsonify({"error": "Unauthorized"}), 401
    return None


@app.post("/api/admin/login")
def api_admin_login():
    """Verify admin password. Frontend calls this to check credentials."""
    data = request.get_json(silent=True) or {}
    pw = data.get("password", "")
    if pw == ADMIN_PASSWORD:
        return jsonify({"ok": True, "token": ADMIN_PASSWORD})
    return jsonify({"ok": False, "error": "Wrong password"}), 401


@app.post("/api/admin/pipeline")
def api_run_pipeline():
    """Trigger the full pipeline manually."""
    err = _check_admin_auth()
    if err:
        return err
    if _pipeline_status["running"]:
        return jsonify({"error": "Pipeline already running"}), 409
    t = threading.Thread(target=run_full_pipeline, daemon=True, name="manual-pipeline")
    t.start()
    return jsonify({"status": "started"})


@app.get("/api/admin/pipeline/status")
def api_pipeline_status():
    err = _check_admin_auth()
    if err:
        return err
    return jsonify(_pipeline_status)


def _run_single_step(name: str, fn):
    """Run a single pipeline step in the background, updating _pipeline_status."""
    if not _pipeline_lock.acquire(blocking=False):
        return
    try:
        _pipeline_status["running"] = True
        _pipeline_status["step"] = name
        _pipeline_status["error"] = None
        result = fn()
        _pipeline_status["step"] = "done"
        _pipeline_status["last_run"] = datetime.now(timezone.utc).isoformat() + "Z"
        _pipeline_status["last_result"] = {name: result}
    except Exception as e:
        _pipeline_status["error"] = str(e)
        app.logger.error(f"Step {name} error: {e}")
    finally:
        _pipeline_status["running"] = False
        _pipeline_lock.release()


@app.post("/api/admin/download")
def api_run_download():
    """Trigger download step only."""
    err = _check_admin_auth()
    if err:
        return err
    if _pipeline_status["running"]:
        return jsonify({"error": "Pipeline already running"}), 409
    threading.Thread(target=_run_single_step, args=("download", run_download), daemon=True).start()
    return jsonify({"status": "started", "step": "download"})


@app.post("/api/admin/index")
def api_run_index():
    """Trigger indexing step only."""
    err = _check_admin_auth()
    if err:
        return err
    if _pipeline_status["running"]:
        return jsonify({"error": "Pipeline already running"}), 409
    threading.Thread(target=_run_single_step, args=("index", run_index), daemon=True).start()
    return jsonify({"status": "started", "step": "index"})


@app.post("/api/admin/ocr")
def api_run_ocr():
    """Trigger OCR step only."""
    err = _check_admin_auth()
    if err:
        return err
    if _pipeline_status["running"]:
        return jsonify({"error": "Pipeline already running"}), 409
    threading.Thread(target=_run_single_step, args=("ocr", run_ocr), daemon=True).start()
    return jsonify({"status": "started", "step": "ocr"})


@app.post("/api/admin/fts-rebuild")
def api_run_fts_rebuild():
    """Trigger FTS rebuild only."""
    err = _check_admin_auth()
    if err:
        return err
    if _pipeline_status["running"]:
        return jsonify({"error": "Pipeline already running"}), 409
    threading.Thread(target=_run_single_step, args=("fts", run_fts_rebuild), daemon=True).start()
    return jsonify({"status": "started", "step": "fts"})


# =====================================================================
# Serve built SPA (optional)
# =====================================================================

if DIST_DIR.exists():
    @app.route("/", defaults={"path": ""})
    @app.route("/<path:path>")
    def spa(path: str):
        target = DIST_DIR / path
        if path and target.is_file():
            return send_from_directory(DIST_DIR, path)
        return send_from_directory(DIST_DIR, "index.html")

# =====================================================================
# CLI entry point
# =====================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exam Archive: serve, download, index, OCR")
    parser.add_argument("--host", default=os.environ.get("HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "5004")))
    parser.add_argument("--threads", type=int, default=int(os.environ.get("THREADS", "4")))
    parser.add_argument("--no-scheduler", action="store_true", help="Disable automatic daily pipeline")
    parser.add_argument("--run-pipeline-now", action="store_true", help="Run full pipeline once before starting server")

    # Pipeline-only modes (no server)
    parser.add_argument("--download-only", action="store_true", help="Run download and exit")
    parser.add_argument("--index-only", action="store_true", help="Run indexing and exit")
    parser.add_argument("--ocr-only", action="store_true", help="Run OCR and exit")
    parser.add_argument("--fts-rebuild-only", action="store_true", help="Rebuild FTS and exit")
    parser.add_argument("--pipeline-only", action="store_true", help="Run full pipeline and exit")

    args = parser.parse_args()

    # Pipeline-only modes
    if args.download_only:
        print(run_download())
        sys.exit(0)
    if args.index_only:
        print(run_index())
        sys.exit(0)
    if args.ocr_only:
        print(run_ocr())
        sys.exit(0)
    if args.fts_rebuild_only:
        print(run_fts_rebuild())
        sys.exit(0)
    if args.pipeline_only:
        print(run_full_pipeline())
        sys.exit(0)

    # Ensure schema exists
    conn = open_pipeline_db()
    ensure_schema(conn)

    # One-time fix: backfill pages column from page rows if NULL
    null_pages = conn.execute("SELECT COUNT(*) FROM documents WHERE pages IS NULL").fetchone()[0]
    if null_pages > 0:
        print(f"[startup] Backfilling pages column for {null_pages} documents...")
        conn.execute("""
            UPDATE documents SET pages = (
                SELECT COUNT(*) FROM pages p WHERE p.doc_id = documents.id
            ) WHERE pages IS NULL AND id IN (SELECT DISTINCT doc_id FROM pages)
        """)
        conn.commit()
        still_null = conn.execute("SELECT COUNT(*) FROM documents WHERE pages IS NULL").fetchone()[0]
        print(f"[startup] pages backfill done ({null_pages - still_null} updated, {still_null} still NULL)")

    conn.close()

    # Run pipeline before starting server if requested
    if args.run_pipeline_now:
        print("Running initial pipeline...")
        result = run_full_pipeline()
        print(f"Pipeline complete: {result}")

    # Start background scheduler
    if not args.no_scheduler:
        start_scheduler()

    # Start server
    USE_WAITRESS = os.environ.get("USE_WAITRESS", "1") == "1"
    CHANNEL_TIMEOUT = int(os.environ.get("CHANNEL_TIMEOUT", "90"))

    if USE_WAITRESS:
        try:
            from waitress import serve
            print(f"Serving on {args.host}:{args.port} (waitress, {args.threads} threads)")
            serve(app, host=args.host, port=args.port, threads=args.threads,
                  channel_timeout=CHANNEL_TIMEOUT, ident="exams-backend")
        except ImportError:
            print("waitress not installed; falling back to Flask dev server")
            app.run(host=args.host, port=args.port, debug=False)
    else:
        app.run(host=args.host, port=args.port, debug=False)

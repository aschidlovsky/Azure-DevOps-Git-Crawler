import os, base64, re, json, logging, asyncio, time
from typing import Dict, Any, List, Set, Optional
from functools import lru_cache
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Query, Body, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# ------------------------------------------------------------
# APP INITIALIZATION
# ------------------------------------------------------------
app = FastAPI(title="ADO Repo Dependency Connector", version="1.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

log = logging.getLogger("uvicorn.error")

# ------------------------------------------------------------
# ENVIRONMENT + AUTH HELPERS
# ------------------------------------------------------------
def _req(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise HTTPException(500, f"Missing required environment variable: {name}")
    return v

def get_env() -> Dict[str, str]:
    return {
        "ORG": _req("ADO_ORG"),
        "PROJECT": _req("ADO_PROJECT"),
        "REPO": _req("ADO_REPO"),
        "PAT": _req("ADO_PAT"),
        "REF": os.getenv("DEFAULT_REF", "main"),
        "API": "7.1-preview.1",
    }

def ado_headers(pat: str) -> Dict[str, str]:
    # Azure DevOps requires a non-empty username for PAT auth
    # (username can be anything non-empty). Keep existing behavior.
    basic = base64.b64encode(f"pat:{pat}".encode()).decode()
    return {
        "Authorization": f"Basic {basic}",
        "X-TFS-FedAuthRedirect": "Suppress",
        "Accept": "application/json",
        "User-Agent": "ado-git-crawler/1.2",
    }

def ado_base(env: Dict[str, str]) -> str:
    return f"https://dev.azure.com/{env['ORG']}/{env['PROJECT']}/_apis/git/repositories/{env['REPO']}"

# ------------------------------------------------------------
# GLOBAL EXCEPTION HANDLER (guarantee JSON for Agent)
# ------------------------------------------------------------
@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    log.exception("Unhandled exception")
    # If it's an HTTPException, FastAPI already handled formatting; this is a final safety net.
    return JSONResponse(status_code=500, content={"error": "internal_error", "message": str(exc)[:300]})

# ------------------------------------------------------------
# SHARED HTTPX CLIENT (no redirects; faster via connection reuse)
# ------------------------------------------------------------
@asynccontextmanager
async def shared_client(timeout=60):
    # follow_redirects=False so we can detect ADO auth redirects or HTML pages
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=False) as client:
        yield client

# ------------------------------------------------------------
# HEALTH CHECK
# ------------------------------------------------------------
@app.get("/health")
@app.get("/health/")
async def health():
    return {"status": "ok"}

# ------------------------------------------------------------
# FETCH FILE CONTENT FROM ADO (HEAD -> DEFAULT_REF normalization)
# ------------------------------------------------------------
async def get_item_content(path: str, ref: Optional[str]) -> Dict[str, Any]:
    env = get_env()
    base = ado_base(env)

    used_ref = ref or env["REF"]
    if isinstance(used_ref, str) and used_ref.upper() == "HEAD":
        used_ref = env["REF"]

    params = {
        "path": f"/{path}" if not path.startswith("/") else path,
        "versionDescriptor.version": used_ref,
        "includeContent": "true",
        "api-version": env["API"],
    }
    url = f"{base}/items"

    log.info(f"[get_item_content] path={params['path']} ref={used_ref}")

    async with shared_client(timeout=60) as client:
        r = await client.get(url, headers=ado_headers(env["PAT"]), params=params)

        # Guard against redirects and HTML (login or proxy pages)
        ctype = r.headers.get("content-type", "")
        if r.status_code in (301, 302, 303, 307, 308) or "text/html" in ctype:
            raise HTTPException(401, "Azure DevOps authentication failed – PAT invalid, expired, or unauthorized")

        if r.status_code == 404:
            raise HTTPException(404, f"File not found in ADO: {path}@{used_ref}")

        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise HTTPException(e.response.status_code, f"ADO error: {e.response.text[:300]}") from e

        try:
            data = r.json()
        except Exception:
            raise HTTPException(502, "Azure DevOps returned non-JSON content (likely auth redirect or proxy page).")

        content = data.get("content")
        if not content:
            raise HTTPException(404, "No content found for this path (might be binary or empty).")

        # base64 -> text if needed
        try:
            content = base64.b64decode(content).decode("utf-8")
        except Exception:
            pass

        return {
            "content": content,
            "sha": data.get("objectId") or data.get("commitId") or "unknown",
            "ref": used_ref,
        }

# ------------------------------------------------------------
# LIGHTWEIGHT PARSER (with kernel/system filtering + Form DS implicit CRUD)
# ------------------------------------------------------------
DEPENDENCY_PATTERNS = [
    (r"\b(select|insert|update|delete)\s+(\w+)", "Table", "statement"),
    (r"\b(\w+)\.DataSource\(", "Table", "DataSource()"),
    (r"\b(\w+)::(construct|new|run)\b", "Class", "static-call"),
    (r"\bnew\s+(\w+)\s*\(", "Class", "new"),
    (r"\b(\w+)\.(insert|update|delete|validateWrite|validateDelete)\s*\(", "Table", "crud-call"),
]

# Form DS extraction patterns (various XPO shapes)
FORM_DS_PATTERNS = [
    r"\bOBJECT\s+FORM\s+DATASOURCE\s+(\w+)",
    r"\bDATA\s+SOURCE\s+NAME\s*:\s*(\w+)",
    r"\bFormDataSource\s+(\w+)",
    r"\bdataSource\s*\(\s*(\w+)\s*\)",
    r"\bDataSource\s*\(\s*(\w+)\s*\)",
    r"\bDATA\s+SOURCE\s+(\w+)",
]

CRUD_SIGNATURE = re.compile(
    r"\b(\w+)\.(insert|update|delete|validateWrite|validateDelete)\s*\(", re.IGNORECASE
)

# Expanded skip: kernel/system names & common false positives
KERNEL_SKIP = {
    # Kernel/system
    "FormRun", "Args", "Set", "SetEnumerator", "Global", "QueryBuildDataSource", "RunBase",
    "Map", "List", "Array", "Tmp", "TmpTable", "Info", "Error", "Warning",
    "Query", "QueryRun", "SysQuery", "SysTable", "SysDictTable", "SysDictField",
    "TableId", "DataAreaId", "Company",
    # Common tokens / short identifiers
    "RecId", "t", "x", "y", "z", "this", "super",
    # X++ keywords accidentally captured
    "sum", "avg", "min", "max", "count", "len", "is", "the", "for", "select", "firstOnly",
    "exists", "update_recordset", "delete_from", "insert_recordset"
}

TYPE_PATHS = {
    "Table": "Tables",
    "Class": "Classes",
    "Form": "Forms",
    "Map": "Maps",
    "Unknown": "Unknown"
}

def _looks_like_form_path(file_path: str) -> bool:
    return file_path.startswith("Forms/") or (file_path.lower().endswith(".xpo") and "/forms/" in file_path.lower())

def extract_dependencies(content: str, file_path: Optional[str] = None) -> Dict[str, Any]:
    deps: List[Dict[str, Any]] = []
    implicit: List[Dict[str, Any]] = []
    business: List[Dict[str, Any]] = []
    dep_keys: Set[str] = set()

    # Direct dependency detection (with folder mapping)
    for pattern, kind, reason in DEPENDENCY_PATTERNS:
        for m in re.finditer(pattern, content, re.IGNORECASE):
            symbol = m.group(2) if reason == "statement" else m.group(1)
            if not symbol:
                continue
            if symbol in KERNEL_SKIP:
                continue
            if len(symbol) < 3:
                continue
            folder = TYPE_PATHS.get(kind, "Unknown")
            path = f"{folder}/{symbol}.xpo"
            if path not in dep_keys:
                deps.append({"path": path, "type": kind, "symbol": symbol, "reason": reason})
                dep_keys.add(path)

    # Explicit CRUD calls found in code
    for m in CRUD_SIGNATURE.finditer(content):
        table_sym = m.group(1)
        if table_sym and table_sym not in KERNEL_SKIP and len(table_sym) >= 3:
            implicit.append({
                "table": table_sym,
                "method": m.group(2) + "()",
                "caller": "unknown",
                "line": m.start()
            })

    # Form DS → implicit dependencies + implicit CRUD on save
    is_form = _looks_like_form_path(file_path or "")
    if is_form:
        ds_tables: Set[str] = set()
        for p in FORM_DS_PATTERNS:
            for mm in re.finditer(p, content, re.IGNORECASE):
                sym = mm.group(1)
                if not sym:
                    continue
                if sym in KERNEL_SKIP or len(sym) < 3:
                    continue
                ds_tables.add(sym)

        if ds_tables:
            for sym in ds_tables:
                table_path = f"Tables/{sym}.xpo"
                if table_path not in dep_keys:
                    deps.append({
                        "path": table_path,
                        "type": "Table",
                        "symbol": sym,
                        "reason": "form-datasource"
                    })
                    dep_keys.add(table_path)

            for sym in ds_tables:
                for method in ["insert()", "update()", "delete()", "validateWrite()", "validateDelete()"]:
                    implicit.append({
                        "table": sym,
                        "method": method,
                        "caller": "FormSaveKernel",
                        "line": -1,
                        "reason": "implicit-form-datasource-crud"
                    })

    # Business-rule signals
    for ln, line in enumerate(content.splitlines(), start=1):
        if any(k in line for k in ["validate", "error(", "ttsBegin", "ttsCommit"]):
            business.append({"line": ln, "context": line.strip()[:160]})

    return {"dependencies": deps, "implicit_crud": implicit, "business_rules": business}

# ------------------------------------------------------------
# FOLLOW-ON FILTERING (re-applies skip rules every hop)
# ------------------------------------------------------------
ALLOWED_FOLDERS = {"Tables", "Classes", "Forms", "Maps"}

def _should_skip_dep_path(p: str) -> bool:
    """
    Returns True if this normalized path should be ignored (kernel/system/junk).
    Expects 'Tables/PurchLine.xpo' etc.
    """
    if not p or ".xpo" not in p:
        return True
    m = re.match(r"^(Tables|Classes|Forms|Maps)/([^/]+)\.xpo$", p)
    if not m:
        return True
    folder, symbol = m.group(1), m.group(2)
    if folder not in ALLOWED_FOLDERS:
        return True
    if symbol in KERNEL_SKIP:
        return True
    if symbol.lower() in {
        "sum","avg","min","max","count","len","is","the","for","select","firstonly",
        "exists","update_recordset","delete_from","insert_recordset","this","super"
    }:
        return True
    if len(symbol) < 3:
        return True
    return False

def _normalize_dep_path(p: str) -> str:
    """
    Strip 'lcl' or 'Lcl' prefix (including 'lclmss') while preserving 'mss'.
    Examples:
      Tables/lclPurchLine.xpo   -> Tables/PurchLine.xpo
      Tables/lclmssBDAck.xpo    -> Tables/mssBDAck.xpo   (keep 'mss')
    """
    return re.sub(
        r"(^|/)(lcl|Lcl)(mss)?([A-Z])",
        lambda m: f"{m.group(1)}{(m.group(3) or '')}{m.group(4)}",
        p or "",
    )

# ------------------------------------------------------------
# ENDPOINT: /file
# ------------------------------------------------------------
@app.get("/file")
@app.get("/file/")
async def file_get(
    path: str = Query(..., description="Repo path (e.g., Forms/CustTable.xpo)"),
    ref: Optional[str] = Query(None),
):
    data = await get_item_content(path, ref)
    return {"path": path, "ref": data["ref"], "sha": data["sha"], "content": data["content"]}

# ------------------------------------------------------------
# ENDPOINT: /deps (single-hop)
# ------------------------------------------------------------
@app.get("/deps")
@app.get("/deps/")
async def deps_get(
    file: str = Query(..., description="Repo path to .xpo file"),
    ref: Optional[str] = Query(None),
    page: int = 1,
    limit: int = 200,
):
    fetched = await get_item_content(file, ref)
    content, sha, used_ref = fetched["content"], fetched["sha"], fetched["ref"]
    parsed = extract_dependencies(content, file_path=file)

    start, end = (page - 1) * limit, (page - 1) * limit + limit
    paged = parsed["dependencies"][start:end]
    total = len(parsed["dependencies"])
    total_pages = (total + limit - 1) // limit if total else 1

    return {
        "file": file,
        "ref": used_ref,
        "sha": sha,
        "dependencies": paged,
        "business_rules": parsed["business_rules"],
        "implicit_crud": parsed["implicit_crud"],
        "unresolved": [],
        "visited": [file],
        "skipped": [],
        "page": page,
        "limit": limit,
        "total_dependencies": total,
        "total_pages": total_pages,
    }

# ------------------------------------------------------------
# ENDPOINT: /resolve (recursive multi-hop BFS)
# ------------------------------------------------------------
@app.post("/resolve")
@app.post("/resolve/")
async def resolve_post(payload: Dict[str, Any] = Body(...)):
    """
    BFS recursion: expands dependencies up to max_depth internally.
    - Normalizes lcl/lclmss prefixes but preserves 'mss'
    - Filters kernel/system noise each hop
    - Adds implicit CRUD for Form DS tables
    - Handles per-node errors without failing the whole request
    Body:
      {
        "start_file": "Forms/mssBDAckTableFurn.xpo",
        "ref": "main",
        "max_depth": 5
      }
    """
    env = get_env()
    start_file: str = payload.get("start_file")
    if not start_file:
        raise HTTPException(400, "Missing required field: start_file")

    used_ref = payload.get("ref") or env["REF"]
    if str(used_ref).upper() == "HEAD":
        used_ref = env["REF"]
    max_depth = int(payload.get("max_depth", 5))

    visited: Set[str] = set()
    graph: List[Dict[str, Any]] = []
    implicit_all: List[Dict[str, Any]] = []
    current_level: List[str] = [start_file]
    depth = 0

    async def safe_deps(file: str):
        try:
            node = await deps_get(file=file, ref=used_ref, page=1, limit=200)  # type: ignore
            implicit_all.extend(node.get("implicit_crud", []))
            return {"file": file, "deps": node.get("dependencies", []), "error": None}
        except Exception as e:
            log.error(f"[resolve] Failed deps_get for {file}: {e}")
            return {"file": file, "deps": None, "error": str(e)}

    while current_level and depth < max_depth:
        log.info(f"[resolve] Depth {depth}: {len(current_level)} files")
        level_files = [f for f in current_level if f not in visited]
        visited.update(level_files)

        results = await asyncio.gather(*[safe_deps(f) for f in level_files])

        next_level: List[str] = []
        for res in results:
            file = res["file"]
            err = res.get("error")
            deps = res.get("deps") or []
            if err:
                graph.append({"file": file, "error": err, "depth": depth, "dependencies": []})
                continue

            dep_paths: List[str] = []
            for d in deps:
                raw = d.get("path") or ""
                clean = _normalize_dep_path(raw)
                if _should_skip_dep_path(clean):
                    continue
                dep_paths.append(clean)

            graph.append({
                "file": file,
                "depth": depth,
                "dependencies": dep_paths,
                "error": None
            })
            next_level.extend(dep_paths)

        depth += 1
        current_level = [f for f in set(next_level) if f not in visited]

    return {
        "root": start_file,
        "ref": used_ref,
        "max_depth": max_depth,
        "depth_completed": depth,
        "total_files": len(visited),
        "visited": sorted(list(visited)),
        "implicit_crud": implicit_all,
        "graph": graph,
        "status": "complete"
    }

# ------------------------------------------------------------
# /report with deadline, summary mode, caching, breadth caps, trims
# ------------------------------------------------------------
class _ReqScopeCache:
    def __init__(self):
        self.file_json: Dict[tuple, Dict[str, Any]] = {}   # (path, ref) → file_get() result
        self.deps_json: Dict[tuple, Dict[str, Any]] = {}   # (file, ref) → deps_get() result

def _now_ms() -> int:
    return int(time.time() * 1000)

def _deadline_expired(deadline_ms: int) -> bool:
    return _now_ms() >= deadline_ms

def _time_left(deadline_ms: int) -> int:
    return max(0, deadline_ms - _now_ms())

from asyncio import Semaphore

async def _analyze_single_file(path: str, ref: Optional[str], file_get_fn, deps_get_fn, deadline_ms: int) -> Dict[str, Any]:
    if _deadline_expired(deadline_ms):
        return {"path": path, "ref": ref, "error": "deadline_exceeded"}
    try:
        file_res = await file_get_fn(path=path, ref=ref)
    except Exception as e:
        return {"path": path, "ref": ref, "error": f"file_get: {e}"}

    try:
        deps_res = await deps_get_fn(file=path, ref=ref)
    except Exception as e:
        return {
            "path": path, "ref": ref,
            "sha": file_res.get("sha"),
            "content_len": len(file_res.get("content", "")),
            "dependencies": [], "business_rules": [], "implicit_crud": [],
            "error": f"deps_get: {e}"
        }

    # Normalize + filter deps (defense in depth)
    norm_deps: List[Dict[str, Any]] = []
    for d in deps_res.get("dependencies", []) or []:
        clean = _normalize_dep_path(d.get("path") or "")
        if _should_skip_dep_path(clean):
            continue
        nd = dict(d)
        nd["path"] = clean
        norm_deps.append(nd)

    # Trim near-deadline
    if _time_left(deadline_ms) < 5000 and len(norm_deps) > 200:
        norm_deps = norm_deps[:200]

    return {
        "path": path,
        "ref": deps_res.get("ref", ref),
        "sha": deps_res.get("sha") or file_res.get("sha", "unknown"),
        "content_len": len(file_res.get("content", "")),
        "dependencies": norm_deps,
        "business_rules": deps_res.get("business_rules", []),
        "implicit_crud": deps_res.get("implicit_crud", []),
        "error": None,
    }

@app.post("/report")
@app.post("/report/")
async def report_post(payload: Dict[str, Any] = Body(...)):
    """
    Full transitive analysis in one call with a hard deadline and summary mode.

    Body:
    {
      "start_file": "Forms/mssBDAckTableFurn.xpo",
      "ref": "main",
      "max_depth": 5,
      "max_concurrency": 8,
      "timeout_ms": 110000,          # optional total deadline (default ~110s)
      "mode": "full" | "summary",    # summary returns counts + top-N
      "level_cap": 400,              # max nodes expanded per depth level
      "top_n": 200                   # summary mode: how many items to include
    }
    """
    env = get_env()
    start_file: str = payload.get("start_file")
    if not start_file:
        raise HTTPException(400, "Missing required field: start_file")

    used_ref = payload.get("ref") or env["REF"]
    if str(used_ref).upper() == "HEAD":
        used_ref = env["REF"]

    # Limits / defaults
    max_depth = int(payload.get("max_depth", 5))
    max_depth = max(1, min(max_depth, 10))

    max_conc = int(payload.get("max_concurrency", int(os.getenv("MAX_CONCURRENCY", "8"))))
    max_conc = max(1, min(max_conc, 16))

    timeout_ms = int(payload.get("timeout_ms", int(os.getenv("REPORT_TIMEOUT_MS", "110000"))))
    deadline = _now_ms() + timeout_ms

    mode = (payload.get("mode") or "full").lower()
    level_cap = int(payload.get("level_cap", int(os.getenv("LEVEL_CAP", "400"))))
    top_n = int(payload.get("top_n", int(os.getenv("SUMMARY_TOP_N", "200"))))

    # Hard caps for runaway graphs
    MAX_VISITED = int(os.getenv("MAX_VISITED", "1500"))
    MAX_EDGES   = int(os.getenv("MAX_EDGES",   "12000"))
    MAX_RULES   = int(os.getenv("MAX_RULES",   "5000"))

    # --------- per-request caches to reduce ADO calls ----------
    scope = _ReqScopeCache()

    async def file_get_cached(path: str, ref: Optional[str]):
        key = (path, ref or used_ref)
        if key in scope.file_json:
            return scope.file_json[key]
        res = await file_get(path=path, ref=ref)  # type: ignore
        scope.file_json[key] = res
        return res

    async def deps_get_cached(file: str, ref: Optional[str]):
        key = (file, ref or used_ref)
        if key in scope.deps_json:
            return scope.deps_json[key]
        res = await deps_get(file=file, ref=ref, page=1, limit=200)  # type: ignore
        scope.deps_json[key] = res
        return res

    # --------- 1) internal resolve (bounded, deadline-aware) ----------
    visited: Set[str] = set()
    graph: List[Dict[str, Any]] = []
    implicit_all: List[Dict[str, Any]] = []
    current_level: List[str] = [start_file]
    depth = 0
    overflow = False

    async def safe_deps(file: str):
        try:
            node = await deps_get_cached(file=file, ref=used_ref)
            implicit_all.extend(node.get("implicit_crud", []))
            return {"file": file, "deps": node.get("dependencies", []), "error": None}
        except Exception as e:
            log.error(f"[report] deps_get failed for {file}: {e}")
            return {"file": file, "deps": None, "error": str(e)}

    while current_level and depth < max_depth and not _deadline_expired(deadline):
        # breadth cap per level to avoid explosion
        if len(current_level) > level_cap:
            current_level = current_level[:level_cap]
            overflow = True

        level_files = [f for f in current_level if f not in visited]
        visited.update(level_files)

        results = await asyncio.gather(*[safe_deps(f) for f in level_files])
        next_level: List[str] = []

        for res in results:
            file = res["file"]
            if res.get("error"):
                graph.append({"file": file, "depth": depth, "dependencies": [], "error": res["error"]})
                continue

            dep_paths: List[str] = []
            for d in (res.get("deps") or []):
                clean = _normalize_dep_path(d.get("path") or "")
                if _should_skip_dep_path(clean):
                    continue
                dep_paths.append(clean)

            graph.append({"file": file, "depth": depth, "dependencies": dep_paths, "error": None})
            next_level.extend(dep_paths)

        depth += 1
        if len(visited) >= MAX_VISITED:
            overflow = True
            break

        # unique and remove already visited
        current_level = [f for f in dict.fromkeys(next_level) if f not in visited]

    visited_list = sorted(list(visited))

    # --------- 2) analyze (bounded concurrency, deadline-aware) ----------
    sem = Semaphore(max_conc)

    async def _guarded_analyze(p: str):
        if _deadline_expired(deadline):
            return {"path": p, "ref": used_ref, "error": "deadline_exceeded"}
        async with sem:
            return await _analyze_single_file(p, used_ref, file_get_cached, deps_get_cached, deadline)

    if mode == "summary" and len(visited_list) > top_n:
        sample = visited_list[:top_n]
        analyses = await asyncio.gather(*[_guarded_analyze(p) for p in sample])
        overflow = True
    else:
        analyses = await asyncio.gather(*[_guarded_analyze(p) for p in visited_list])

    # --------- 3) merge into objects/edges/rules ----------
    objects: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []
    business_rules_all: List[Dict[str, Any]] = []
    implicit_crud_all: List[Dict[str, Any]] = list(implicit_all)

    depth_index = {g["file"]: g["depth"] for g in graph}
    for a in analyses:
        objects.append({
            "path": a.get("path"),
            "ref": a.get("ref", used_ref),
            "sha": a.get("sha", "unknown"),
            "depth": depth_index.get(a.get("path"), -1),
            "error": a.get("error")
        })
        for d in a.get("dependencies", []) or []:
            edges.append({
                "from": a.get("path"),
                "to": d.get("path"),
                "reason": d.get("reason"),
                "type": d.get("type"),
                "depth_from_root": depth_index.get(a.get("path"), -1) + 1
            })
        business_rules_all.extend(a.get("business_rules", []) or [])
        implicit_crud_all.extend(a.get("implicit_crud", []) or [])

    # de-dupe edges
    seen = set()
    dedup_edges = []
    for e in edges:
        key = (e["from"], e["to"], e.get("reason"))
        if key in seen:
            continue
        seen.add(key)
        dedup_edges.append(e)

    # hard trims for oversized
    trimmed = False
    if len(dedup_edges) > MAX_EDGES:
        dedup_edges = dedup_edges[:MAX_EDGES]
        trimmed = True
    if len(business_rules_all) > MAX_RULES:
        business_rules_all = business_rules_all[:MAX_RULES]
        trimmed = True

    status_parts = []
    if overflow:
        status_parts.append("partial: breadth capped")
    if trimmed:
        status_parts.append("partial: payload trimmed")
    if _deadline_expired(deadline):
        status_parts.append("partial: deadline exceeded")
    if not status_parts:
        status_parts.append("complete")

    return {
        "root": start_file,
        "ref": used_ref,
        "max_depth": max_depth,
        "depth_completed": depth,
        "visited": visited_list if mode == "full" else visited_list[:top_n],
        "graph": graph if mode == "full" else graph[:top_n],
        "objects": objects if mode == "full" else objects[:top_n],
        "edges": dedup_edges if mode == "full" else dedup_edges[:top_n],
        "business_rules": business_rules_all if mode == "full" else business_rules_all[:top_n],
        "implicit_crud": implicit_crud_all if mode == "full" else implicit_crud_all[:top_n],
        "status": " ; ".join(status_parts)
    }

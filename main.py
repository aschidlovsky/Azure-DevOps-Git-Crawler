import os, base64, re, json, logging, asyncio, time
from typing import Dict, Any, List, Set, Optional
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Query, Body, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from asyncio import Semaphore

# ------------------------------------------------------------
# APP INITIALIZATION
# ------------------------------------------------------------
app = FastAPI(title="ADO Repo Dependency Connector", version="1.5.0")

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
    basic = base64.b64encode(f"pat:{pat}".encode()).decode()
    return {
        "Authorization": f"Basic {basic}",
        "X-TFS-FedAuthRedirect": "Suppress",
        "Accept": "application/json",
        "User-Agent": "ado-git-crawler/1.5",
    }

def ado_base(env: Dict[str, str]) -> str:
    return f"https://dev.azure.com/{env['ORG']}/{env['PROJECT']}/_apis/git/repositories/{env['REPO']}"

# ------------------------------------------------------------
# GLOBAL EXCEPTION HANDLER (safety net)
# ------------------------------------------------------------
@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    log.exception("Unhandled exception")
    return JSONResponse(status_code=500, content={"error": "internal_error", "message": str(exc)[:300]})

# ------------------------------------------------------------
# SHARED HTTPX CLIENT (no redirects; reuse connections)
# ------------------------------------------------------------
@asynccontextmanager
async def shared_client(timeout=60):
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
# LIGHTWEIGHT PARSER (kernel/system filter + Form DS implicit CRUD)
# ------------------------------------------------------------
DEPENDENCY_PATTERNS = [
    # prefer table refs via from/join to avoid EDT/field names
    (r"\b(from|join)\s+([A-Z][A-Za-z0-9_]{2,}|mss[A-Za-z0-9_]{2,})\b", "Table", "from-join"),
    (r"\b(\w+)\.DataSource\(", "Table", "DataSource()"),
    (r"\b(\w+)::(construct|new|run)\b", "Class", "static-call"),
    (r"\bnew\s+(\w+)\s*\(", "Class", "new"),
    (r"\b(\w+)\.(insert|update|delete|validateWrite|validateDelete)\s*\(", "Table", "crud-call"),
]

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

# ---------- Symbol validation ----------
ALLOWED_SYMBOL = re.compile(r"^(?:[A-Z][A-Za-z0-9_]{2,}|mss[A-Za-z0-9_]{2,})$")

# XPO/property tokens and other junk we never want as objects
EXTRA_SKIP = {
    "override", "method", "methods", "reservation", "buffer",
    "forupdate", "localinventtrans", "maxof", "minof", "sales",
    "_salesline", "_inventtrans", "_tmpfrmvirtual", "mapmovement", "mapmovementissue",
    "endproperties", "properties", "objectpool", "datasource",
    "name", "table", "allowcreate", "allowdelete", "allowedit",
    "modelwithoptions", "modelwithoption", "options"
}

# Framework/utility prefix block to cut fan-out + 404s
FRAMEWORK_PREFIXES = (
    "Dict", "Sys", "ImageListAppl_", "Dialog", "ExecutePermission",
    "ListIterator", "Xml", "MapIterator", "Ledger", "CustVendPaymSched",
    "PriceDisc", "SalesTotals", "SalesCalcTax"
)

def is_valid_symbol(symbol: str) -> bool:
    if not symbol:
        return False
    s = symbol.strip()
    low = s.lower()
    if low in KERNEL_SKIP or low in EXTRA_SKIP:
        return False
    # hard block framework/utility prefixes
    if any(s.startswith(p) for p in FRAMEWORK_PREFIXES):
        return False
    # Reject ALL-UPPERCASE tokens (filters ENDPROPERTIES, SMD, etc.)
    if s.isupper():
        return False
    return bool(ALLOWED_SYMBOL.match(s))

# ---------- variable → type binding (resolve purchTable → PurchTable) ----------
DECLARATION_SIG = re.compile(
    r"\b([A-Z][A-Za-z0-9_]{2,}|mss[A-Za-z0-9_]{2,})\s+([a-z][A-Za-z0-9_]*)\s*;", re.MULTILINE
)

def _collect_var_types(content: str) -> Dict[str, str]:
    """Map lowercase buffer names -> PascalCase type (e.g., purchTable -> PurchTable)."""
    types: Dict[str, str] = {}
    for m in DECLARATION_SIG.finditer(content):
        typ, var = m.group(1), m.group(2)
        if is_valid_symbol(typ):
            types[var] = typ
    return types

# ---------- Parse Form DataSource PROPERTIES blocks ----------
DS_BLOCK = re.compile(
    r"\bDATASOURCE\b.*?\bPROPERTIES\b(.*?)\bENDPROPERTIES\b",
    re.IGNORECASE | re.DOTALL
)

def _parse_form_datasources(content: str) -> List[Dict[str, Optional[str]]]:
    """
    Extract DataSource 'Table' and permission flags from a Form XPO.
    Returns a list of dicts: {table, allow_create, allow_delete, allow_edit} with values:
      - table: 'mssBDAckTable' (string) or None
      - flags: True / False / None (None = unspecified)
    """
    results: List[Dict[str, Optional[str]]] = []
    for m in DS_BLOCK.finditer(content):
        props = m.group(1)

        def _find_str(name: str) -> Optional[str]:
            mm = re.search(rf"\b{name}\b\s*[:=]?\s*#?([A-Za-z0-9_]+)", props, re.IGNORECASE)
            return mm.group(1) if mm else None

        def _find_bool(name: str) -> Optional[bool]:
            mm = re.search(rf"\b{name}\b\s*[:=]?\s*#?(Yes|No)", props, re.IGNORECASE)
            if not mm:
                return None
            return mm.group(1).lower() == "yes"

        table = _find_str("Table")
        allow_create = _find_bool("AllowCreate")
        allow_delete = _find_bool("AllowDelete")
        allow_edit   = _find_bool("AllowEdit")

        results.append({
            "table": table,
            "allow_create": allow_create,
            "allow_delete": allow_delete,
            "allow_edit": allow_edit,
        })
    return results

def extract_dependencies(content: str, file_path: Optional[str] = None) -> Dict[str, Any]:
    deps: List[Dict[str, Any]] = []
    implicit: List[Dict[str, Any]] = []
    business: List[Dict[str, Any]] = []
    dep_keys: Set[str] = set()

    # variable type table (for resolving lowercase buffers in CRUD)
    var_types = _collect_var_types(content)

    # Direct dependency detection (with folder mapping)
    for pattern, kind, reason in DEPENDENCY_PATTERNS:
        for m in re.finditer(pattern, content, re.IGNORECASE):
            symbol = m.group(2) if reason in ("from-join", "statement") else m.group(1)
            if not symbol:
                continue
            if not is_valid_symbol(symbol):
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

    # Explicit CRUD calls found in code (resolve variable -> declared type)
    for m in CRUD_SIGNATURE.finditer(content):
        raw_sym = m.group(1)            # could be 'PurchTable' or 'purchTable'
        method = m.group(2) + "()"      # e.g., update()

        resolved: Optional[str] = None
        if is_valid_symbol(raw_sym):
            resolved = raw_sym
        else:
            cand = var_types.get(raw_sym)
            if cand and is_valid_symbol(cand):
                resolved = cand

        if resolved:
            implicit.append({
                "table": resolved,
                "method": method,
                "caller": "unknown",
                "line": m.start()
            })
            # Also promote as a dependency edge to the Table
            table_path = f"Tables/{resolved}.xpo"
            if table_path not in dep_keys:
                deps.append({
                    "path": table_path,
                    "type": "Table",
                    "symbol": resolved,
                    "reason": "crud-call"
                })
                dep_keys.add(table_path)

    # Form DS → dependency on the DS Table + CRUD methods gated by Allow* flags
    is_form = _looks_like_form_path(file_path or "")
    if is_form:
        # 1) Prefer explicit PROPERTIES parsing for authoritative DS config
        ds_list = _parse_form_datasources(content)

        # 2) Fallback: legacy patterns (kept for compatibility)
        if not ds_list:
            legacy_ds: Set[str] = set()
            for p in FORM_DS_PATTERNS:
                for mm in re.finditer(p, content, re.IGNORECASE):
                    sym = mm.group(1)
                    if sym and is_valid_symbol(sym) and sym not in KERNEL_SKIP and len(sym) >= 3:
                        legacy_ds.add(sym)
            ds_list = [{"table": t, "allow_create": None, "allow_delete": None, "allow_edit": None} for t in legacy_ds]

        for ds in ds_list:
            sym = (ds.get("table") or "").strip()
            if not sym or not is_valid_symbol(sym):
                continue

            # Always add the Table dependency for the DS
            table_path = f"Tables/{sym}.xpo"
            if table_path not in dep_keys:
                deps.append({
                    "path": table_path,
                    "type": "Table",
                    "symbol": sym,
                    "reason": "form-datasource"
                })
                dep_keys.add(table_path)

            # Determine allowed CRUD from flags; unspecified ⇒ permissive (include)
            allow_create = ds.get("allow_create")
            allow_delete = ds.get("allow_delete")
            allow_edit   = ds.get("allow_edit")

            methods: List[str] = []
            # Edit controls UPDATE (+ validateWrite + write)
            if allow_edit is not False:
                methods.extend(["update()", "validateWrite()", "write()"])
            # Create controls INSERT
            if allow_create is not False:
                methods.append("insert()")
            # Delete controls DELETE (+ validateDelete)
            if allow_delete is not False:
                methods.extend(["delete()", "validateDelete()"])

            for method in methods:
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
    """
    if not p or ".xpo" not in p:
        return True
    m = re.match(r"^(Tables|Classes|Forms|Maps)/([^/]+)\.xpo$", p)
    if not m:
        return True
    folder, symbol = m.group(1), m.group(2)
    if folder not in ALLOWED_FOLDERS:
        return True
    # block framework/utility prefixes at path level too
    if any(symbol.startswith(pref) for pref in FRAMEWORK_PREFIXES):
        return True
    # enforce valid AX-like symbol form
    if not is_valid_symbol(symbol):
        return True
    if symbol in KERNEL_SKIP:
        return True
    if symbol.lower() in {
        "sum","avg","min","max","count","len","is","the","for","select","firstonly",
        "exists","update_recordset","delete_from","insert_recordset","this","super"
    }:
        return True
    if symbol.isupper():  # extra guard to block tokens like ENDPROPERTIES, SMD
        return True
    if len(symbol) < 3:
        return True
    return False

def _normalize_dep_path(p: str) -> str:
    """
    Strip 'lcl' or 'Lcl' prefix (including 'lclmss') while preserving 'mss'.
    Examples:
      Tables/lclPurchLine.xpo   -> Tables/PurchLine.xpo
      Tables/lclmssBDAck.xpo    -> Tables/mssBDAck.xpo
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
                clean = _normalize_dep_path(d.get("path") or "")
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
# HELPERS: deadlines, budgeted responses for /report
# ------------------------------------------------------------
def _now_ms() -> int:
    return int(time.time() * 1000)

def _deadline_expired(deadline_ms: int) -> bool:
    return _now_ms() >= deadline_ms

def _time_left(deadline_ms: int) -> int:
    return max(0, deadline_ms - _now_ms())

def _shrink_report_for_budget(result: Dict[str, Any], budget_bytes: int) -> Dict[str, Any]:
    """
    Iteratively trims big arrays until the utf-8 JSON is under budget.
    Preserves structure and types; only reduces list lengths / long strings.
    """
    def size(d: Dict[str, Any]) -> int:
        return len(json.dumps(d, ensure_ascii=False).encode("utf-8"))

    if size(result) <= budget_bytes:
        return result

    r = dict(result)
    arrays = ["objects", "edges", "graph", "business_rules", "implicit_crud", "visited"]
    trim_steps = [0.5, 0.25, 0.1]

    # Drop non-essential heavy keys inside objects (keep id fields)
    if "objects" in r and isinstance(r["objects"], list):
        for o in r["objects"]:
            if isinstance(o, dict):
                for k in list(o.keys()):
                    if k not in ("path", "ref", "sha", "depth", "error"):
                        o.pop(k, None)
    if size(r) <= budget_bytes:
        return r

    # Trim arrays progressively
    for step in trim_steps:
        for key in arrays:
            if key in r and isinstance(r[key], list) and len(r[key]) > 0:
                keep = max(25, int(len(r[key]) * step))
                r[key] = r[key][:keep]
        # shorten business_rule context strings
        if "business_rules" in r and isinstance(r["business_rules"], list):
            for br in r["business_rules"]:
                if isinstance(br, dict) and isinstance(br.get("context"), str) and len(br["context"]) > 120:
                    br["context"] = br["context"][:120]
        if size(r) <= budget_bytes:
            return r

    r["status"] = (r.get("status", "") + " ; partial: payload budget trim").strip(" ;")
    for key in arrays:
        if key in r and isinstance(r[key], list):
            r[key] = r[key][:25]
    return r

def _json_ok(result: Dict[str, Any], budget_bytes: int = 3_000_000) -> JSONResponse:
    """Return a JSON 200 with charset, enforcing a byte budget."""
    safe = _shrink_report_for_budget(result, budget_bytes)
    # Log final payload size for Agent debugging
    log.info(f"[report] payload_bytes={len(json.dumps(safe, ensure_ascii=False).encode('utf-8'))} status={safe.get('status')}")
    return JSONResponse(
        content=safe,
        status_code=200,
        media_type="application/json; charset=utf-8",
    )

# ------------------------------------------------------------
# /report with deadline, summary mode, caching, breadth caps, trims
# ------------------------------------------------------------
class _ReqScopeCache:
    def __init__(self):
        self.file_json: Dict[tuple, Dict[str, Any]] = {}   # (path, ref) → file_get() result
        self.deps_json: Dict[tuple, Dict[str, Any]] = {}   # (file, ref) → deps_get() result

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

    # Normalize + filter deps
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
        "timeout_ms": 110000,
        "mode": "full" | "summary",
        "level_cap": 400,
        "top_n": 200
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

    # Per-request caches
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

    # De-dupe edges
    seen = set()
    dedup_edges = []
    for e in edges:
        key = (e["from"], e["to"], e.get("reason"))
        if key in seen:
            continue
        seen.add(key)
        dedup_edges.append(e)

    # Hard trims for oversized
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

    result = {
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
    # Enforce a response size budget (default ~3MB) and force JSON+charset
    return _json_ok(result, budget_bytes=int(os.getenv("REPORT_BUDGET_BYTES", "3000000")))

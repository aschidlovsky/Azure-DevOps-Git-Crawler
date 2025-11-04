import os, base64, re, json, logging, asyncio
from typing import Dict, Any, List, Set, Optional, Tuple
from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
import httpx
from asyncio import Semaphore

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
    basic = base64.b64encode(f"pat:{pat}".encode()).decode()
    return {
        "Authorization": f"Basic {basic}",
        "X-TFS-FedAuthRedirect": "Suppress",
        "Accept": "application/json",
        "User-Agent": "ado-git-crawler/1.0",
    }

def ado_base(env: Dict[str, str]) -> str:
    return f"https://dev.azure.com/{env['ORG']}/{env['PROJECT']}/_apis/git/repositories/{env['REPO']}"

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

    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(url, headers=ado_headers(env["PAT"]), params=params)

        # Guard against HTML login redirects
        if r.status_code == 302 or "text/html" in (r.headers.get("content-type") or ""):
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
    (r"\b(\w+)\.(insert|update|delete|validateWrite|validateDelete|write)\s*\(", "Table", "crud-call"),
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
    r"\b(\w+)\.(insert|update|delete|validateWrite|validateDelete|write)\s*\(",
    re.IGNORECASE
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

# Additional symbols that are usually metadata/framework (tend to 404 in custom repos)
LIKELY_KERNEL_META_PREFIXES = ("Dict", "ImageListAppl_", "Menu", "MenuFunction", "Label")
LIKELY_KERNEL_META_EXACT = {
    "DictTable", "DictField", "DictEnum", "DictRelation", "DictType", "Label"
}

TYPE_PATHS = {
    "Table": "Tables",
    "Class": "Classes",
    "Form": "Forms",
    "Map": "Maps",
    "Unknown": "Unknown"
}

ALLOWED_FOLDERS = {"Tables", "Classes", "Forms", "Maps"}

# ------------------------------------------------------------
# Invocation extraction (form & DS methods → called targets)
# ------------------------------------------------------------
METHOD_HEADER = re.compile(
    r"\b(BEGINMETHOD\s+)?(?P<name>(init|run|closeOk|close|clicked|modified|active|write|validateWrite|validateDelete))\s*\(",
    re.IGNORECASE
)
DS_METHOD_REGION = re.compile(
    r"\bOBJECT\s+FORM\s+DATASOURCE\b.*?\bMETHODS\b(?P<body>.*?)\bENDMETHODS\b",
    re.IGNORECASE | re.DOTALL
)
CALL_SITE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(", re.MULTILINE)
INTERESTING_TARGET = re.compile(r"\b(mss[A-Za-z0-9_]*Comparer|completeAckConfirm|initAck|acceptPrices)\b")

def _extract_invocation_paths(content: str) -> List[Dict[str, Any]]:
    """
    Heuristic: find entrypoint-like methods, then list notable calls inside.
    Returns: [{"entry": "init()", "calls": ["initAck()", "mssBDAckTableComparer::..."]}, ...]
    """
    invocations: List[Dict[str, Any]] = []

    def scan_block(block_text: str, entry_label_prefix: str = ""):
        for m in METHOD_HEADER.finditer(block_text):
            entry = m.group("name")
            start = m.start()
            next_m = METHOD_HEADER.search(block_text, pos=m.end())
            end = next_m.start() if next_m else len(block_text)
            body = block_text[start:end]

            calls: List[str] = []
            for cm in CALL_SITE.finditer(body):
                callee = cm.group(1)
                if INTERESTING_TARGET.search(callee):
                    calls.append(callee + "()")

            if calls:
                label = f"{entry_label_prefix}{entry}()"
                seen = set()
                ordered = []
                for c in calls:
                    if c not in seen:
                        seen.add(c)
                        ordered.append(c)
                invocations.append({"entry": label, "calls": ordered})

    # 1) Top-level form methods
    scan_block(content, entry_label_prefix="")
    # 2) DataSource method regions
    for dsm in DS_METHOD_REGION.finditer(content):
        ds_body = dsm.group("body") or ""
        scan_block(ds_body, entry_label_prefix="DataSource.")
    return invocations

# ------------------------------------------------------------
# HELPERS: path normalization, filtering, and reclassification retries
# ------------------------------------------------------------
def _looks_like_form_path(file_path: str) -> bool:
    return file_path.startswith("Forms/") or (file_path.lower().endswith(".xpo") and "/forms/" in file_path.lower())

EDT_LIKE_SUFFIXES = ("Id", "Qty", "Price", "Amount", "Code", "Group", "TypeId")
def _looks_like_edt_name(symbol: str) -> bool:
    # coarse heuristic: starts uppercase, short-ish, and ends like an EDT
    if not symbol or len(symbol) < 3:
        return False
    if not symbol[0].isalpha() or not symbol[0].isupper():
        return False
    return symbol.endswith(EDT_LIKE_SUFFIXES)

def _should_skip_dep_path(p: str) -> bool:
    """
    Returns True if this normalized path should be ignored (kernel/system/junk/EDT-likes).
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
    if symbol.startswith(LIKELY_KERNEL_META_PREFIXES) or symbol in LIKELY_KERNEL_META_EXACT:
        return True
    if len(symbol) < 3:
        return True
    # If we ever mis-detected EDTs as Tables, prefer to skip them entirely.
    if folder == "Tables" and _looks_like_edt_name(symbol):
        return True
    return False

def _normalize_dep_path(p: str) -> str:
    """
    Strip 'lcl' or 'Lcl' prefix (including 'lclmss') while preserving 'mss'
    Examples:
      Tables/lclPurchLine.xpo   -> Tables/PurchLine.xpo
      Tables/lclmssBDAck.xpo    -> Tables/mssBDAck.xpo   (keep 'mss')
    """
    return re.sub(
        r"(^|/)(lcl|Lcl)(mss)?([A-Z])",
        lambda m: f"{m.group(1)}{(m.group(3) or '')}{m.group(4)}",
        p or "",
    )

def _dep_tuple(dep: Dict[str, Any]) -> Tuple[str, str]:
    return (dep.get("type") or "", dep.get("symbol") or "")

def _initial_dep_path(kind: str, symbol: str) -> str:
    folder = TYPE_PATHS.get(kind, "Unknown")
    return f"{folder}/{symbol}.xpo"

def _fallback_kinds_on_404(kind: str, symbol: str) -> List[str]:
    """
    If we 404 on the initial guess, try these kinds in order.
    Key fix: PurchLineType/SalesLineType often detected as Tables, but are Classes.
    """
    # Always avoid EDTs (we don't support EDT folders, and they should be filtered)
    if _looks_like_edt_name(symbol):
        return []

    fallbacks: List[str] = []
    if kind == "Table":
        # If it smells like a Type class, try Class first.
        if symbol.endswith("Type") or symbol.startswith("Ax") or symbol in {"PriceDisc", "Dialog"}:
            fallbacks.extend(["Class", "Form", "Map"])
        else:
            fallbacks.extend(["Class", "Form", "Map"])
    elif kind == "Class":
        fallbacks.extend(["Table", "Form", "Map"])
    elif kind == "Form":
        fallbacks.extend(["Class", "Table", "Map"])
    else:
        fallbacks.extend(["Class", "Table", "Form", "Map"])
    return fallbacks

# ------------------------------------------------------------
# CORE EXTRACTOR
# ------------------------------------------------------------
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

            # Skip likely kernel/meta early
            if symbol.startswith(LIKELY_KERNEL_META_PREFIXES) or symbol in LIKELY_KERNEL_META_EXACT:
                continue
            # Skip likely EDTs early if detected as Table by the "statement" rule
            if kind == "Table" and _looks_like_edt_name(symbol):
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
                # Filter out EDT-ish names that occasionally appear in DS blocks
                if _looks_like_edt_name(sym):
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
                for method in ["insert()", "update()", "delete()", "validateWrite()", "write()", "validateDelete()"]:
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

    invocations = _extract_invocation_paths(content)

    return {
        "dependencies": deps,
        "implicit_crud": implicit,
        "business_rules": business,
        "invocation_paths": invocations
    }

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
        "invocation_paths": parsed.get("invocation_paths", []),
        "unresolved": [],
        "visited": [file],
        "skipped": [],
        "page": page,
        "limit": limit,
        "total_dependencies": total,
        "total_pages": total_pages,
    }

# ------------------------------------------------------------
# INTERNAL: safe deps_get with 404 reclassification + filtering
# ------------------------------------------------------------
async def _safe_deps_normalized(file: str, used_ref: str) -> Dict[str, Any]:
    """
    Calls deps_get for a 'file' path. If 404, attempts to reclassify the symbol
    across alternative folders (Table→Class, etc.). Applies normalization + skip filter.
    Returns: {"file": <original>, "deps": [normalized dep dicts], "implicit": [...],
              "business": [...], "invocations": [...], "error": None or str}
    """
    async def _attempt(path: str) -> Optional[Dict[str, Any]]:
        try:
            node = await deps_get(file=path, ref=used_ref, page=1, limit=200)  # type: ignore
            return node
        except HTTPException as he:
            if he.status_code == 404:
                return None
            raise
        except Exception:
            return None

    # First try original
    node = await _attempt(file)
    tried_paths = [file]

    # If we failed, try reclassification based on symbol/kind
    if node is None:
        m = re.match(r"^(Tables|Classes|Forms|Maps)/([^/]+)\.xpo$", file)
        if m:
            orig_folder, symbol = m.group(1), m.group(2)
            # Skip kernel/meta or EDT-ish misses outright
            if symbol.startswith(LIKELY_KERNEL_META_PREFIXES) or symbol in LIKELY_KERNEL_META_EXACT:
                return {"file": file, "deps": [], "implicit": [], "business": [], "invocations": [], "error": None}

            if _looks_like_edt_name(symbol):
                return {"file": file, "deps": [], "implicit": [], "business": [], "invocations": [], "error": None}

            # compute kind
            inv_kind = None
            for k, folder in TYPE_PATHS.items():
                if folder == orig_folder:
                    inv_kind = k
                    break
            kinds_to_try = _fallback_kinds_on_404(inv_kind or "Unknown", symbol)
            for k in kinds_to_try:
                alt = f"{TYPE_PATHS.get(k, 'Unknown')}/{symbol}.xpo"
                if alt in tried_paths:
                    continue
                node = await _attempt(alt)
                tried_paths.append(alt)
                if node is not None:
                    break

    # If still nothing, return empty set (not an error)
    if node is None:
        return {"file": file, "deps": [], "implicit": [], "business": [], "invocations": [], "error": None}

    # Normalize & filter deps for this node
    norm_deps: List[Dict[str, Any]] = []
    for d in node.get("dependencies", []) or []:
        raw = d.get("path") or ""
        clean = _normalize_dep_path(raw)
        if _should_skip_dep_path(clean):
            continue
        nd = dict(d)
        nd["path"] = clean
        norm_deps.append(nd)

    return {
        "file": file,
        "deps": norm_deps,
        "implicit": node.get("implicit_crud", []) or [],
        "business": node.get("business_rules", []) or [],
        "invocations": node.get("invocation_paths", []) or [],
        "error": None
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

    while current_level and depth < max_depth:
        log.info(f"[resolve] Depth {depth}: {len(current_level)} files")
        level_files = [f for f in current_level if f not in visited]
        visited.update(level_files)

        results = await asyncio.gather(*[_safe_deps_normalized(f, used_ref) for f in level_files])

        next_level: List[str] = []
        for res in results:
            file = res["file"]
            err = res.get("error")
            deps = res.get("deps") or []
            implicit_all.extend(res.get("implicit", []))

            if err:
                graph.append({"file": file, "error": err, "depth": depth, "dependencies": []})
                continue

            dep_paths: List[str] = [d["path"] for d in deps]
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
# INTERNAL: analyze a single file for /report
# ------------------------------------------------------------
async def _analyze_single_file(path: str, ref: Optional[str], used_ref: str) -> Dict[str, Any]:
    """
    Fetches content (/file) and direct analysis (/deps) for a single path.
    Returns a compact dict used by /report aggregation.
    Applies reclassification retries and normalization.
    """
    # Try to fetch file first (for sha/content_len)
    try:
        file_res = await file_get(path=path, ref=ref)  # type: ignore
    except Exception as e:
        file_res = {"sha": None, "content": "", "ref": used_ref}
        file_err = f"file_get: {e}"
    else:
        file_err = None

    # Now run deps with retries/normalization
    node = await _safe_deps_normalized(path, used_ref)

    # Normalize and filter dependencies (defense in depth)
    norm_deps: List[Dict[str, Any]] = []
    for d in node.get("deps", []) or []:
        raw = d.get("path") or ""
        clean = _normalize_dep_path(raw)
        if _should_skip_dep_path(clean):
            continue
        nd = dict(d)
        nd["path"] = clean
        norm_deps.append(nd)

    return {
        "path": path,
        "ref": node.get("ref", used_ref),
        "sha": file_res.get("sha"),
        "content_len": len(file_res.get("content", "")),
        "dependencies": norm_deps,
        "business_rules": node.get("business", []),
        "implicit_crud": node.get("implicit", []),
        "invocation_paths": node.get("invocations", []),
        "error": file_err or node.get("error"),
    }

# ------------------------------------------------------------
# ENDPOINT: /report (resolve + analyze every visited file)
# ------------------------------------------------------------
@app.post("/report")
@app.post("/report/")
async def report_post(payload: Dict[str, Any] = Body(...)):
    """
    Full transitive analysis in one call:
      1) resolve -> full visited set (depth <= max_depth)  [with reclass + filtering]
      2) analyze each visited file -> (/file + /deps)      [with reclass + filtering]
      3) merge into a single report payload

    Body:
    {
      "start_file": "Forms/mssBDAckTableFurn.xpo",
      "ref": "main",
      "max_depth": 5,
      "max_concurrency": 8   # optional
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
    max_conc = int(payload.get("max_concurrency", int(os.getenv("MAX_CONCURRENCY", "8"))))

    # --------- 1) internal resolve with reclassification ----------
    visited: Set[str] = set()
    graph: List[Dict[str, Any]] = []
    implicit_all: List[Dict[str, Any]] = []
    current_level: List[str] = [start_file]
    depth = 0

    while current_level and depth < max_depth:
        level_files = [f for f in current_level if f not in visited]
        visited.update(level_files)

        results = await asyncio.gather(*[_safe_deps_normalized(f, used_ref) for f in level_files])

        next_level: List[str] = []
        for res in results:
            file = res["file"]
            err = res.get("error")
            deps = res.get("deps") or []
            implicit_all.extend(res.get("implicit", []))
            if err:
                graph.append({"file": file, "depth": depth, "dependencies": [], "error": err})
                continue

            dep_paths: List[str] = [d["path"] for d in deps]
            graph.append({"file": file, "depth": depth, "dependencies": dep_paths, "error": None})
            next_level.extend(dep_paths)

        depth += 1
        current_level = [f for f in set(next_level) if f not in visited]

    visited_list = sorted(list(visited))

    # --------- 2) analyze each visited file with bounded concurrency ----------
    sem = Semaphore(max_conc)
    async def _guarded_analyze(p: str):
        async with sem:
            return await _analyze_single_file(p, used_ref, used_ref)

    analyses = await asyncio.gather(*[_guarded_analyze(p) for p in visited_list])

    # --------- 3) merge into objects/edges/rules ----------
    objects: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []
    business_rules_all: List[Dict[str, Any]] = []
    implicit_crud_all: List[Dict[str, Any]] = list(implicit_all)
    invocation_all: List[Dict[str, Any]] = []

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
        invocation_all.extend(a.get("invocation_paths", []) or [])

    # dedupe edges
    seen = set()
    dedup_edges = []
    for e in edges:
        key = (e["from"], e["to"], e.get("reason"))
        if key in seen:
            continue
        seen.add(key)
        dedup_edges.append(e)

    return {
        "root": start_file,
        "ref": used_ref,
        "max_depth": max_depth,
        "depth_completed": depth,
        "visited": visited_list,
        "graph": graph,
        "objects": objects,
        "edges": dedup_edges,
        "business_rules": business_rules_all,
        "implicit_crud": implicit_crud_all,
        "invocation_paths": invocation_all,
        "status": "complete"
    }

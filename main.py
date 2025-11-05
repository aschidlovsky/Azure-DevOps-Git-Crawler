import os, base64, re, json, logging, asyncio, time, math
from typing import Dict, Any, List, Set, Optional, Tuple
from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
import httpx
from asyncio import Semaphore

# =========================================
# APP INIT
# =========================================
app = FastAPI(title="ADO Repo Dependency Connector", version="1.3.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

log = logging.getLogger("uvicorn.error")

# =========================================
# ENV + AUTH
# =========================================
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
    basic = base64.b64encode(f"pat:{pat}".encode()).decode()
    return {
        "Authorization": f"Basic {basic}",
        "X-TFS-FedAuthRedirect": "Suppress",
        "Accept": "application/json",
        "User-Agent": "ado-git-crawler/1.3",
    }

def ado_base(env: Dict[str, str]) -> str:
    return f"https://dev.azure.com/{env['ORG']}/{env['PROJECT']}/_apis/git/repositories/{env['REPO']}"

# =========================================
# CONSTANTS / FILTERS
# =========================================
ALLOWED_FOLDERS = {"Tables", "Classes", "Forms", "Maps"}
CLASSIFY_FOLDERS = ["Tables", "Classes", "Forms", "Maps"]  # order matters for retries
EDT_FOLDER = "EDTs"  # hard filter

# Kernel/system & false positives
KERNEL_SKIP = {
    "FormRun", "Args", "Set", "SetEnumerator", "Global", "QueryBuildDataSource", "RunBase",
    "Map", "List", "Array", "Tmp", "TmpTable", "Info", "Error", "Warning",
    "Query", "QueryRun", "SysQuery", "SysTable", "SysDictTable", "SysDictField",
    "TableId", "DataAreaId", "Company",
    # common tokens
    "RecId", "t", "x", "y", "z", "this", "super",
    # x++ keywords
    "sum", "avg", "min", "max", "count", "len", "is", "the", "for", "select", "firstOnly",
    "exists", "update_recordset", "delete_from", "insert_recordset"
}

TYPE_PATHS = {
    "Table": "Tables",
    "Class": "Classes",
    "Form": "Forms",
    "Map": "Maps",
    "Unknown": "Unknown",
}

DEPENDENCY_PATTERNS = [
    (r"\b(select|insert|update|delete)\s+(\w+)", "Table", "statement"),
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
    r"\b(\w+)\.(insert|update|delete|validateWrite|validateDelete)\s*\(",
    re.IGNORECASE,
)

# =========================================
# HELPERS
# =========================================
def _normalize_dep_path(p: str) -> str:
    # Strip lcl/Lcl while preserving "mss"
    return re.sub(
        r"(^|/)(lcl|Lcl)(mss)?([A-Z])",
        lambda m: f"{m.group(1)}{(m.group(3) or '')}{m.group(4)}",
        p or "",
    )

def _should_skip_dep_path(p: str) -> bool:
    if not p or ".xpo" not in p:
        return True
    if p.startswith(f"{EDT_FOLDER}/"):
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

def _looks_like_form_path(file_path: str) -> bool:
    return file_path.startswith("Forms/") or (file_path.lower().endswith(".xpo") and "/forms/" in file_path.lower())

def _classify_on_name_heuristics(symbol: str, suggested_kind: str) -> List[str]:
    """
    Given a symbol & suggested kind (Table, Class, Form, Map),
    produce an ordered list of folder guesses to try.
    Heuristics: *Type, Ax* are usually Classes.
    """
    tries = []
    if suggested_kind == "Table":
        # If looks like a type class, prefer Classes first
        if symbol.endswith("Type") or symbol.startswith("Ax"):
            tries = ["Classes", "Tables", "Forms", "Maps"]
        else:
            tries = ["Tables", "Classes", "Forms", "Maps"]
    elif suggested_kind == "Class":
        tries = ["Classes", "Tables", "Forms", "Maps"]
    elif suggested_kind == "Form":
        tries = ["Forms", "Classes", "Tables", "Maps"]
    else:
        tries = CLASSIFY_FOLDERS
    return tries

async def _ado_item_exists(path: str, ref: Optional[str]) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """Ping ADO for the item and tell if it exists (without raising)."""
    try:
        data = await get_item_content(path, ref)
        return True, data
    except HTTPException as e:
        if e.status_code == 404:
            return False, None
        # other errors should bubble so we don't hide auth etc.
        raise
    except Exception:
        raise

# =========================================
# ADO FETCH
# =========================================
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

        if r.status_code == 302 or "text/html" in r.headers.get("content-type", ""):
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

# =========================================
# LIGHT PARSER
# =========================================
def extract_dependencies(content: str, file_path: Optional[str] = None) -> Dict[str, Any]:
    deps: List[Dict[str, Any]] = []
    implicit: List[Dict[str, Any]] = []
    business: List[Dict[str, Any]] = []
    dep_keys: Set[str] = set()

    # direct deps
    for pattern, kind, reason in DEPENDENCY_PATTERNS:
        for m in re.finditer(pattern, content, re.IGNORECASE):
            symbol = m.group(2) if reason == "statement" else m.group(1)
            if not symbol:
                continue
            if symbol in KERNEL_SKIP or len(symbol) < 3:
                continue
            folder = TYPE_PATHS.get(kind, "Unknown")
            path = f"{folder}/{symbol}.xpo"
            if path not in dep_keys:
                deps.append({"path": path, "type": kind, "symbol": symbol, "reason": reason})
                dep_keys.add(path)

    # explicit CRUD calls
    for m in CRUD_SIGNATURE.finditer(content):
        table_sym = m.group(1)
        if table_sym and table_sym not in KERNEL_SKIP and len(table_sym) >= 3:
            implicit.append({
                "table": table_sym,
                "method": m.group(2) + "()",
                "caller": "unknown",
                "line": m.start()
            })

    # Form DS → implicit DS tables + implicit CRUD
    is_form = _looks_like_form_path(file_path or "")
    ds_tables: Set[str] = set()
    if is_form:
        for p in FORM_DS_PATTERNS:
            for mm in re.finditer(p, content, re.IGNORECASE):
                sym = mm.group(1)
                if sym and sym not in KERNEL_SKIP and len(sym) >= 3:
                    ds_tables.add(sym)

        if ds_tables:
            for sym in ds_tables:
                tpath = f"Tables/{sym}.xpo"
                if tpath not in dep_keys:
                    deps.append({"path": tpath, "type": "Table", "symbol": sym, "reason": "form-datasource"})
                    dep_keys.add(tpath)
            for sym in ds_tables:
                for method in ["insert()", "update()", "delete()", "validateWrite()", "validateDelete()"]:
                    implicit.append({
                        "table": sym, "method": method, "caller": "FormSaveKernel",
                        "line": -1, "reason": "implicit-form-datasource-crud"
                    })

    # Business-rule signals
    for ln, line in enumerate(content.splitlines(), start=1):
        if any(k in line for k in ["validate", "error(", "ttsBegin", "ttsCommit"]):
            business.append({"line": ln, "context": line.strip()[:200]})

    log.info(f"[extract] file={file_path or '<mem>'} deps={len(deps)} ds={len(ds_tables)} crud={len(implicit)} rules={len(business)}")
    return {"dependencies": deps, "implicit_crud": implicit, "business_rules": business}

# =========================================
# CLASSIFICATION + RETRY
# =========================================
async def _fetch_with_reclassify(raw_path: str, ref: Optional[str]) -> Dict[str, Any]:
    """
    Try the given path; on 404 retry alternative folder classifications.
    Also detect EDTs and skip them (no exception).
    """
    # First attempt as-is
    try:
        return await get_item_content(raw_path, ref)
    except HTTPException as e:
        if e.status_code != 404:
            raise
        # try reclassify only for ALLOWED_FOLDERS
        m = re.match(r"^(?P<folder>Tables|Classes|Forms|Maps)/(?P<name>[^/]+)\.xpo$", raw_path)
        if not m:
            raise
        name = m.group("name")
        folder = m.group("folder")
        # EDT check (skip if exists there)
        edt_path = f"{EDT_FOLDER}/{name}.xpo"
        exists_edt, _ = await _ado_item_exists(edt_path, ref)
        if exists_edt:
            # skip EDTs silently
            raise HTTPException(404, f"EDT filtered: {edt_path}")
        # retry folders per heuristics
        tries = _classify_on_name_heuristics(name, "Table" if folder == "Tables" else folder[:-1].title())
        for f in tries:
            if f == folder:
                continue
            alt = f"{f}/{name}.xpo"
            ok, data = await _ado_item_exists(alt, ref)
            if ok:
                return data
        # if nothing works, bubble 404
        raise

# =========================================
# HEALTH
# =========================================
@app.get("/health")
@app.get("/health/")
async def health():
    return {"status": "ok"}

# =========================================
# /file
# =========================================
@app.get("/file")
@app.get("/file/")
async def file_get(
    path: str = Query(..., description="Repo path (e.g., Forms/CustTable.xpo)"),
    ref: Optional[str] = Query(None),
):
    # use reclassify fetch for consistency
    data = await _fetch_with_reclassify(path, ref)
    return {"path": path, "ref": data["ref"], "sha": data["sha"], "content": data["content"]}

# =========================================
# /deps (single hop)
# =========================================
@app.get("/deps")
@app.get("/deps/")
async def deps_get(
    file: str = Query(..., description="Repo path to .xpo file"),
    ref: Optional[str] = Query(None),
    page: int = 1,
    limit: int = 200,
):
    fetched = await _fetch_with_reclassify(file, ref)
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

# =========================================
# /resolve (BFS graph only)
# =========================================
async def _deps_paths_only(file: str, ref: str) -> Tuple[List[str], List[Dict[str, Any]]]:
    node = await deps_get(file=file, ref=ref, page=1, limit=200)  # type: ignore
    dep_paths: List[str] = []
    for d in node.get("dependencies", []):
        raw = d.get("path") or ""
        clean = _normalize_dep_path(raw)
        if _should_skip_dep_path(clean):
            continue
        dep_paths.append(clean)
    # carry implicit for aggregation
    return dep_paths, node.get("implicit_crud", [])

@app.post("/resolve")
@app.post("/resolve/")
async def resolve_post(payload: Dict[str, Any] = Body(...)):
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

    async def safe(file: str):
        try:
            deps, impl = await _deps_paths_only(file, used_ref)
            implicit_all.extend(impl)
            return {"file": file, "deps": deps, "error": None}
        except Exception as e:
            log.error(f"[resolve] Failed deps_get for {file}: {e}")
            return {"file": file, "deps": None, "error": str(e)}

    while current_level and depth < max_depth:
        level_files = [f for f in current_level if f not in visited]
        visited.update(level_files)
        results = await asyncio.gather(*[safe(f) for f in level_files])
        next_level: List[str] = []
        for res in results:
            if res["error"]:
                graph.append({"file": res["file"], "error": res["error"], "depth": depth, "dependencies": []})
                continue
            deps = res["deps"] or []
            graph.append({"file": res["file"], "depth": depth, "dependencies": deps, "error": None})
            next_level.extend(deps)
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

# =========================================
# /report (full transitive, single-call)
# =========================================
async def _analyze_single_file(path: str, ref: Optional[str]) -> Dict[str, Any]:
    # fetch file
    try:
        file_res = await file_get(path=path, ref=ref)  # type: ignore
    except Exception as e:
        return {"path": path, "ref": ref, "error": f"file_get: {e}"}

    # analyze deps
    try:
        deps_res = await deps_get(file=path, ref=ref, page=1, limit=200)  # type: ignore
    except Exception as e:
        return {
            "path": path, "ref": ref, "sha": file_res.get("sha"),
            "content_len": len(file_res.get("content", "")),
            "dependencies": [], "business_rules": [], "implicit_crud": [],
            "error": f"deps_get: {e}"
        }

    # normalize/filter
    norm_deps: List[Dict[str, Any]] = []
    for d in deps_res.get("dependencies", []):
        raw = d.get("path") or ""
        clean = _normalize_dep_path(raw)
        if _should_skip_dep_path(clean):
            continue
        nd = dict(d)
        nd["path"] = clean
        norm_deps.append(nd)

    return {
        "path": path,
        "ref": deps_res.get("ref", ref),
        "sha": deps_res.get("sha") or file_res.get("sha"),
        "content_len": len(file_res.get("content", "")),
        "dependencies": norm_deps,
        "business_rules": deps_res.get("business_rules", []),
        "implicit_crud": deps_res.get("implicit_crud", []),
        "error": None,
    }

@app.post("/report")
@app.post("/report/")
async def report_post(payload: Dict[str, Any] = Body(...)):
    env = get_env()
    start_file: str = payload.get("start_file")
    if not start_file:
        raise HTTPException(400, "Missing required field: start_file")
    used_ref = payload.get("ref") or env["REF"]
    if str(used_ref).upper() == "HEAD":
        used_ref = env["REF"]
    max_depth = int(payload.get("max_depth", 5))
    max_conc = int(payload.get("max_concurrency", int(os.getenv("MAX_CONCURRENCY", "8"))))

    # internal resolve
    visited: Set[str] = set()
    graph: List[Dict[str, Any]] = []
    implicit_all: List[Dict[str, Any]] = []
    current_level: List[str] = [start_file]
    depth = 0

    async def safe(file: str):
        try:
            node = await deps_get(file=file, ref=used_ref, page=1, limit=200)  # type: ignore
            implicit_all.extend(node.get("implicit_crud", []))
            dep_paths: List[str] = []
            for d in node.get("dependencies", []):
                clean = _normalize_dep_path(d.get("path") or "")
                if _should_skip_dep_path(clean):
                    continue
                dep_paths.append(clean)
            return {"file": file, "deps": dep_paths, "error": None}
        except Exception as e:
            log.error(f"[report] Failed deps_get for {file}: {e}")
            return {"file": file, "deps": None, "error": str(e)}

    while current_level and depth < max_depth:
        level_files = [f for f in current_level if f not in visited]
        visited.update(level_files)
        results = await asyncio.gather(*[safe(f) for f in level_files])

        next_level: List[str] = []
        for res in results:
            if res["error"]:
                graph.append({"file": res["file"], "depth": depth, "dependencies": [], "error": res["error"]})
                continue
            deps = res["deps"] or []
            graph.append({"file": res["file"], "depth": depth, "dependencies": deps, "error": None})
            next_level.extend(deps)

        depth += 1
        current_level = [f for f in set(next_level) if f not in visited]

    visited_list = sorted(list(visited))

    # analyze each visited with bounded concurrency
    sem = Semaphore(max_conc)
    async def _guarded(p: str):
        async with sem:
            return await _analyze_single_file(p, used_ref)

    analyses = await asyncio.gather(*[_guarded(p) for p in visited_list])

    # merge
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
        "status": "complete"
    }

# =========================================
# /report.paged (cursor-based pagination to avoid timeouts)
# =========================================
def _b64(data: Dict[str, Any]) -> str:
    return base64.b64encode(json.dumps(data, separators=(",", ":")).encode("utf-8")).decode("utf-8")

# --- patched robust decoder ---
def _unb64(tok: str) -> Dict[str, Any]:
    """
    Decode opaque cursor tokens:
    - Strips known prefixes (e.g., 'opaque:', 'Bearer ')
    - Accepts URL-safe base64
    - Auto-pads to multiple of 4
    - Raises 400 with a clear message on failure
    """
    if not tok or not isinstance(tok, str):
        raise HTTPException(400, "Invalid cursor: empty or non-string.")

    t = tok.strip()
    for pref in ("opaque:", "Opaque:", "bearer ", "Bearer "):
        if t.startswith(pref):
            t = t[len(pref):]
            break

    t = t.replace("-", "+").replace("_", "/")
    pad = (-len(t)) % 4
    if pad:
        t += "=" * pad

    try:
        raw = base64.b64decode(t.encode("utf-8"))
        return json.loads(raw.decode("utf-8"))
    except Exception as e:
        raise HTTPException(400, f"Invalid cursor: {e!s}")

def _approx_size(**parts) -> int:
    """Quick and safe size estimator for response truncation."""
    try:
        return len(json.dumps(parts, ensure_ascii=False).encode("utf-8"))
    except Exception:
        return 0

def _apply_size_budget(max_bytes: int, resp: Dict[str, Any], fields_order: List[str]) -> Tuple[Dict[str, Any], bool]:
    """
    Trim list fields to fit under max_bytes budget.
    Returns (resp, truncated_flag).
    """
    truncated = False
    cur_bytes = _approx_size(**resp)
    if cur_bytes <= max_bytes:
        return resp, truncated
    for f in fields_order:
        if f not in resp:
            continue
        v = resp[f]
        if isinstance(v, list) and v:
            # drop from the tail in chunks
            drop = max(1, math.ceil(len(v) * 0.15))
            resp[f] = v[:-drop]
            truncated = True
            cur_bytes = _approx_size(**resp)
            if cur_bytes <= max_bytes:
                break
    return resp, truncated

async def _paged_bfs_step(
    used_ref: str,
    frontier: List[str],
    visited: Set[str],
    depth: int,
    max_depth: int,
    page_limit_nodes: int,
    max_concurrency: int,
    wall_time_s: float,
) -> Tuple[List[str], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], int]:
    """
    Process a slice of BFS respecting node/page/time limits.
    Returns:
      visited_delta, graph_delta, objects_delta, edges_delta, rules_delta, implicit_delta, depth_completed
    """
    start_t = time.monotonic()
    processed = 0
    graph_delta: List[Dict[str, Any]] = []
    objects_delta: List[Dict[str, Any]] = []
    edges_delta: List[Dict[str, Any]] = []
    rules_delta: List[Dict[str, Any]] = []
    implicit_delta: List[Dict[str, Any]] = []
    visited_delta: List[str] = []

    depth_index: Dict[str, int] = {}

    async def safe(file: str):
        try:
            node = await deps_get(file=file, ref=used_ref, page=1, limit=200)  # type: ignore
            implicit_delta.extend(node.get("implicit_crud", []))
            dep_paths: List[str] = []
            for d in node.get("dependencies", []):
                clean = _normalize_dep_path(d.get("path") or "")
                if _should_skip_dep_path(clean):
                    continue
                dep_paths.append(clean)
            return {"file": file, "deps": dep_paths, "error": None}
        except Exception as e:
            return {"file": file, "deps": [], "error": str(e)}

    sem = Semaphore(max_concurrency)

    async def _guarded(f: str):
        async with sem:
            return await safe(f)

    depth_completed = depth
    next_frontier: List[str] = []

    cur_level = [f for f in frontier if f not in visited]
    while cur_level and depth < max_depth:
        # time guard
        if (time.monotonic() - start_t) >= wall_time_s:
            break

        # process this level
        visited.update(cur_level)
        visited_delta.extend(cur_level)

        results = await asyncio.gather(*[_guarded(f) for f in cur_level])

        for res in results:
            if res["error"]:
                graph_delta.append({"file": res["file"], "depth": depth, "dependencies": [], "error": res["error"]})
                depth_index[res["file"]] = depth
                continue
            deps = res["deps"] or []
            graph_delta.append({"file": res["file"], "depth": depth, "dependencies": deps, "error": None})
            depth_index[res["file"]] = depth
            next_frontier.extend(deps)

        # analyze each visited node this round (bounded)
        async def _analyze_guarded(p: str):
            async with sem:
                return await _analyze_single_file(p, used_ref)

        analyses = await asyncio.gather(*[_analyze_guarded(p) for p in cur_level])

        for a in analyses:
            objects_delta.append({
                "path": a.get("path"),
                "ref": a.get("ref", used_ref),
                "sha": a.get("sha", "unknown"),
                "depth": depth_index.get(a.get("path"), -1),
                "error": a.get("error")
            })
            for d in a.get("dependencies", []) or []:
                edges_delta.append({
                    "from": a.get("path"),
                    "to": d.get("path"),
                    "reason": d.get("reason"),
                    "type": d.get("type"),
                    "depth_from_root": depth_index.get(a.get("path"), -1) + 1
                })
            rules_delta.extend(a.get("business_rules", []) or [])

        processed += len(cur_level)
        if processed >= page_limit_nodes:
            depth_completed = depth
            # processed current level up to limit; resume with next_frontier
            break

        # move to next depth
        depth += 1
        cur_level = [f for f in set(next_frontier) if f not in visited]
        next_frontier = []

        # time guard again
        if (time.monotonic() - start_t) >= wall_time_s:
            break

    depth_completed = max(depth_completed, depth)
    return visited_delta, graph_delta, objects_delta, edges_delta, rules_delta, implicit_delta, depth_completed

@app.post("/report.paged")
async def report_paged(payload: Dict[str, Any] = Body(...)):
    """
    Cursor-based, paged report to avoid timeouts.
    Request (first page):
      {
        "start_file": "Forms/mssBDAckTableFurn.xpo",
        "ref": "main",
        "max_depth": 5,
        "page_limit_nodes": 200,
        "max_concurrency": 8,
        "max_bytes": 1200000,
        "max_wall_time": 25
      }
    Or continue with:
      { "cursor": "<opaque string>" }
    """
    env = get_env()
    max_bytes = int(payload.get("max_bytes", int(os.getenv("MAX_BYTES", "1200000"))))
    max_concurrency = int(payload.get("max_concurrency", int(os.getenv("MAX_CONCURRENCY", "8"))))
    page_limit_nodes = int(payload.get("page_limit_nodes", 200))
    wall_time = float(payload.get("max_wall_time", 25.0))

    cursor_val = payload.get("cursor")

    if cursor_val:
        state = _unb64(cursor_val)
        # sanity-check required keys
        for k in ("root", "ref", "max_depth", "depth", "frontier", "visited"):
            if k not in state:
                raise HTTPException(400, f"Invalid cursor: missing '{k}'.")
        root = state["root"]
        used_ref = state["ref"]
        max_depth = int(state["max_depth"])
        depth = int(state["depth"])
        frontier = list(state["frontier"] or [])
        visited_list = list(state["visited"] or [])
        visited: Set[str] = set(visited_list)
    else:
        root = payload.get("start_file")
        if not root:
            raise HTTPException(400, "Missing required field: start_file (or provide a valid cursor).")
        used_ref = payload.get("ref") or env["REF"]
        if str(used_ref).upper() == "HEAD":
            used_ref = env["REF"]
        max_depth = int(payload.get("max_depth", 5))
        depth = 0
        frontier = [root]
        visited = set()

    # run one page
    visited_delta, graph_delta, objects_delta, edges_delta, rules_delta, implicit_delta, depth_completed = await _paged_bfs_step(
        used_ref=used_ref,
        frontier=frontier,
        visited=visited,
        depth=depth,
        max_depth=max_depth,
        page_limit_nodes=page_limit_nodes,
        max_concurrency=max_concurrency,
        wall_time_s=wall_time,
    )

    # determine next frontier from nodes at depth_completed
    next_frontier: List[str] = []
    for g in graph_delta:
        if g.get("depth") == depth_completed and not g.get("error"):
            next_frontier.extend(g.get("dependencies", []))
    next_frontier = [f for f in set(next_frontier) if f not in visited]

    # Prepare base response
    resp: Dict[str, Any] = {
        "root": root,
        "ref": used_ref,
        "depth_completed": depth_completed,
        "objects": objects_delta,
        "edges": edges_delta,
        "business_rules": rules_delta,
        "implicit_crud": implicit_delta,
        "graph": graph_delta,
        "visited_delta": visited_delta,
        "approx_bytes": 0,            # filled after budget
        "truncated": False,
        "stats": {
            "page_nodes": len(visited_delta),
            "total_seen": len(visited),
            "page_limit_nodes": page_limit_nodes
        },
        "next_cursor": None
    }

    # attach cursor if more work remains
    if next_frontier and depth_completed < max_depth:
        cursor_state = {
            "root": root,
            "ref": used_ref,
            "max_depth": max_depth,
            "depth": depth_completed + 1,
            "frontier": next_frontier,
            "visited": sorted(list(visited)),
        }
        resp["next_cursor"] = _b64(cursor_state)

    # size budget
    order = ["edges", "objects", "business_rules", "implicit_crud", "graph"]
    sized, truncated = _apply_size_budget(max_bytes, resp, order)
    sized["truncated"] = truncated
    sized["approx_bytes"] = _approx_size(**sized)
    return sized

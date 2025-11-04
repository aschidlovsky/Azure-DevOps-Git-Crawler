import os, base64, re, json, logging, asyncio
from typing import Dict, Any, List, Set, Optional
from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
import httpx

# ------------------------------------------------------------
# APP INITIALIZATION
# ------------------------------------------------------------
app = FastAPI(title="ADO Repo Dependency Connector", version="1.1.0")

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
            for m in re.finditer(p, content, re.IGNORECASE):
                sym = m.group(1)
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
# ENDPOINT: /report (resolve + analyze every visited file)
# ------------------------------------------------------------
from asyncio import Semaphore

async def _analyze_single_file(path: str, ref: Optional[str]) -> Dict[str, Any]:
    """
    Fetches content (/file) and direct analysis (/deps) for a single path.
    Returns a compact dict used by /report aggregation.
    """
    try:
        file_res = await file_get(path=path, ref=ref)  # type: ignore
    except Exception as e:
        return {"path": path, "ref": ref, "error": f"file_get: {e}"}

    try:
        deps_res = await deps_get(file=path, ref=ref, page=1, limit=200)  # type: ignore
    except Exception as e:
        return {
            "path": path, "ref": ref, "sha": file_res.get("sha"),
            "content_len": len(file_res.get("content", "")),
            "dependencies": [], "business_rules": [], "implicit_crud": [],
            "error": f"deps_get: {e}"
        }

    # Normalize and filter dependencies (defense in depth)
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
    """
    Full transitive analysis in one call:
      1) resolve -> full visited set (depth <= max_depth)
      2) analyze each visited file -> (/file + /deps)
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

    # --------- 1) internal resolve (reuse resolve_post logic without HTTP hop) ----------
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
            log.error(f"[report] Failed deps_get for {file}: {e}")
            return {"file": file, "deps": None, "error": str(e)}

    while current_level and depth < max_depth:
        level_files = [f for f in current_level if f not in visited]
        visited.update(level_files)
        results = await asyncio.gather(*[safe_deps(f) for f in level_files])

        next_level: List[str] = []
        for res in results:
            file = res["file"]
            err = res.get("error")
            deps = res.get("deps") or []
            if err:
                graph.append({"file": file, "depth": depth, "dependencies": [], "error": err})
                continue

            dep_paths: List[str] = []
            for d in deps:
                clean = _normalize_dep_path(d.get("path") or "")
                if _should_skip_dep_path(clean):
                    continue
                dep_paths.append(clean)

            graph.append({"file": file, "depth": depth, "dependencies": dep_paths, "error": None})
            next_level.extend(dep_paths)

        depth += 1
        current_level = [f for f in set(next_level) if f not in visited]

    visited_list = sorted(list(visited))

    # --------- 2) analyze each visited file with bounded concurrency ----------
    sem = Semaphore(max_conc)
    async def _guarded_analyze(p: str):
        async with sem:
            return await _analyze_single_file(p, used_ref)

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

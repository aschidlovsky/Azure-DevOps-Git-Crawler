import asyncio
import base64
import json
import logging
import math
import os
import re
import time
from asyncio import Semaphore
from collections.abc import Mapping
from typing import Any, Dict, List, Optional, Set, Tuple

import httpx
from fastapi import Body, FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse

# =========================================
# APP INIT
# =========================================

app = FastAPI(title="ADO Repo Dependency Connector", version="1.3.2")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

log = logging.getLogger("uvicorn.error")


# Convert FastAPI 422 validation errors into 400s with clear hints
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    return JSONResponse(
        status_code=400,
        content={
            "error": "Bad Request",
            "detail": "Invalid or missing request payload.",
            "hints": [
                "Send JSON with Content-Type: application/json.",
                "For /report.paged: either provide start_file (+ optional ref, max_depth, etc.) or provide cursor.",
                "GET query params are also supported for /report.paged.",
            ],
            "pydantic": exc.errors(),
        },
    )


# =========================================
# ENV + AUTH
# =========================================

def _req(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise HTTPException(500, f"Missing required environment variable: {name}")
    return value


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
    "FormRun",
    "Args",
    "Set",
    "SetEnumerator",
    "Global",
    "QueryBuildDataSource",
    "RunBase",
    "Map",
    "List",
    "Array",
    "Tmp",
    "TmpTable",
    "Info",
    "Error",
    "Warning",
    "Query",
    "QueryRun",
    "SysQuery",
    "SysTable",
    "SysDictTable",
    "SysDictField",
    "TableId",
    "DataAreaId",
    "Company",
    # common tokens
    "RecId",
    "t",
    "x",
    "y",
    "z",
    "this",
    "super",
    # x++ keywords
    "sum",
    "avg",
    "min",
    "max",
    "count",
    "len",
    "is",
    "the",
    "for",
    "select",
    "firstOnly",
    "exists",
    "update_recordset",
    "delete_from",
    "insert_recordset",
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
    (r"\b(\w+).DataSource\(", "Table", "DataSource()"),
    (r"\b(\w+)::(construct|new|run)\b", "Class", "static-call"),
    (r"\bnew\s+(\w+)\s*\(", "Class", "new"),
    (r"\b(\w+)\.(insert|update|delete|validateWrite|validateDelete)\s*\(", "Table", "crud-call"),
]

FORM_DS_PATTERNS = [
    r"\bOBJECT\s+FORM\s+DATASOURCE\s+(\w+)",
    r"\bDATA\s+SOURCE\s+NAME\s*:\s*(\w+)",
    r"\bFormDataSource\s+(\w+)",
    r"\bdataSource\s*(\s*(\w+)\s*)",
    r"\bDataSource\s*(\s*(\w+)\s*)",
    r"\bDATA\s+SOURCE\s+(\w+)",
]

CRUD_SIGNATURE = re.compile(
    r"\b(\w+)\.(insert|update|delete|validateWrite|validateDelete)\s*\(",
    re.IGNORECASE,
)


# =========================================
# HELPERS
# =========================================

def _normalize_dep_path(path: str) -> str:
    # Strip lcl/Lcl while preserving "mss"
    return re.sub(
        r"(^|/)(lcl|Lcl)(mss)?([A-Z])",
        lambda match: f"{match.group(1)}{(match.group(3) or '')}{match.group(4)}",
        path or "",
    )


def _should_skip_dep_path(path: str) -> bool:
    if not path or ".xpo" not in path:
        return True
    if path.startswith(f"{EDT_FOLDER}/"):
        return True
    match = re.match(r"^(Tables|Classes|Forms|Maps)/([^/]+)\.xpo$", path)
    if not match:
        return True
    folder, symbol = match.group(1), match.group(2)
    if folder not in ALLOWED_FOLDERS:
        return True
    if symbol in KERNEL_SKIP:
        return True
    if symbol.lower() in {
        "sum",
        "avg",
        "min",
        "max",
        "count",
        "len",
        "is",
        "the",
        "for",
        "select",
        "firstonly",
        "exists",
        "update_recordset",
        "delete_from",
        "insert_recordset",
        "this",
        "super",
    }:
        return True
    if len(symbol) < 3:
        return True
    return False


def _looks_like_form_path(file_path: str) -> bool:
    lowered = file_path.lower()
    return lowered.startswith("forms/") or (lowered.endswith(".xpo") and "/forms/" in lowered)


def _classify_on_name_heuristics(symbol: str, suggested_kind: str) -> List[str]:
    """
    Given a symbol & suggested kind (Table, Class, Form, Map),
    produce an ordered list of folder guesses to try.
    Heuristics: *Type, Ax* are usually Classes.
    """
    tries: List[str] = []
    if suggested_kind == "Table":
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
    except HTTPException as exc:
        if exc.status_code == 404:
            return False, None
        raise
    except Exception:
        raise


# =========================================
# ADO FETCH
# =========================================

async def get_item_content(path: str, ref: Optional[str]) -> Dict[str, Any]:
    env = get_env()
    base = ado_base(env)

    used_ref: Optional[str] = ref or env["REF"]
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
        response = await client.get(url, headers=ado_headers(env["PAT"]), params=params)

        if response.status_code == 302 or "text/html" in response.headers.get("content-type", ""):
            raise HTTPException(
                401, "Azure DevOps authentication failed – PAT invalid, expired, or unauthorized"
            )

        if response.status_code == 404:
            raise HTTPException(404, f"File not found in ADO: {path}@{used_ref}")

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(exc.response.status_code, f"ADO error: {exc.response.text[:300]}") from exc

        try:
            data = response.json()
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                502, "Azure DevOps returned non-JSON content (likely auth redirect or proxy page)."
            ) from exc

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
        for match in re.finditer(pattern, content, re.IGNORECASE):
            symbol = match.group(2) if reason == "statement" else match.group(1)
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
    for match in CRUD_SIGNATURE.finditer(content):
        table_sym = match.group(1)
        if table_sym and table_sym not in KERNEL_SKIP and len(table_sym) >= 3:
            implicit.append(
                {
                    "table": table_sym,
                    "method": match.group(2) + "()",
                    "caller": "unknown",
                    "line": match.start(),
                }
            )

    # Form DS → implicit DS tables + implicit CRUD
    is_form = _looks_like_form_path(file_path or "")
    ds_tables: Set[str] = set()
    if is_form:
        for pattern in FORM_DS_PATTERNS:
            for form_match in re.finditer(pattern, content, re.IGNORECASE):
                symbol = form_match.group(1)
                if symbol and symbol not in KERNEL_SKIP and len(symbol) >= 3:
                    ds_tables.add(symbol)

        if ds_tables:
            for symbol in ds_tables:
                table_path = f"Tables/{symbol}.xpo"
                if table_path not in dep_keys:
                    deps.append({"path": table_path, "type": "Table", "symbol": symbol, "reason": "form-datasource"})
                    dep_keys.add(table_path)
            for symbol in ds_tables:
                for method in ["insert()", "update()", "delete()", "validateWrite()", "validateDelete()"]:
                    implicit.append(
                        {
                            "table": symbol,
                            "method": method,
                            "caller": "FormSaveKernel",
                            "line": -1,
                            "reason": "implicit-form-datasource-crud",
                        }
                    )

    # Business-rule signals
    for line_no, line in enumerate(content.splitlines(), start=1):
        if any(token in line for token in ["validate", "error(", "ttsBegin", "ttsCommit"]):
            business.append({"line": line_no, "context": line.strip()[:200]})

    log.info(
        "[extract] file=%s deps=%d ds=%d crud=%d rules=%d",
        file_path or "<mem>",
        len(deps),
        len(ds_tables),
        len(implicit),
        len(business),
    )
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
    except HTTPException as exc:
        if exc.status_code != 404:
            raise

    # try reclassify only for ALLOWED_FOLDERS
    match = re.match(r"^(?P<folder>Tables|Classes|Forms|Maps)/(?P<name>[^/]+)\.xpo$", raw_path)
    if not match:
        raise

    name = match.group("name")
    folder = match.group("folder")

    # EDT check (skip if exists there)
    edt_path = f"{EDT_FOLDER}/{name}.xpo"
    exists_edt, _ = await _ado_item_exists(edt_path, ref)
    if exists_edt:
        # skip EDTs silently
        raise HTTPException(404, f"EDT filtered: {edt_path}")

    # retry folders per heuristics
    base_kind = "Table" if folder == "Tables" else folder[:-1].title()
    for candidate in _classify_on_name_heuristics(name, base_kind):
        if candidate == folder:
            continue
        alt_path = f"{candidate}/{name}.xpo"
        ok, data = await _ado_item_exists(alt_path, ref)
        if ok:
            return data

    # if nothing works, bubble 404
    raise


# =========================================
# HEALTH
# =========================================

@app.get("/health")
@app.get("/health/")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


# =========================================
# /file
# =========================================

@app.get("/file")
@app.get("/file/")
async def file_get(
    path: str = Query(..., description="Repo path (e.g., Forms/CustTable.xpo)"),
    ref: Optional[str] = Query(None),
) -> Dict[str, Any]:
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
) -> Dict[str, Any]:
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
    node = await deps_get(file=file, ref=ref, page=1, limit=200)  # type: ignore[call-arg]
    dep_paths: List[str] = []
    for dep in node.get("dependencies", []):
        raw = dep.get("path") or ""
        clean = _normalize_dep_path(raw)
        if _should_skip_dep_path(clean):
            continue
        dep_paths.append(clean)
    # carry implicit for aggregation
    return dep_paths, node.get("implicit_crud", [])


@app.post("/resolve")
@app.post("/resolve/")
async def resolve_post(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    env = get_env()
    start_file: str = payload.get("start_file")
    if not start_file:
        raise HTTPException(400, "Missing required field: start_file")

    used_ref: str = payload.get("ref") or env["REF"]
    if str(used_ref).upper() == "HEAD":
        used_ref = env["REF"]
    max_depth = int(payload.get("max_depth", 5))

    visited: Set[str] = set()
    graph: List[Dict[str, Any]] = []
    implicit_all: List[Dict[str, Any]] = []
    current_level: List[str] = [start_file]
    depth = 0

    async def safe(file_path: str) -> Dict[str, Any]:
        try:
            deps, implicit = await _deps_paths_only(file_path, used_ref)
            implicit_all.extend(implicit)
            return {"file": file_path, "deps": deps, "error": None}
        except Exception as exc:  # noqa: BLE001
            log.error("[resolve] Failed deps_get for %s: %s", file_path, exc)
            return {"file": file_path, "deps": None, "error": str(exc)}

    while current_level and depth < max_depth:
        level_files = [f for f in current_level if f not in visited]
        visited.update(level_files)
        results = await asyncio.gather(*[safe(f) for f in level_files])
        next_level: List[str] = []
        for res in results:
            if res["error"]:
                graph.append(
                    {"file": res["file"], "error": res["error"], "depth": depth, "dependencies": []}
                )
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
        "status": "complete",
    }


# =========================================
# /report (full transitive, single-call)
# =========================================

async def _analyze_single_file(path: str, ref: Optional[str]) -> Dict[str, Any]:
    # fetch file
    try:
        file_res = await file_get(path=path, ref=ref)  # type: ignore[call-arg]
    except Exception as exc:  # noqa: BLE001
        return {"path": path, "ref": ref, "error": f"file_get: {exc}"}

    # analyze deps
    try:
        deps_res = await deps_get(file=path, ref=ref, page=1, limit=200)  # type: ignore[call-arg]
    except Exception as exc:  # noqa: BLE001
        return {
            "path": path,
            "ref": ref,
            "sha": file_res.get("sha"),
            "content_len": len(file_res.get("content", "")),
            "dependencies": [],
            "business_rules": [],
            "implicit_crud": [],
            "error": f"deps_get: {exc}",
        }

    # normalize/filter
    norm_deps: List[Dict[str, Any]] = []
    for dep in deps_res.get("dependencies", []):
        raw = dep.get("path") or ""
        clean = _normalize_dep_path(raw)
        if _should_skip_dep_path(clean):
            continue
        norm_dep = dict(dep)
        norm_dep["path"] = clean
        norm_deps.append(norm_dep)

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
async def report_post(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    env = get_env()
    start_file: str = payload.get("start_file")
    if not start_file:
        raise HTTPException(400, "Missing required field: start_file")
    used_ref: str = payload.get("ref") or env["REF"]
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

    async def safe(file_path: str) -> Dict[str, Any]:
        try:
            node = await deps_get(file=file_path, ref=used_ref, page=1, limit=200)  # type: ignore[call-arg]
            implicit_all.extend(node.get("implicit_crud", []))
            dep_paths: List[str] = []
            for dep in node.get("dependencies", []):
                clean = _normalize_dep_path(dep.get("path") or "")
                if _should_skip_dep_path(clean):
                    continue
                dep_paths.append(clean)
            return {"file": file_path, "deps": dep_paths, "error": None}
        except Exception as exc:  # noqa: BLE001
            log.error("[report] Failed deps_get for %s: %s", file_path, exc)
            return {"file": file_path, "deps": None, "error": str(exc)}

    while current_level and depth < max_depth:
        level_files = [f for f in current_level if f not in visited]
        visited.update(level_files)
        results = await asyncio.gather(*[safe(f) for f in level_files])

        next_level: List[str] = []
        for res in results:
            if res["error"]:
                graph.append(
                    {"file": res["file"], "depth": depth, "dependencies": [], "error": res["error"]}
                )
                continue
            deps = res["deps"] or []
            graph.append({"file": res["file"], "depth": depth, "dependencies": deps, "error": None})
            next_level.extend(deps)

        depth += 1
        current_level = [f for f in set(next_level) if f not in visited]

    visited_list = sorted(list(visited))

    # analyze each visited with bounded concurrency
    sem = Semaphore(max_conc)

    async def _guarded(path: str) -> Dict[str, Any]:
        async with sem:
            return await _analyze_single_file(path, used_ref)

    analyses = await asyncio.gather(*[_guarded(path) for path in visited_list])

    # merge
    objects: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []
    business_rules_all: List[Dict[str, Any]] = []
    implicit_crud_all: List[Dict[str, Any]] = list(implicit_all)

    depth_index = {node["file"]: node["depth"] for node in graph}
    for analysis in analyses:
        objects.append(
            {
                "path": analysis.get("path"),
                "ref": analysis.get("ref", used_ref),
                "sha": analysis.get("sha", "unknown"),
                "depth": depth_index.get(analysis.get("path"), -1),
                "error": analysis.get("error"),
            }
        )
        for dep in analysis.get("dependencies", []) or []:
            edges.append(
                {
                    "from": analysis.get("path"),
                    "to": dep.get("path"),
                    "reason": dep.get("reason"),
                    "type": dep.get("type"),
                    "depth_from_root": depth_index.get(analysis.get("path"), -1) + 1,
                }
            )
        business_rules_all.extend(analysis.get("business_rules", []) or [])
        implicit_crud_all.extend(analysis.get("implicit_crud", []) or [])

    # dedupe edges
    seen_edges = set()
    dedup_edges: List[Dict[str, Any]] = []
    for edge in edges:
        key = (edge["from"], edge["to"], edge.get("reason"))
        if key in seen_edges:
            continue
        seen_edges.add(key)
        dedup_edges.append(edge)

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
        "status": "complete",
    }


# =========================================
# /report.paged (cursor-based pagination to avoid timeouts)
# =========================================

def _b64(data: Dict[str, Any]) -> str:
    return base64.b64encode(json.dumps(data, separators=(",", ":")).encode("utf-8")).decode("utf-8")


def _unb64(token: str) -> Dict[str, Any]:
    """
    Decode opaque cursor tokens:
    - Strips known prefixes (e.g., 'opaque:', 'Bearer ')
    - Accepts URL-safe base64
    - Auto-pads to multiple of 4
    - Raises 400 with a clear message on failure
    """
    if not token or not isinstance(token, str):
        raise HTTPException(400, "Invalid cursor: empty or non-string.")

    cleaned = token.strip()
    for prefix in ("opaque:", "Opaque:", "bearer ", "Bearer "):
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix) :]
            break

    cleaned = cleaned.replace("-", "+").replace("_", "/")
    pad = (-len(cleaned)) % 4
    if pad:
        cleaned += "=" * pad

    try:
        raw = base64.b64decode(cleaned.encode("utf-8"))
        return json.loads(raw.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, f"Invalid cursor: {exc!s}") from exc


def _approx_size(**parts: Any) -> int:
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
    for field in fields_order:
        if field not in resp:
            continue
        value = resp[field]
        if isinstance(value, list) and value:
            # drop from the tail in chunks
            drop = max(1, math.ceil(len(value) * 0.15))
            resp[field] = value[:-drop]
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
    start_time = time.monotonic()
    processed = 0
    graph_delta: List[Dict[str, Any]] = []
    objects_delta: List[Dict[str, Any]] = []
    edges_delta: List[Dict[str, Any]] = []
    rules_delta: List[Dict[str, Any]] = []
    implicit_delta: List[Dict[str, Any]] = []
    visited_delta: List[str] = []

    depth_index: Dict[str, int] = {}

    async def safe(file_path: str) -> Dict[str, Any]:
        try:
            node = await deps_get(file=file_path, ref=used_ref, page=1, limit=200)  # type: ignore[call-arg]
            implicit_delta.extend(node.get("implicit_crud", []))
            dep_paths: List[str] = []
            for dep in node.get("dependencies", []):
                clean = _normalize_dep_path(dep.get("path") or "")
                if _should_skip_dep_path(clean):
                    continue
                dep_paths.append(clean)
            return {"file": file_path, "deps": dep_paths, "error": None}
        except Exception as exc:  # noqa: BLE001
            return {"file": file_path, "deps": [], "error": str(exc)}

    sem = Semaphore(max_concurrency)

    async def _guarded(file_path: str) -> Dict[str, Any]:
        async with sem:
            return await safe(file_path)

    depth_completed = depth
    next_frontier: List[str] = []

    cur_level = [f for f in frontier if f not in visited]
    while cur_level and depth < max_depth:
        # time guard
        if (time.monotonic() - start_time) >= wall_time_s:
            break

        # process this level
        visited.update(cur_level)
        visited_delta.extend(cur_level)

        results = await asyncio.gather(*[_guarded(f) for f in cur_level])

        for res in results:
            if res["error"]:
                graph_delta.append(
                    {"file": res["file"], "depth": depth, "dependencies": [], "error": res["error"]}
                )
                depth_index[res["file"]] = depth
                continue
            deps = res["deps"] or []
            graph_delta.append({"file": res["file"], "depth": depth, "dependencies": deps, "error": None})
            depth_index[res["file"]] = depth
            next_frontier.extend(deps)

        # analyze each visited node this round (bounded)
        async def _analyze_guarded(path: str) -> Dict[str, Any]:
            async with sem:
                return await _analyze_single_file(path, used_ref)

        analyses = await asyncio.gather(*[_analyze_guarded(path) for path in cur_level])

        for analysis in analyses:
            objects_delta.append(
                {
                    "path": analysis.get("path"),
                    "ref": analysis.get("ref", used_ref),
                    "sha": analysis.get("sha", "unknown"),
                    "depth": depth_index.get(analysis.get("path"), -1),
                    "error": analysis.get("error"),
                }
            )
            for dep in analysis.get("dependencies", []) or []:
                edges_delta.append(
                    {
                        "from": analysis.get("path"),
                        "to": dep.get("path"),
                        "reason": dep.get("reason"),
                        "type": dep.get("type"),
                        "depth_from_root": depth_index.get(analysis.get("path"), -1) + 1,
                    }
                )
            rules_delta.extend(analysis.get("business_rules", []) or [])

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
        if (time.monotonic() - start_time) >= wall_time_s:
            break

    depth_completed = max(depth_completed, depth)
    return visited_delta, graph_delta, objects_delta, edges_delta, rules_delta, implicit_delta, depth_completed


@app.post("/report.paged")
@app.get("/report.paged")  # allow GET for connectors that can't POST JSON
async def report_paged(
    request: Request,
    # Body is optional; query params also accepted
    payload: Optional[Dict[str, Any]] = Body(default=None),
    start_file: Optional[str] = Query(None),
    ref: Optional[str] = Query(None),
    cursor: Optional[str] = Query(None),
    max_depth: int = Query(5),
    page_limit_nodes: int = Query(200),
    max_concurrency: int = Query(8),
    max_bytes: int = Query(1_200_000),
    max_wall_time: float = Query(25.0),
) -> Dict[str, Any]:
    env = get_env()

    # --- tolerate weird callers ------------------------------------------------
    # If the body wasn't parsed but there is a raw body, try to interpret:
    body: Dict[str, Any] = payload or {}
    try:
        if payload is None:
            raw = await request.body()
            if raw:
                text = raw.decode("utf-8", errors="ignore").strip()
                if text and text[0] in "{[":
                    body = json.loads(text)
                elif text:
                    # treat as cursor string
                    body = {"cursor": text}
    except Exception:  # noqa: BLE001
        body = payload or {}

    # Pull query params into a plain dict for easier reuse (FastAPI Query uses exact names)
    query_data: Dict[str, Any] = {k: request.query_params.get(k) for k in request.query_params}

    # Accept alternate keys from legacy callers
    def first_key(data: Mapping[str, Any], keys: List[str]) -> Optional[Any]:
        if not isinstance(data, Mapping):
            return None

        def is_present(value: Any) -> bool:
            if value is None:
                return False
            if isinstance(value, str):
                return bool(value.strip())
            if isinstance(value, (list, tuple, set)) and not value:
                return False
            return True

        # Build a case-insensitive lookup while preserving original keys
        str_items = {k: data[k] for k in data if isinstance(k, str)}
        lowered = {k.lower(): k for k in str_items}

        for key in keys:
            if key in str_items and is_present(str_items[key]):
                return str_items[key]
            actual = lowered.get(key.lower())
            if actual and is_present(str_items[actual]):
                return str_items[actual]
        return None

    def _variants(name: str, extra: Optional[List[str]] = None) -> List[str]:
        variants: List[str] = []
        seen: Set[str] = set()

        def _add(value: str) -> None:
            lower = value.lower()
            if lower not in seen:
                variants.append(value)
                seen.add(lower)

        if extra:
            for item in extra:
                if isinstance(item, str):
                    _add(item)

        _add(name)
        _add(name.lower())
        if "_" in name:
            parts = name.split("_")
            camel = parts[0] + "".join(p.title() for p in parts[1:])
            pascal = "".join(p.title() for p in parts)
            _add(camel)
            _add(pascal)
        else:
            _add(name.capitalize())
        return variants

    def _pull_value(keys: List[str]) -> Optional[Any]:
        value = first_key(body, keys)
        if value is None:
            value = first_key(query_data, keys)
        return value

    # unify cursor from body or query, accepting alternates
    cursor_val = _pull_value(_variants("cursor", ["next_cursor", "token"]))
    if not cursor_val:
        cursor_val = cursor

    # ref precedence: body -> query -> env
    ref_value = _pull_value(_variants("ref", ["branch", "version", "branch_name"]))
    used_ref = ref_value or ref or env["REF"]
    if str(used_ref).upper() == "HEAD":
        used_ref = env["REF"]

    # numeric knobs: body overrides query if present
    def _get_int(key: str, default: int) -> int:
        try:
            value = _pull_value(_variants(key))
            return int(value if value is not None else default)
        except Exception:
            return default

    def _get_float(key: str, default: float) -> float:
        try:
            value = _pull_value(_variants(key))
            return float(value if value is not None else default)
        except Exception:
            return default

    max_depth = _get_int("max_depth", max_depth)
    page_limit_nodes = _get_int("page_limit_nodes", page_limit_nodes)
    max_concurrency = _get_int("max_concurrency", max_concurrency)
    max_bytes = _get_int("max_bytes", max_bytes)
    wall_time = _get_float("max_wall_time", max_wall_time)

    # Determine the root:
    # - prefer cursor if present
    # - else body.start_file | body.file | body.start | body.path
    # - else query start_file
    # - else env DEFAULT_START_FILE
    default_start_file = os.getenv("DEFAULT_START_FILE")
    if cursor_val:
        state = _unb64(cursor_val)
        for key in ("root", "ref", "max_depth", "depth", "frontier", "visited"):
            if key not in state:
                raise HTTPException(400, f"Invalid cursor: missing '{key}'.")
        root = state["root"]
        used_ref = state["ref"]
        max_depth = int(state["max_depth"])
        depth = int(state["depth"])
        frontier = list(state["frontier"] or [])
        visited: Set[str] = set(list(state["visited"] or []))
    else:
        root_keys = _variants("start_file", ["file", "start", "path", "startfile", "filepath"])
        body_start = _pull_value(root_keys)
        root = body_start or start_file or default_start_file
        if not root:
            raise HTTPException(
                400,
                "Missing required field: start_file (accepts: start_file|file|start|path, "
                "or provide 'cursor', or set env DEFAULT_START_FILE).",
            )
        depth = 0
        frontier = [root]
        visited = set()

    # run one page
    (
        visited_delta,
        graph_delta,
        objects_delta,
        edges_delta,
        rules_delta,
        implicit_delta,
        depth_completed,
    ) = await _paged_bfs_step(
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
    for node in graph_delta:
        if node.get("depth") == depth_completed and not node.get("error"):
            next_frontier.extend(node.get("dependencies", []))
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
        "approx_bytes": 0,  # filled after budget
        "truncated": False,
        "stats": {
            "page_nodes": len(visited_delta),
            "total_seen": len(visited),
            "page_limit_nodes": page_limit_nodes,
        },
        "next_cursor": None,
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

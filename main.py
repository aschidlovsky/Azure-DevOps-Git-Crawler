import asyncio
import base64
import json
import logging
import math
import os
import re
import time
from asyncio import Semaphore
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
                "Use /report.firsthop with a start_file to fetch immediate dependencies.",
                "Use /report.branch for deeper exploration of a specific dependency.",
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

ENTRY_METHOD_NAMES = {
    "init",
    "run",
    "clicked",
    "write",
    "active",
    "modified",
    "close",
    "closeok",
    "executequery",
    "lookup",
    "refresh",
    "activate",
    "leave",
    "enter",
}
CRUD_METHOD_NAMES = {"insert", "update", "delete", "write", "validatewrite", "validatedelete"}
METHOD_DEF_PATTERN = re.compile(
    r"^#?\s*(?:\[[^\]]+\]\s*)*"
    r"(?:(?:public|protected|private|static|final|override|client|server|display|editable|edit|internal|external)\s+)*"
    r"(?P<rtype>[A-Za-z_][A-Za-z0-9_]*)\s+"
    r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*\(",
    re.IGNORECASE,
)
ALLOW_FLAG_PATTERN = re.compile(
    r"\bAllow(Create|Edit|Delete)\b\s*[:=]\s*(Yes|No)",
    re.IGNORECASE,
)

UI_CONTROL_PATTERN = re.compile(r"^CONTROL\s+([A-Za-z]+)\s*$", re.IGNORECASE)
NAME_PROPERTY_PATTERN = re.compile(r"^\s*Name\s*#(.+)$")

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


DEFAULT_MAX_DEPTH = _env_int("MAX_DEPTH_DEFAULT", 5)
DEFAULT_MAX_CONCURRENCY = _env_int("MAX_CONCURRENCY_DEFAULT", 6)
DEFAULT_MAX_BYTES = _env_int("MAX_BYTES_DEFAULT", 350_000)
DEFAULT_MAX_FILES = _env_int("MAX_FILES_DEFAULT", 80)

_CACHE_TTL_SECONDS = _env_int("ITEM_CACHE_TTL", 600)
_ITEM_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_NEGATIVE_CACHE: Dict[str, float] = {}
_CACHE_LOCK = asyncio.Lock()

# =========================================
# SIZE UTILITIES
# =========================================

def _approx_size(obj: Dict[str, Any]) -> int:
    try:
        return len(json.dumps(obj, ensure_ascii=False).encode("utf-8"))
    except Exception:
        return 0


def _apply_size_budget(max_bytes: int, resp: Dict[str, Any], fields_order: List[str]) -> Tuple[Dict[str, Any], bool]:
    """
    Trim list fields repeatedly until the response fits under the byte budget.
    """
    truncated = False
    current = _approx_size(resp)
    if current <= max_bytes:
        return resp, truncated

    while current > max_bytes:
        trimmed = False
        for field in fields_order:
            value = resp.get(field)
            if isinstance(value, list) and value:
                drop = max(1, math.ceil(len(value) * 0.15))
                resp[field] = value[:-drop]
                truncated = True
                trimmed = True
                break
        if not trimmed:
            break
        current = _approx_size(resp)
    return resp, truncated


def _strip_xpo_hash(line: str) -> str:
    """Remove leading XPO hash markers while preserving indentation as much as reasonable."""
    if not line:
        return line
    # Remove leading whitespace followed by '#', but keep one level of indentation.
    stripped = line.lstrip()
    if stripped.startswith("#"):
        stripped = stripped[1:]
    return stripped.rstrip("\n")


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
    env = get_env()
    used_ref = ref or env["REF"]
    if isinstance(used_ref, str) and used_ref.upper() == "HEAD":
        used_ref = env["REF"]
    cache_key = f"{path}@{used_ref}"
    now = time.monotonic()
    async with _CACHE_LOCK:
        cached = _ITEM_CACHE.get(cache_key)
        if cached and (now - cached[0]) <= _CACHE_TTL_SECONDS:
            return True, cached[1]
        negative = _NEGATIVE_CACHE.get(cache_key)
        if negative and (now - negative) <= _CACHE_TTL_SECONDS:
            return False, None

    try:
        data = await get_item_content(path, used_ref)
        return True, data
    except HTTPException as exc:
        if exc.status_code == 404:
            async with _CACHE_LOCK:
                _NEGATIVE_CACHE[cache_key] = now
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

    cache_key = f"{path}@{used_ref}"
    now = time.monotonic()
    async with _CACHE_LOCK:
        cached = _ITEM_CACHE.get(cache_key)
        if cached and (now - cached[0]) <= _CACHE_TTL_SECONDS:
            log.debug("[cache] hit path=%s ref=%s", path, used_ref)
            return cached[1]
        negative = _NEGATIVE_CACHE.get(cache_key)
        if negative and (now - negative) <= _CACHE_TTL_SECONDS:
            log.debug("[cache] negative hit path=%s ref=%s", path, used_ref)
            raise HTTPException(404, f"File not found in ADO: {path}@{used_ref}")

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
            async with _CACHE_LOCK:
                _NEGATIVE_CACHE[cache_key] = now
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
        async with _CACHE_LOCK:
            _NEGATIVE_CACHE[cache_key] = now
        raise HTTPException(404, "No content found for this path (might be binary or empty).")

    # base64 -> text if needed
    try:
        content = base64.b64decode(content).decode("utf-8")
    except Exception:
        pass

    result = {
        "content": content,
        "sha": data.get("objectId") or data.get("commitId") or "unknown",
        "ref": used_ref,
    }
    store_time = time.monotonic()
    async with _CACHE_LOCK:
        _ITEM_CACHE[cache_key] = (store_time, result)
        _NEGATIVE_CACHE.pop(cache_key, None)
    return result


# =========================================
# LIGHT PARSER
# =========================================

def extract_dependencies(content: str, file_path: Optional[str] = None) -> Dict[str, Any]:
    deps: List[Dict[str, Any]] = []
    implicit: List[Dict[str, Any]] = []
    business: List[Dict[str, Any]] = []
    dep_keys: Set[str] = set()
    entry_methods: List[Dict[str, Any]] = []
    crud_methods: List[Dict[str, Any]] = []
    seen_entry: Set[Tuple[str, int]] = set()
    seen_crud: Set[Tuple[str, int]] = set()
    allow_flags: List[Dict[str, Any]] = []
    ui_controls: List[Dict[str, Any]] = []
    control_stack: List[Dict[str, str]] = []
    name_stack: List[Dict[str, str]] = []

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

    lines = content.splitlines()

    def _method_context(idx: int) -> str:
        context_name: Optional[str] = None
        context_type: Optional[str] = None
        for back in range(idx, -1, -1):
            stripped = _strip_xpo_hash(lines[back]).strip()
            if not context_name and stripped.startswith("Name"):
                parts = stripped.split("#", 1)
                if len(parts) > 1:
                    context_name = parts[1].strip()
            if not context_type:
                if stripped.startswith("CONTROL "):
                    tokens = stripped.split()
                    if len(tokens) >= 2:
                        context_type = f"CONTROL {tokens[1]}"
                elif stripped.startswith("DATASOURCE"):
                    context_type = "DATASOURCE"
                elif stripped.startswith("FORM "):
                    context_type = "FORM"
                elif stripped.startswith("OBJECTPOOL"):
                    context_type = "OBJECTPOOL"
            if context_name and context_type:
                break
        parts: List[str] = []
        if context_type:
            parts.append(context_type)
        if context_name:
            parts.append(context_name)
        return " > ".join(parts) if parts else "FORM"

    def _capture_method_body(idx: int) -> str:
        snippet_lines: List[str] = []
        brace_depth = 0
        seen_body = False
        for pointer in range(idx, len(lines)):
            cleaned = _strip_xpo_hash(lines[pointer])
            snippet_lines.append(cleaned.rstrip())
            brace_depth += cleaned.count("{")
            brace_depth -= cleaned.count("}")
            if "{" in cleaned:
                seen_body = True
            if seen_body and brace_depth <= 0:
                break
            if len(snippet_lines) >= 80:
                break
        return "\n".join(snippet_lines).strip()

    def _extract_method_operations(method_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        snippet_text = method_info.get("snippet") or ""
        if not snippet_text:
            return []
        operations: List[Dict[str, Any]] = []
        base_line = method_info.get("line", 0)
        for offset, snippet_line in enumerate(snippet_text.splitlines(), start=0):
            stripped_line = snippet_line.strip()
            if not stripped_line:
                continue
            match = re.search(r"\bupdate_recordset\s+([A-Za-z_][A-Za-z0-9_]*)", stripped_line, re.IGNORECASE)
            if match:
                operations.append(
                    {
                        "operation": "update_recordset",
                        "target": match.group(1),
                        "code": stripped_line,
                        "line": base_line + offset,
                    }
                )
            match = re.search(r"\binsert_recordset\s+([A-Za-z_][A-Za-z0-9_]*)", stripped_line, re.IGNORECASE)
            if match:
                operations.append(
                    {
                        "operation": "insert_recordset",
                        "target": match.group(1),
                        "code": stripped_line,
                        "line": base_line + offset,
                    }
                )
            match = re.search(r"\bdelete_from\s+([A-Za-z_][A-Za-z0-9_]*)", stripped_line, re.IGNORECASE)
            if match:
                operations.append(
                    {
                        "operation": "delete_from",
                        "target": match.group(1),
                        "code": stripped_line,
                        "line": base_line + offset,
                    }
                )
            match = re.search(r"\b([A-Za-z_][A-Za-z0-9_]*)\.(insert|update|delete)\s*\(", stripped_line, re.IGNORECASE)
            if match:
                operations.append(
                    {
                        "operation": match.group(2).lower(),
                        "target": match.group(1),
                        "code": stripped_line,
                        "line": base_line + offset,
                    }
                )
        return operations
    
    # UI controls stack
    for line in lines:
        stripped = _strip_xpo_hash(line).strip()
        if not stripped:
            continue
        match_control = UI_CONTROL_PATTERN.match(stripped)
        if match_control:
            control_type = match_control.group(1)
            control_stack.append(control_type)
            continue
        if stripped.startswith("ENDCONTROL") and control_stack:
            control_stack.pop()
            if name_stack:
                name_stack.pop()
            continue
        match_name = NAME_PROPERTY_PATTERN.match(stripped)
        if match_name and control_stack:
            control_name = match_name.group(1).strip()
            ui_controls.append(
                {
                    "name": control_name,
                    "control_type": control_stack[-1],
                }
            )
            name_stack.append(control_name)

    # Business-rule signals
    for line_no, raw_line in enumerate(lines, start=1):
        normalized = _strip_xpo_hash(raw_line).strip()
        if not normalized:
            continue

        if any(token in normalized for token in ["validate", "error(", "ttsBegin", "ttsCommit"]):
            business.append({"line": line_no, "context": normalized[:200]})

        method_match = METHOD_DEF_PATTERN.match(normalized)
        if method_match:
            name = method_match.group("name") or ""
            lower = name.lower()
            info = {
                "name": name,
                "line": line_no,
                "signature": normalized[:200],
                "context": _method_context(line_no - 1),
                "snippet": _capture_method_body(line_no - 1),
            }
            info["operations"] = _extract_method_operations(info)
            if lower in ENTRY_METHOD_NAMES and (lower, line_no) not in seen_entry:
                entry_methods.append(info)
                seen_entry.add((lower, line_no))
            if lower in CRUD_METHOD_NAMES and (lower, line_no) not in seen_crud:
                crud_methods.append(info)
                seen_crud.add((lower, line_no))

        allow_match = ALLOW_FLAG_PATTERN.search(normalized)
        if allow_match:
            flag = {
                "property": f"Allow{allow_match.group(1)}",
                "value": allow_match.group(2).capitalize(),
                "line": line_no,
                "context": normalized[:200],
            }
            allow_flags.append(flag)

    crud_operations: List[Dict[str, Any]] = []
    for method in entry_methods + crud_methods:
        for operation in method.get("operations") or []:
            enriched = dict(operation)
            enriched.update(
                {
                    "method": method.get("name"),
                    "method_context": method.get("context"),
                    "method_line": method.get("line"),
                    "method_snippet": method.get("snippet"),
                }
            )
            crud_operations.append(enriched)

    log.info(
        "[extract] file=%s deps=%d ds=%d crud=%d rules=%d",
        file_path or "<mem>",
        len(deps),
        len(ds_tables),
        len(implicit),
        len(business),
    )
    return {
        "dependencies": deps,
        "implicit_crud": implicit,
        "business_rules": business,
        "entry_methods": entry_methods,
        "crud_methods": crud_methods,
        "allow_flags": allow_flags,
        "crud_operations": crud_operations,
        "ui_controls": ui_controls,
    }


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
        "entry_methods": parsed["entry_methods"],
        "crud_methods": parsed["crud_methods"],
        "allow_flags": parsed["allow_flags"],
        "crud_operations": parsed["crud_operations"],
        "unresolved": [],
        "visited": [file],
        "skipped": [],
        "page": page,
        "limit": limit,
        "total_dependencies": total,
        "total_pages": total_pages,
    }


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
            "entry_methods": [],
            "crud_methods": [],
            "allow_flags": [],
            "crud_operations": [],
            "ui_controls": [],
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
        "entry_methods": deps_res.get("entry_methods", []),
        "crud_methods": deps_res.get("crud_methods", []),
        "allow_flags": deps_res.get("allow_flags", []),
        "crud_operations": deps_res.get("crud_operations", []),
        "ui_controls": deps_res.get("ui_controls", []),
        "error": None,
    }


# =========================================
# BRANCH COLLECTOR
# =========================================

async def _collect_branch(
    start_file: str,
    used_ref: str,
    max_depth: int,
    max_files: int,
    max_concurrency: int,
) -> Dict[str, Any]:
    sem = Semaphore(max_concurrency)
    analysis_cache: Dict[str, Dict[str, Any]] = {}
    depths: Dict[str, int] = {}
    visit_order: List[str] = []
    overflow: List[str] = []
    method_summary: List[Dict[str, Any]] = []
    crud_operations_all: List[Dict[str, Any]] = []
    ui_controls_all: List[Dict[str, Any]] = []

    async def _analyze_guarded(path: str) -> Dict[str, Any]:
        if path in analysis_cache:
            return analysis_cache[path]
        async with sem:
            analysis = await _analyze_single_file(path, used_ref)
        analysis_cache[path] = analysis
        return analysis

    frontier: List[str] = [start_file]
    depth = 0

    while frontier and depth <= max_depth and len(visit_order) < max_files:
        level: List[str] = []
        for path in frontier:
            if path in depths:
                continue
            if len(visit_order) >= max_files:
                break
            depths[path] = depth
            visit_order.append(path)
            level.append(path)

        if not level:
            break

        analyses = await asyncio.gather(*[_analyze_guarded(path) for path in level])

        next_frontier: List[str] = []
        for path, analysis in zip(level, analyses):
            if analysis.get("error"):
                continue
            if depth >= max_depth:
                continue
            for dep in analysis.get("dependencies", []) or []:
                dep_path = dep.get("path")
                if not dep_path or dep_path in depths:
                    continue
                if dep_path in next_frontier:
                    continue
                if len(visit_order) + len(next_frontier) < max_files:
                    next_frontier.append(dep_path)
                else:
                    overflow.append(dep_path)

        frontier = next_frontier
        depth += 1

    depth_completed = max(depths.values()) if depths else 0
    limit_reached = len(visit_order) >= max_files
    more_available = bool(frontier) or bool(overflow)

    graph: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []
    business_rules: List[Dict[str, Any]] = []
    implicit_crud: List[Dict[str, Any]] = []
    objects: List[Dict[str, Any]] = []
    seen_edges: Set[Tuple[str, str, Optional[str]]] = set()

    for path in visit_order:
        analysis = await _analyze_guarded(path)
        depth_idx = depths.get(path, -1)
        entry_methods = analysis.get("entry_methods", [])
        crud_methods = analysis.get("crud_methods", [])
        allow_flags = analysis.get("allow_flags", [])
        crud_operations = analysis.get("crud_operations", [])
        ui_controls = analysis.get("ui_controls", [])
        method_summary.append(
            {
                "path": path,
                "entry_methods": entry_methods,
                "crud_methods": crud_methods,
                "allow_flags": allow_flags,
                "crud_operations": crud_operations,
                "ui_controls": ui_controls,
            }
        )
        objects.append(
            {
                "path": analysis.get("path", path),
                "ref": analysis.get("ref", used_ref),
                "sha": analysis.get("sha", "unknown"),
                "depth": depth_idx,
                "error": analysis.get("error"),
                "entry_methods": entry_methods,
                "crud_methods": crud_methods,
                "allow_flags": allow_flags,
                "crud_operations": crud_operations,
                "ui_controls": ui_controls,
            }
        )

        business_rules.extend(analysis.get("business_rules", []) or [])
        implicit_crud.extend(analysis.get("implicit_crud", []) or [])
        crud_operations_all.extend(crud_operations or [])
        ui_controls_all.extend(ui_controls or [])

        if analysis.get("error"):
            graph.append({"file": path, "depth": depth_idx, "dependencies": [], "error": analysis["error"]})
            continue

        deps = []
        for dep in analysis.get("dependencies", []) or []:
            dep_path = dep.get("path")
            if not dep_path:
                continue
            deps.append(dep_path)
            edge_key = (path, dep_path, dep.get("reason"))
            if edge_key in seen_edges:
                continue
            seen_edges.add(edge_key)
            edges.append(
                {
                    "from": path,
                    "to": dep_path,
                    "reason": dep.get("reason"),
                    "type": dep.get("type"),
                    "depth_from_root": depth_idx + 1,
                }
            )

        graph.append({"file": path, "depth": depth_idx, "dependencies": deps, "error": None})

    pending = sorted(set(frontier + overflow))
    status = "complete" if not more_available else "partial"

    return {
        "root": start_file,
        "ref": used_ref,
        "max_depth": max_depth,
        "depth_completed": depth_completed,
        "visited": visit_order,
        "visited_count": len(visit_order),
        "limit_reached": limit_reached,
        "pending": pending,
        "graph": graph,
        "objects": objects,
        "edges": edges,
        "business_rules": business_rules,
        "implicit_crud": implicit_crud,
        "status": status,
        "method_summary": method_summary,
        "crud_operations": crud_operations_all,
        "ui_controls": ui_controls_all,
    }


@app.post("/report.firsthop")
@app.post("/report.firsthop/")
async def report_firsthop(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    env = get_env()
    start_file = payload.get("start_file")
    if not start_file:
        raise HTTPException(400, "Missing required field: start_file")

    used_ref: str = payload.get("ref") or env["REF"]
    if str(used_ref).upper() == "HEAD":
        used_ref = env["REF"]

    analysis = await _analyze_single_file(start_file, used_ref)
    status = "ok" if not analysis.get("error") else "error"

    dep_summary: Dict[str, int] = {}
    direct: List[Dict[str, Any]] = []
    for dep in analysis.get("dependencies", []) or []:
        dtype = dep.get("type", "Unknown")
        dep_summary[dtype] = dep_summary.get(dtype, 0) + 1
        direct.append(
            {
                "path": dep.get("path"),
                "type": dep.get("type"),
                "symbol": dep.get("symbol"),
                "reason": dep.get("reason"),
            }
        )

    response: Dict[str, Any] = {
        "root": start_file,
        "ref": analysis.get("ref", used_ref),
        "sha": analysis.get("sha", "unknown"),
        "content_len": analysis.get("content_len", 0),
        "status": status,
        "error": analysis.get("error"),
        "direct_dependencies": direct,
        "dependency_counts": dep_summary,
        "implicit_crud": analysis.get("implicit_crud", []),
        "business_rules": analysis.get("business_rules", []),
        "total_dependencies": len(direct),
        "entry_methods": analysis.get("entry_methods", []),
        "crud_methods": analysis.get("crud_methods", []),
        "allow_flags": analysis.get("allow_flags", []),
        "crud_operations": analysis.get("crud_operations", []),
        "ui_controls": analysis.get("ui_controls", []),
    }
    response["approx_bytes"] = _approx_size(response)
    log.info(
        "[report.firsthop] root=%s deps=%s approx_bytes=%s status=%s",
        start_file,
        len(direct),
        response["approx_bytes"],
        status,
    )
    return response


@app.post("/report.branch")
@app.post("/report.branch/")
async def report_branch(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    env = get_env()
    start_file: Optional[str] = payload.get("start_file")
    if not start_file:
        raise HTTPException(400, "Missing required field: start_file")

    used_ref: str = payload.get("ref") or env["REF"]
    if str(used_ref).upper() == "HEAD":
        used_ref = env["REF"]

    try:
        max_depth = int(payload.get("max_depth", DEFAULT_MAX_DEPTH))
    except Exception:
        max_depth = DEFAULT_MAX_DEPTH
    try:
        max_files = int(payload.get("max_files", DEFAULT_MAX_FILES))
    except Exception:
        max_files = DEFAULT_MAX_FILES
    try:
        max_concurrency = int(payload.get("max_concurrency", DEFAULT_MAX_CONCURRENCY))
    except Exception:
        max_concurrency = DEFAULT_MAX_CONCURRENCY
    try:
        max_bytes = int(payload.get("max_bytes", DEFAULT_MAX_BYTES))
    except Exception:
        max_bytes = DEFAULT_MAX_BYTES

    branch = await _collect_branch(
        start_file=start_file,
        used_ref=used_ref,
        max_depth=max_depth,
        max_files=max_files,
        max_concurrency=max_concurrency,
    )

    branch["max_depth"] = max_depth
    branch["max_files"] = max_files
    branch["max_concurrency"] = max_concurrency
    branch["max_bytes"] = max_bytes

    trimmed, truncated = _apply_size_budget(
        max_bytes,
        branch,
        ["edges", "objects", "business_rules", "implicit_crud", "graph", "method_summary", "crud_operations", "ui_controls"],
    )
    trimmed["truncated"] = truncated
    trimmed["approx_bytes"] = _approx_size(trimmed)
    log.info(
        "[report.branch] root=%s depth=%s visited=%s approx_bytes=%s truncated=%s status=%s",
        start_file,
        trimmed.get("depth_completed"),
        trimmed.get("visited_count"),
        trimmed["approx_bytes"],
        truncated,
        trimmed.get("status"),
    )
    return trimmed

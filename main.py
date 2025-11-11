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

app = FastAPI(title="ADO Repo Dependency Connector", version="1.4.0")

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
DEPENDENCY_REGEXES = [(re.compile(pattern, re.IGNORECASE), kind, reason) for pattern, kind, reason in DEPENDENCY_PATTERNS]
FORM_DS_REGEXES = [re.compile(pattern, re.IGNORECASE) for pattern in FORM_DS_PATTERNS]

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
FIELD_REF_PATTERN = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)\b")
CALL_KEYWORDS = {
    "if",
    "while",
    "for",
    "switch",
    "case",
    "else",
    "catch",
    "try",
    "ttsbegin",
    "ttscommit",
    "return",
    "break",
    "continue",
}
CALL_PATTERN = re.compile(
    r"\b([A-Za-z_][A-Za-z0-9_]*(?:::[A-Za-z_][A-Za-z0-9_]*)?(?:\.[A-Za-z_][A-Za-z0-9_]*)*)\s*\("
)
CONTROL_PROPERTY_PATTERN = re.compile(
    r"^(?P<prop>[A-Za-z_][A-Za-z0-9_]*)\s*(?:[:=]|#)\s*(?P<value>.+)$"
)
ALLOW_FLAG_PATTERN = re.compile(
    r"\bAllow(Create|Edit|Delete)\b\s*[:=]\s*(Yes|No)",
    re.IGNORECASE,
)

UI_CONTROL_PATTERN = re.compile(r"^CONTROL\s+([A-Za-z]+)\s*$", re.IGNORECASE)
NAME_PROPERTY_PATTERN = re.compile(r"^\s*Name\s*#(.+)$")

CUSTOM_PREFIXES = {"mss", "wb", "zx", "axm"}
STANDARD_OBJECTS_BASE = {
    "purchtable",
    "purchline",
    "purchparmline",
    "purchparmtable",
    "purchreqtable",
    "purchreqline",
    "salestable",
    "salesline",
    "salesparmtable",
    "salesparmline",
    "inventtable",
    "inventtrans",
    "inventsum",
    "inventdim",
    "inventserial",
    "inventbatch",
    "vendtable",
    "custtable",
    "dirpartytable",
    "logisticspostaladdress",
    "taxtable",
    "markuptable",
    "ledgerjournaltrans",
    "ledgerjournaltable",
    "projtable",
    "projsalesprice",
    "projbudgetline",
}
_EXTRA_STANDARD = {
    symbol.strip().lower()
    for symbol in os.getenv("STANDARD_OBJECTS_EXTRA", "").split(",")
    if symbol.strip()
}
STANDARD_OBJECTS = STANDARD_OBJECTS_BASE.union(_EXTRA_STANDARD)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _symbol_category(symbol: Optional[str]) -> str:
    if not symbol:
        return "unknown"
    lowered = symbol.lower()
    if lowered in STANDARD_OBJECTS:
        return "standard"
    for prefix in CUSTOM_PREFIXES:
        if lowered.startswith(prefix):
            return "custom"
    return "custom"


def _merge_field_usage(entries: Optional[List[Dict[str, Any]]]) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    result: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    if not entries:
        return result
    for entry in entries:
        table = entry.get("table")
        field = entry.get("field")
        occurrences = entry.get("occurrences") or []
        if not table or not field:
            continue
        table_map = result.setdefault(table, {})
        field_list = table_map.setdefault(field, [])
        field_list.extend(occurrences)
    return result


def _summarize_field_usage(entries: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    merged = _merge_field_usage(entries)
    summary: List[Dict[str, Any]] = []
    for table, fields in merged.items():
        summary.append(
            {
                "table": table,
                "fields": [
                    {"name": field, "occurrences": occs}
                    for field, occs in sorted(fields.items())
                ],
            }
        )
    return summary


def _summarize_dependencies(
    dependencies: Optional[List[Dict[str, Any]]],
    filtered: Optional[List[Dict[str, Any]]],
) -> Dict[str, List[Dict[str, Any]]]:
    catalog: Dict[str, Dict[str, Dict[str, Any]]] = {
        "custom": {},
        "standard": {},
    }
    if dependencies:
        for dep in dependencies:
            symbol = dep.get("symbol")
            category = _symbol_category(symbol)
            if category not in catalog:
                continue
            catalog[category][symbol or dep.get("path", "")] = {
                "symbol": symbol,
                "type": dep.get("type"),
                "path": dep.get("path"),
                "reason": dep.get("reason"),
            }
    summary = {key: sorted(value.values(), key=lambda item: (item.get("symbol") or "")) for key, value in catalog.items()}
    summary["filtered"] = filtered or []
    return summary


DEFAULT_MAX_DEPTH = _env_int("MAX_DEPTH_DEFAULT", 5)
DEFAULT_MAX_CONCURRENCY = _env_int("MAX_CONCURRENCY_DEFAULT", 6)
DEFAULT_MAX_BYTES = _env_int("MAX_BYTES_DEFAULT", 350_000)
DEFAULT_SOURCE_BUDGET = _env_int("SOURCE_BYTES_DEFAULT", 750_000)
DEFAULT_MAX_FILES = _env_int("MAX_FILES_DEFAULT", 80)
DEFAULT_MAX_FANOUT = _env_int("MAX_FANOUT_DEFAULT", 12)

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


def _index_methods(methods: Optional[List[Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
    indexed: Dict[str, Dict[str, Any]] = {}
    if not methods:
        return indexed
    for method in methods:
        name = method.get("name")
        if not name or name in indexed:
            continue
        indexed[name] = method
    return indexed


def _classify_dependency_edge(
    dependency: Dict[str, Any],
    method_index: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    roles: Set[str] = set()
    reason_details = dependency.get("reason_details") or []
    dep_type = (dependency.get("type") or "").lower()
    call_sites = dependency.get("call_sites") or []
    lowered_reasons = [reason.lower() for reason in reason_details if isinstance(reason, str)]

    if dep_type == "table":
        if any(reason.startswith("crud") for reason in lowered_reasons):
            roles.add("data_write")
        elif any(reason in {"statement", "form-datasource", "datasource()"} for reason in lowered_reasons):
            roles.add("data_read")
        else:
            if any((occ.get("snippet") or "").lower().startswith("select") for occ in dependency.get("occurrences", []) or []):
                roles.add("data_read")

    for site in call_sites:
        context = (site.get("context") or "").lower()
        if context in {"condition", "return"}:
            roles.add("control_flow")
        if context == "assignment":
            roles.add("calculation")
        if site.get("ui_related"):
            roles.add("ui_hook")
        method = method_index.get(site.get("method"))
        if not method:
            continue
        if method.get("crud_operations"):
            roles.add("data_write")
        if method.get("transactions"):
            roles.add("transactional")
        if "ui_entry" in (method.get("roles") or []):
            roles.add("ui_hook")

    if not roles and dependency.get("reason") == "static-call" and call_sites:
        roles.add("control_flow")

    if "data_write" in roles or "control_flow" in roles or "ui_hook" in roles or "transactional" in roles:
        impact = "high"
    elif "data_read" in roles or "calculation" in roles:
        impact = "medium"
    else:
        impact = "low"

    should_traverse = impact != "low"
    skip_reason = None if should_traverse else "low-impact"

    return {
        "edge_roles": sorted(roles),
        "impact": impact,
        "should_traverse": should_traverse,
        "skip_reason": skip_reason,
    }

def _new_method_state(name: str, start_line: int) -> Dict[str, Any]:
    lowered = name.lower()
    roles: List[str] = []
    if lowered in ENTRY_METHOD_NAMES:
        roles.append("ui_entry")
    if lowered in CRUD_METHOD_NAMES:
        roles.append("crud_hook")
    return {
        "name": name,
        "start_line": start_line,
        "end_line": start_line,
        "signature": "",
        "roles": roles,
        "calls": [],
        "crud_operations": [],
        "transactions": [],
        "field_refs": [],
    }


def _detect_call_context(line: str, call_start: int) -> str:
    prefix = line[:call_start].strip().lower()
    tokens = prefix.replace("(", " ").replace(")", " ").split()
    ordered = list(reversed(tokens))
    for token in ordered:
        if token in {"return"}:
            return "return"
        if token in {"if", "while", "for", "switch", "case", "catch"}:
            return "condition"
        if token in {"else"}:
            return "condition"
    if "=" in prefix:
        return "assignment"
    return "statement"


def _infer_field_context(line: str, span_start: int) -> str:
    eq_pos = line.find("=")
    if eq_pos == -1 or span_start > eq_pos:
        return "read"
    # treat "==" comparisons as reads
    next_char = line[eq_pos + 1] if eq_pos + 1 < len(line) else ""
    if next_char == "=":
        return "read"
    return "write"


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
    filtered_symbols: Dict[str, Dict[str, Any]] = {}
    dependencies: Dict[str, Dict[str, Any]] = {}
    methods: List[Dict[str, Any]] = []
    field_entries: List[Dict[str, Any]] = []

    lines = [_strip_xpo_hash(line) for line in content.splitlines()]
    current_method = _new_method_state("<class>", 1)

    def _record_dependency(
        symbol: Optional[str],
        kind: str,
        reason: str,
        line_no: Optional[int] = None,
        method_name: Optional[str] = None,
        snippet: Optional[str] = None,
    ) -> None:
        if not symbol:
            return
        trimmed = symbol.strip()
        if not trimmed or trimmed in KERNEL_SKIP or len(trimmed) < 2:
            filtered_symbols.setdefault(
                trimmed or symbol or "<unknown>",
                {
                    "symbol": trimmed or symbol,
                    "reason": "kernel-filter",
                },
            )
            return
        folder = TYPE_PATHS.get(kind, "Unknown")
        path = f"{folder}/{trimmed}.xpo"
        entry = dependencies.setdefault(
            path,
            {
                "path": path,
                "type": kind,
                "symbol": trimmed,
                "reasons": set(),
                "occurrences": [],
            },
        )
        entry["reasons"].add(reason)
        if line_no is not None:
            entry["occurrences"].append(
                {
                    "line": line_no,
                    "method": method_name,
                    "context": reason,
                    "snippet": snippet[:300] if snippet else None,
                }
            )

    for line_no, raw_line in enumerate(lines, start=1):
        stripped = raw_line.strip()
        method_match = METHOD_DEF_PATTERN.match(stripped)
        if method_match:
            current_method["end_line"] = line_no - 1 if line_no > 1 else line_no
            methods.append(current_method)
            method_name = method_match.group("name")
            current_method = _new_method_state(method_name, line_no)
            current_method["signature"] = stripped
            continue

        lowered = stripped.lower()
        if "ttsbegin" in lowered:
            current_method["transactions"].append({"type": "ttsbegin", "line": line_no})
        if "ttscommit" in lowered:
            current_method["transactions"].append({"type": "ttscommit", "line": line_no})

        for crud_match in CRUD_SIGNATURE.finditer(stripped):
            table = crud_match.group(1)
            op = crud_match.group(2)
            current_method["crud_operations"].append({"table": table, "operation": op, "line": line_no})
            _record_dependency(table, "Table", f"crud-{op.lower()}", line_no, current_method["name"], stripped)

        for table, field in FIELD_REF_PATTERN.findall(stripped):
            context = _infer_field_context(stripped, stripped.find(f"{table}.{field}"))
            entry = {
                "table": table,
                "field": field,
                "line": line_no,
                "context": context,
                "method": current_method["name"],
            }
            current_method["field_refs"].append(entry)
            field_entries.append(
                {
                    "table": table,
                    "field": field,
                    "occurrences": [
                        {
                            "line": line_no,
                            "context": context,
                            "method": current_method["name"],
                        }
                    ],
                }
            )

        for regex, kind, reason in DEPENDENCY_REGEXES:
            for match in regex.finditer(stripped):
                if reason == "statement":
                    symbol = match.group(2)
                else:
                    symbol = match.group(1)
                _record_dependency(symbol, kind, reason, line_no, current_method["name"], stripped)

        for call_match in CALL_PATTERN.finditer(stripped):
            expression = call_match.group(1)
            base_symbol: Optional[str] = None
            call_type = "instance"
            if "::" in expression:
                base_symbol = expression.split("::", 1)[0]
                call_type = "static"
            elif "." in expression:
                base_symbol = expression.split(".", 1)[0]
            else:
                base_symbol = expression
            norm_symbol = base_symbol.strip() if base_symbol else None
            if norm_symbol and norm_symbol.lower() in CALL_KEYWORDS:
                norm_symbol = None
            context = _detect_call_context(stripped, call_match.start())
            ui_related = bool(norm_symbol and (norm_symbol.lower().endswith("_ds") or norm_symbol.lower() == "element"))
            call_info = {
                "expression": expression,
                "line": line_no,
                "context": context,
                "symbol": norm_symbol,
                "call_type": call_type,
                "ui_related": ui_related or ("element." in expression.lower()),
            }
            current_method["calls"].append(call_info)

    current_method["end_line"] = len(lines)
    methods.append(current_method)

    if _looks_like_form_path(file_path or ""):
        for regex in FORM_DS_REGEXES:
            for match in regex.finditer(content):
                _record_dependency(match.group(1), "Table", "form-datasource", None, None, None)

    dependencies_list: List[Dict[str, Any]] = []
    for entry in dependencies.values():
        reasons = sorted(entry.pop("reasons"))
        entry["reason"] = reasons[0] if reasons else None
        entry["reason_details"] = reasons
        if entry.get("occurrences"):
            entry["occurrences"] = sorted(entry["occurrences"], key=lambda item: item.get("line") or 0)
        dependencies_list.append(entry)

    symbol_map = {dep.get("symbol", "").lower(): dep for dep in dependencies_list}
    for method in methods:
        for call in method.get("calls", []):
            symbol = (call.get("symbol") or "").lower()
            if not symbol:
                continue
            dep = symbol_map.get(symbol)
            if not dep:
                continue
            call_sites = dep.setdefault("call_sites", [])
            call_sites.append(
                {
                    "method": method["name"],
                    "line": call["line"],
                    "context": call["context"],
                    "expression": call["expression"],
                    "ui_related": call["ui_related"],
                }
            )

    filtered_list = list(filtered_symbols.values())
    log.info(
        "[extract] file=%s deps=%d filtered=%d methods=%d",
        file_path or "<mem>",
        len(dependencies_list),
        len(filtered_list),
        len(methods),
    )
    return {
        "dependencies": dependencies_list,
        "filtered_dependencies": filtered_list,
        "methods": methods,
        "field_usage": _summarize_field_usage(field_entries),
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
@app.get("//file")
@app.get("//file/")
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
@app.get("//deps")
@app.get("//deps/")
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
        "filtered_dependencies": parsed["filtered_dependencies"],
        "methods": parsed.get("methods", []),
        "field_usage": parsed.get("field_usage", []),
        "page": page,
        "limit": limit,
        "total_dependencies": total,
        "total_pages": total_pages,
    }


async def _analyze_single_file(path: str, ref: Optional[str], return_content: bool = False) -> Dict[str, Any]:
    # fetch file
    try:
        file_res = await file_get(path=path, ref=ref)  # type: ignore[call-arg]
    except Exception as exc:  # noqa: BLE001
        return {"path": path, "ref": ref, "error": f"file_get: {exc}"}
    file_content = file_res.get("content") if return_content else None

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
            "filtered_dependencies": [],
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
        "filtered_dependencies": deps_res.get("filtered_dependencies", []),
        "methods": deps_res.get("methods", []),
        "field_usage": deps_res.get("field_usage", []),
        "error": None,
    }
    if return_content and file_content is not None:
        result["content"] = file_content
    return result


# =========================================
# BRANCH COLLECTOR
# =========================================

async def _collect_branch(
    start_file: str,
    used_ref: str,
    max_depth: int,
    max_files: int,
    max_concurrency: int,
    include_sources: bool = False,
    source_budget: Optional[int] = None,
    fanout_limit: Optional[int] = None,
) -> Dict[str, Any]:
    sem = Semaphore(max_concurrency)
    analysis_cache: Dict[str, Dict[str, Any]] = {}
    depths: Dict[str, int] = {}
    visit_order: List[str] = []
    overflow: List[str] = []
    dependency_catalog: Dict[str, Dict[str, Dict[str, Any]]] = {
        "custom": {},
        "standard": {},
    }
    filtered_all: List[Dict[str, Any]] = []

    async def _analyze_guarded(path: str, need_content: bool = False) -> Dict[str, Any]:
        cached = analysis_cache.get(path)
        if cached and (not need_content or cached.get("content") is not None):
            return cached
        async with sem:
            analysis = await _analyze_single_file(path, used_ref, return_content=need_content)
        if cached:
            merged = dict(cached)
            for key, value in analysis.items():
                if key == "content" and not need_content:
                    continue
                merged[key] = value
            analysis_cache[path] = merged
            return merged
        analysis_cache[path] = analysis
        return analysis

    graph: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []
    seen_edges: Set[Tuple[str, str, Optional[str]]] = set()

    frontier: List[str] = [start_file]
    depth = 0
    if fanout_limit is None:
        fanout_cap: Optional[int] = DEFAULT_MAX_FANOUT
    elif fanout_limit <= 0:
        fanout_cap = None
    else:
        fanout_cap = fanout_limit

    def _edge_priority(entry: Dict[str, Any]) -> Tuple[int, str]:
        rank = {"high": 0, "medium": 1, "low": 2}.get(entry.get("impact"), 2)
        return (rank, entry.get("symbol") or entry.get("path") or "")

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

        analyses = await asyncio.gather(*[_analyze_guarded(path, include_sources) for path in level])

        next_frontier: List[str] = []
        for path, analysis in zip(level, analyses):
            if analysis.get("error"):
                continue

            depth_idx = depths.get(path, -1)
            deps = analysis.get("dependencies", []) or []
            filtered_local = analysis.get("filtered_dependencies", []) or []
            filtered_all.extend(filtered_local)
            method_index = _index_methods(analysis.get("methods"))

            classified_deps: List[Dict[str, Any]] = []
            for dep in deps:
                dep_path = dep.get("path")
                if not dep_path:
                    continue
                enriched = dict(dep)
                enriched.update(_classify_dependency_edge(dep, method_index))
                enriched["from"] = path
                enriched["depth_from_root"] = depth_idx + 1
                classified_deps.append(enriched)

                symbol = dep.get("symbol")
                category = _symbol_category(symbol)
                if category in dependency_catalog:
                    key = symbol or dep_path
                    dependency_catalog[category][key] = {
                        "symbol": symbol,
                        "type": dep.get("type"),
                        "path": dep_path,
                        "reason": dep.get("reason"),
                        "impact": enriched.get("impact"),
                        "roles": enriched.get("edge_roles"),
                    }

            classified_deps.sort(key=_edge_priority)

            fanout_used = 0
            for dep in classified_deps:
                if depth >= max_depth:
                    dep["skip_reason"] = dep.get("skip_reason") or "depth-limit"
                    continue
                target = dep.get("path")
                if not target or target in depths or target in next_frontier:
                    continue
                if not dep.get("should_traverse"):
                    continue
                if fanout_cap is not None and fanout_used >= fanout_cap:
                    dep["skip_reason"] = dep.get("skip_reason") or "fanout-limit"
                    continue
                if len(visit_order) + len(next_frontier) >= max_files:
                    overflow.append(target)
                    dep["skip_reason"] = dep.get("skip_reason") or "file-limit"
                    continue
                next_frontier.append(target)
                fanout_used += 1

            graph.append(
                {
                    "file": path,
                    "depth": depth_idx,
                    "dependencies": [dep.get("path") for dep in classified_deps if dep.get("path")],
                    "dependency_details": classified_deps,
                }
            )

            for dep in classified_deps:
                dep_path = dep.get("path")
                if not dep_path:
                    continue
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
                        "depth_from_root": dep.get("depth_from_root"),
                        "impact": dep.get("impact"),
                        "roles": dep.get("edge_roles"),
                        "should_traverse": dep.get("should_traverse"),
                        "skip_reason": dep.get("skip_reason"),
                    }
                )

        frontier = next_frontier
        depth += 1

    depth_completed = max(depths.values()) if depths else 0
    limit_reached = len(visit_order) >= max_files
    more_available = bool(frontier) or bool(overflow)

    pending = sorted(set(frontier + overflow))
    status = "complete" if not more_available else "partial"

    filtered_dedup: Dict[str, Dict[str, Any]] = {}
    for entry in filtered_all:
        key = (entry.get("symbol") or entry.get("reason") or "").lower()
        if key in filtered_dedup:
            continue
        filtered_dedup[key] = entry

    dependency_summary = {
        "custom": sorted(dependency_catalog["custom"].values(), key=lambda item: (item.get("symbol") or "")),
        "standard": sorted(dependency_catalog["standard"].values(), key=lambda item: (item.get("symbol") or "")),
        "filtered": sorted(filtered_dedup.values(), key=lambda item: (item.get("symbol") or "")),
    }

    sources: List[Dict[str, Any]] = []
    if include_sources:
        remaining = source_budget if source_budget and source_budget > 0 else None
        for path in visit_order:
            analysis = analysis_cache.get(path)
            if not analysis or analysis.get("content") is None:
                analysis = await _analyze_guarded(path, True)
            content = analysis.get("content")
            if not content:
                continue
            size_bytes = len(content.encode("utf-8"))
            if remaining is not None and size_bytes > remaining:
                continue
            sources.append(
                {
                    "path": path,
                    "ref": analysis.get("ref", used_ref),
                    "sha": analysis.get("sha", "unknown"),
                    "bytes": size_bytes,
                    "content": content,
                }
            )
            if remaining is not None:
                remaining -= size_bytes
                if remaining <= 0:
                    break

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
        "edges": edges,
        "fanout_limit": fanout_cap,
        "status": status,
        "dependency_summary": dependency_summary,
        "sources": sources,
    }
@app.post("/report.firsthop")
@app.post("/report.firsthop/")
@app.post("//report.firsthop")
@app.post("//report.firsthop/")
async def report_firsthop(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    env = get_env()
    start_file = payload.get("start_file")
    if not start_file:
        raise HTTPException(400, "Missing required field: start_file")

    used_ref: str = payload.get("ref") or env["REF"]
    if str(used_ref).upper() == "HEAD":
        used_ref = env["REF"]

    include_source_raw = payload.get("include_source")
    include_source = True if include_source_raw is None else bool(include_source_raw)

    analysis = await _analyze_single_file(start_file, used_ref, return_content=include_source)
    status = "ok" if not analysis.get("error") else "error"

    response: Dict[str, Any] = {
        "root": start_file,
        "ref": analysis.get("ref", used_ref),
        "sha": analysis.get("sha", "unknown"),
        "content_len": analysis.get("content_len", 0),
        "status": status,
        "error": analysis.get("error"),
    }
    if include_source:
        source_content = analysis.get("content")
        if source_content is not None:
            response["source"] = {
                "path": start_file,
                "ref": analysis.get("ref", used_ref),
                "sha": analysis.get("sha", "unknown"),
                "bytes": len(source_content.encode("utf-8")),
                "content": source_content,
            }
    response["approx_bytes"] = _approx_size(response)
    log.info(
        "[report.firsthop] root=%s approx_bytes=%s status=%s",
        start_file,
        response["approx_bytes"],
        status,
    )
    return response


@app.post("/report.dependencies")
@app.post("/report.dependencies/")
@app.post("//report.dependencies")
@app.post("//report.dependencies/")
async def report_dependencies(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    env = get_env()
    start_file = payload.get("start_file")
    if not start_file:
        raise HTTPException(400, "Missing required field: start_file")

    used_ref: str = payload.get("ref") or env["REF"]
    if str(used_ref).upper() == "HEAD":
        used_ref = env["REF"]

    analysis = await _analyze_single_file(start_file, used_ref, return_content=False)
    status = "ok" if not analysis.get("error") else "error"
    direct = analysis.get("dependencies", []) or []
    dependency_summary = _summarize_dependencies(direct, analysis.get("filtered_dependencies"))

    response = {
        "root": start_file,
        "ref": analysis.get("ref", used_ref),
        "sha": analysis.get("sha", "unknown"),
        "status": status,
        "error": analysis.get("error"),
        "total_dependencies": len(direct),
        "direct_dependencies": direct,
        "dependency_summary": dependency_summary,
        "filtered_dependencies": analysis.get("filtered_dependencies", []),
        "methods": analysis.get("methods", []),
        "field_usage": analysis.get("field_usage", []),
    }
    response["approx_bytes"] = _approx_size(response)
    log.info(
        "[report.dependencies] root=%s deps=%s approx_bytes=%s status=%s",
        start_file,
        len(direct),
        response["approx_bytes"],
        status,
    )
    return response


@app.post("/report.branch")
@app.post("/report.branch/")
@app.post("//report.branch")
@app.post("//report.branch/")
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

    fanout_limit: Optional[int] = None
    if "fanout_limit" in payload:
        raw_fanout = payload.get("fanout_limit")
        try:
            fanout_limit = int(raw_fanout)
        except Exception:
            fanout_limit = DEFAULT_MAX_FANOUT

    include_source = bool(payload.get("include_source", False))
    source_limit: Optional[int] = None
    if include_source:
        raw_limit = payload.get("include_source_limit")
        try:
            source_limit = int(raw_limit) if raw_limit is not None else DEFAULT_SOURCE_BUDGET
        except Exception:
            source_limit = DEFAULT_SOURCE_BUDGET
        if source_limit is not None and source_limit < 0:
            source_limit = None

    branch = await _collect_branch(
        start_file=start_file,
        used_ref=used_ref,
        max_depth=max_depth,
        max_files=max_files,
        max_concurrency=max_concurrency,
        include_sources=include_source,
        source_budget=source_limit,
        fanout_limit=fanout_limit,
    )

    branch["max_depth"] = max_depth
    branch["max_files"] = max_files
    branch["max_concurrency"] = max_concurrency
    branch["max_bytes"] = max_bytes
    if include_source:
        branch["include_source_limit"] = source_limit

    trimmed, truncated = _apply_size_budget(
        max_bytes,
        branch,
        [
            "edges",
            "graph",
            "sources",
        ],
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

# main.py
import os, base64, re, json
from typing import List, Dict, Any, Set
from fastapi import FastAPI, HTTPException, Query, Body
import httpx

app = FastAPI(title="ADO Repo Dependency Connector")

ADO_ORG     = os.environ["ADO_ORG"]
ADO_PROJECT = os.environ["ADO_PROJECT"]
ADO_REPO    = os.environ["ADO_REPO"]
ADO_PAT     = os.environ["ADO_PAT"]
DEFAULT_REF = os.getenv("DEFAULT_REF", "HEAD")

# ADO Git REST base
BASE = f"https://dev.azure.com/{ADO_ORG}/{ADO_PROJECT}/_apis/git/repositories/{ADO_REPO}"
APIv = "7.1-preview.1"

# kernel/system objects to skip
KERNEL_SKIP = { "FormRun", "Args", "Set", "SetEnumerator", "Global", "QueryBuildDataSource", "RunBase" }

def ado_headers() -> Dict[str, str]:
    # Basic auth with PAT (PAT as username or password both work with blank other field)
    token = base64.b64encode(f":{ADO_PAT}".encode()).decode()
    return {"Authorization": f"Basic {token}"}

async def get_item_content(path: str, ref: str) -> Dict[str, Any]:
    # GET item content as text
    params = {
        "path": f"/{path}" if not path.startswith("/") else path,
        "versionDescriptor.version": ref or DEFAULT_REF,
        "includeContent": "true",
        "api-version": APIv
    }
    url = f"{BASE}/items"
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(url, headers=ado_headers(), params=params)
        if r.status_code == 404:
            raise HTTPException(404, f"File not found: {path}@{ref}")
        r.raise_for_status()
        data = r.json()
        # ADO returns content under 'content' for text files
        content = data.get("content")
        if content is None:
            # Some responses require a second hop to blob:
            if "objectId" in data:
                blob_url = f"{BASE}/blobs/{data['objectId']}?api-version={APIv}"
                b = await client.get(blob_url, headers=ado_headers())
                b.raise_for_status()
                body = b.json()
                content = body.get("content")  # base64? sometimes text
        if content is None:
            raise HTTPException(413, "Unable to inline content; try a smaller file")
        # ADO may return base64; detect and decode if so
        try:
            # if base64, decode and ensure it's text
            decoded = base64.b64decode(content).decode("utf-8")
            content = decoded
        except Exception:
            pass
        sha = data.get("objectId") or data.get("commitId") or "unknown"
        return {"content": content, "sha": sha}

# ---- AX/D365 XPO parsing helpers ----

TYPE_HINTS = {
    "Tables/": "Table",
    "Classes/": "Class",
    "Forms/": "Form",
    "Maps/": "Map"
}

def guess_type(path: str, content: str) -> str:
    for prefix, t in TYPE_HINTS.items():
        if path.startswith(prefix):
            return t
    # inference if not in typed folder
    name = path.split("/")[-1].replace(".xpo", "")
    if re.search(r"\b(new|::construct|::run)\b", content):
        return "Class"
    if re.search(r"\b(select|insert|update|delete|validateWrite|validateDelete)\b", content):
        return "Table"
    if re.search(r"\bdesign\b|\bdataSource\b", content, re.IGNORECASE):
        return "Form"
    return "Unknown"

DEPENDENCY_PATTERNS = [
    # Table & DS usage
    (r"\b(select|insert|update|delete)\s+(\w+)", "Table", "statement"),
    (r"\b(\w+)\.DataSource\(", "Table", "DataSource()"),
    # Class new/construct/run/static calls
    (r"\b(\w+)::(construct|new|run)\b", "Class", "static-call"),
    (r"new\s+(\w+)\s*\(", "Class", "new"),
    # Method calls like PurchLine.update()
    (r"\b(\w+)\.(insert|update|delete|validateWrite|validateDelete)\s*\(", "Table", "crud-call"),
    # Form references (rare in code, but keep)
    (r"\b(form|design)\s+(\w+)", "Form", "form-metadata")
]

CRUD_SIGNATURE = re.compile(r"\b(\w+)\.(insert|update|delete|validateWrite|validateDelete)\s*\(", re.IGNORECASE)

def extract_dependencies(content: str) -> Dict[str, Any]:
    deps = []
    implicit = []
    seen: Set[str] = set()

    for pattern, kind, reason in DEPENDENCY_PATTERNS:
        for m in re.finditer(pattern, content, flags=re.IGNORECASE):
            symbol = m.group(2) if reason in ("statement","form-metadata") else m.group(1)
            sym_norm = symbol
            if sym_norm in KERNEL_SKIP:
                continue
            if sym_norm not in seen:
                seen.add(sym_norm)
                # We don’t know path yet; we’ll try to resolve via path heuristics
                deps.append({"path": f"UNKNOWN/{sym_norm}.xpo", "type": kind, "symbol": sym_norm, "reason": reason})

    for m in CRUD_SIGNATURE.finditer(content):
        table = m.group(1)
        method = m.group(2) + "()"
        implicit.append({"table": table, "method": method, "caller": "unknown", "line": m.start()})

    # business rules (very rough tokens; you can enrich as needed)
    business = []
    for ln, line in enumerate(content.splitlines(), start=1):
        if "validate" in line or "error(" in line or "ttsBegin" in line or "ttsCommit" in line:
            business.append({"line": ln, "context": line.strip()[:180]})

    return {"dependencies": deps, "implicit_crud": implicit, "business_rules": business}

def resolve_path_guess(symbol: str, kind: str) -> List[str]:
    # try common folders first
    candidates = []
    if kind == "Table":
        candidates += [f"Tables/{symbol}.xpo", f"DataDictionary/Tables/{symbol}.xpo"]
    elif kind == "Class":
        candidates += [f"Classes/{symbol}.xpo"]
    elif kind == "Form":
        candidates += [f"Forms/{symbol}.xpo"]
    else:
        candidates += [f"Classes/{symbol}.xpo", f"Tables/{symbol}.xpo", f"Forms/{symbol}.xpo", f"Maps/{symbol}.xpo"]
    return candidates

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/file")
async def file_get(path: str = Query(...), ref: str = Query(DEFAULT_REF)):
    data = await get_item_content(path, ref)
    return {"path": path, "ref": ref, "sha": data["sha"], "content": data["content"]}

@app.get("/deps")
async def deps_get(file: str = Query(...), ref: str = Query(DEFAULT_REF), page: int = 1, limit: int = 200):
    data = await get_item_content(file, ref)
    content = data["content"]
    parsed = extract_dependencies(content)

    # Try to resolve UNKNOWN paths by probing common folders
    resolved = []
    unresolved = []
    async with httpx.AsyncClient(timeout=30) as client:
        for d in parsed["dependencies"]:
            if not d["path"].startswith("UNKNOWN/"):
                resolved.append(d)
                continue
            symbol, kind = d["symbol"], d["type"]
            found_path = None
            for cand in resolve_path_guess(symbol, kind):
                url = f"{BASE}/items"
                params = {
                    "path": f"/{cand}",
                    "versionDescriptor.version": ref,
                    "api-version": APIv
                }
                r = await client.get(url, headers=ado_headers(), params=params)
                if r.status_code == 200:
                    found_path = cand
                    break
            if found_path:
                d["path"] = found_path
                resolved.append(d)
            else:
                unresolved.append(d)

    # crude pagination on resolved list
    start = (page - 1) * limit
    end = start + limit
    paged = resolved[start:end]
    total_pages = (len(resolved) + limit - 1) // limit

    return {
        "file": file,
        "ref": ref,
        "sha": data["sha"],
        "dependencies": paged,
        "business_rules": parsed["business_rules"],
        "implicit_crud": parsed["implicit_crud"],
        "unresolved": unresolved,
        "visited": [file],
        "skipped": [],  # we add kernel objects to skip list when encountered
        "page": page,
        "limit": limit,
        "total_dependencies": len(resolved),
        "total_pages": total_pages
    }

@app.post("/resolve")
async def resolve_post(payload: Dict[str, Any] = Body(...)):
    start_file = payload["start_file"]
    ref = payload.get("ref", DEFAULT_REF)
    max_depth = int(payload.get("max_depth", 5))
    visited: Set[str] = set(payload.get("visited", []))

    graph = []
    queue = [(start_file, 0)]
    implicit_all = []
    skipped = set()

    while queue:
        file, depth = queue.pop(0)
        if file in visited: 
            continue
        visited.add(file)

        try:
            node = await deps_get(file=file, ref=ref)  # reuse handler
        except HTTPException as e:
            # if missing, just record and continue
            graph.append({"file": file, "error": f"{e.status_code} {e.detail}"})
            continue

        deps = node["dependencies"]
        # mark kernel objects as skipped (we already filtered by symbol above)
        # collect implicit CRUD
        implicit_all.extend(node["implicit_crud"])

        graph.append({"file": file, "dependencies": [d["path"] for d in deps]})

        if depth < max_depth:
            for d in deps:
                next_file = d["path"]
                if next_file not in visited:
                    queue.append((next_file, depth + 1))

    return {
        "root": start_file,
        "depth": max_depth,
        "graph": graph,
        "implicit_crud": implicit_all,
        "skipped": sorted(list(skipped)),
        "visited": sorted(list(visited))
    }

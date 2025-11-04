import os, base64, re, json, logging
from typing import Dict, Any, List, Set, Tuple, Optional
from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
import httpx

app = FastAPI(title="ADO Repo Dependency Connector", version="1.0.6")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=False, allow_methods=["*"], allow_headers=["*"],
)
log = logging.getLogger("uvicorn.error")

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
        "User-Agent": "ado-git-crawler/1.0",
    }

def ado_base(env: Dict[str, str]) -> str:
    return f"https://dev.azure.com/{env['ORG']}/{env['PROJECT']}/_apis/git/repositories/{env['REPO']}"

@app.get("/health"); @app.get("/health/")
async def health(): return {"status": "ok"}

async def get_item_content(path: str, ref: Optional[str]) -> Dict[str, Any]:
    env = get_env(); base = ado_base(env)
    used_ref = ref or env["REF"]
    if str(used_ref).upper() == "HEAD": used_ref = env["REF"]
    params = {
        "path": f"/{path}" if not path.startswith("/") else path,
        "versionDescriptor.version": used_ref,
        "includeContent": "true",
        "api-version": env["API"],
    }
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(f"{base}/items", headers=ado_headers(env["PAT"]), params=params)
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
            raise HTTPException(502, "Azure DevOps returned non-JSON content.")
        content = data.get("content")
        if not content:
            raise HTTPException(404, "No content found (might be binary or empty).")
        try: content = base64.b64decode(content).decode("utf-8")
        except Exception: pass
        return {"content": content, "sha": data.get("objectId") or data.get("commitId") or "unknown", "ref": used_ref}

# ---------- Parser ----------
DEPENDENCY_PATTERNS = [
    (r"\b(select|insert|update|delete)\s+(\w+)", "Table", "statement"),
    (r"\b(\w+)\.DataSource\(", "Table", "DataSource()"),
    (r"\b(\w+)::(construct|new|run)\b", "Class", "static-call"),
    (r"\bnew\s+(\w+)\s*\(", "Class", "new"),
    (r"\b(\w+)\.(insert|update|delete|validateWrite|validateDelete)\s*\(", "Table", "crud-call"),
]
CRUD_SIGNATURE = re.compile(r"\b(\w+)\.(insert|update|delete|validateWrite|validateDelete)\s*\(", re.IGNORECASE)

KERNEL_SKIP = {"FormRun","Args","Set","SetEnumerator","Global","QueryBuildDataSource","RunBase"}
STOPWORDS = {"forupdate","firstonly","nofetch","this","the","recid","map","true","false","null"}

TYPE_DIR = {"Table":"Tables","Class":"Classes","Form":"Forms","Map":"Maps"}

def normalize_symbol(symbol: str) -> str:
    """Strip leading 'lcl'/'Lcl' when it denotes a local stub (e.g., lclPurchLine -> PurchLine)."""
    if len(symbol) > 3 and symbol[:3].lower() == "lcl" and symbol[3].isupper():
        return symbol[3:]
    return symbol

def default_path_for(symbol: str, kind: str) -> str:
    folder = TYPE_DIR.get(kind, "UNKNOWN")
    return f"{folder}/{symbol}.xpo"

def is_noise_symbol(symbol: str) -> bool:
    s = symbol.lower()
    return s in STOPWORDS or len(s) <= 2

def extract_dependencies(content: str) -> Dict[str, Any]:
    deps: List[Dict[str, Any]] = []; implicit: List[Dict[str, Any]] = []; business: List[Dict[str, Any]] = []
    seen: Set[Tuple[str, str]] = set()  # (kind, normalized_symbol)

    for pattern, kind, reason in DEPENDENCY_PATTERNS:
        for m in re.finditer(pattern, content, re.IGNORECASE):
            raw = m.group(2) if reason == "statement" else m.group(1)
            if not raw: continue
            if raw in KERNEL_SKIP or is_noise_symbol(raw): continue
            sym = normalize_symbol(raw)
            key = (kind, sym)
            if key in seen: continue
            seen.add(key)
            deps.append({
                "path": default_path_for(sym, kind),
                "type": kind,
                "symbol": sym,           # ← normalized symbol exposed
                "reason": reason
            })

    for m in CRUD_SIGNATURE.finditer(content):
        tbl = normalize_symbol(m.group(1))
        implicit.append({"table": tbl, "method": m.group(2) + "()", "caller": "unknown", "line": m.start()})

    for ln, line in enumerate(content.splitlines(), start=1):
        if any(k in line for k in ["validate","error(","ttsBegin","ttsCommit"]):
            business.append({"line": ln, "context": line.strip()[:160]})

    return {"dependencies": deps, "implicit_crud": implicit, "business_rules": business}

# ---------- Endpoints ----------
@app.get("/file"); @app.get("/file/")
async def file_get(path: str = Query(...), ref: Optional[str] = Query(None)):
    data = await get_item_content(path, ref)
    return {"path": path, "ref": data["ref"], "sha": data["sha"], "content": data["content"]}

@app.get("/deps"); @app.get("/deps/")
async def deps_get(file: str = Query(...), ref: Optional[str] = Query(None), page: int = 1, limit: int = 200):
    fetched = await get_item_content(file, ref)
    content, sha, used_ref = fetched["content"], fetched["sha"], fetched["ref"]
    parsed = extract_dependencies(content)
    start, end = (page - 1) * limit, (page - 1) * limit + limit
    paged = parsed["dependencies"][start:end]
    total = len(parsed["dependencies"])
    total_pages = (total + limit - 1) // limit if total else 1
    return {
        "file": file, "ref": used_ref, "sha": sha,
        "dependencies": paged,
        "business_rules": parsed["business_rules"],
        "implicit_crud": parsed["implicit_crud"],
        "unresolved": [d for d in parsed["dependencies"] if d["path"].startswith("UNKNOWN/")],
        "visited": [file], "skipped": [],
        "page": page, "limit": limit, "total_dependencies": total, "total_pages": total_pages,
    }

@app.post("/resolve"); @app.post("/resolve/")
async def resolve_post(payload: Dict[str, Any] = Body(...)):
    """
    Recursively resolves dependencies up to max_depth.
    Options:
      - ref
      - include_content (bool)
      - verify_paths (bool): probe ADO before recursing to avoid 404 branches
    """
    start_file: str = payload["start_file"]
    max_depth: int = int(payload.get("max_depth", 5))
    req_ref: Optional[str] = payload.get("ref")
    include_content: bool = bool(payload.get("include_content", False))
    verify_paths: bool = bool(payload.get("verify_paths", False))

    env = get_env()
    used_ref = (req_ref or env["REF"])
    if str(used_ref).upper() == "HEAD": used_ref = env["REF"]

    visited: Set[str] = set()
    graph: List[Dict[str, Any]] = []
    implicit_all: List[Dict[str, Any]] = []

    async def enqueue_node(file: str, depth: int, deps: Optional[List[Dict[str, Any]]], error: Optional[str]):
        node: Dict[str, Any] = {"file": file, "depth": depth, "error": error,
                                "dependencies": None if deps is None else [d["path"] for d in deps]}
        if include_content and error is None:
            try:
                content_obj = await get_item_content(file, used_ref)
                node["ref"] = content_obj["ref"]; node["sha"] = content_obj["sha"]; node["content"] = content_obj["content"]
            except Exception as e:
                node["content_error"] = str(e)
        graph.append(node)

    async def crawl(file: str, depth: int):
        if depth > max_depth: return
        if file in visited: return
        visited.add(file)
        try:
            node = await deps_get(file=file, ref=used_ref, page=1, limit=200)  # type: ignore
            deps = node["dependencies"]; implicit_all.extend(node["implicit_crud"])
            await enqueue_node(file, depth, deps, None)
            for d in deps:
                target = d["path"]
                if target in visited: continue
                if verify_paths:
                    try: await get_item_content(target, used_ref)
                    except Exception as e:
                        await enqueue_node(target, depth + 1, None, str(e)); continue
                await crawl(target, depth + 1)
        except Exception as e:
            await enqueue_node(file, depth, None, str(e))

    await crawl(start_file, 0)
    return {"root": start_file, "ref": used_ref, "depth": max_depth, "total_files": len(visited),
            "visited": sorted(list(visited)), "implicit_crud": implicit_all, "graph": graph, "status": "complete"}

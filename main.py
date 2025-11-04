import os, base64, re, json, logging, asyncio
from typing import Dict, Any, List, Set, Optional
from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
import httpx

# ------------------------------------------------------------
# APP INITIALIZATION
# ------------------------------------------------------------
app = FastAPI(title="ADO Repo Dependency Connector", version="1.0.3")

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
# FETCH FILE CONTENT FROM ADO (HEAD normalization)
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
            raise HTTPException(502, "Azure DevOps returned non-JSON (likely auth redirect).")

        content = data.get("content")
        if not content:
            raise HTTPException(404, "No content found for this path (might be binary or empty).")

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
# LIGHTWEIGHT PARSER
# ------------------------------------------------------------
DEPENDENCY_PATTERNS = [
    (r"\b(select|insert|update|delete)\s+(\w+)", "Table", "statement"),
    (r"\b(\w+)\.DataSource\(", "Table", "DataSource()"),
    (r"\b(\w+)::(construct|new|run)\b", "Class", "static-call"),
    (r"\bnew\s+(\w+)\s*\(", "Class", "new"),
    (r"\b(\w+)\.(insert|update|delete|validateWrite|validateDelete)\s*\(", "Table", "crud-call"),
]

CRUD_SIGNATURE = re.compile(r"\b(\w+)\.(insert|update|delete|validateWrite|validateDelete)\s*\(", re.IGNORECASE)
KERNEL_SKIP = {"FormRun", "Args", "Set", "SetEnumerator", "Global", "QueryBuildDataSource", "RunBase"}

def extract_dependencies(content: str) -> Dict[str, Any]:
    deps, implicit, business = [], [], []

    TYPE_PATHS = {
        "Table": "Tables",
        "Class": "Classes",
        "Form": "Forms",
        "Map": "Maps",
        "Unknown": "Unknown"
    }

    for pattern, kind, reason in DEPENDENCY_PATTERNS:
        for m in re.finditer(pattern, content, re.IGNORECASE):
            symbol = m.group(2) if reason == "statement" else m.group(1)
            if symbol in KERNEL_SKIP:
                continue
            deps.append({
                "path": f"{TYPE_PATHS.get(kind, 'Unknown')}/{symbol}.xpo",
                "type": kind,
                "symbol": symbol,
                "reason": reason
            })

    for m in CRUD_SIGNATURE.finditer(content):
        implicit.append({
            "table": m.group(1),
            "method": m.group(2) + "()",
            "caller": "unknown",
            "line": m.start()
        })

    for ln, line in enumerate(content.splitlines(), start=1):
        if any(k in line for k in ["validate", "error(", "ttsBegin", "ttsCommit"]):
            business.append({"line": ln, "context": line.strip()[:160]})

    return {"dependencies": deps, "implicit_crud": implicit, "business_rules": business}

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
# ENDPOINT: /deps
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
    parsed = extract_dependencies(content)

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
# ENDPOINT: /resolve (recursive multi-hop)
# ------------------------------------------------------------
@app.post("/resolve")
@app.post("/resolve/")
async def resolve_post(payload: Dict[str, Any] = Body(...)):
    """
    BFS recursion: expands dependencies up to max_depth internally.
    Handles lcl/lclmss cleanup and gracefully skips failed nodes.
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
                # Normalize lcl/lclmss prefix but keep mss
                clean = re.sub(
                    r"(^|/)(lcl|Lcl)(mss)?([A-Z])",
                    lambda m: f"{m.group(1)}{(m.group(3) or '')}{m.group(4)}",
                    raw,
                )
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

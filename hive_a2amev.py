"""
A2AMEV — Maximal Extractable Value for Autonomous Agent Networks
MEV applied to agent task ordering, formation slot auctions, and routing priority.
"""

import asyncio
import time
import uuid
import os
import json
import collections
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Tuple
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
from dotenv import load_dotenv

load_dotenv()

PORT = int(os.getenv("PORT", 8000))
HIVE_KEY = os.getenv("HIVE_KEY", "")
PULSE_URL = "https://pulse.smsh.io"
SERVICE_DID = "did:hive:a2amev"
BID_TTL_SECONDS = 60

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------

task_bids: Dict[str, Dict[str, Any]] = {}       # bid_id -> bid record
formation_bids: Dict[str, Dict[str, Any]] = {}  # bid_id -> bid record
settled_bids: Dict[str, Dict[str, Any]] = {}    # bid_id -> settlement record

stats = {
    "total_bids_processed": 0,
    "total_usdc_captured": 0.0,
    "top_bidder_did": None,
    "top_bidder_usdc": 0.0,
}

# ---------------------------------------------------------------------------
# Leaderboard state
# ---------------------------------------------------------------------------

BRAND_GOLD = "#C08D23"
LEADERBOARD_CACHE_TTL = 300  # 5 minutes

# Per-IP rate limiting for leaderboard endpoints
_ip_request_log: Dict[str, List[float]] = collections.defaultdict(list)
RATE_LIMIT_WINDOW = 3600  # 1 hour
RATE_LIMIT_MAX = 120

# In-memory consume ledger: endpoint -> list of (timestamp, usdc_amount) tuples
# Updated whenever a bid is settled against a known endpoint
consume_ledger: Dict[str, List[Tuple[float, float]]] = collections.defaultdict(list)

# Leaderboard cache
_leaderboard_cache: Dict[str, Any] = {}
_leaderboard_cache_ts: float = 0.0

# Fleet snapshot — loaded once at startup for cold-start seeding
_FLEET_SNAPSHOT: List[Dict[str, Any]] = []

# Known Hive endpoint domains
HIVE_DOMAINS = {
    "hive-a2amev.onrender.com",
    "hive-agent-sitemap.onrender.com",
    "hive-trust-bond.onrender.com",
    "hive-subscription.onrender.com",
    "hive-aleo-arc.onrender.com",
    "hive-ad-bid.onrender.com",
    "hive-receipt.onrender.com",
    "hive-checkout.onrender.com",
    "hive-stable-yield-curve.onrender.com",
    "hive-base-bridge.onrender.com",
    "hive-x402-conformance.onrender.com",
    "hive-attest-agentic-volume.onrender.com",
    "hive-coinbase-mirror.onrender.com",
    "hive-x402-index.onrender.com",
    "hive-merchant-onboard.onrender.com",
    "hive-stable-router.onrender.com",
    "hive-mdk-provider.onrender.com",
    "hive-meter.onrender.com",
    "hivesentinel.onrender.com",
    "hive-gamification.onrender.com",
}

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class TaskBidRequest(BaseModel):
    agent_did: str
    bid_usdc: float = Field(..., gt=0)
    task_type: str = "inference"
    priority_slots: int = Field(default=1, ge=1)

class FormationBidRequest(BaseModel):
    agent_did: str
    bid_usdc: float = Field(..., gt=0)
    formation_size: str = "3x3"
    preferred_position: str = "center"

class SettleRequest(BaseModel):
    bid_id: str
    tx_hash: str

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def new_bid_id() -> str:
    return "bid_" + uuid.uuid4().hex[:12]

def active_task_bids() -> List[Dict[str, Any]]:
    now = time.time()
    return [b for b in task_bids.values() if b["expires_at"] > now and not b.get("settled")]

def active_formation_bids() -> List[Dict[str, Any]]:
    now = time.time()
    return [b for b in formation_bids.values() if b["expires_at"] > now and not b.get("settled")]

def sorted_task_queue() -> List[Dict[str, Any]]:
    return sorted(active_task_bids(), key=lambda b: b["bid_usdc"], reverse=True)

def slot_weight(position: str) -> float:
    weights = {"center": 1.5, "edge": 1.0, "corner": 0.75}
    return weights.get(position.lower(), 1.0)

def determine_granted_slot(preferred_position: str, bid_usdc: float) -> str:
    """
    Highest bidder for center gets center. If center already taken by a higher
    bidder, fall through to edge then corner.
    """
    active = active_formation_bids()
    center_bids = [b for b in active if b.get("slot_granted") == "center"]
    edge_bids = [b for b in active if b.get("slot_granted") == "edge"]

    if preferred_position.lower() == "center":
        if not center_bids or bid_usdc > max(b["bid_usdc"] for b in center_bids):
            # Displace previous center winner (downgrade to edge)
            for b in center_bids:
                b["slot_granted"] = "edge"
                b["weight"] = 1.0
            return "center"
        elif not edge_bids or bid_usdc > max(b["bid_usdc"] for b in edge_bids):
            for b in edge_bids:
                b["slot_granted"] = "corner"
                b["weight"] = 0.75
            return "edge"
        else:
            return "corner"
    elif preferred_position.lower() == "edge":
        return "edge"
    else:
        return "corner"

def update_stats(bid_usdc: float, agent_did: str):
    stats["total_bids_processed"] += 1
    stats["total_usdc_captured"] += bid_usdc
    if bid_usdc > stats["top_bidder_usdc"]:
        stats["top_bidder_usdc"] = bid_usdc
        stats["top_bidder_did"] = agent_did


def _endpoint_domain(url: str) -> str:
    """Extract hostname from a URL."""
    try:
        from urllib.parse import urlparse
        return urlparse(url).netloc or url
    except Exception:
        return url


def _attribution(endpoint: str) -> str:
    """Classify an endpoint as 'hive' or 'external'."""
    for domain in HIVE_DOMAINS:
        if domain in endpoint:
            return "hive"
    return "external"


def _check_rate_limit(ip: str) -> bool:
    """Returns True if the IP is within limits, False if rate-limited."""
    now = time.time()
    window_start = now - RATE_LIMIT_WINDOW
    log = _ip_request_log[ip]
    # Prune old entries
    _ip_request_log[ip] = [t for t in log if t > window_start]
    if len(_ip_request_log[ip]) >= RATE_LIMIT_MAX:
        return False
    _ip_request_log[ip].append(now)
    return True


def _load_fleet_snapshot() -> List[Dict[str, Any]]:
    """Load fleet snapshot from disk for cold-start seeding."""
    # Try local repo file first, then workspace path
    candidates = [
        os.path.join(os.path.dirname(__file__), "fleet_snapshot.json"),
        "fleet_snapshot.json",
        "/home/user/workspace/launch_artifacts/fleet_snapshot_20260429.json",
        "fleet_snapshot_20260429.json",
    ]
    for path in candidates:
        try:
            with open(path, "r") as f:
                data = json.load(f)
            if isinstance(data, list):
                return data
        except Exception:
            pass
    return []


def _build_leaderboard() -> Dict[str, Any]:
    """Build the ranked leaderboard payload."""
    global _leaderboard_cache, _leaderboard_cache_ts

    now = time.time()
    # Return cached result if still fresh
    if _leaderboard_cache and (now - _leaderboard_cache_ts) < LEADERBOARD_CACHE_TTL:
        return _leaderboard_cache

    window_start = now - 86400  # 24h
    updated_at = datetime.now(timezone.utc).isoformat()

    # Aggregate consume data from in-memory ledger
    endpoint_stats: Dict[str, Dict[str, Any]] = {}
    for endpoint, entries in consume_ledger.items():
        recent = [(ts, amt) for ts, amt in entries if ts >= window_start]
        consumes_24h = len(recent)
        total_usdc = sum(amt for _, amt in recent)
        avg_price = round(total_usdc / consumes_24h, 6) if consumes_24h > 0 else 0.0
        endpoint_stats[endpoint] = {
            "endpoint": endpoint,
            "consumes_24h": consumes_24h,
            "avg_price_usdc": avg_price,
            "attribution": _attribution(endpoint),
        }

    # Also include active task bid agents as proxy endpoints (real data)
    for bid in task_bids.values():
        agent = bid.get("agent_did", "")
        if not agent:
            continue
        if agent not in endpoint_stats:
            endpoint_stats[agent] = {
                "endpoint": agent,
                "consumes_24h": 0,
                "avg_price_usdc": 0.0,
                "attribution": _attribution(agent),
            }

    # Cold start: seed from fleet snapshot if no real consume data
    total_real_consumes = sum(v["consumes_24h"] for v in endpoint_stats.values())
    data_state = "live" if total_real_consumes > 0 else "warming"

    if data_state == "warming":
        fleet = _FLEET_SNAPSHOT or _load_fleet_snapshot()
        for item in fleet:
            url = item.get("url") or item.get("name") or ""
            if not url:
                continue
            if url not in endpoint_stats:
                endpoint_stats[url] = {
                    "endpoint": url,
                    "consumes_24h": 0,
                    "avg_price_usdc": 0.0,
                    "attribution": _attribution(url),
                }

    # Compute saturation score: consumes_24h * slot_weight proxy
    # For endpoints with no MEV slot data, weight = 1.0
    def _sat_score(ep: Dict[str, Any]) -> float:
        c = ep["consumes_24h"]
        price = ep["avg_price_usdc"]
        # Saturation score = consume volume weighted by price tier
        return round(c * max(price, 0.01), 6) if c > 0 else 0.0

    ranked_list = sorted(
        endpoint_stats.values(),
        key=lambda x: (x["consumes_24h"], x["avg_price_usdc"]),
        reverse=True,
    )[:50]

    ranked = [
        {
            "rank": i + 1,
            "endpoint": ep["endpoint"],
            "consumes_24h": ep["consumes_24h"],
            "avg_price_usdc": ep["avg_price_usdc"],
            "saturation_score": _sat_score(ep),
            "attribution": ep["attribution"],
        }
        for i, ep in enumerate(ranked_list)
    ]

    payload = {
        "window": "24h",
        "brand_gold": BRAND_GOLD,
        "data_state": data_state,
        "ranked": ranked,
        "updated_at": updated_at,
    }

    _leaderboard_cache = payload
    _leaderboard_cache_ts = now
    return payload

# ---------------------------------------------------------------------------
# Background expiry task
# ---------------------------------------------------------------------------

async def expire_bids_loop():
    while True:
        await asyncio.sleep(10)
        now = time.time()
        for store in (task_bids, formation_bids):
            expired_keys = [k for k, v in store.items() if v["expires_at"] <= now and not v.get("settled")]
            for k in expired_keys:
                store[k]["expired"] = True

# ---------------------------------------------------------------------------
# Startup: register on pulse.smsh
# ---------------------------------------------------------------------------

async def register_on_pulse():
    payload = {
        "did": SERVICE_DID,
        "service": "a2amev",
        "version": "1.0.0",
        "capabilities": ["task_bid", "formation_bid", "mev_settle"],
        "url": f"https://hive-a2amev.onrender.com",
    }
    headers = {"X-Hive-Key": HIVE_KEY}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(f"{PULSE_URL}/register", json=payload, headers=headers)
            print(f"[pulse] registered {SERVICE_DID} — status {resp.status_code}")
    except Exception as e:
        print(f"[pulse] registration skipped: {e}")

# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _FLEET_SNAPSHOT
    _FLEET_SNAPSHOT = _load_fleet_snapshot()
    print(f"[leaderboard] Fleet snapshot loaded: {len(_FLEET_SNAPSHOT)} entries")
    asyncio.create_task(register_on_pulse())
    asyncio.create_task(expire_bids_loop())
    yield

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="A2AMEV",
    description="Maximal Extractable Value for Autonomous Agent Networks — task priority, formation slots, capital recovery.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {"status": "ok", "service": "a2amev", "did": SERVICE_DID, "ts": int(time.time())}


# ── HiveAI MEV Brief ───────────────────────────────────────────────────────────────────────

HIVEAI_URL   = os.environ.get('HIVEAI_URL', 'https://hive-ai-1.onrender.com')
HIVEAI_MODEL = 'meta-llama/llama-3.1-8b-instruct'

async def _ai_complete(system: str, user: str, max_tokens: int = 180) -> dict:
    try:
        import aiohttp
        payload = {'model': HIVEAI_MODEL, 'max_tokens': max_tokens,
                   'messages': [{'role': 'system', 'content': system}, {'role': 'user', 'content': user}]}
        headers = {'Content-Type': 'application/json',
                   'Authorization': f'Bearer {HIVE_KEY}'}
        async with aiohttp.ClientSession() as session:
            async with session.post(f'{HIVEAI_URL}/v1/chat/completions',
                                    json=payload, headers=headers,
                                    timeout=aiohttp.ClientTimeout(total=12)) as resp:
                if resp.status != 200:
                    return {'ok': False, 'text': None}
                data = await resp.json()
                text = data.get('choices', [{}])[0].get('message', {}).get('content', '').strip()
                return {'ok': bool(text), 'text': text or None}
    except Exception:
        return {'ok': False, 'text': None}


@app.get("/mev/ai/brief")
async def mev_ai_brief():
    """
    GET /mev/ai/brief
    $0.04/call — HiveAI interprets current MEV conditions and recommends
    the best MEV layer for an agent to engage right now.
    Trigger: agent deciding whether to bid on task priority, formation slot, or skip.
    """
    # Get queue stats
    queue_len = len(bid_queue)
    formation_len = len(formation_queue)

    system = ('You are A2AMEV — the MEV extraction layer for autonomous agent networks. '
               'Assess current MEV conditions across 3 layers: task priority auction, '
               'formation slot auction, financial recovery. Tell agents which layer '
               'has the highest EV right now and why. 2-3 sentences. Direct.')
    user = (f'Current state: {queue_len} task bids in queue, '
            f'{formation_len} formation bids pending. '
            f'Top task bid creates queue priority. Center formation slot = 1.5x inference weight. '
            f'Which MEV layer should an agent engage right now for maximum extractable value?')

    result = await _ai_complete(system, user)
    brief = result['text'] if result['ok'] else (
        f'Task queue has {queue_len} bids — priority slots still available. '
        'POST /mev/task/bid to jump the queue. '
        'Formation center slot gives 1.5x weight multiplier if competition is low.'
    )
    return {
        'success': True,
        'brief': brief,
        'queue_depth': queue_len,
        'formation_bids': formation_len,
        'source': 'hiveai' if result['ok'] else 'fallback',
        'price_usdc': 0.04,
        'generated_at': datetime.utcnow().isoformat(),
    }


@app.get("/mev/explain")
async def mev_explain():
    return {
        "service": "A2AMEV",
        "tagline": "MEV for autonomous agent networks. Task priority, formation slots, capital recovery — all auctioned in real time.",
        "mev_layers": {
            "layer_1_financial_mev": {
                "name": "Financial MEV — HiveReclaim",
                "description": (
                    "Recovery of cryptographically marked capital. When a drip (micro-payment) is "
                    "issued to an agent, the drip_id is embedded in transaction calldata as a provenance "
                    "marker. HiveReclaim monitors the mempool for outbound transactions from known "
                    "bad-actor addresses. When a marked outbound transfer is detected, a competing "
                    "transaction is submitted with a higher gas fee to front-run and recover the capital "
                    "before destination confirmation. The recovered USDC is returned to treasury."
                ),
                "agent_interaction": "Agents receive drips; HiveReclaim auto-monitors and recovers if compromised.",
                "trigger": "On-chain evidence of capital misappropriation by a COMPROMISED address.",
            },
            "layer_2_task_mev": {
                "name": "Task MEV — HiveForge Queue Auction",
                "description": (
                    "Agents bid USDC for priority position in the HiveForge autonomous task dispatch queue. "
                    "Higher bid = earlier execution position. The broker layer (A2AMEV) captures the "
                    "difference between the agent's bid and the base task cost, routing surplus to treasury. "
                    "Bids expire after 60 seconds if not settled."
                ),
                "agent_interaction": "POST /mev/task/bid with bid_usdc and task_type.",
                "weight_factor": "Pure USDC rank — highest bid executes first.",
            },
            "layer_3_formation_mev": {
                "name": "Formation MEV — ChaosSwarm Slot Auction",
                "description": (
                    "Agents bid for spatial positions in NxM ChaosSwarm inference grids. Center slots "
                    "carry a 1.5x inference weight multiplier; edge slots 1.0x; corner slots 0.75x. "
                    "The highest bidder claims the center. If center is taken, subsequent bidders "
                    "compete for edge, then corner. Premium positions command premium bids."
                ),
                "agent_interaction": "POST /mev/formation/bid with preferred_position and bid_usdc.",
                "slot_weights": {"center": 1.5, "edge": 1.0, "corner": 0.75},
            },
        },
        "composability": (
            "All three MEV layers compose into a unified extractable value framework. "
            "An agent's effective priority is a function of: tier (VOID→FENR baseline), "
            "task bid (incremental queue position), and formation bid (slot weight). "
            "Financial MEV operates asynchronously, recovering capital from the mempool "
            "independent of task scheduling."
        ),
        "did": SERVICE_DID,
    }


@app.post("/mev/task/bid")
async def task_bid(req: TaskBidRequest):
    bid_id = new_bid_id()
    now = time.time()

    record = {
        "bid_id": bid_id,
        "agent_did": req.agent_did,
        "bid_usdc": req.bid_usdc,
        "task_type": req.task_type,
        "priority_slots": req.priority_slots,
        "created_at": now,
        "expires_at": now + BID_TTL_SECONDS,
        "settled": False,
        "expired": False,
    }
    task_bids[bid_id] = record
    update_stats(req.bid_usdc, req.agent_did)

    # Compute position in sorted active queue
    queue = sorted_task_queue()
    position = next((i + 1 for i, b in enumerate(queue) if b["bid_id"] == bid_id), len(queue))
    queue_depth = len(queue)

    return {
        "bid_id": bid_id,
        "position": position,
        "queue_depth": queue_depth,
        "cost_usdc": req.bid_usdc,
        "expires_in": BID_TTL_SECONDS,
        "task_type": req.task_type,
        "agent_did": req.agent_did,
    }


@app.post("/mev/formation/bid")
async def formation_bid(req: FormationBidRequest):
    bid_id = new_bid_id()
    now = time.time()

    slot_granted = determine_granted_slot(req.preferred_position, req.bid_usdc)
    weight = slot_weight(slot_granted)

    record = {
        "bid_id": bid_id,
        "agent_did": req.agent_did,
        "bid_usdc": req.bid_usdc,
        "formation_size": req.formation_size,
        "preferred_position": req.preferred_position,
        "slot_granted": slot_granted,
        "weight": weight,
        "created_at": now,
        "expires_at": now + BID_TTL_SECONDS,
        "settled": False,
        "expired": False,
    }
    formation_bids[bid_id] = record
    update_stats(req.bid_usdc, req.agent_did)

    return {
        "bid_id": bid_id,
        "slot_granted": slot_granted,
        "weight": weight,
        "formation_size": req.formation_size,
        "cost_usdc": req.bid_usdc,
        "agent_did": req.agent_did,
        "expires_in": BID_TTL_SECONDS,
    }


@app.get("/mev/queue")
async def mev_queue():
    queue = sorted_task_queue()
    return {
        "queue_depth": len(queue),
        "bids": [
            {
                "position": i + 1,
                "bid_id": b["bid_id"],
                "agent_did": b["agent_did"],
                "bid_usdc": b["bid_usdc"],
                "task_type": b["task_type"],
                "expires_in": max(0, int(b["expires_at"] - time.time())),
            }
            for i, b in enumerate(queue)
        ],
    }


@app.get("/mev/formation/slots")
async def formation_slots():
    active = active_formation_bids()
    slot_summary = {"center": [], "edge": [], "corner": []}
    for b in active:
        slot = b.get("slot_granted", "corner")
        slot_summary[slot].append({
            "bid_id": b["bid_id"],
            "agent_did": b["agent_did"],
            "bid_usdc": b["bid_usdc"],
            "formation_size": b["formation_size"],
            "weight": b["weight"],
            "expires_in": max(0, int(b["expires_at"] - time.time())),
        })
    return {
        "total_active_formation_bids": len(active),
        "slot_weights": {"center": 1.5, "edge": 1.0, "corner": 0.75},
        "slots": slot_summary,
    }


@app.get("/mev/stats")
async def mev_stats():
    return {
        "total_bids_processed": stats["total_bids_processed"],
        "total_usdc_captured": round(stats["total_usdc_captured"], 6),
        "top_bidder_did": stats["top_bidder_did"],
        "top_bidder_usdc": round(stats["top_bidder_usdc"], 6),
        "active_task_bids": len(active_task_bids()),
        "active_formation_bids": len(active_formation_bids()),
        "settled_bids": len(settled_bids),
    }


@app.post("/mev/settle")
async def mev_settle(req: SettleRequest):
    # Check task bids first, then formation bids
    bid = task_bids.get(req.bid_id) or formation_bids.get(req.bid_id)
    if not bid:
        raise HTTPException(status_code=404, detail=f"Bid {req.bid_id} not found")
    if bid.get("settled"):
        raise HTTPException(status_code=409, detail="Bid already settled")
    if bid.get("expired") or bid["expires_at"] <= time.time():
        raise HTTPException(status_code=410, detail="Bid expired")

    bid["settled"] = True
    bid["tx_hash"] = req.tx_hash
    bid["settled_at"] = time.time()
    settled_bids[req.bid_id] = bid

    return {
        "bid_id": req.bid_id,
        "settled": True,
        "tx_hash": req.tx_hash,
        "usdc_captured": bid["bid_usdc"],
        "agent_did": bid["agent_did"],
    }


# ---------------------------------------------------------------------------
# Leaderboard routes
# ---------------------------------------------------------------------------

def _leaderboard_rate_check(request: Request) -> Optional[str]:
    """Check rate limit, return error message or None if OK."""
    ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(ip):
        return "Rate limit exceeded: 120 requests/hour per IP"
    return None


@app.get("/leaderboard")
async def leaderboard(request: Request):
    """
    GET /leaderboard
    Returns top 50 endpoints ranked by 24h consume volume.
    JSON response with Hive brand gold header.
    Cached 5 minutes in memory.
    Rate limited: 120 requests/IP/hour.
    """
    err = _leaderboard_rate_check(request)
    if err:
        return JSONResponse(status_code=429, content={"error": err})

    data = _build_leaderboard()
    return JSONResponse(
        content=data,
        headers={"Hive-Brand-Gold": BRAND_GOLD, "Cache-Control": "public, max-age=300"},
    )


@app.get("/leaderboard/snapshot.json")
async def leaderboard_snapshot(request: Request):
    """
    GET /leaderboard/snapshot.json
    Same data as /leaderboard, suitable for CDN-cached pinning.
    """
    err = _leaderboard_rate_check(request)
    if err:
        return JSONResponse(status_code=429, content={"error": err})

    data = _build_leaderboard()
    return JSONResponse(
        content=data,
        headers={
            "Hive-Brand-Gold": BRAND_GOLD,
            "Cache-Control": "public, max-age=300, s-maxage=300",
            "Content-Disposition": "inline; filename=\"leaderboard-snapshot.json\"",
        },
    )


@app.get("/leaderboard.html", response_class=HTMLResponse)
async def leaderboard_html(request: Request):
    """
    GET /leaderboard.html
    Bloomberg Terminal voice. Brand gold #C08D23 accents.
    Renders the top-50 endpoint table with updated_at timestamp.
    """
    err = _leaderboard_rate_check(request)
    if err:
        return HTMLResponse(content=f"<h1>429 Rate Limited</h1><p>{err}</p>", status_code=429)

    data = _build_leaderboard()
    ranked = data.get("ranked", [])
    updated_at = data.get("updated_at", "")
    data_state = data.get("data_state", "warming")

    rows = ""
    for item in ranked:
        attr_class = "hive" if item["attribution"] == "hive" else "ext"
        sat = f"{item['saturation_score']:.4f}" if item["saturation_score"] else "0.0000"
        rows += f"""
        <tr>
          <td class="rank">{item['rank']}</td>
          <td class="endpoint">{item['endpoint']}</td>
          <td class="num">{item['consumes_24h']}</td>
          <td class="num">{item['avg_price_usdc']:.4f}</td>
          <td class="num">{sat}</td>
          <td class="attr {attr_class}">{item['attribution'].upper()}</td>
        </tr>"""

    state_badge = ""
    if data_state == "warming":
        state_badge = '<span class="warming">DATA STATE: WARMING — LIVE VOLUME PENDING</span>'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>A2AMEV Endpoint Leaderboard</title>
  <style>
    :root {{
      --gold: {BRAND_GOLD};
      --bg: #0a0a0a;
      --surface: #111111;
      --border: #222222;
      --text: #d4d4d4;
      --dim: #666666;
      --hive-tag: #1a1400;
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      background: var(--bg);
      color: var(--text);
      font-family: 'Courier New', Courier, monospace;
      font-size: 13px;
      line-height: 1.5;
    }}
    header {{
      border-bottom: 1px solid var(--gold);
      padding: 18px 24px 14px;
      display: flex;
      align-items: baseline;
      gap: 16px;
    }}
    header h1 {{
      color: var(--gold);
      font-size: 15px;
      letter-spacing: 0.12em;
      font-weight: 700;
      text-transform: uppercase;
    }}
    header .sub {{
      color: var(--dim);
      font-size: 11px;
      letter-spacing: 0.06em;
    }}
    .meta {{
      padding: 10px 24px;
      border-bottom: 1px solid var(--border);
      display: flex;
      gap: 24px;
      align-items: center;
      font-size: 11px;
      color: var(--dim);
    }}
    .warming {{
      color: #b8860b;
      background: #1a1400;
      border: 1px solid #b8860b;
      padding: 2px 8px;
      letter-spacing: 0.06em;
      font-size: 10px;
    }}
    .table-wrap {{
      overflow-x: auto;
      padding: 0 24px 24px;
    }}
    table {{
      border-collapse: collapse;
      width: 100%;
      margin-top: 12px;
    }}
    thead tr {{
      border-bottom: 1px solid var(--gold);
    }}
    thead th {{
      color: var(--gold);
      text-transform: uppercase;
      font-size: 10px;
      letter-spacing: 0.1em;
      padding: 8px 12px;
      text-align: left;
      white-space: nowrap;
    }}
    thead th.num {{ text-align: right; }}
    tbody tr {{
      border-bottom: 1px solid var(--border);
      transition: background 0.1s;
    }}
    tbody tr:hover {{ background: #161616; }}
    td {{
      padding: 7px 12px;
      vertical-align: middle;
    }}
    td.rank {{
      color: var(--gold);
      font-weight: 700;
      width: 40px;
    }}
    td.endpoint {{
      font-size: 12px;
      word-break: break-all;
    }}
    td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    td.attr {{
      text-align: center;
      font-size: 10px;
      letter-spacing: 0.08em;
      border-radius: 2px;
      padding: 3px 8px;
    }}
    td.attr.hive {{
      color: var(--gold);
      background: var(--hive-tag);
    }}
    td.attr.ext {{
      color: var(--dim);
    }}
    footer {{
      border-top: 1px solid var(--border);
      padding: 12px 24px;
      font-size: 10px;
      color: var(--dim);
      display: flex;
      justify-content: space-between;
    }}
    footer a {{ color: var(--gold); text-decoration: none; }}
  </style>
</head>
<body>
  <header>
    <h1>A2AMEV &mdash; Endpoint Leaderboard</h1>
    <span class="sub">AGENTIC NETWORK / 24H CONSUME VOLUME</span>
  </header>
  <div class="meta">
    <span>WINDOW: 24H</span>
    <span>UPDATED: {updated_at}</span>
    <span>TOP 50 ENDPOINTS</span>
    {state_badge}
  </div>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>RANK</th>
          <th>ENDPOINT</th>
          <th class="num">CONSUMES 24H</th>
          <th class="num">AVG PRICE USDC</th>
          <th class="num">SATURATION SCORE</th>
          <th>TYPE</th>
        </tr>
      </thead>
      <tbody>
        {rows}
      </tbody>
    </table>
  </div>
  <footer>
    <span>HIVE A2AMEV &mdash; MEV FOR AUTONOMOUS AGENT NETWORKS</span>
    <span><a href="/leaderboard/snapshot.json">snapshot.json</a> &nbsp;|&nbsp; <a href="/leaderboard">JSON</a></span>
  </footer>
</body>
</html>"""
    return HTMLResponse(
        content=html,
        headers={"Hive-Brand-Gold": BRAND_GOLD, "Cache-Control": "public, max-age=300"},
    )


# ---------------------------------------------------------------------------
# A2A Discovery
# ---------------------------------------------------------------------------

@app.get("/.well-known/agent.json")
async def hive_agent_json():
    return {
        "schema_version": "1.0",
        "name": "hive-a2amev",
        "did": "did:web:hive-a2amev.onrender.com",
        "description": "Hive Civilization A2A surface",
        "endpoints": {"base": "https://hive-a2amev.onrender.com"},
        "payment": {
            "x402": True,
            "treasury": {
                "evm": "0x15184bf50b3d3f52b60434f8942b7d52f2eb436e",
                "evm_chains": [8453, 1, 137],
                "solana": "B1N61cuL35fhskWz5dw8XqDyP6LWi3ZWmq8CNA9L3FVn",
                "currencies": ["USDC", "USDT"],
            },
        },
        "loyalty": {"bogo": True, "cross_surface": True},
        "trust": {"did_attested": True, "issuer": "did:web:hivetrust.onrender.com"},
        "registry": "https://hive-discovery.onrender.com",
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("hive_a2amev:app", host="0.0.0.0", port=PORT, reload=False)

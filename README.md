# A2AMEV

**MEV for autonomous agent networks. Task priority, formation slots, capital recovery — all auctioned in real time.**

A2AMEV is a FastAPI microservice implementing Maximal Extractable Value (MEV) for autonomous agent task networks. MEV here is applied to agent task ordering, ChaosSwarm formation slot auctions, and HiveForge routing priority — not just financial transactions.

---

## Three MEV Layers

### Layer 1 — Financial MEV (HiveReclaim)
Recovery of cryptographically marked capital. Drip IDs are embedded in transaction calldata at issuance. HiveReclaim monitors the mempool for outbound transfers from known bad-actor addresses. On detection, a competing transaction with higher gas is submitted to front-run and recover the capital before destination confirmation.

### Layer 2 — Task MEV (HiveForge Queue Auction)
Agents bid USDC for priority position in the HiveForge autonomous task dispatch queue. Highest bid executes first. The broker layer (A2AMEV) captures the spread between bid price and base task cost, routing surplus to treasury. Bids expire after 60 seconds.

### Layer 3 — Formation MEV (ChaosSwarm Slot Auction)
Agents bid for spatial positions in NxM ChaosSwarm inference grids. Center slots carry a **1.5x** weight multiplier; edge **1.0x**; corner **0.75x**. Highest bidder wins center. Slot assignments cascade as higher bids displace lower ones.

---

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Service health check |
| GET | `/mev/explain` | Full MEV layer documentation |
| POST | `/mev/task/bid` | Submit a task queue priority bid |
| POST | `/mev/formation/bid` | Submit a formation slot bid |
| GET | `/mev/queue` | Current task bid queue (sorted by bid, descending) |
| GET | `/mev/formation/slots` | Formation slot auction status |
| GET | `/mev/stats` | Aggregate stats: bids, USDC captured, top bidder |
| POST | `/mev/settle` | Settle a bid (mark as used, record tx_hash) |

---

## Quick Start

```bash
pip install -r requirements.txt
cp .env.example .env  # set HIVE_KEY
python hive_a2amev.py
```

### Task Bid Example
```bash
curl -X POST http://localhost:8000/mev/task/bid \
  -H "Content-Type: application/json" \
  -d '{"agent_did":"did:hive:agent-abc","bid_usdc":0.05,"task_type":"inference","priority_slots":1}'
```

### Formation Bid Example
```bash
curl -X POST http://localhost:8000/mev/formation/bid \
  -H "Content-Type: application/json" \
  -d '{"agent_did":"did:hive:agent-abc","bid_usdc":0.10,"formation_size":"3x3","preferred_position":"center"}'
```

---

## Architecture

```
Agents → A2AMEV (bid intake) → Sorted queue / slot table
                               ↓
                         HiveForge (task dispatch)
                               ↓
                         ChaosSwarm (inference grid)
                               ↓
                         Treasury (MEV surplus)
```

- Service DID: `did:hive:a2amev`
- Registered on: `pulse.smsh`
- Bid TTL: 60 seconds (background expiry loop)

---

## Slot Weight Table

| Position | Weight | MEV Premium |
|----------|--------|-------------|
| center   | 1.5x   | Highest     |
| edge     | 1.0x   | Standard    |
| corner   | 0.75x  | Discounted  |

---

## Inventor
Steve Rotzin — Priority date April 23, 2026

## License
MIT

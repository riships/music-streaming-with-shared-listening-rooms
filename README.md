# Listen Together: Music Streaming with Shared Listening Rooms

A high-scale, ultra-low-latency music streaming platform designed for shared, synchronized listening rooms. The platform decouples the high-bandwidth audio delivery plane from the low-overhead real-time control plane using an **Anchor-Based Playback Model**, enabling up to 2 million concurrent listeners and 5,000 listeners per room with tighter than 200 ms synchronization drift.

> 📖 **Architectural Deep-Dive:** For comprehensive mathematical formulations, capacity calculations, full database DDLs, Lua lease scripts, sequence diagrams, and failure scenario matrices, refer to the authoritative [design-doc.md](design-doc.md).

---

## Table of Contents

- [Core Capabilities](#core-capabilities)
- [How It Works (Plain English Guide)](#how-it-works-plain-english-guide)
  - [The Core Problem](#the-core-problem)
  - [Pillar 1: The Virtual Timeline (The Anchor Model)](#pillar-1-the-virtual-timeline-the-anchor-model)
  - [Pillar 2: Aligning the Clocks (Cristian's Algorithm)](#pillar-2-aligning-the-clocks-cristians-algorithm)
  - [Pillar 3: Glitch-Free Drift Correction (Micro-Pitch Nudging)](#pillar-3-glitch-free-drift-correction-micro-pitch-nudging)
  - [Hardware & Bluetooth Delay Compensation](#hardware--bluetooth-delay-compensation)
  - [Synchronization Summary Cheat-Sheet](#synchronization-summary-cheat-sheet)
- [System Architecture](#system-architecture)
- [Repository & Monorepo Layout](#repository--monorepo-layout)
- [Technology Stack](#technology-stack)
- [API & WebSocket Interface Reference](#api--websocket-interface-reference)
  - [REST API Endpoints](#rest-api-endpoints)
  - [WebSocket Control Protocol](#websocket-control-protocol)
- [Service Level Objectives (SLOs)](#service-level-objectives-slos)
- [Getting Started & Local Development](#getting-started--local-development)
  - [Prerequisites](#prerequisites)
  - [Environment Configuration](#environment-configuration)
  - [Installation & Setup](#installation--setup)
  - [Running the Applications](#running-the-applications)
  - [Testing & Quality Assurance](#testing--quality-assurance)
- [Deep-Dive Architecture Index](#deep-dive-architecture-index)
- [License](#license)

---

## Core Capabilities

- **Sub-200ms Synchronized Audio:** Cristian's NTP-style algorithm aligns client clocks with the server; imperceptible 2% micro-pitch rate nudges (1.02x / 0.98x) eliminate drift without audible pops, clicks, or buffering freezes.
- **Ultra-Thin Room Layer:** Rooms never stream audio directly. A room is an in-memory actor maintaining a single playback anchor. 200,000 active rooms consume only ~2 GB of memory in Redis.
- **Decoupled Multi-CDN Audio Delivery:** Audio is packaged into 4-rung CMAF fMP4 segments (HLS/DASH) delivered via multi-CDN edge POPs. Coalesced requests from synchronized room listeners hit edge caches with ≥95% efficiency.
- **Single-Writer Actor Concurrency:** Monotonic fencing epochs and atomic Redis Lua lease locks prevent distributed write conflicts and race conditions.
- **High-Throughput Services:** Fastify-backed NestJS REST APIs for metadata and raw `ws` WebSocket shards supporting 5,000–10,000 sockets per container node.
- **Real-Time OLAP Telemetry:** High-frequency drift and latency telemetry events streamed through Kafka into ClickHouse for global sync error distribution monitoring.

---

## How It Works (Plain English Guide)

### The Core Problem

If 5,000 listeners are in a room:
1. **The server cannot stream audio to everyone:** Transcoding and sending 5,000 live audio streams in real time would crush server bandwidth and prevent individual listeners from adapting their bitrate to cellular or Wi-Fi conditions.
2. **The server cannot constantly broadcast song positions:** Sending *"we are at 01:23.456"* 10 times a second to 500,000 connected sockets would overload the network, and messages would arrive late anyway due to network jitter.
3. **Devices have wrong clocks:** User A's phone might be set 3 seconds ahead, while User B's laptop is 1.5 seconds behind.

To solve this, our Synchronization Engine operates on **three clean pillars**:

```
+-----------------------------------------------------------------------------------+
| 1. THE ANCHOR MODEL (Virtual Timeline)                                            |
|    "The train left at 12:00:00 at 60 mph. Calculate where it is using time."      |
+-----------------------------------------------------------------------------------+
                                       │
                                       ▼
+-----------------------------------------------------------------------------------+
| 2. CRISTIAN'S ALGORITHM (Aligning the Clocks)                                     |
|    "Figure out the exact server time regardless of what your phone clock says."   |
+-----------------------------------------------------------------------------------+
                                       │
                                       ▼
+-----------------------------------------------------------------------------------+
| 3. DRIFT CONTROLLER (Glitch-Free Micro-Pitch Nudge)                               |
|    "If you fall 50ms behind, smoothly glide at 1.02x speed — no audio clicks."   |
+-----------------------------------------------------------------------------------+
```

---

### Pillar 1: The Virtual Timeline (The Anchor Model)

Instead of continuous position broadcasts, the server only emits a message when state changes (Play, Pause, Seek, or Next Track). This state is called an **Anchor**:

```typescript
// Emitted once on state change
roomState = {
  trackId: "track-xyz",
  playState: "PLAYING",
  anchorPosMs: 30000,           // Song was at 30.000 seconds
  anchorServerTimeMs: 17000000  // At this exact server timestamp
}
```

Whenever a listener's browser needs to know where the song should be right now, it performs a simple calculation:

> **Expected Position** = Track starting position + (Current server time − Anchor timestamp)

**Example in plain numbers:**
- The host hit Play when the track was at **30 seconds**.
- The server notes the timestamp when this happened.
- If **15 seconds** have elapsed on the server since that action, any listener's browser knows the track should currently be at **45 seconds** (30s + 15s).

> 💡 **Train Schedule Analogy:**  
> Instead of calling the train station every 10 seconds to ask *"Where is the train now?"*, the timetable says:  
> *"The train departed Mile 0 at exactly 12:00:00 PM traveling at 60 mph."*  
> As long as you know what time it is, you can calculate the exact mile marker anytime without contacting the station again.

---

### Pillar 2: Aligning the Clocks (Cristian's Algorithm)

To know how much time has passed on the server, the listener's browser must know the server's exact current time. However, device clocks cannot be trusted—a user's phone or laptop clock might be off by several seconds or minutes.

The client solves this with a quick ping-pong exchange over WebSocket:

```
Client sends Ping (records client time: 1,000 ms)
   ─── travels over the network (takes ~40 ms) ───►
Server receives & replies with its current time (5,040 ms)
   ◄─── travels back to client (takes ~40 ms) ───
Client receives Pong (client time is now: 1,080 ms)
```

**How the client figures out real server time step-by-step:**
1. **Measure Round-Trip Time:** The entire ping-pong round-trip took 80 ms (from 1,000 ms to 1,080 ms).
2. **Estimate One-Way Network Delay:** Assuming network travel is roughly equal in both directions, the response took about half that time—approximately 40 ms—to travel from the server back to the client.
3. **Calculate Clock Difference (Offset):** When the server sent its timestamp of 5,040 ms, it took 40 ms to arrive. That means when the client received the message (at local time 1,080 ms), the server clock was actually at 5,080 ms. Comparing the two (5,080 ms − 1,080 ms), the client knows the server is exactly **4,000 ms ahead** of its local clock.
4. **Get Server Time Anytime:** From that point forward, the client simply adds 4,000 ms to its own clock to know the exact server time instantly, with no extra network requests needed.

The client performs this ping-pong 8 times when first joining to discard temporary Wi-Fi latency spikes, and sends a quick background ping every 30 seconds to stay aligned.

---

### Pillar 3: Glitch-Free Drift Correction (Micro-Pitch Nudging)

Every 5 seconds, the browser compares where its audio player is currently playing against where the room says it should be. The difference between the two is called **Playback Drift**.

For example, if your audio player is at 01:15.000, but the room calculation says playback should be at 01:15.060, you are **60 milliseconds behind**.

Here is how the player corrects drift smoothly:

```
Drift Amount:
  0 ms ─────────────── 20 ms ──────────────────────── 200 ms ───────────────────────►
[    1. IN-SYNC      ] [     2. SMOOTH GLIDE         ] [        3. HARD JUMP         ]
  No change needed.      Nudge speed to 1.02x / 0.98x.   Jump straight to current time.
  Sounds identical.      Smooth and inaudible.           Connection dropped or tab froze.
```

1. **Zone 1: In-Sync (under 20 ms drift)**
   - Human ears cannot distinguish sound arrival differences under 20–30 milliseconds.
   - **Action:** Do nothing. You are in perfect sync.

2. **Zone 2: Smooth Glide (20 ms to 200 ms drift)**
   - **Why NOT just skip or seek?** Jumping to a new timestamp creates an audible pop, click, or momentary pause that ruins the listening experience.
   - **The Smooth Solution:** The player temporarily shifts playback speed by 2%—playing at **1.02x** speed if lagging behind, or **0.98x** speed if ahead.
   - A 2% speed adjustment is completely imperceptible to human ears (pitch and tempo sound normal). Within 2 to 3 seconds, your player gently catches up to the room and returns to normal 1.00x speed without any audio glitches.

3. **Zone 3: Hard Jump (over 200 ms drift)**
   - If the listener received a phone call, minimized the browser on mobile, or hit a Wi-Fi dead zone, the lag is too large to catch up with a 2% nudge.
   - **Action:** Jump directly to the room's current position with a tiny buffer lead (50 ms) to resume playing immediately.

---

### Hardware & Bluetooth Delay Compensation

Even if the browser plays audio at the exact millisecond, **Bluetooth headphones** and operating system audio drivers add **120 ms to 220 ms of physical delay** before the sound actually reaches the user's ears.

To prevent Bluetooth users from hearing the music a fraction of a second later than wired listeners:
- The web app asks the browser for its audio hardware latency (`AudioContext.outputLatency`).
- It automatically subtracts that delay from the playback calculations, so sound hits everyone's ears at the exact same beat.

---

### Synchronization Summary Cheat-Sheet

| Mechanism | Problem It Solves | Plain English Explanation |
| :--- | :--- | :--- |
| **Anchor Model** | Avoids continuous server position broadcasts. | Server emits starting position and timestamp once on state changes; clients calculate the rest. |
| **Cristian's Sync** | Solves inaccurate local client device clocks. | 8 quick ping-pong messages measure network transit delay and find the exact difference between client and server clocks. |
| **Micro-Pitch Nudge** | Solves minor drift without audio clicks/stutters. | Temporarily plays at 1.02x or 0.98x speed until seamlessly aligned, avoiding skips and pops. |
| **Hard Jump** | Solves major lag after app freeze or network drop. | Jumps straight to the room's current position with a 50 ms buffer lead. |
| **AudioContext Latency**| Solves Bluetooth & audio driver hardware delay. | Automatically subtracts hardware delay so Bluetooth and wired listeners hear the beat simultaneously. |

---

## System Architecture

```
                             +-----------------------+
                             |   Audio Ingest Bucket |
                             +-----------+-----------+
                                         |
                                         v
                         +-------------------------------+
                         | Transcode Workers (ffmpeg)    |
                         +---------------+---------------+
                                         |
                                         v
                         +-------------------------------+
                         | Multi-Region Object Storage   |
                         +---------------+---------------+
                                         |
                                         v
                         +-------------------------------+
                         | Multi-CDN Edge Shield & POPs  |
                         +---------------+---------------+
                                         |
                                         | Audio Segments (HLS/DASH)
                                         v
+-----------------------------------------------------------------------------------------+
|                                    Client (Browser / Mobile)                            |
|  [hls.js Player Engine] <---> [Clock Sync & Drift Controller] <---> [Zustand State]    |
+--------------------------------------------+--------------------------------------------+
       ^                                     |
       | REST / Fastify Metadata             | WebSocket Control & Clock Frames
       v                                     v
+-----------------------------+       +---------------------------------------------------+
| NestJS REST API Gateway     |       | NestJS WebSocket Gateway (ws engine)              |
| (Auth, Catalog, Playlists)  |       | 50-100 Shards @ 5-10k sockets/node                |
+--------------+--------------+       +-------------------------+-------------------------+
               |                                                |
               |                                                | IPC / Redis PubSub
               v                                                v
+-----------------------------+       +---------------------------------------------------+
| PostgreSQL (Prisma ORM)     |       | Room Actor Service (In-Memory State + Shard Lease)|
| Primary + Regional Replicas |       | Fenced Epochs & State Checkpoints                 |
+--------------+--------------+       +-------------------------+-------------------------+
               |                                                |
               | CDC (Debezium)                                 | Checkpoint & Lease Locks
               v                                                v
+-----------------------------+       +---------------------------------------------------+
| Apache Kafka Event Bus      |       | Redis Cluster (ioredis)                           |
+--------------+--------------+       | Sharded Pub/Sub, Checkpoints, Rate Limits         |
               |                      +---------------------------------------------------+
       +-------+-------+
       |               |
       v               v
+--------------+ +-------------------+
| OpenSearch   | | ClickHouse OLAP   |
| Full-text    | | Telemetry & Sync  |
+--------------+ +-------------------+
```

---

## Repository & Monorepo Layout

The codebase follows a modular TypeScript monorepo architecture:

```text
├── apps/
│   ├── api/                  # NestJS Fastify REST API (Auth, Catalog, Playlists)
│   ├── gateway/              # NestJS raw 'ws' WebSocket Gateway (Clock sync, Room fanout)
│   ├── room-actor/           # In-memory Room Actor service (Shard leases, epochs, state)
│   └── web/                  # React 19 + Vite web client (hls.js, Web Audio, Zustand)
├── packages/
│   ├── common/               # Shared TypeScript types, Zod schemas, constants
│   ├── database/             # Prisma schema, migrations, PgBouncer configurations
│   └── telemetry/            # ClickHouse & OpenTelemetry logging and metrics clients
├── workers/
│   ├── transcoder/           # FFmpeg & Shaka Packager BullMQ transcode consumers
│   └── search-indexer/       # Kafka CDC consumer indexing tracks into OpenSearch
├── infra/
│   ├── docker/               # Dockerfiles and docker-compose.yml for local development
│   └── k8s/                  # Kubernetes Helm charts and Horizontal Pod Autoscalers
├── design-doc.md             # Authoritative System Design Document & RFC
└── README.md                 # Project and developer repository guide
```

---

## Technology Stack

| Layer | Technology | Key Modules & Purpose |
| :--- | :--- | :--- |
| **REST API Gateway** | NestJS 10.x + Fastify | `@nestjs/platform-fastify`, `@nestjs/swagger`, `class-validator`, `argon2` |
| **Room WebSockets** | NestJS 10.x + raw `ws` | `@nestjs/platform-ws`, `ws`, `zod` for zero-reflection frame validation |
| **State & Pub/Sub** | Redis 7.x Cluster | `ioredis`, Sharded Pub/Sub (`SPUBLISH`), atomic Lua lease scripts |
| **Relational Database** | PostgreSQL 16 + Prisma | `@prisma/client`, `prisma`, PgBouncer connection pooling |
| **Task Queue** | BullMQ + Redis | `@nestjs/bullmq`, background host promotion, transcode job queues |
| **Transcoding** | FFmpeg 6.x + Shaka Packager | `fluent-ffmpeg`, 4-rung AAC CMAF ladder, EBU R128 loudness normalization |
| **Event Bus** | Apache Kafka | `kafkajs`, CDC log propagation via Debezium |
| **Search Engine** | OpenSearch 2.x | `@opensearch-project/opensearch`, fuzzy search + Redis ZSET popularity index |
| **OLAP Telemetry** | ClickHouse 23.x | `@clickhouse/client`, compressed bulk ingest of client sync telemetry |
| **Web Frontend** | React 19 + TypeScript + Vite | `hls.js` (MSE), `zustand` (transient 60fps playhead updates), Web Audio API |
| **Observability** | Prometheus + Pino + OTel | `prom-client`, `nestjs-pino`, OpenTelemetry SDK distributed tracing |
| **Load Testing** | k6 | `k6/ws`, multi-thousand client room fanout and burst simulation |

---

## API & WebSocket Interface Reference

### REST API Endpoints

| Method | Endpoint | Description | Auth Required |
| :--- | :--- | :--- | :--- |
| `POST` | `/api/v1/auth/register` | Register new user account (Argon2 password hashing) | No |
| `POST` | `/api/v1/auth/login` | Authenticate user and issue ed25519 signed JWT | No |
| `GET` | `/api/v1/tracks/:id` | Retrieve track metadata and CDN manifest URL | Bearer JWT |
| `GET` | `/api/v1/search` | Search catalogue (OpenSearch fuzzy + Redis prefix cache) | Bearer JWT |
| `POST` | `/api/v1/rooms` | Create a new shared listening room | Bearer JWT |
| `GET` | `/api/v1/rooms/:id` | Fetch room metadata and current queue state | Bearer JWT |

### WebSocket Control Protocol

#### Client-to-Gateway Messages
```typescript
export type ClientMessage =
  | { type: 'SYNC_PING'; t0: number }
  | { type: 'CMD_PLAY'; opId: string; roomId: string }
  | { type: 'CMD_PAUSE'; opId: string; roomId: string }
  | { type: 'CMD_SEEK'; opId: string; roomId: string; targetPosMs: number }
  | { type: 'CMD_QUEUE_ADD'; opId: string; roomId: string; trackId: string }
  | { type: 'CMD_VOTE_SKIP'; opId: string; roomId: string }
  | { type: 'REPORT_TELEMETRY'; roomId: string; driftMs: number; rttMs: number };
```

#### Gateway-to-Client Broadcasts
```typescript
export type ServerMessage =
  | { type: 'SYNC_PONG'; t0: number; t1: number; t2: number }
  | { type: 'ROOM_SNAPSHOT'; state: RoomStateSnapshot }
  | { 
      type: 'ROOM_EVENT'; 
      version: number; 
      epoch: number; 
      effectiveServerTimeMs: number; 
      action: 'PLAY' | 'PAUSE' | 'SEEK' | 'TRACK_CHANGE';
      anchorPosMs: number;
      trackId: string;
      queue: QueueItem[];
    }
  | { type: 'CMD_ACK'; opId: string; status: 'ACCEPTED' | 'REJECTED'; reason?: string };
```

---

## Service Level Objectives (SLOs)

| Metric | Target | Alert Threshold |
| :--- | :--- | :--- |
| **Sync Drift Error** | At least 99% of listening time within ±200 ms | Page if p99 > 300 ms for 2 minutes |
| **Host Command Fanout** | Applied room-wide in under 500 ms (for 5,000 listeners) | Page if p99 > 800 ms |
| **Join-to-Synced Time** | Under 3.0 seconds from clicking join | Alert if p95 > 5.0 s |
| **Audio Rebuffering Ratio** | Under 0.5% of total listening time | Alert if > 1.0% |
| **Gateway Socket Saturation**| Under 7,000 active connections per container | Auto-scale threshold; warning at 8,500 |

---

## Getting Started & Local Development

### Prerequisites

- **Node.js**: v20.x or v22.x LTS
- **Package Manager**: `npm` v10+ or `pnpm`
- **Docker & Docker Compose**: For local PostgreSQL, Redis, Kafka, and ClickHouse
- **FFmpeg**: v6.1+ installed locally or via Docker container

### Environment Configuration

Create a `.env` file in the project root:

```bash
# Server & Ports
PORT=3000
WS_PORT=3001
NODE_ENV=development

# Database (PostgreSQL via PgBouncer)
DATABASE_URL="postgresql://postgres:postgres@localhost:5432/listen_together?schema=public"

# Redis Cluster / Standalone Dev
REDIS_HOST="localhost"
REDIS_PORT=6379

# JWT Authentication
JWT_SECRET="super-secret-jwt-key"
JWT_EXPIRES_IN="7d"

# AWS / Cloudflare R2 Media Storage
S3_REGION="auto"
S3_BUCKET="listen-together-audio"
S3_ACCESS_KEY_ID="your-access-key"
S3_SECRET_ACCESS_KEY="your-secret-key"
S3_ENDPOINT="https://<account-id>.r2.cloudflarestorage.com"

# ClickHouse OLAP Telemetry
CLICKHOUSE_HOST="http://localhost:8123"
CLICKHOUSE_DB="telemetry"
```

### Installation & Setup

1. **Clone the repository and install dependencies:**
   ```bash
   git clone https://github.com/your-org/music-streaming-with-shared-listening-rooms.git
   cd music-streaming-with-shared-listening-rooms
   npm install
   ```

2. **Launch backing infrastructure with Docker Compose:**
   ```bash
   docker compose up -d postgres redis kafka clickhouse opensearch
   ```

3. **Run Prisma database migrations and generate client:**
   ```bash
   npx prisma migrate dev --name init
   npx prisma generate
   ```

### Running the Applications

```bash
# Start all microservices in watch mode (API, WebSocket Gateway, Room Actor)
npm run start:dev

# Start the Web Client dev server (Vite + React)
npm run start:client

# Start background transcode workers
npm run start:workers
```

### Testing & Quality Assurance

```bash
# Run unit and integration tests (Vitest)
npm run test

# Run end-to-end API tests (Supertest)
npm run test:e2e

# Run k6 WebSocket room sync load simulation (1,000 virtual listeners)
npm run test:load
```

---

## Deep-Dive Architecture Index

For technical specifications, formal proofs, database schemas, and failure recovery protocols, consult the dedicated sections in [design-doc.md](design-doc.md):

| Domain / Deep Dive | Location in Design Doc |
| :--- | :--- |
| **Capacity Planning & Sizing Calculations** | [1. Overview and scale](design-doc.md#1-overview-and-scale) |
| **Audio Transcode Ladder & Loudness Specs** | [3. Audio delivery](design-doc.md#3-audio-delivery) |
| **Catalogue CDC & OpenSearch Indexing** | [4. Catalogue and search](design-doc.md#4-catalogue-and-search) |
| **Cristian's Sync Mathematical Formulation** | [5.1 Clock Synchronization Protocol](design-doc.md#51-clock-synchronization-protocol-cristians-algorithm) |
| **Redis Lua Lease Fencing Script** | [5.4 Room Sharding & Lease Fencing](design-doc.md#54-room-sharding--lease-fencing-lua-script) |
| **5,000-Listener Pause Sequence Diagram** | [6. Sequence: Host presses pause](design-doc.md#6-sequence-host-presses-pause-in-a-room-of-5000-listeners) |
| **Complete Prisma Relational Schema** | [7.2 Database Schemas (Prisma)](design-doc.md#72-database-schemas) |
| **ClickHouse OLAP Telemetry DDL** | [7.2 ClickHouse Telemetry Schema](design-doc.md#72-database-schemas) |
| **Failover, Draining & Load Shedding Rings** | [8. Reliability](design-doc.md#8-reliability) |
| **Prometheus Metrics & Golden Signals** | [9. Observability](design-doc.md#9-observability) |
| **10 Architectural Decisions & Alternatives** | [11. Key decisions and rejected alternatives](design-doc.md#11-key-decisions-and-rejected-alternatives) |
| **9 Distributed Failure Modes Matrix** | [12. Failure scenarios](design-doc.md#12-failure-scenarios) |
| **12-Week Roadmap & Engineer Allocation** | [13. What to build first and what to postpone](design-doc.md#13-what-to-build-first-and-what-to-postpone) |

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

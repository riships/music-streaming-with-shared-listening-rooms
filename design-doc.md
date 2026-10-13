# Listen Together: Music Streaming Architecture & Design Doc

> **Author:** @Rishi · **Date:** Oct 13, 2026  
> **Repository Guide:** See [README.md](README.md) for quick-start, local setup, and repository layout.

---

## Table of Contents

1. [Overview and Scale](#1-overview-and-scale)
2. [Architecture](#2-architecture)
3. [Audio Delivery](#3-audio-delivery)
4. [Catalogue and Search](#4-catalogue-and-search)
5. [Room Sync & Protocol Specification](#5-room-sync--protocol-specification)
   - [5.1 Clock Synchronization Protocol (Cristian's Algorithm)](#51-clock-synchronization-protocol-cristians-algorithm)
   - [5.2 Drift Correction Engine](#52-drift-correction-engine)
   - [5.3 WebSocket Wire Protocol Schemas](#53-websocket-wire-protocol-schemas)
   - [5.4 Room Sharding & Lease Fencing (Lua Script)](#54-room-sharding--lease-fencing-lua-script)
6. [Sequence: Host Presses Pause in a Room of 5,000 Listeners](#6-sequence-host-presses-pause-in-a-room-of-5000-listeners)
7. [Data and Storage](#7-data-and-storage)
   - [7.1 Storage Matrix](#71-storage-matrix)
   - [7.2 Database Schemas (Prisma & ClickHouse)](#72-database-schemas)
8. [Reliability](#8-reliability)
9. [Observability](#9-observability)
10. [Technology Choices: Libraries, Frameworks & Ecosystem](#10-technology-choices-libraries-frameworks--ecosystem)
11. [Key Decisions and Rejected Alternatives](#11-key-decisions-and-rejected-alternatives)
12. [Failure Scenarios](#12-failure-scenarios)
13. [What to Build First, and What to Postpone (12-Week Roadmap)](#13-what-to-build-first-and-what-to-postpone)

---

## 1. Overview and scale

My approach is to keep rooms thin. A room doesn't stream anything to anyone; it just keeps a shared **playback clock** that everybody agrees on. Each listener still pulls their own audio from the CDN, like any other user, so the room layer stays tiny (about 2 GB of state for 200k rooms) and the expensive part, audio, scales the way normal streaming already does.

| Quantity | Estimate | Basis |
| --- | --- | --- |
| Concurrent listeners | 2M | Given |
| Audio egress at peak | ≈ 0.3 Tbps | 2M × \~160 kbps average over the bitrate ladder |
| Audio storage | ≈ 0.8 PB | 50M tracks × \~16 MB for a 4-rung ladder (3.5 min average) |
| Catalogue metadata | 50–100 GB | 50M tracks × 1–2 KB |
| Listeners inside rooms | ≈ 500k | Assumption: 25% of listeners are in a room |
| Room WebSockets | 500k, 50–100 gateway nodes | 5–10k sockets per node |
| Room state | ≈ 2 GB | 200k rooms × \~10 KB including queue |
| Track-advance events | ≈ 1k/s | 200k rooms ÷ \~210 s per track |

Treat these as planning numbers, not measurements. What worries me isn't throughput, it's connection count and bursts: one pause in a 5,000-person room means 5,000 messages inside roughly 100 ms.

**Targets (SLOs):** I'd hold us to these: listeners within 200 ms of each other 99% of the time, first audio in under 1.5 s (p95), join-to-synced under 3 s (p95), and a host command applied room-wide in under 500 ms (p99).

## 2. Architecture

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
       | REST / GraphQL Metadata             | WebSocket Control & Clock Frames
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

Listeners pull audio from the CDN and keep one WebSocket open to a gateway. The room service owns each room's playback anchor, so the room side only ever carries small control messages while the audio side carries all the bandwidth. In the audio plane, arrows point toward the listener.

## 3. Audio delivery

Audio is ordinary segmented streaming over a couple of CDNs. I deliberately gave rooms no special audio path, because that keeps per-listener bitrate, DRM and royalty counting working exactly as they do for solo listening.

- **Ingest and encoding.** Labels send masters to an ingest bucket. Batch workers (`ffmpeg` via `fluent-ffmpeg` and `shaka-packager` behind a BullMQ queue) transcode each track once into four rungs:
  - 24 kbps AAC-HE (cellular degraded fallback)
  - 96 kbps AAC-LC (mobile standard)
  - 160 kbps AAC-LC (desktop default)
  - 320 kbps AAC-LC (high-fidelity premium)
  Loudness is normalized to -14 LUFS (EBU R128 `loudnorm` filter). Output is packaged into CMAF (Common Media Application Format) fMP4 chunks with paired HLS (`.m3u8`) and DASH (`.mpd`) manifests. Opus rungs for web and Android are added in Phase 2.
- **Storage.** Segments and manifests sit in multi-region object storage (`@aws-sdk/client-s3`, Cloudflare R2 or AWS S3: ~0.8 PB for one codec ladder), with an active replication policy to a secondary region. Segments are immutable with `Cache-Control: public, max-age=31536000, immutable`.
- **Protection.** Segments are encrypted with Common Encryption (CENC, AES-128-CTR). Manifests and initial initialization chunks require short-lived HMAC-signed URLs (`@aws-sdk/s3-request-presigner`). An entitlement service issues keys via Widevine Modular and FairPlay Streaming.
- **CDN.** Multi-CDN setup (Fastly + Cloudflare), with the client holding an automated failover manifest list, plus an origin shield layer to absorb cache misses. Edge cache hit ratio target is ≥ 95%. Synchronized room listeners generate coalesced requests on identical segments within sub-second intervals, maximizing edge cache hits.
- **Fast start.** The first segment of each ladder rung is cut at 2.0 s; subsequent segments are 4.0 s. Players boot on the 96 kbps rung, buffering 1 segment before playback start, and shift up via ABR heuristics. Prefetching triggers 30 s before track completion.
- **Adaptive bitrate.** Throughput and buffer-occupancy-based ABR (BOLA / throughput sliding average via `hls.js`). Room sync never forces player stalls; struggling clients downshift bitrates autonomously.

## 4. Catalogue and search

Postgres is the source of truth for metadata. Search and caches are derived copies fed by change-data-capture, so search traffic never touches the primary.

- **Metadata.** Tracks, albums, artists, label rights, and territorial availability reside in PostgreSQL (managed AWS RDS or Aurora PostgreSQL), mapped through **Prisma ORM**. Multi-AZ primary with regional read replicas. Hot entities (top 100k tracks/artists) are cached in Redis via `@keyv/redis` with cache stamps to prevent thundering herds.
- **Search as you type.** OpenSearch (2.x) indexes track titles, artists, albums, ISRC codes, and fuzzy phonetic names. Prefix autocomplete is accelerated using a Redis ZSET index (`ZRANGEBYLEX`) holding the top 2M popular search stems weighted by play counts. Client queries debounce at 100 ms with an `AbortController` cancellation token. p95 query latency SLO < 100 ms.
- **New releases.** Ingested metadata and audio are sealed with `release_at` timestamps and embargo flags. Debezium captures CDC changes from PostgreSQL write-ahead logs (WAL) to Apache Kafka topics, where indexer workers update OpenSearch within 60 seconds. At `release_at`, a Redis boolean flag flips, cache tags invalidate, and CDN edge workers pre-warm the first 2 chunks of anticipated hit tracks.

## 5. Room sync & protocol specification

The key idea: the room doesn't stream a position, it publishes an **anchor**, and every client works out where playback should be. A nice side effect is that music keeps playing even if the room server disappears, which is what makes failover cheap.

```text
roomState = { epoch, version, trackId, playState, anchorPosMs, anchorServerTimeMs, queue[], votes{} }
expectedPos(now) = anchorPosMs + (serverNow(now) − anchorServerTimeMs)      // while playing
```

### 5.1 Clock Synchronization Protocol (Cristian's Algorithm)

Clients never rely on local device wall clocks. Instead, they determine `serverNow(t)` via a continuous NTP-style Cristian's algorithm exchange:

```text
Client (t0) -------- Ping(t0) --------> Server (t1)
Client (t3) <------- Pong(t0, t1, t2) - Server (t2)
```

1. **Round-Trip Time (RTT):** $\text{RTT} = (t_3 - t_0) - (t_2 - t_1)$
2. **Server Time Offset ($\theta$):** $\theta = \frac{(t_1 - t_0) + (t_2 - t_3)}{2}$
3. **Current Estimated Server Time:** $\text{serverNow}(t) = t_{\text{client}} + \theta$
4. **Sampling Policy:** The client exchanges 8 ping/pong pairs upon initial connection, discards the highest 25% RTT samples, and computes a weighted average offset over the lowest-RTT samples. Samples with RTT > 300 ms are dropped. A background keep-alive ping runs every 30 seconds to adjust for local clock drift.

### 5.2 Drift Correction Engine

Every 5 seconds (or immediately following unpause/seek), the client calculates drift:
$$\Delta = \text{playheadPositionMs} - \text{expectedPos}(\text{serverNow}())$$

```text
         |Δ| < 20 ms    --> In-sync deadband: No action taken.
 20 ms ≤ |Δ| ≤ 200 ms   --> Micro-pitch rate nudge:
                            Playback rate altered to 1.02x (if lagging) or 0.98x (if leading)
                            for duration D = |Δ| / 0.02 ms. Transparent to human ear.
         |Δ| > 200 ms   --> Hard resync:
                            Direct seek to expectedPos + 50 ms buffer lead.
```

Clients also compensate for device audio output latency retrieved via the Web Audio API (`AudioContext.outputLatency + AudioContext.baseLatency`). Bluetooth A2DP audio devices (typically introducing 120–220 ms uncalibrated delay) report their device class to trigger an optional client-side user calibration slider.

### 5.3 WebSocket Wire Protocol Schemas

WebSocket payloads use compact typed JSON (or Protobuf in Phase 2) with an envelope schema:

```typescript
// Client -> Gateway Messages
export type ClientMessage =
  | { type: 'SYNC_PING'; t0: number }
  | { type: 'CMD_PLAY'; opId: string; roomId: string }
  | { type: 'CMD_PAUSE'; opId: string; roomId: string }
  | { type: 'CMD_SEEK'; opId: string; roomId: string; targetPosMs: number }
  | { type: 'CMD_QUEUE_ADD'; opId: string; roomId: string; trackId: string }
  | { type: 'CMD_VOTE_SKIP'; opId: string; roomId: string }
  | { type: 'REPORT_TELEMETRY'; roomId: string; driftMs: number; rttMs: number };

// Gateway -> Client Broadcasts & Responses
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

export interface RoomStateSnapshot {
  roomId: string;
  epoch: number;
  version: number;
  trackId: string;
  playState: 'PLAYING' | 'PAUSED' | 'BUFFERING';
  anchorPosMs: number;
  anchorServerTimeMs: number;
  queue: QueueItem[];
  hostUserId: string;
  listenerCount: number;
}

export interface QueueItem {
  id: string;
  trackId: string;
  addedByUserId: string;
  skipVotes: string[];
}
```

### 5.4 Room Sharding & Lease Fencing (Lua Script)

To guarantee exactly-one writer per room without distributed deadlocks, shard ownership is governed by a Redis lease secured with a monotonic fencing epoch:

```lua
-- Redis Shard Lease Acquisition Script: acquire_room_lease.lua
-- KEYS[1]: shard_lock:{shardId}
-- ARGV[1]: nodeId, ARGV[2]: ttlMs (3000)
local current = redis.call('get', KEYS[1])
if (not current) or (current == ARGV[1]) then
    redis.call('set', KEYS[1], ARGV[1], 'PX', ARGV[2])
    local epochKey = 'shard_epoch:' .. KEYS[1]
    local newEpoch = redis.call('incr', epochKey)
    return {1, newEpoch}
else
    return {0, -1}
end
```

When a node heartbeat fails (misses 3s TTL), another room actor node takes over the shard, reads the highest checkpoint from Redis, increments the epoch, and rejects any incoming write tagged with an older epoch number.

## 6. Sequence: host presses pause in a room of 5,000 listeners

```
Host Client        Gateway (Host)     Room Actor (Shard)    Redis Cluster      Gateway (Pool 1..50)    Listeners (5,000)
    |                    |                     |                  |                     |                     |
    |-- CMD_PAUSE(opId)->|                     |                  |                     |                     |
    |   [t_client]       |-- Route to Actor -->|                  |                     |                     |
    |                    |                     |-- Verify Host &  |                     |                     |
    |                    |                     |   Dedupe (opId)  |                     |                     |
    |                    |                     |-- Calc T = now+L |                     |                     |
    |                    |                     |   (L = 200 ms)   |                     |                     |
    |                    |                     |-- Persist State->|                     |                     |
    |                    |                     |<-- Ack (Epoch)---|                     |                     |
    |                    |                     |-- Publish Event ---------------------->|                     |
    |                    |                     |   [Room Pub/Sub]                       |-- Fanout Frame ---->|
    |<-- CMD_ACK(opId) --|                     |                                        |   [5000 sockets]    |
    |                    |                     |                                        |                     |
    +---- Wait until serverNow == T ----------+----------------------------------------+---- Wait until T ---+
    |                    |                     |                                        |                     |
    |== Pause Audio =====|=====================|========================================|=====================|== Pause Audio =====
    |   [At exact Pos]   |                     |                                        |                     |   [At exact Pos]
```

1. **Action initiated:** The host clicks Pause. Client attaches idempotency UUID `opId: "uuid-v4"` and emits `CMD_PAUSE`.
2. **Gateway validation:** Gateway verifies JWT session claims and dispatches the command across the internal IPC channel to the actor node holding the shard.
3. **Actor scheduling:** The actor dedupes `opId`. It inspects the room's rolling network RTT distribution and picks scheduled timestamp $T = \text{now} + 200\text{ ms}$. It calculates the exact expected track position at timestamp $T$, switches `playState` to `PAUSED`, and increments `version`.
4. **Fenced checkpoint:** The actor writes the new snapshot atomically to Redis via `ioredis` pipeline with its lease epoch.
5. **Bus distribution:** Actor publishes a single event payload to Redis channel `room:pubsub:{roomId}`.
6. **Socket fanout:** 50 gateway nodes subscribed to `room:pubsub:{roomId}` deserialize the single payload once into a binary/JSON buffer and blast it across their respective ~100 local client WebSocket connections. Total delivery window: < 50 ms.
7. **Synchronized execution:** Clients receive the message at $T - 150\text{ ms}$. Each client arms a precision timer (`setTimeout` or Web Audio scheduled pause). Exactly when `serverNow() == T`, all 5,000 clients halt audio playback at identical sample positions.
8. **Feedback loop:** Clients piggyback execution deltas on their next heartbeat to update sync error metrics in ClickHouse.

## 7. Data and storage

### 7.1 Storage Matrix

| Data Domain | Store | Access Pattern & Key Structure | Why |
| --- | --- | --- | --- |
| Catalogue, rights, accounts, billing | PostgreSQL 16 (Primary + Multi-AZ Read Replicas) | Normalized relational; indexed by ISRC, artist_id, territory | ACID compliance, strict foreign key integrity, schema migrations via Prisma |
| Search & autocomplete | OpenSearch 2.x + Redis ZSET prefix index | Text queries: fuzzy BM25; Autocomplete: `ZREVRANGEBYLEX` on popularity | Typo-tolerance, n-gram stemming, millisecond autocomplete |
| Live room state | In-memory Room Actor + Redis Checkpoints | Key: `room:state:{roomId}` (Hash/String with TTL 24h) | Sub-millisecond reads/writes, durable across process restarts |
| Room metadata & archives | PostgreSQL 16 | Key: `rooms` table with relations to users, playlists | Durable audit trail, historical room sessions |
| User playlists & track sequences | ScyllaDB / Amazon DynamoDB | Partition Key: `playlist_id`, Sort Key: fractional index `order_key` | High write throughput, millions of concurrent playlist edits |
| User listening history | ScyllaDB / Amazon DynamoDB | Partition Key: `user_id`, Sort Key: `played_at_timestamp` | High-volume time-series append-only log |
| Play events & playback telemetry | Apache Kafka → ClickHouse | Topic: `telemetry.playback.v1` → Table: `sync_telemetry` | Multi-gigabyte ingest, real-time aggregation and sync error distribution |
| Royalty & licensing ledger | Kafka (acks=all) → PostgreSQL Ledger + S3 Parquet | Idempotent transaction ledger with double-entry accounting | Immutable audit trail for label royalties; exactly-once semantics |
| Audio masters, CMAF segments, manifests | Cloudflare R2 / AWS S3 + Fastly CDN | `/audio/{track_id}/{rung}/segment_{index}.m4s` | Cost-effective immutable blob storage, global caching |

### 7.2 Database Schemas

#### Prisma Schema (`schema.prisma` excerpt)

```prisma
datasource db {
  provider = "postgresql"
  url      = env("DATABASE_URL")
}

generator client {
  provider = "prisma-client-js"
}

model Track {
  id           String      @id @default(uuid())
  isrc         String      @unique
  title        String
  durationMs   Int
  artistId     String
  albumId      String?
  explicit     Boolean     @default(false)
  manifestUrl  String
  loudnessLufs Float       @default(-14.0)
  artist       Artist      @relation(fields: [artistId], references: [id])
  album        Album?      @relation(fields: [albumId], references: [id])
  territories  String[]    // ISO 3166-1 alpha-2 codes
  createdAt    DateTime    @default(now())
  updatedAt    DateTime    @updatedAt

  @@index([artistId])
  @@index([albumId])
}

model Artist {
  id        String   @id @default(uuid())
  name      String
  bio       String?
  tracks    Track[]
  createdAt DateTime @default(now())
}

model Album {
  id          String   @id @default(uuid())
  title       String
  releaseDate DateTime
  tracks      Track[]
  createdAt   DateTime @default(now())
}

model Room {
  id           String     @id @default(uuid())
  title        String
  hostUserId   String
  isPrivate    Boolean    @default(false)
  inviteCode   String?    @unique
  currentTrack String?
  createdAt    DateTime   @default(now())
  closedAt     DateTime?

  @@index([hostUserId])
}

model RoyaltyLedgerEntry {
  id           BigInt   @id @default(autoincrement())
  eventId      String   @unique
  userId       String
  trackId      String
  durationSec  Int
  territory    String
  streamType   String   // SOLO | ROOM
  recordedAt   DateTime @default(now())

  @@index([trackId, recordedAt])
}
```

#### ClickHouse Telemetry Schema (`sync_telemetry`)

```sql
CREATE TABLE sync_telemetry (
    recorded_time   DateTime64(3, 'UTC'),
    room_id         UUID,
    user_id         UUID,
    room_size       UInt32,
    drift_ms        Int32,
    rtt_ms          UInt16,
    playback_rate   Float32,
    action_type     LowCardinality(String),
    cdn_provider    LowCardinality(String),
    device_type     LowCardinality(String),
    output_channel  LowCardinality(String) -- Speaker | Headphones | Bluetooth
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(recorded_time)
ORDER BY (room_id, recorded_time, user_id)
TTL recorded_time + INTERVAL 90 DAY;
```

## 8. Reliability

The goal is that audio keeps playing through almost any control-plane failure. What degrades is our ability to change things, not the music.

- **Regional outage.** Every room has a home region (where it was created), recorded in its id. Other regions proxy to it, and the extra latency is absorbed by clock sync. Room checkpoints replicate asynchronously to a standby region via Redis active-passive replication. If a region goes down, Route53 / Cloudflare global load balancing sends clients to the standby, which rebuilds rooms from the replicated state. Expect to lose a few seconds of control events at most and to recover in about a minute. Audio is completely unaffected because S3/R2 storage is multi-region and we use two independent CDNs.
- **Deploys that don't drop rooms.** Gateways drain: they stop taking new connections and send a `DRAIN_RECONNECT` WebSocket frame advising clients to reconnect with randomized backoff (0–15s jitter), turning over no more than ~5% of sockets a minute. Room shards hand off by lease transfer: flush state to Redis, release key, new owner acquires lease and hydrates rooms in < 1 second per shard while clients play uninterrupted from their anchors.
- **Degraded load shedding.** Under extreme load, systems degrade gracefully by shedding features in prioritized rings:
  1. *Ring 0 (Protected Core):* Segment streaming, clock sync, host play/pause/seek, track advance.
  2. *Ring 1 (Queue & Voting):* Queue additions and skip voting restricted to rate limits.
  3. *Ring 2 (Social):* Chat messages, listener avatar presence lists, reactions dropped or throttled.
  4. *Ring 3 (Presence Sampling):* Rooms > 500 listeners replace individual join/leave socket broadcasts with an aggregated listener count emitted once every 2.5 seconds.

## 9. Observability

The primary golden metric is the share of listener-time spent within 200 ms of where the room should be, measured on the client.

| Metric | Metric Name & Type | Target / Alert Threshold |
| --- | --- | --- |
| Sync Error Rate | `room_sync_drift_ms` (Histogram) | ≥ 99% of samples within [-200ms, +200ms]; Page if p99 > 300ms for 2 min |
| Command Fanout Latency | `room_cmd_fanout_duration_ms` (Gauge) | p99 < 500 ms at 5,000 listeners |
| Join-to-Synced Time | `room_join_to_sync_duration_ms` (Histogram) | p95 < 3.0 s |
| Rebuffering Ratio | `audio_rebuffer_ratio` (Gauge) | < 0.5% of total listening time |
| Clock Offset Variance | `client_clock_offset_variance_ms` (Gauge) | Alert if σ > 25 ms across a cluster region |
| WebSocket Gateway Capacity | `gateway_active_sockets` (Gauge) | Warning at 8,500 sockets/node; auto-scale threshold at 7,000 |
| Shard Lease Churn | `shard_lease_takeover_total` (Counter) | Alert on > 3 takeovers / 5 min |

All metrics are tagged with dimensions: `region`, `room_size_bucket` (1-10, 11-100, 101-1000, 1000+), `cdn`, `output_type` (Internal, Bluetooth, Wired), and `network_type` (Wifi, 4G, 5G).

## 10. Technology choices: libraries, frameworks & ecosystem

To maximize velocity for a lean engineering team while achieving high throughput and sub-millisecond control latencies, the entire stack uses **TypeScript on Node.js** (backend services) and **React + TypeScript** (frontend).

```
+----------------------------------------------------------------------------------------------------+
|                                    TECHNOLOGY ECOSYSTEM OVERVIEW                                   |
+--------------------+-----------------------------------------------+-------------------------------+
| Tier               | Core Framework / Library                      | Key Modules & Supporting Pkgs |
+--------------------+-----------------------------------------------+-------------------------------+
| Room Gateways      | NestJS 10.x + ws engine                       | @nestjs/platform-ws, ws, zod  |
| State & Pub/Sub    | Redis 7.x Cluster + ioredis                   | ioredis, redlock, lua scripts |
| REST / GraphQL API | NestJS 10.x (Fastify Adapter)                 | @nestjs/platform-fastify,     |
|                    |                                               | @nestjs/swagger, class-val    |
| Persistence & ORM  | PostgreSQL 16 + Prisma ORM 5.x                | @prisma/client, prisma, pg    |
| Queue & Jobs       | BullMQ + Redis                                | @nestjs/bullmq, bullmq        |
| Event Streaming    | Apache Kafka + kafkajs                        | kafkajs                       |
| Search Engine      | OpenSearch 2.x                                | @opensearch-project/opensearch|
| Media Transcoding  | FFmpeg 6.x + Shaka Packager                   | fluent-ffmpeg, ffmpeg-static  |
| Cloud Storage SDK  | AWS SDK v3                                    | @aws-sdk/client-s3            |
| Web Client Engine  | React 19 + TypeScript + Vite                  | hls.js, zustand, tailwindcss  |
| Telemetry & Logs   | OpenTelemetry SDK + Pino + ClickHouse         | nestjs-pino, prom-client      |
| Testing & QA       | Vitest, Supertest, Playwright, k6             | k6/ws, @faker-js/faker        |
+--------------------+-----------------------------------------------+-------------------------------+
```

### 10.1 Backend Microservice Architecture & Frameworks

- **NestJS (`@nestjs/core`, `@nestjs/common`, `@nestjs/microservices`):**
  - **Why:** Enforces SOLID modular architecture, unified dependency injection, robust decorator-based lifecycle hooks, and native microservices transport support.
  - **HTTP Platform Adapter:** Configured with `@nestjs/platform-fastify` instead of Express for the REST API tier. Fastify yields ~2.5x higher JSON serialization throughput and lower latency overhead under high concurrency.
  - **Validation & Serialization:** `class-validator` and `class-transformer` for standard HTTP DTOs; `zod` for ultra-fast, zero-reflection validation of high-frequency WebSocket frames.
  - **API Documentation:** `@nestjs/swagger` with OpenAPI 3.1 specification generation.

- **Authentication & Security:**
  - `@nestjs/passport`, `passport-jwt`, and `jsonwebtoken` for stateless, signed ed25519 or RS256 JWT validation.
  - `argon2` for password hashing on user accounts.
  - `@nestjs/throttler` backed by `ioredis` for distributed IP and user-rate limiting.
  - `helmet` and `cors` for HTTP header security.

### 10.2 Real-time Room Gateway & Pub/Sub Engine

- **WebSocket Engine (`@nestjs/websockets`, `@nestjs/platform-ws`, `ws`):**
  - **Why `ws` instead of `socket.io`:** `socket.io` adds protocol wrappers, heartbeats, and room abstraction layers that add significant memory overhead per socket (40–80 KB vs ~5–10 KB in `ws`). At 10,000 active sockets per node, raw `ws` saves ~500 MB of heap memory per container and minimizes per-frame serialization overhead.
  - Gateways run in stateless clusters behind an AWS Network Load Balancer (NLB) with TLS termination and TCP keep-alives enabled.
  - Connection authentication happens in the WebSocket upgrade handshake: the JWT is validated before the connection is established.

- **Redis Pub/Sub & Sharded Driver (`ioredis`):**
  - `ioredis` is selected for native support of Redis Cluster, automatic pipelining, Sentinel failover, and Lua script execution.
  - Room events leverage **Redis Sharded Pub/Sub (`SPUBLISH` / `SSUBSCRIBE`)** introduced in Redis 7.0, restricting message broadcast traffic to the specific cluster hash slot owning the room key, preventing cluster-wide broadcast amplification.

### 10.3 Persistence, Database & ORM

- **Prisma ORM (`@prisma/client`, `prisma`):**
  - Type-safe query generation, migrations (`prisma migrate`), and schema definition for PostgreSQL.
  - Integrated with connection pooling via **PgBouncer** (or Prisma Accelerate) to support up to thousands of short-lived backend connections without exhausting PostgreSQL process limits.
- **Analytical Storage Client (`@clickhouse/client`):**
  - Direct HTTP streaming client pushing batches of 5,000–10,000 telemetry events to ClickHouse with gzip compression.

### 10.4 Background Workers & Media Transcoding Pipeline

- **Job Queue (`@nestjs/bullmq`, `bullmq`):**
  - Backed by Redis streams. Provides robust delayed jobs (e.g., scheduling room lease expiration, automatic host promotion after 30s disconnect), retries with exponential backoff, and concurrency control.
- **Transcoding Stack:**
  - `fluent-ffmpeg` wrapping system `ffmpeg` binaries (FFmpeg 6.1 with `libfdk_aac` and `libopus`).
  - Loudness normalization via FFmpeg filter `loudnorm=I=-14:LRA=11:TP=-1.5`.
  - Packaging via Google's `shaka-packager` CLI invoked via child process workers. Generates CMAF chunks with segment duration targets, initialization segment `init.mp4`, and master HLS/DASH playlists.
- **Storage Operations (`@aws-sdk/client-s3`):**
  - Modular AWS SDK v3 with fine-grained tree-shaking. Uses `@aws-sdk/s3-request-presigner` for signed upload and download token issuance.

### 10.5 Web Client & Audio Sync Engine

- **Core UI Framework:**
  - **React 19** with **TypeScript** and **Vite** build tooling for sub-second HMR and optimized production bundles.
  - **Zustand (`zustand`):** Chosen over standard Redux for real-time playhead state. Zustand allows selector subscriptions that bypass React component re-rendering entirely (transient updates) for the 60fps playhead progress bar, eliminating React rendering overhead during active playback.
  - UI Component styling: Modern Vanilla CSS & Tailwind CSS tokens for a responsive, modern dark-mode aesthetic.

- **Audio Playback Engine (`hls.js`):**
  - Media Source Extensions (MSE) player for segmented CMAF/HLS audio playback.
  - Configured with custom buffer tuning:
    - `maxBufferLength: 30` (30 seconds ahead)
    - `maxMaxBufferLength: 60`
    - `backBufferLength: 15`
  - Integrated with custom drift controller hooking into `hls.on(Hls.Events.FRAG_LOADED)` to track segment PTS (Presentation Time Stamp) vs room anchor calculations.

- **Web Audio API (`AudioContext`, `AudioWorkletNode`):**
  - Used for sample-accurate scheduling of pause/play transitions, volume ducking, and cross-fading.
  - Accesses hardware audio latency offsets (`AudioContext.outputLatency` and `AudioContext.baseLatency`) to subtract audio hardware delay from the synchronization loop.

### 10.6 Telemetry, Observability & Analytics

- **Logging:** `pino` with `nestjs-pino` for high-performance structured JSON logging with zero event-loop blocking.
- **Metrics:** `prom-client` with `@willsoto/nestjs-prometheus` exporting `/metrics` endpoints scraped by Prometheus every 15s.
- **Distributed Tracing:** OpenTelemetry SDK (`@opentelemetry/sdk-node`, `@opentelemetry/api`, `@opentelemetry/auto-instrumentations-node`) injecting W3C `traceparent` headers across HTTP, Kafka, and WebSocket messages.

### 10.7 Testing & Load Simulation Suite

- **Unit & Integration:** `Vitest` for lightning-fast unit tests and mock execution; `supertest` for NestJS e2e REST API testing.
- **Load Testing (`k6` with `k6/ws`):**
  - Custom load scenarios running in Docker:
    - 50,000 sustained WebSocket connections simulating 10 concurrent 5,000-listener rooms.
    - Burst test: 5,000 simultaneous pause commands executed within a 100 ms window to verify gateway fanout latency and buffer allocations.
- **Browser Automation:** `Playwright` with Chromium headless instances configured with mock Web Audio contexts to test end-to-end client sync in automated CI pipelines.

## 11. Key decisions and rejected alternatives

| Decision | Chosen | Rejected, and why |
| --- | --- | --- |
| How rooms sync | Shared playback clock with client-side scheduling (anchor model) | **One server-mixed stream per room:** 200k live encoders, no per-listener ABR, breaks DRM and per-listener royalty counts. **Peer-to-peer:** unreliable on mobile and NAT, hard to license. |
| Playback barrier | Live semantics; slow listeners catch up | **Wait for the slowest listener:** one bad connection stalls up to 5,000 people. |
| Room consistency | Single-writer actor per room, shard leases with fencing epochs | **Multi-writer CRDT or distributed locks per command:** more complexity than a low-write, small-state problem needs. |
| Room state store | In-memory + Redis checkpoint on change | **Postgres per command:** latency and load for no gain. **Event-sourced via Kafka:** slow recovery; events are still emitted for analytics. |
| Client transport | WebSocket (`ws` engine) over TLS | **WebRTC data channels:** heavy for server fan-out, complex signaling. **HTTP long polling:** high latency jitter, cannot hit < 200 ms target. **Socket.io:** high memory footprint per socket at scale. |
| Audio delivery | CMAF segments (HLS/DASH), AAC ladder, multi-CDN | **Progressive single file:** no mid-track ABR. **Own CDN or P2P:** capital cost and low hit ratio versus buying CDN capacity. |
| API Layer | NestJS with Fastify Adapter | **Express:** 2.5x lower throughput, excessive overhead for high-frequency REST requests. |
| Client State | Zustand (transient updates) + React | **Redux standard hooks:** triggers excessive component re-renders on continuous 60fps playhead changes. |
| Multi-region | Home region per room, async-replicated standby (active-passive) | **Active-active rooms:** cross-region consensus for state that rarely changes. |
| Search | OpenSearch plus a popularity-weighted prefix index | **Postgres full-text:** weak typo tolerance and latency at 50M tracks. |
| Royalties | Dedicated ledger from per-listener stream events | **Counting from room state:** couples money to the least durable component. |

## 12. Failure scenarios

| # | Failure | Listener impact | How the system handles it |
| --- | --- | --- | --- |
| 1 | Room node dies | Audio continues; controls pause ~3–5 s | Lease in Redis expires (3s TTL). Another node takes the shard via Lua script, reloads checkpoints from Redis, bumps the epoch; stale writers are fenced out. |
| 2 | Gateway dies or is drained for deploy | Socket drops, audio continues | Client reconnects with jittered backoff to another gateway and resumes from its last version (delta or snapshot); reconnects are rate-limited with `@nestjs/throttler` to avoid a thundering herd. |
| 3 | Host disconnects or leaves | Room keeps playing the queue | A BullMQ delayed job triggers after 30s grace period: the longest-present listener (or a designated co-host) is promoted; the room is never left without controls. |
| 4 | Whole region lost | Brief control outage; audio unaffected | Global load balancer routes to the standby region, which rehydrates rooms from replicated Redis state; multi-region S3/R2 storage and multi-CDN keep audio flowing. |
| 5 | CDN provider degraded or origin slow | Rebuffering for affected users | Manifest lists primary and secondary CDNs; `hls.js` player fails over after 2 failed chunk requests; origin shield serves stale; ABR automatically downshifts rung. |
| 6 | Redis checkpoint store unavailable | None for existing rooms | Actors keep serving from memory and buffer checkpoints; periodic snapshots to Postgres bound the loss; an unrecoverable room restarts at the queue head. |
| 7 | Search cluster degraded | Slower or reduced suggestions | Circuit breaker trips; API serves cached popularity prefix index from Redis and exact-title lookup from Postgres read replicas. |
| 8 | New-release stampede at midnight | Possible slow starts | Pre-warmed first segments, edge request coalescing, and priority shedding of presence and reactions. |
| 9 | Client with a bad clock or poor network | One listener out of sync | Offset comes from Cristian's RTT sampling, not wall clock; outliers are discarded; a client that cannot hold sync is shown "best effort" and jumps to live. |

## 13. What to build first, and what to postpone

With three engineers, I'd build the part that's hardest to retrofit, the sync model and how we measure it, on managed infrastructure in a single region. Scale and redundancy can wait, as long as the design leaves room for them.

**Build first (about 12 weeks)**

| Engineer | Scope & Tech Deliverables |
| --- | --- |
| **A: Audio & Media** | Ingest worker pipeline (`fluent-ffmpeg`, `shaka-packager`), S3 bucket hierarchy, CMAF packaging with 3 AAC rungs (96, 160, 320 kbps), Fastly CDN signed URLs, web player engine (`hls.js` + Web Audio API) with gapless prefetch. |
| **B: Real-time Rooms** | NestJS WebSocket gateway (`ws` engine), in-memory Room Actor, `ioredis` checkpointing and Lua lease locks, Cristian's clock sync handshake, drift PID/nudge controller, queue & skip voting. |
| **C: Platform & Core** | NestJS REST API (Fastify adapter), Prisma ORM + PostgreSQL, authentication (JWT + Argon2), OpenSearch ingest via Kafka, Prometheus/Pino observability, and `k6` sync load test scripts. |

**Milestones:**
- **Week 4:** Two separate web browsers within 100 ms of each other in a room playing synchronized audio with Cristian's algorithm clock sync.
- **Week 8:** A bot-driven 1,000-listener room simulated via `k6/ws` demonstrating p99 sync error under 200 ms and automated failover recovery in < 5s.
- **Week 12:** Closed beta deployment with real-time ClickHouse sync SLO dashboard, automated canary deployments, and chaos test drill passed.

**Postpone (but design for)**
- Multi-region active-passive standby and multi-CDN automated steering.
- Hierarchical fan-out and presence sampling for rooms above ~500 listeners.
- Opus rungs, HE-AAC and lossless, offline downloads, personalized ML recommendations.
- Interactive room chat, emoji reactions, and CRDT collaborative playlist ordering.
- Full studio DRM (Widevine / FairPlay), substituting signed S3 URLs at initial beta launch.

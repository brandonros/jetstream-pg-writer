# jetstream-pg-writer

Async writes through NATS JetStream with idempotent Postgres writes and client polling.

## Why

HTTP client wants durable writes with eventual confirmation. Solution: publish to JetStream (fire-and-forget), consumer writes to Postgres and updates status, client polls for completion.

```
Client ──▶ write-gateway ──▶ JetStream ──▶ write-processor ──▶ Postgres
   │           │ (202 pending)                    │ (updates status)
   │           ▼                                  │
   └────▶ poll /status/:id ◀── read-api ◀─────────┘

Postgres WAL ──▶ Debezium ──▶ JetStream ──▶ read-api ──▶ Redis invalidation
```

This gives true durability: if the processor is down, messages queue in JetStream and complete when it recovers. The client can poll indefinitely (or timeout and retry later with the same idempotency key).

## Quick start

Requires [just](https://github.com/casey/just). Run `just` to see all commands.

```bash
just up        # Start everything locally (http://localhost)
just dev       # Infrastructure only (nats, postgres, redis, debezium)
just clean     # Wipe volumes

just go        # Deploy to Vultr + wait for ready
just ssh       # Connect to server
just update    # git pull + restart on server
just logs      # Tail server logs
just destroy   # Tear down infrastructure
```

## Structure

```
packages/
  shared/           # Types
  write-gateway/    # HTTP API → publishes writes to JetStream
  write-processor/  # JetStream → Postgres (with cache invalidation)
  read-api/         # HTTP API → reads from Redis/Postgres
  frontend/         # React UI
```

## Cache invalidation (CDC)

Debezium Server connects to Postgres logical replication and streams WAL changes to NATS JetStream. Configuration is in `docker-compose.yaml` (debezium service environment variables).

- **Source**: Postgres with `wal_level=logical`
- **Sink**: NATS JetStream (`DebeziumStream`)
- **Tables**: `public.users`, `public.orders`
- **Events**: Published to `cdc.public.users`, `cdc.public.orders`

The read-api creates a durable JetStream consumer and invalidates Redis keys when CDC events arrive. This captures ALL changes (application writes, migrations, manual SQL) without requiring manual invalidation code.

## Idempotency & Status Tracking

Writes are idempotent via a `write_operations` table that also tracks async status:

```sql
CREATE TABLE write_operations (
  operation_id UUID PRIMARY KEY,
  entity_table TEXT NOT NULL,
  entity_id UUID NOT NULL,
  op_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',  -- pending, completed, failed
  error TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  completed_at TIMESTAMPTZ
);
```

Flow:
1. Client sends write request with `operationId` (Idempotency-Key header)
2. write-gateway publishes to JetStream, returns `{ status: "pending", operationId }`
3. write-processor inserts `write_operations` with status='pending' (idempotency check)
4. If duplicate → skip (already processed or in progress)
5. If new → insert domain row, update status='completed', invalidate cache
6. Client polls `/status/:operationId` until completed/failed/timeout

This separates transport concerns (idempotency, async status) from domain concerns (entity IDs).

## Key design decisions

- **Async with polling** — Gateway returns immediately (202), client polls for completion. True durability: processor downtime = queued messages, not lost writes.
- **Separate operation_id from entity_id** — `write_operations` table tracks idempotency and status. Domain tables use natural IDs (`user_id`, `order_id`).
- **Transaction-based idempotency** — Idempotency check and domain write in same transaction. Status updated atomically.
- **Filtered consumers** — Each table has its own JetStream consumer (`users-writer`, `orders-writer`).
- **FK enforced** — Orders require valid user_id. Frontend tracks created users.
- **Cache invalidation via CDC** — Debezium captures Postgres WAL changes and publishes to JetStream. Reader consumes CDC events and invalidates Redis keys. Eventually consistent.

## Limitations

This is a proof-of-concept for learning, not a production template.

- **Complexity vs. scale mismatch** — Two CRUD tables don't need NATS, Debezium, and Redis. A direct HTTP→Postgres write would be simpler with identical semantics for this workload.
- **No cleanup** — The `write_operations` table grows forever.
- **Redis required** — Read path fails completely if Redis is down; no fallback to Postgres.

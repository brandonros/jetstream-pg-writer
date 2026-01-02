# jetstream-pg-writer

Request/reply pattern through NATS JetStream with idempotent Postgres writes.

## Why

HTTP client wants synchronous confirmation that a write succeeded, but we also want durability and decoupling. Solution: publish to JetStream with a reply inbox, consumer writes to Postgres and replies.

```
Client ──▶ Producer (HTTP) ──▶ JetStream ──▶ Consumer ──▶ Postgres
                ◀───────────────────────────────────◀──── reply

Postgres WAL ──▶ Debezium ──▶ JetStream ──▶ Reader ──▶ Redis invalidation
```

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
  shared/     # Types
  producer/   # Fastify API → publishes to JetStream
  consumer/   # JetStream → Postgres
  reader/     # Fastify API → reads from Redis/Postgres
  frontend/   # React UI
```

## Cache invalidation (CDC)

Debezium Server connects to Postgres logical replication and streams WAL changes to NATS JetStream. Configuration is in `docker-compose.yaml` (debezium service environment variables).

- **Source**: Postgres with `wal_level=logical`
- **Sink**: NATS JetStream (`DebeziumStream`)
- **Tables**: `public.users`, `public.orders`
- **Events**: Published to `cdc.public.users`, `cdc.public.orders`

The reader creates a durable JetStream consumer and invalidates Redis keys when CDC events arrive. This captures ALL changes (application writes, migrations, manual SQL) without requiring manual invalidation code.

## Idempotency

Writes are idempotent via a `write_operations` table:

```sql
CREATE TABLE write_operations (
  operation_id UUID PRIMARY KEY,
  entity_table TEXT NOT NULL,
  entity_id UUID NOT NULL,
  op_type TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
```

Flow:
1. Client sends write request with `operationId`
2. Consumer attempts `INSERT INTO write_operations` (idempotency check)
3. If duplicate → return existing `entity_id` (idempotent success)
4. If new → insert domain row, return new `entity_id`
5. Both operations in same transaction

This separates transport concerns (idempotency) from domain concerns (entity IDs).

## Key design decisions

- **Separate operation_id from entity_id** — `write_operations` table tracks idempotency. Domain tables use natural IDs (`user_id`, `order_id`).
- **Transaction-based idempotency** — Idempotency check and domain write in same transaction. No CDC waiting.
- **Filtered consumers** — Each table has its own JetStream consumer (`users-writer`, `orders-writer`).
- **FK enforced** — Orders require valid user_id. Frontend tracks created users.
- **Cache invalidation via CDC** — Debezium captures Postgres WAL changes and publishes to JetStream. Reader consumes CDC events and invalidates Redis keys. Eventually consistent.

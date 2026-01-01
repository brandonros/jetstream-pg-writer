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

Debezium Server connects to Postgres logical replication and streams WAL changes to NATS JetStream.

```
config/debezium/application.properties  # Debezium config
```

- **Source**: Postgres with `wal_level=logical`
- **Sink**: NATS JetStream (`DebeziumStream`)
- **Tables**: `public.users`, `public.orders`
- **Events**: Published to `cdc.public.users`, `cdc.public.orders`

The reader creates a durable JetStream consumer and invalidates Redis keys when CDC events arrive. This captures ALL changes (application writes, migrations, manual SQL) without requiring manual invalidation code.

## Key design decisions

- **operationId = entity id** — The idempotency key becomes the primary key. No separate tracking table needed.
- **PK constraint = idempotency** — Duplicate inserts fail with unique violation, treated as success.
- **Filtered consumers** — Each table has its own JetStream consumer (`users-writer`, `orders-writer`).
- **FK enforced** — Orders require valid user_id. Frontend tracks created users.
- **Cache invalidation via CDC** — Debezium captures Postgres WAL changes and publishes to JetStream. Reader consumes CDC events and invalidates Redis keys. No manual invalidation code needed.

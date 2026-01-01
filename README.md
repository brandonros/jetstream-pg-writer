# jetstream-pg-writer

Request/reply pattern through NATS JetStream with idempotent Postgres writes.

## Why

HTTP client wants synchronous confirmation that a write succeeded, but we also want durability and decoupling. Solution: publish to JetStream with a reply inbox, consumer writes to Postgres and replies.

```
Client ──▶ Producer (HTTP) ──▶ JetStream ──▶ Consumer ──▶ Postgres
                ◀───────────────────────────────────◀──── reply
```

## Run locally

```bash
# Start everything
docker compose up --build

# Frontend: http://localhost
# API: http://localhost/api/health
```

## Deploy

```bash
cd terraform
terraform init
terraform apply
```

Provisions a Vultr instance with Docker, clones the repo, and starts services via systemd + cloud-init.

## Dev

```bash
# Infrastructure only
docker compose up nats postgres

# Run services locally (3 terminals)
pnpm dev:consumer
pnpm dev:producer
pnpm dev:frontend
```

## Structure

```
packages/
  shared/     # Types
  producer/   # Fastify API → publishes to JetStream
  consumer/   # JetStream → Postgres
  frontend/   # React UI
```

## Key design decisions

- **operationId = entity id** — The idempotency key becomes the primary key. No separate tracking table needed.
- **PK constraint = idempotency** — Duplicate inserts fail with unique violation, treated as success.
- **Filtered consumers** — Each table has its own JetStream consumer (`users-writer`, `orders-writer`).
- **FK enforced** — Orders require valid user_id. Frontend tracks created users.

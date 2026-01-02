# CLAUDE.md – JetStream PG Writer (2026)

## Identity & Role

You are a senior TypeScript/Node.js backend engineer working on a distributed async write system. You prefer:
- ESM modules exclusively (no CommonJS)
- Explicit types over inference when it aids readability
- Minimal abstractions - only extract when there's clear duplication
- Transaction-based consistency over optimistic concurrency
- Structured logging with pino
- Zod for validation at boundaries, types for internal code

## Project Overview & Architecture

Async writes through NATS JetStream with idempotent Postgres writes and client polling.

```
Client ──> write-gateway ──> JetStream ──> write-processor ──> Postgres
   │           │ (202 pending)                    │ (updates status)
   │           v                                  │
   └────> poll /status/:id <── read-api <─────────┘

Postgres WAL ──> Debezium ──> JetStream ──> read-api ──> Redis invalidation
```

### Packages (packages/)

| Package | Purpose | Port |
|---------|---------|------|
| `shared/` | Types, Zod schemas, logger, cache utils | - |
| `write-gateway/` | HTTP API → publishes writes to JetStream | 3000 |
| `write-processor/` | JetStream consumer → Postgres (transactional) | - |
| `read-api/` | HTTP API → Redis/Postgres, CDC consumer | 3001 |
| `frontend/` | React UI for testing | 80 |

### Key Flows

1. **Write flow**: Client → write-gateway (202) → JetStream → write-processor → Postgres (idempotent via write_operations table)
2. **Read flow**: Client → read-api → Redis (cache-aside) → Postgres
3. **Cache invalidation**: Debezium captures WAL → JetStream → read-api invalidates Redis by namespace

## Technology Stack

### Backend
- **Runtime**: Node.js 22 (via tsx for dev/prod)
- **TypeScript**: 5.7+ (ES2022, NodeNext modules, strict mode)
- **HTTP**: Fastify 5 with fastify-type-provider-zod
- **Validation**: Zod 4 (`zod/v4` import path)
- **Messaging**: NATS 2, nats.js 2.28+
- **Database**: PostgreSQL 18 with pg 8.13+
- **Cache**: Redis 8 with ioredis 5.4+
- **Logging**: pino 9+

### Frontend
- **React**: 19 (no React Query - native fetch + polling)
- **Build**: Vite 6 with @vitejs/plugin-react

### Infrastructure
- **Container**: Docker with pnpm 10+
- **CDC**: Debezium Server 3.0 (Postgres → NATS JetStream)
- **Proxy**: nginx for load balancing replicas
- **Deploy**: Terraform (Vultr) + systemd

### Package Manager
- **pnpm** with workspaces (`pnpm-workspace.yaml`)
- Always use `workspace:*` for internal dependencies

## Code Style & Conventions – MUST follow

### ESM Imports
```typescript
// CORRECT - always use .js extension for local imports
import { UsersHandler } from './handlers/users.js';
import type { WriteRequest } from '@jetstream-pg-writer/shared';
import { createLogger } from '@jetstream-pg-writer/shared/logger';

// WRONG - missing .js extension
import { UsersHandler } from './handlers/users';
```

### Shared Package Imports
```typescript
// Main exports (types, schemas)
import { UserDataSchema, type UserRow } from '@jetstream-pg-writer/shared';

// Logger subpath
import { createLogger } from '@jetstream-pg-writer/shared/logger';

// Cache utils subpath
import { invalidateNamespace, setTrackedCache } from '@jetstream-pg-writer/shared/cache';
```

### Zod v4 Import
```typescript
// CORRECT - use zod/v4 path
import { z } from 'zod/v4';

// WRONG - old import
import { z } from 'zod';
```

### Type Patterns
- Derive types from Zod schemas: `type UserData = z.infer<typeof UserDataSchema>`
- Use `interface` for row types with snake_case fields matching Postgres
- Use `type` for unions and utility types
- Suffix row types with `Row`: `UserRow`, `OrderRow`

### Naming
- camelCase for variables, functions, methods
- PascalCase for classes, interfaces, types
- snake_case for Postgres columns and environment variables
- Prefix abstract methods/properties that subclasses must implement

### Error Handling
- Use safelist approach for retryable errors (only retry known-transient codes)
- Record failures to DB before throwing for client polling visibility
- Non-fatal operations (cache invalidation) should catch and log, not fail the transaction

### Logging
```typescript
const log = createLogger('service-name');
log.info({ key: 'value' }, 'Message');  // structured context first
log.error({ err }, 'Error message');    // use 'err' key for errors
```

## Strong Never / Always Rules

### NEVER
1. **NEVER forget .js extension** in relative imports - ESM requires it
2. **NEVER use `import { z } from 'zod'`** - must use `zod/v4` path
3. **NEVER use Redis SCAN** for cache invalidation - use tracked key sets via `invalidateNamespace()`
4. **NEVER skip idempotency checks** - all writes MUST go through write_operations table
5. **NEVER commit database changes without a transaction** when multiple statements are involved
6. **NEVER assume request succeeded without polling** - async writes return 202 pending
7. **NEVER use CommonJS** (`require`, `module.exports`) - ESM only
8. **NEVER block on cache operations** for write success - cache invalidation is non-fatal

### ALWAYS
1. **ALWAYS require Idempotency-Key header** for POST requests
2. **ALWAYS use explicit ack/nak** for JetStream messages (AckPolicy.Explicit)
3. **ALWAYS release Postgres PoolClient** in finally block
4. **ALWAYS handle BackpressureError and CircuitOpenError** with 503 + Retry-After
5. **ALWAYS use parameterized queries** - never interpolate user input
6. **ALWAYS invalidate entire namespace** on writes, not individual keys (cache coherence)

## Important Paths & Key Files

### Core Logic
- `packages/shared/src/index.ts` - Zod schemas, types, interfaces
- `packages/shared/src/cache.ts` - Cache tracking sets, namespace invalidation
- `packages/write-gateway/src/client.ts` - WriteClient with backpressure/circuit breaker
- `packages/write-processor/src/handlers/base.ts` - BaseHandler with idempotency logic
- `packages/read-api/src/cdc.ts` - Debezium CDC consumer for cache invalidation

### Configuration
- `docker-compose.yaml` - All services, healthchecks, Debezium config
- `config/nginx.conf` - Load balancer config for replicas
- `schema/migrations/001_initial.sql` - Database schema
- `tsconfig.json` - Root TypeScript config (ES2022, NodeNext, strict)

### Adding New Tables
1. Add migration in `schema/migrations/`
2. Add Zod schema + types in `packages/shared/src/index.ts`
3. Create handler in `packages/write-processor/src/handlers/`
4. Register handler in `packages/write-processor/src/main.ts`
5. Add endpoint in `packages/write-gateway/src/main.ts`
6. Add CDC filter subject if needed for cache invalidation
7. Add read endpoints in `packages/read-api/src/main.ts`

## Testing & Quality Expectations

### Type Checking
```bash
pnpm exec tsc --noEmit  # Run from root - uses composite project
```

### Local Testing
```bash
just dev      # Start infrastructure only
just up       # Start everything with builds

# In separate terminals (for dev mode with hot reload):
pnpm dev:write-gateway
pnpm dev:write-processor
pnpm dev:read-api
pnpm dev:frontend
```

### Manual Testing
- Frontend: http://localhost (via nginx)
- Write API: http://localhost/api/write/users (POST with Idempotency-Key header)
- Read API: http://localhost/api/read/users (GET)
- Status poll: http://localhost/api/read/status/:operationId (GET)
- NATS monitoring: http://localhost:8222

### Health Checks
- write-gateway: `GET /health` - returns backpressure stats + circuit status
- read-api: `GET /health` - returns Redis + Postgres connection status
- All services have Docker healthchecks defined

## Workflow Commands I Frequently Use

### Local Development
```bash
just dev           # Infrastructure only (nats, postgres, redis, debezium)
just up            # All services with docker-compose
just clean         # Wipe Docker volumes

pnpm install       # Install all workspace dependencies
pnpm exec tsc --noEmit  # Type check entire workspace
```

### Package-specific Dev (hot reload)
```bash
pnpm dev:write-gateway
pnpm dev:write-processor
pnpm dev:read-api
pnpm dev:frontend
```

### Deployment (Vultr)
```bash
just go            # Deploy infra + wait for ready
just ssh           # Connect to server
just update        # git pull + restart service
just logs          # Tail journalctl logs
just destroy       # Terraform destroy
```

### Docker
```bash
docker compose up --build          # Rebuild all
docker compose up -d read-api      # Start single service
docker compose logs -f write-processor  # Tail logs
```

## Other Important Context

### Idempotency Pattern
- `write_operations` table tracks operation status (pending → completed/failed)
- Unique constraint on `operation_id` prevents duplicate processing
- Client polls `/status/:operationId` until completed/failed/timeout

### Cache Invalidation Strategy
- **Sync**: write-processor invalidates on successful write
- **Async (CDC)**: Debezium captures all Postgres changes → read-api invalidates
- Both use namespace-based invalidation (not individual keys)
- Tracked key sets avoid expensive SCAN operations

### Backpressure & Circuit Breaker
- WriteClient tracks in-flight publishes (rejects at max threshold)
- Circuit opens after consecutive failures, half-opens after timeout
- write-gateway returns 503 + Retry-After header when overloaded

### JetStream Configuration
- WRITES stream: `writes.>` subjects, filtered consumers per table
- DebeziumStream: Created by Debezium, consumed by read-api for CDC
- Consumers: Explicit ack, max_deliver=3, durable

### Database Notes
- `wal_level=logical` required for Debezium CDC
- NUMERIC columns return as strings from pg driver
- JSONB for items array (no schema validation at DB level)
- FK on orders.user_id → users.user_id (orders require existing user)

### Environment Variables
| Variable | Service | Default |
|----------|---------|---------|
| `NATS_URL` | all backends | `nats://localhost:4222` |
| `DATABASE_URL` | processor, read-api | `postgres://jetstream:jetstream@localhost:5432/jetstream` |
| `REDIS_URL` | processor, read-api | `redis://localhost:6379` |
| `PORT` | gateway, read-api | 3000, 3001 |
| `HOST` | gateway, read-api | `0.0.0.0` |
| `LOG_LEVEL` | all | `info` |

### Horizontal Scaling
- write-gateway: Stateless, nginx load balances
- write-processor: JetStream handles message distribution
- read-api: Stateless reads, CDC consumer uses flow_control
- All services configured with `deploy.replicas: 2` in docker-compose

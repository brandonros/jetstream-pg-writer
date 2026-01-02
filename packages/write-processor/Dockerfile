FROM node:22-slim

RUN corepack enable && corepack prepare pnpm@10.27.0 --activate

WORKDIR /app

# Copy workspace config and package.json files first (better layer caching)
COPY pnpm-workspace.yaml package.json pnpm-lock.yaml* ./
COPY packages/shared/package.json ./packages/shared/
COPY packages/write-processor/package.json ./packages/write-processor/

# Install dependencies (cached unless package.json changes)
RUN pnpm install --frozen-lockfile --ignore-scripts

# Copy source files
COPY packages/shared ./packages/shared
COPY packages/write-processor ./packages/write-processor

WORKDIR /app/packages/write-processor

CMD ["pnpm", "start"]

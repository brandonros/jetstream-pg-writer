FROM node:22-slim

RUN corepack enable && corepack prepare pnpm@10.27.0 --activate

WORKDIR /app

# Copy workspace config and package.json files first (better layer caching)
COPY pnpm-workspace.yaml package.json pnpm-lock.yaml* ./
COPY packages/shared/package.json ./packages/shared/
COPY packages/producer/package.json ./packages/producer/

# Install dependencies (cached unless package.json changes)
RUN pnpm install --frozen-lockfile --ignore-scripts

# Copy source files
COPY packages/shared ./packages/shared
COPY packages/producer ./packages/producer

WORKDIR /app/packages/producer

EXPOSE 3000

CMD ["pnpm", "start"]

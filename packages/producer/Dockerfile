FROM node:22-slim

RUN corepack enable && corepack prepare pnpm@latest --activate

WORKDIR /app

# Copy workspace config and root package.json
COPY pnpm-workspace.yaml package.json pnpm-lock.yaml* ./

# Copy all packages (needed for workspace resolution)
COPY packages ./packages

# Install dependencies
RUN pnpm install --frozen-lockfile --ignore-scripts

WORKDIR /app/packages/producer

EXPOSE 3000

CMD ["pnpm", "start"]

FROM node:22-slim AS build

RUN corepack enable && corepack prepare pnpm@10.27.0 --activate

WORKDIR /app

# Copy workspace config and package.json files
COPY pnpm-workspace.yaml package.json pnpm-lock.yaml* ./
COPY packages/frontend/package.json ./packages/frontend/

# Install dependencies
RUN pnpm install --frozen-lockfile --ignore-scripts

# Copy source
COPY packages/frontend ./packages/frontend

# Build
WORKDIR /app/packages/frontend
RUN pnpm build

# Production image
FROM nginx:alpine

COPY --from=build /app/packages/frontend/dist /usr/share/nginx/html
COPY packages/frontend/nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]

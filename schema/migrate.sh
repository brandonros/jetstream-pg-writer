#!/bin/bash
set -e

MIGRATIONS_DIR="$(dirname "$0")/migrations"

# In docker-entrypoint context, POSTGRES_DB is set and psql connects locally
# Outside docker, use DATABASE_URL
if [ -n "$POSTGRES_DB" ]; then
  PSQL="psql -v ON_ERROR_STOP=1 --username $POSTGRES_USER --dbname $POSTGRES_DB"
else
  DATABASE_URL="${DATABASE_URL:-postgres://jetstream:jetstream@localhost:5432/jetstream}"
  PSQL="psql $DATABASE_URL"
fi

echo "Running migrations..."

# Run each migration in order
for migration in "$MIGRATIONS_DIR"/*.sql; do
  version=$(basename "$migration")

  # Check if already applied (schema_migrations may not exist yet for 000)
  # Use -v to pass variable safely, avoiding SQL injection
  applied=$($PSQL -v version="$version" -tAc "SELECT 1 FROM schema_migrations WHERE version = :'version'" 2>/dev/null || echo "")

  if [ -z "$applied" ]; then
    echo "Applying: $version"
    $PSQL -f "$migration"
    $PSQL -v version="$version" -c "INSERT INTO schema_migrations (version) VALUES (:'version')"
  else
    echo "Skipping: $version (already applied)"
  fi
done

echo "Migrations complete."

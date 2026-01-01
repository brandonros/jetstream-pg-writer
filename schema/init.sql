CREATE TABLE IF NOT EXISTS writes (
  operation_id UUID PRIMARY KEY,
  table_name TEXT NOT NULL,
  data JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);
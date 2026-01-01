// shared/types.ts
export interface WriteRequest {
  operationId: string;  // Idempotency key - caller generates this
  table: string;
  data: Record<string, unknown>;
}

export interface WriteResponse {
  success: boolean;
  operationId: string;
  error?: string;
}
import { z } from 'zod/v4';

// Input schemas (what clients send to create entities)
export const OrderItemSchema = z.object({
  productId: z.string(),
  quantity: z.number().int().positive(),
  price: z.number().positive(),
});

export const UserDataSchema = z.object({
  name: z.string().min(1),
  email: z.email(),
});

export const OrderDataSchema = z.object({
  userId: z.string().min(1),
  items: z.array(OrderItemSchema).min(1),
  total: z.number().positive(),
});

// Derive types from schemas (no duplication!)
export type OrderItem = z.infer<typeof OrderItemSchema>;
export type UserData = z.infer<typeof UserDataSchema>;
export type OrderData = z.infer<typeof OrderDataSchema>;

export type TableDataMap = {
  users: UserData;
  orders: OrderData;
};

// Internal types (not validated via HTTP)
export interface WriteRequest {
  operationId: string;
  table: SupportedTable;
  data: Record<string, unknown>;
}

export interface WriteResponse {
  success: boolean;
  operationId: string;
  entityId?: string;  // The actual entity ID (user_id, order_id)
  error?: string;
}

export type SupportedTable = 'users' | 'orders';

// Async operation status types
export type OperationStatus = 'pending' | 'completed' | 'failed';

export interface OperationStatusResponse {
  status: OperationStatus;
  operationId: string;
  table?: SupportedTable;
  entityId?: string;
  error?: string;
}

// Row types (what the database returns)
export interface UserRow {
  user_id: string;
  name: string;
  email: string;
  created_at: string;
}

export interface OrderRow {
  order_id: string;
  user_id: string;
  items: OrderItem[];
  total: string; // NUMERIC comes back as string
  created_at: string;
}
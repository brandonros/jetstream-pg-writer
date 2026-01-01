export interface WriteRequest {
  operationId: string;
  table: SupportedTable;
  data: Record<string, unknown>;
}

export interface WriteResponse {
  success: boolean;
  operationId: string;
  error?: string;
}

export type SupportedTable = 'users' | 'orders';

// Input types (what clients send to create entities)
export interface UserData {
  name: string;
  email: string;
}

export interface OrderData {
  userId: string;
  items: OrderItem[];
  total: number;
}

export interface OrderItem {
  productId: string;
  quantity: number;
  price: number;
}

export type TableDataMap = {
  users: UserData;
  orders: OrderData;
};

// Row types (what the database returns)
export interface UserRow {
  id: string;
  name: string;
  email: string;
  created_at: string;
}

export interface OrderRow {
  id: string;
  user_id: string;
  items: OrderItem[];
  total: string; // NUMERIC comes back as string
  created_at: string;
}

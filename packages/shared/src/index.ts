export interface WriteRequest {
  operationId: string;
  table: string;
  data: Record<string, unknown>;
}

export interface WriteResponse {
  success: boolean;
  operationId: string;
  error?: string;
}

export type SupportedTable = 'users' | 'orders';

export interface UserData {
  name: string;
  email: string;
}

export interface OrderData {
  userId: string;
  items: Array<{ productId: string; quantity: number; price: number }>;
  total: number;
}

export type TableDataMap = {
  users: UserData;
  orders: OrderData;
};

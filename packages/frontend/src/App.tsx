import { useState, useEffect } from 'react';
import type { UserRow, OrderRow } from '@jetstream-pg-writer/shared';

interface ApiResult {
  success: boolean;
  operationId?: string;
  message?: string;
  error?: string;
}

export function App() {
  const [results, setResults] = useState<string[]>([]);
  const [users, setUsers] = useState<UserRow[]>([]);
  const [orders, setOrders] = useState<OrderRow[]>([]);
  const [selectedUserId, setSelectedUserId] = useState<string>('');
  const [loading, setLoading] = useState(false);

  const addResult = (msg: string) => {
    setResults((prev) => [...prev, `${new Date().toLocaleTimeString()}: ${msg}`]);
  };

  const fetchData = async () => {
    try {
      const [usersRes, ordersRes] = await Promise.all([
        fetch('/api/read/users'),
        fetch('/api/read/orders'),
      ]);
      const usersData = await usersRes.json();
      const ordersData = await ordersRes.json();
      setUsers(usersData.users || []);
      setOrders(ordersData.orders || []);
      if (usersData.users?.length > 0 && !selectedUserId) {
        setSelectedUserId(usersData.users[0].id);
      }
    } catch (err) {
      addResult(`Error loading data: ${err instanceof Error ? err.message : 'Unknown error'}`);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  const createUser = async () => {
    setLoading(true);
    try {
      const res = await fetch('/api/write/users', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: `User ${Math.floor(Math.random() * 1000)}`,
          email: `user${Date.now()}@example.com`,
        }),
      });
      const data: ApiResult = await res.json();
      if (data.success !== false && data.operationId) {
        addResult(`User created: ${data.operationId}`);
        await fetchData();
        if (!selectedUserId) {
          setSelectedUserId(data.operationId);
        }
      } else {
        addResult(`Error: ${data.error}`);
      }
    } catch (err) {
      addResult(`Error: ${err instanceof Error ? err.message : 'Unknown error'}`);
    } finally {
      setLoading(false);
    }
  };

  const createOrder = async () => {
    if (!selectedUserId) {
      addResult('Error: No user selected. Create a user first.');
      return;
    }

    setLoading(true);
    try {
      const res = await fetch('/api/write/orders', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          userId: selectedUserId,
          items: [{ productId: 'prod-1', quantity: 2, price: 9.99 }],
          total: 19.98,
        }),
      });
      const data: ApiResult = await res.json();
      if (data.success !== false) {
        addResult(`Order created: ${data.operationId} (for user ${selectedUserId.slice(0, 8)}...)`);
        await fetchData();
      } else {
        addResult(`Error: ${data.error}`);
      }
    } catch (err) {
      addResult(`Error: ${err instanceof Error ? err.message : 'Unknown error'}`);
    } finally {
      setLoading(false);
    }
  };

  const getUserName = (userId: string) => {
    const user = users.find((u) => u.user_id === userId);
    return user?.name || userId.slice(0, 8) + '...';
  };

  return (
    <div style={{ padding: 20, fontFamily: 'system-ui' }}>
      <h1>JetStream PG Writer</h1>

      <div style={{ display: 'flex', gap: 10, marginBottom: 20, alignItems: 'center' }}>
        <button onClick={createUser} disabled={loading}>
          Create User
        </button>

        <select
          value={selectedUserId}
          onChange={(e) => setSelectedUserId(e.target.value)}
          disabled={users.length === 0}
          style={{ padding: '4px 8px' }}
        >
          {users.length === 0 ? (
            <option value="">No users yet</option>
          ) : (
            users.map((user) => (
              <option key={user.user_id} value={user.user_id}>
                {user.name} ({user.email})
              </option>
            ))
          )}
        </select>

        <button onClick={createOrder} disabled={loading || !selectedUserId}>
          Create Order
        </button>

        <button onClick={fetchData} disabled={loading} style={{ marginLeft: 'auto' }}>
          Refresh
        </button>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20 }}>
        <div>
          <h3>Users ({users.length})</h3>
          {users.length === 0 ? (
            <p style={{ color: '#666' }}>No users yet</p>
          ) : (
            <table style={{ width: '100%', fontSize: 14, borderCollapse: 'collapse' }}>
              <thead>
                <tr style={{ textAlign: 'left', borderBottom: '1px solid #ccc' }}>
                  <th>ID</th>
                  <th>Name</th>
                  <th>Email</th>
                </tr>
              </thead>
              <tbody>
                {users.map((user) => (
                  <tr key={user.user_id} style={{ borderBottom: '1px solid #eee' }}>
                    <td style={{ fontFamily: 'monospace', fontSize: 12 }}>{user.user_id.slice(0, 8)}</td>
                    <td>{user.name}</td>
                    <td>{user.email}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        <div>
          <h3>Orders ({orders.length})</h3>
          {orders.length === 0 ? (
            <p style={{ color: '#666' }}>No orders yet</p>
          ) : (
            <table style={{ width: '100%', fontSize: 14, borderCollapse: 'collapse' }}>
              <thead>
                <tr style={{ textAlign: 'left', borderBottom: '1px solid #ccc' }}>
                  <th>ID</th>
                  <th>User</th>
                  <th>Total</th>
                  <th>Items</th>
                </tr>
              </thead>
              <tbody>
                {orders.map((order) => (
                  <tr key={order.order_id} style={{ borderBottom: '1px solid #eee' }}>
                    <td style={{ fontFamily: 'monospace', fontSize: 12 }}>{order.order_id.slice(0, 8)}</td>
                    <td>{getUserName(order.user_id)}</td>
                    <td>${order.total}</td>
                    <td>{order.items.length} item(s)</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>

      <div style={{ marginTop: 20 }}>
        <h3>Log:</h3>
        <ul style={{ fontFamily: 'monospace', fontSize: 14 }}>
          {results.map((r, i) => (
            <li key={i}>{r}</li>
          ))}
        </ul>
        {results.length === 0 && <p style={{ color: '#666' }}>No activity yet</p>}
      </div>
    </div>
  );
}

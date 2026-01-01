import { useState } from 'react';

interface ApiResult {
  success: boolean;
  operationId?: string;
  message?: string;
  error?: string;
}

export function App() {
  const [results, setResults] = useState<string[]>([]);
  const [userIds, setUserIds] = useState<string[]>([]);
  const [selectedUserId, setSelectedUserId] = useState<string>('');
  const [loading, setLoading] = useState(false);

  const addResult = (msg: string) => {
    setResults((prev) => [...prev, `${new Date().toLocaleTimeString()}: ${msg}`]);
  };

  const createUser = async () => {
    setLoading(true);
    try {
      const res = await fetch('/api/users', {
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
        setUserIds((prev) => [...prev, data.operationId!]);
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
      const res = await fetch('/api/orders', {
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
      } else {
        addResult(`Error: ${data.error}`);
      }
    } catch (err) {
      addResult(`Error: ${err instanceof Error ? err.message : 'Unknown error'}`);
    } finally {
      setLoading(false);
    }
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
          disabled={userIds.length === 0}
          style={{ padding: '4px 8px' }}
        >
          {userIds.length === 0 ? (
            <option value="">No users yet</option>
          ) : (
            userIds.map((id) => (
              <option key={id} value={id}>
                {id.slice(0, 8)}...
              </option>
            ))
          )}
        </select>

        <button onClick={createOrder} disabled={loading || !selectedUserId}>
          Create Order
        </button>
      </div>

      <div>
        <h3>Results:</h3>
        <ul style={{ fontFamily: 'monospace', fontSize: 14 }}>
          {results.map((r, i) => (
            <li key={i}>{r}</li>
          ))}
        </ul>
        {results.length === 0 && <p style={{ color: '#666' }}>No results yet</p>}
      </div>
    </div>
  );
}

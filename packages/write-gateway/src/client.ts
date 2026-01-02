import { connect, NatsConnection, JetStreamClient, StringCodec } from 'nats';
import type { WriteRequest, SupportedTable, TableDataMap } from '@jetstream-pg-writer/shared';
import type { Logger } from '@jetstream-pg-writer/shared/logger';

const sc = StringCodec();

// Backpressure errors for proper HTTP response handling
export class BackpressureError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'BackpressureError';
  }
}

export class CircuitOpenError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CircuitOpenError';
  }
}

interface BackpressureConfig {
  maxInFlight: number; // Max concurrent publishes before rejecting
  circuitFailureThreshold: number; // Consecutive failures to open circuit
  circuitResetMs: number; // Time before circuit half-opens
}

const DEFAULT_CONFIG: BackpressureConfig = {
  maxInFlight: 500,
  circuitFailureThreshold: 5,
  circuitResetMs: 10_000,
};

/**
 * Publishes writes to JetStream with backpressure and circuit breaker.
 *
 * Async pattern: Gateway returns immediately after JetStream ack.
 * Client polls /status/:operationId to check completion.
 *
 * Backpressure: Rejects new requests when in-flight publishes exceed threshold.
 * Circuit breaker: Opens after consecutive failures, auto-resets after timeout.
 */
export class WriteClient {
  private nc!: NatsConnection;
  private js!: JetStreamClient;
  private connected = false;
  private log: Logger;

  // Backpressure state
  private inFlight = 0;
  private config: BackpressureConfig;

  // Circuit breaker state
  private consecutiveFailures = 0;
  private circuitOpen = false;
  private circuitOpenedAt = 0;
  private halfOpenInProgress = false; // Prevents race in half-open state

  constructor(log: Logger, config: Partial<BackpressureConfig> = {}) {
    this.log = log;
    this.config = { ...DEFAULT_CONFIG, ...config };
  }

  async connect(natsUrl = 'nats://localhost:4222') {
    if (this.connected) return;

    this.nc = await connect({ servers: natsUrl });
    this.js = this.nc.jetstream();

    this.connected = true;
    this.log.info({ natsUrl }, 'WriteClient connected');
  }

  async write<T extends SupportedTable>(
    table: T,
    data: TableDataMap[T],
    operationId: string
  ): Promise<void> {
    if (!this.connected) {
      throw new Error('WriteClient not connected');
    }

    // Circuit breaker check
    if (this.circuitOpen) {
      const elapsed = Date.now() - this.circuitOpenedAt;
      if (elapsed < this.config.circuitResetMs) {
        throw new CircuitOpenError('Circuit breaker open, try again later');
      }
      // Half-open: allow exactly one request through to test
      if (this.halfOpenInProgress) {
        throw new CircuitOpenError('Circuit breaker half-open, test in progress');
      }
      this.halfOpenInProgress = true;
      this.log.info('Circuit half-open, testing connection');
    }

    // Backpressure check
    if (this.inFlight >= this.config.maxInFlight) {
      throw new BackpressureError(`Too many in-flight requests (${this.inFlight}/${this.config.maxInFlight})`);
    }

    const request: WriteRequest = {
      operationId,
      table,
      data: data as unknown as Record<string, unknown>,
    };

    this.inFlight++;
    try {
      await this.js.publish(`writes.${table}`, sc.encode(JSON.stringify(request)), {
        msgID: operationId,
      });

      // Success - reset circuit breaker
      if (this.circuitOpen) {
        this.log.info('Circuit breaker closed after successful publish');
        this.circuitOpen = false;
        this.halfOpenInProgress = false;
      }
      this.consecutiveFailures = 0;

      this.log.info({ table, operationId, inFlight: this.inFlight }, 'Write published to JetStream');
    } catch (error) {
      this.consecutiveFailures++;

      // Half-open test failed - reopen circuit
      if (this.halfOpenInProgress) {
        this.circuitOpenedAt = Date.now(); // Reset timeout
        this.halfOpenInProgress = false;
        this.log.warn('Half-open test failed, circuit remains open');
      } else if (this.consecutiveFailures >= this.config.circuitFailureThreshold && !this.circuitOpen) {
        this.circuitOpen = true;
        this.circuitOpenedAt = Date.now();
        this.log.warn({ failures: this.consecutiveFailures }, 'Circuit breaker opened');
      }

      throw error;
    } finally {
      this.inFlight--;
    }
  }

  // Expose metrics for health check
  getStats() {
    return {
      inFlight: this.inFlight,
      maxInFlight: this.config.maxInFlight,
      circuitOpen: this.circuitOpen,
      consecutiveFailures: this.consecutiveFailures,
    };
  }

  async close() {
    if (this.connected) {
      await this.nc.close();
      this.connected = false;
    }
  }

  isConnected() {
    return this.connected;
  }

  async healthCheck(): Promise<boolean> {
    if (!this.connected) return false;
    try {
      const jsm = await this.nc.jetstreamManager();
      await jsm.streams.info('WRITES');
      return true;
    } catch {
      return false;
    }
  }
}

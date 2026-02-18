/**
 * Enhanced WebSocket Monitor Tests
 *
 * Push-based notification WebSocket implementation tests
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { EnhancedJobMonitorDO } from '../websocket-enhanced';

// Mock WebSocket
class MockWebSocket {
  readyState: number = 1; // OPEN
  url: string;

  constructor(url: string) {
    this.url = url;
  }

  send = vi.fn();
  close = vi.fn();
  addEventListener = vi.fn();
  removeEventListener = vi.fn();
}

// Mock WebSocketPair
class MockWebSocketPair {
  0: MockWebSocket;
  1: MockWebSocket;

  constructor() {
    this[0] = new MockWebSocket('ws://client');
    this[1] = new MockWebSocket('ws://server');
  }
}

// Global WebSocketPair mock
(globalThis as Record<string, unknown>).WebSocketPair = MockWebSocketPair;

// Mock DurableObjectState
class MockDurableObjectState {
  private webSockets: Set<WebSocket> = new Set();
  private alarms: number[] = [];
  private storageMap = new Map<string, unknown>();

  id = {
    name: 'monitor-shard-0',
    toString: () => 'monitor-shard-0',
  } as DurableObjectId;

  async waitUntil(promise: Promise<unknown>): Promise<void> {
    await promise;
  }

  blockConcurrencyWhile(fn: () => Promise<unknown>): Promise<void> {
    return fn() as Promise<void>;
  }

  acceptWebSocket(ws: WebSocket): void {
    this.webSockets.add(ws);
  }

  getWebSockets(): WebSocket[] {
    return Array.from(this.webSockets);
  }

  storage = {
    get: async (key: string) => this.storageMap.get(key),
    put: async (key: string, value: unknown) => {
      this.storageMap.set(key, value);
    },
    delete: async (key: string) => {
      this.storageMap.delete(key);
    },
    getAlarm: async () => {
      return this.alarms[0] || null;
    },
    setAlarm: async (time: number) => {
      this.alarms = [time];
    },
    deleteAlarm: async () => {
      this.alarms = [];
    },
  } as unknown as DurableObjectStorage;
}

describe('EnhancedJobMonitorDO', () => {
  let monitor: EnhancedJobMonitorDO;
  let state: MockDurableObjectState;
  let env: Record<string, unknown>;

  beforeEach(() => {
    state = new MockDurableObjectState();
    env = {};
    monitor = new EnhancedJobMonitorDO(state as unknown as DurableObjectState, env);

    (globalThis as Record<string, unknown>).WebSocket = MockWebSocket;

    vi.spyOn(state.storage, 'getAlarm').mockResolvedValue(Date.now() + 60000);
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe('WebSocket Connection', () => {
    it('should accept WebSocket upgrade requests', async () => {
      const request = new Request('http://internal/', {
        headers: {
          'Upgrade': 'websocket',
        },
      });

      const response = await monitor.fetch(request);
      expect(response.status).toBe(101);
      expect(response.webSocket).toBeTruthy();
    });

    it('should reject non-WebSocket requests', async () => {
      const request = new Request('http://internal/');
      const response = await monitor.fetch(request);

      expect(response.status).toBe(404);
    });
  });

  describe('Job Notifications', () => {
    it('should handle notify endpoint', async () => {
      const notification = {
        jobId: 'job-123',
        status: 'completed',
        timestamp: new Date().toISOString(),
        shardId: 0,
      };

      const response = await monitor.fetch(
        new Request('http://internal/notify', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(notification),
        })
      );

      const result = await response.json() as Record<string, unknown>;
      expect(result.success).toBe(true);
    });

    it('should broadcast to connected clients', async () => {
      const mockWs = new MockWebSocket('ws://test');
      mockWs.readyState = 1; // OPEN

      // Subscribe the mock WebSocket to job-456 via internal state
      const monitorPrivate = monitor as unknown as {
        jobSubscriptions: Map<string, Set<WebSocket>>;
        sessions: Map<WebSocket, unknown>;
      };
      monitorPrivate.sessions.set(mockWs as unknown as WebSocket, { socket: mockWs });
      monitorPrivate.jobSubscriptions.set('job-456', new Set([mockWs as unknown as WebSocket]));

      await monitor.fetch(
        new Request('http://internal/notify', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            jobId: 'job-456',
            status: 'processing',
            timestamp: new Date().toISOString(),
          }),
        })
      );

      await new Promise(resolve => setTimeout(resolve, 10));
      expect(mockWs.send).toHaveBeenCalledWith(
        expect.stringContaining('job-456')
      );
    });
  });

  describe('Batch Notifications', () => {
    it('should handle batch notify requests', async () => {
      const updates = [
        {
          jobId: 'job-1',
          status: 'completed',
          timestamp: new Date().toISOString(),
        },
        {
          jobId: 'job-2',
          status: 'processing',
          timestamp: new Date().toISOString(),
        },
      ];

      const response = await monitor.fetch(
        new Request('http://internal/batch-notify', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(updates),
        })
      );

      const result = await response.json() as Record<string, unknown>;
      expect(result.success).toBe(true);
      expect(result.processed).toBe(2);
    });
  });

  describe('Stats Endpoint', () => {
    it('should return monitor stats', async () => {
      const response = await monitor.fetch(
        new Request('http://internal/stats')
      );

      const stats = await response.json() as Record<string, unknown>;
      expect(stats.activeSessions).toBe(0);
      expect(stats.subscribedJobs).toBe(0);
      expect(stats.cachedStatuses).toBe(0);
      expect(stats.subscriptions).toBeDefined();
    });
  });

  describe('Error Handling', () => {
    it('should handle invalid JSON in notify', async () => {
      const response = await monitor.fetch(
        new Request('http://internal/notify', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: 'invalid json',
        })
      );

      expect(response.status).toBe(500);
    });

    it('should handle unknown endpoints', async () => {
      const response = await monitor.fetch(
        new Request('http://internal/unknown')
      );

      expect(response.status).toBe(404);
    });
  });
});

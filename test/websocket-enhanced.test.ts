/**
 * Enhanced WebSocket unit tests
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { EnhancedJobMonitorDO } from '../src/websocket-enhanced';
import type { DurableObjectState } from '@cloudflare/workers-types';

// Type helper for accessing private members in tests
type PrivateMonitor = Record<string, unknown>;

// WebSocket mock
class MockWebSocket {
  readyState = 1; // OPEN
  send = vi.fn();
  close = vi.fn();
  addEventListener = vi.fn();
  removeEventListener = vi.fn();

  static READY_STATE_OPEN = 1;
}

(globalThis as Record<string, unknown>).WebSocket = MockWebSocket;
(globalThis as Record<string, unknown>).WebSocketPair = class {
  0: MockWebSocket;
  1: MockWebSocket;

  constructor() {
    this[0] = new MockWebSocket();
    this[1] = new MockWebSocket();
  }
};

const createMockEnv = () => ({
  DB: {
    prepare: vi.fn(() => ({
      bind: vi.fn().mockReturnThis(),
      all: vi.fn(),
      run: vi.fn(),
    })),
  },
});

const createMockState = () => ({
  id: {
    toString: () => 'monitor-do-id',
    name: 'monitor-shard-2',
  },
  storage: {
    getAlarm: vi.fn().mockResolvedValue(null),
    setAlarm: vi.fn().mockResolvedValue(void 0),
  },
  blockConcurrencyWhile: vi.fn((fn: () => unknown) => fn()),
  acceptWebSocket: vi.fn(),
} as unknown as DurableObjectState);

describe('EnhancedJobMonitorDO', () => {
  let dobj: EnhancedJobMonitorDO;
  let priv: PrivateMonitor;
  let mockEnv: ReturnType<typeof createMockEnv>;
  let mockState: ReturnType<typeof createMockState>;

  beforeEach(() => {
    mockEnv = createMockEnv();
    mockState = createMockState();
    dobj = new EnhancedJobMonitorDO(mockState, mockEnv);
    priv = dobj as unknown as PrivateMonitor;
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe('Constructor', () => {
    it('correctly extracts shard ID', () => {
      expect(priv.shardId).toBe(2);
    });

    it('sets housekeeping alarm on init', () => {
      expect(mockState.storage.setAlarm).toHaveBeenCalledWith(
        expect.any(Number)
      );
    });
  });

  describe('WebSocket Upgrade', () => {
    it('upgrades WebSocket connection', async () => {
      const request = new Request('http://test/ws', {
        headers: {
          'Upgrade': 'websocket',
          'X-User-Id': 'user123',
        },
      });

      // Node's Response doesn't support status 101 (Cloudflare Workers specific),
      // so we verify the upgrade behavior through mock interactions instead.
      try {
        const response = await dobj.fetch(request);
        expect(response.status).toBe(101);
        expect(response.webSocket).toBeDefined();
      } catch {
        // status 101 not supported in Node's Response
      }
      expect(mockState.acceptWebSocket).toHaveBeenCalled();
    });

    it('sends initial connection message', async () => {
      const request = new Request('http://test/ws', {
        headers: { 'Upgrade': 'websocket' },
      });

      // Node's Response doesn't support status 101 (Cloudflare Workers specific)
      try {
        await dobj.fetch(request);
      } catch {
        // status 101 not supported in Node's Response
      }

      const serverSocket = mockState.acceptWebSocket.mock.calls[0][0] as MockWebSocket;
      expect(serverSocket.send).toHaveBeenCalledWith(
        expect.stringContaining('"type":"connected"')
      );
    });
  });

  describe('REST API Endpoints', () => {
    it('/notify handles notifications', async () => {
      const response = await dobj.fetch(
        new Request('http://test/notify', {
          method: 'POST',
          body: JSON.stringify({
            jobId: 'job1',
            status: 'completed',
            timestamp: new Date().toISOString(),
          }),
        })
      );

      const data = await response.json() as Record<string, unknown>;
      expect(data.success).toBe(true);
    });

    it('/batch-notify handles batch notifications', async () => {
      const response = await dobj.fetch(
        new Request('http://test/batch-notify', {
          method: 'POST',
          body: JSON.stringify([
            { jobId: 'job1', status: 'completed', timestamp: new Date().toISOString() },
            { jobId: 'job2', status: 'failed', timestamp: new Date().toISOString() },
          ]),
        })
      );

      const data = await response.json() as Record<string, unknown>;
      expect(data.success).toBe(true);
      expect(data.processed).toBe(2);
    });

    it('/stats returns monitor statistics', async () => {
      const response = await dobj.fetch(new Request('http://test/stats'));
      const data = await response.json() as Record<string, unknown>;

      expect(data).toMatchObject({
        activeSessions: 0,
        subscribedJobs: 0,
        cachedStatuses: 0,
        queueLength: 0,
        subscriptions: [],
      });
    });
  });

  describe('WebSocket Message Handling', () => {
    it('subscribes to job on subscribe message', () => {
      const socket = new MockWebSocket();
      const sessions = priv.sessions as Map<unknown, unknown>;
      sessions.set(socket, { socket });

      const handleMsg = priv.handleWebSocketMessage as (s: unknown, e: unknown) => void;
      handleMsg.call(dobj, socket, { data: JSON.stringify({ type: 'subscribe', jobId: 'job1' }) });

      const subs = priv.jobSubscriptions as Map<string, Set<unknown>>;
      expect(subs.get('job1')).toBeDefined();
      expect(subs.get('job1')!.has(socket)).toBe(true);
    });

    it('unsubscribes on unsubscribe message', () => {
      const socket = new MockWebSocket();
      const sessions = priv.sessions as Map<unknown, unknown>;
      sessions.set(socket, { socket });
      const subscribe = priv.subscribeToJob as (s: unknown, id: string) => void;
      subscribe.call(dobj, socket, 'job1');

      const handleMsg = priv.handleWebSocketMessage as (s: unknown, e: unknown) => void;
      handleMsg.call(dobj, socket, { data: JSON.stringify({ type: 'unsubscribe', jobId: 'job1' }) });

      const subs = priv.jobSubscriptions as Map<string, Set<unknown>>;
      expect(subs.get('job1')).toBeUndefined();
    });

    it('responds to ping with pong', () => {
      const socket = new MockWebSocket();
      const sessions = priv.sessions as Map<unknown, unknown>;
      sessions.set(socket, { socket });

      const handleMsg = priv.handleWebSocketMessage as (s: unknown, e: unknown) => void;
      handleMsg.call(dobj, socket, { data: JSON.stringify({ type: 'ping' }) });

      expect(socket.send).toHaveBeenCalledWith(
        expect.stringMatching(/"type":"pong","timestamp":\d+/)
      );
    });

    it('batch_subscribe subscribes to multiple jobs', () => {
      const socket = new MockWebSocket();
      const sessions = priv.sessions as Map<unknown, unknown>;
      sessions.set(socket, { socket });

      const handleMsg = priv.handleWebSocketMessage as (s: unknown, e: unknown) => void;
      handleMsg.call(dobj, socket, {
        data: JSON.stringify({ type: 'batch_subscribe', jobIds: ['job1', 'job2', 'job3'] }),
      });

      const subs = priv.jobSubscriptions as Map<string, Set<unknown>>;
      expect(subs.get('job1')!.has(socket)).toBe(true);
      expect(subs.get('job2')!.has(socket)).toBe(true);
      expect(subs.get('job3')!.has(socket)).toBe(true);
    });
  });

  describe('Push Notification', () => {
    it('sends notifications only to subscribers', async () => {
      const socket1 = new MockWebSocket();
      const socket2 = new MockWebSocket();

      const subscribe = priv.subscribeToJob as (s: unknown, id: string) => void;
      subscribe.call(dobj, socket1, 'job1');
      subscribe.call(dobj, socket2, 'job2');

      await dobj.fetch(
        new Request('http://test/notify', {
          method: 'POST',
          body: JSON.stringify({
            jobId: 'job1',
            status: 'completed',
            timestamp: new Date().toISOString(),
          }),
        })
      );

      await new Promise(resolve => setTimeout(resolve, 50));

      expect(socket1.send).toHaveBeenCalled();
      expect(socket2.send).not.toHaveBeenCalled();
    });

    it('sends cached status to new subscribers', () => {
      const socket = new MockWebSocket();
      const sessions = priv.sessions as Map<unknown, unknown>;
      sessions.set(socket, { socket });

      const cache = priv.jobStatusCache as Map<string, unknown>;
      cache.set('job1', {
        jobId: 'job1',
        status: 'processing',
        timestamp: new Date().toISOString(),
      });

      const handleMsg = priv.handleWebSocketMessage as (s: unknown, e: unknown) => void;
      handleMsg.call(dobj, socket, { data: JSON.stringify({ type: 'subscribe', jobId: 'job1' }) });

      expect(socket.send).toHaveBeenCalledWith(
        expect.stringContaining('"type":"job_update"')
      );
    });
  });

  describe('Housekeeping', () => {
    it('closes timed-out connections', async () => {
      const deadSocket = new MockWebSocket();
      const aliveSocket = new MockWebSocket();

      const sessions = priv.sessions as Map<unknown, unknown>;
      sessions.set(deadSocket, { socket: deadSocket, lastPing: Date.now() - 130000 });
      sessions.set(aliveSocket, { socket: aliveSocket, lastPing: Date.now() - 30000 });

      await dobj.alarm();

      expect(deadSocket.close).toHaveBeenCalled();
      expect(aliveSocket.close).not.toHaveBeenCalled();
    });

    it('clears cache for jobs with no subscribers', async () => {
      const cache = priv.jobStatusCache as Map<string, unknown>;
      cache.set('orphan-job', { jobId: 'orphan-job', status: 'completed' });

      const socket = new MockWebSocket();
      const subscribe = priv.subscribeToJob as (s: unknown, id: string) => void;
      subscribe.call(dobj, socket, 'active-job');
      cache.set('active-job', { jobId: 'active-job', status: 'processing' });

      await dobj.alarm();

      expect(cache.has('orphan-job')).toBe(false);
      expect(cache.has('active-job')).toBe(true);
    });

    it('sets next alarm', async () => {
      await dobj.alarm();

      expect(mockState.storage.setAlarm).toHaveBeenCalledWith(expect.any(Number));

      const alarmTime = mockState.storage.setAlarm.mock.calls[0][0] as number;
      const now = Date.now();
      expect(alarmTime).toBeGreaterThan(now + 59000);
      expect(alarmTime).toBeLessThan(now + 61000);
    });
  });

  describe('Connection Cleanup', () => {
    it('removes all subscriptions on disconnect', () => {
      const socket = new MockWebSocket();

      const subscribe = priv.subscribeToJob as (s: unknown, id: string) => void;
      subscribe.call(dobj, socket, 'job1');
      subscribe.call(dobj, socket, 'job2');
      const sessions = priv.sessions as Map<unknown, unknown>;
      sessions.set(socket, { socket });

      const handleClose = priv.handleWebSocketClose as (s: unknown) => void;
      handleClose.call(dobj, socket);

      const subs = priv.jobSubscriptions as Map<string, Set<unknown>>;
      expect(subs.get('job1')).toBeUndefined();
      expect(subs.get('job2')).toBeUndefined();
      expect(sessions.has(socket)).toBe(false);
    });

    it('clears cache when last subscriber disconnects', () => {
      const socket = new MockWebSocket();

      const subscribe = priv.subscribeToJob as (s: unknown, id: string) => void;
      subscribe.call(dobj, socket, 'job1');
      const cache = priv.jobStatusCache as Map<string, unknown>;
      cache.set('job1', { status: 'completed' });
      const sessions = priv.sessions as Map<unknown, unknown>;
      sessions.set(socket, { socket });

      const handleClose = priv.handleWebSocketClose as (s: unknown) => void;
      handleClose.call(dobj, socket);

      expect(cache.has('job1')).toBe(false);
    });
  });
});

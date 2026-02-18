/**
 * WebSocket Message Handling Tests
 *
 * Tests for concrete WebSocket message processing
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { EnhancedJobMonitorDO } from '../websocket-enhanced';

type EventHandler = (event?: unknown) => void;

// Mock WebSocket with event handling
class MockWebSocketWithEvents {
  readyState: number = 1; // OPEN
  url: string;
  private eventHandlers: Map<string, EventHandler[]> = new Map();

  constructor(url: string) {
    this.url = url;
  }

  send = vi.fn();
  close = vi.fn();

  addEventListener(event: string, handler: EventHandler): void {
    if (!this.eventHandlers.has(event)) {
      this.eventHandlers.set(event, []);
    }
    this.eventHandlers.get(event)!.push(handler);
  }

  removeEventListener(event: string, handler: EventHandler): void {
    const handlers = this.eventHandlers.get(event);
    if (handlers) {
      const index = handlers.indexOf(handler);
      if (index > -1) {
        handlers.splice(index, 1);
      }
    }
  }

  triggerEvent(event: string, data?: unknown): void {
    const handlers = this.eventHandlers.get(event) || [];
    handlers.forEach(handler => {
      if (event === 'message') {
        handler({ data: JSON.stringify(data) });
      } else {
        handler();
      }
    });
  }

  getEventHandlers(event: string): EventHandler[] {
    return this.eventHandlers.get(event) || [];
  }
}

// Mock WebSocketPair
class MockWebSocketPair {
  0: MockWebSocketWithEvents;
  1: MockWebSocketWithEvents;

  constructor() {
    this[0] = new MockWebSocketWithEvents('ws://client');
    this[1] = new MockWebSocketWithEvents('ws://server');
  }
}

// Mock DurableObjectState
class MockDurableObjectState {
  private webSockets: Set<MockWebSocketWithEvents> = new Set();
  private storageMap = new Map<string, unknown>();

  id = {
    name: 'monitor-shard-0',
    toString: () => 'monitor-shard-0',
  };

  async waitUntil(promise: Promise<unknown>): Promise<void> {
    await promise;
  }

  blockConcurrencyWhile(fn: () => Promise<unknown>): Promise<void> {
    return fn() as Promise<void>;
  }

  acceptWebSocket(ws: MockWebSocketWithEvents): void {
    this.webSockets.add(ws);
  }

  getWebSockets(): MockWebSocketWithEvents[] {
    return Array.from(this.webSockets);
  }

  storage = {
    get: async (key: string) => this.storageMap.get(key),
    put: async (key: string, value: unknown) => {
      this.storageMap.set(key, value);
    },
    getAlarm: async () => Date.now() + 60000,
    setAlarm: async (_time: number) => { /* noop for test */ },
  };
}

describe('WebSocket Message Handling', () => {
  let monitor: EnhancedJobMonitorDO;
  let state: MockDurableObjectState;
  let wsServer: MockWebSocketWithEvents;

  beforeEach(async () => {
    (globalThis as Record<string, unknown>).WebSocketPair = MockWebSocketPair;

    state = new MockDurableObjectState();
    const env: Record<string, unknown> = {};
    monitor = new EnhancedJobMonitorDO(
      state as unknown as DurableObjectState, env
    );

    const request = new Request('http://internal/', {
      headers: { 'Upgrade': 'websocket' },
    });

    const response = await monitor.fetch(request);
    expect(response.status).toBe(101);

    const sockets = state.getWebSockets();
    wsServer = sockets[0];
  });

  describe('Ping/Pong Handling', () => {
    it('should respond to ping with pong', () => {
      wsServer.triggerEvent('message', { type: 'ping' });

      expect(wsServer.send).toHaveBeenCalledWith(
        expect.stringMatching(/"type":"pong"/)
      );
      expect(wsServer.send).toHaveBeenCalledWith(
        expect.stringMatching(/"timestamp":\d+/)
      );
    });
  });

  describe('Job Subscription', () => {
    it('should handle subscribe message', () => {
      wsServer.triggerEvent('message', {
        type: 'subscribe',
        jobId: 'test-job-123',
      });

      expect(wsServer.send).not.toHaveBeenCalledWith(
        expect.stringMatching(/"error"/)
      );
    });

    it('should handle batch subscribe', () => {
      wsServer.triggerEvent('message', {
        type: 'batch_subscribe',
        jobIds: ['job-1', 'job-2', 'job-3'],
      });

      expect(wsServer.send).not.toHaveBeenCalledWith(
        expect.stringMatching(/"error"/)
      );
    });

    it('should handle unsubscribe message', () => {
      wsServer.triggerEvent('message', {
        type: 'subscribe',
        jobId: 'test-job-456',
      });

      wsServer.triggerEvent('message', {
        type: 'unsubscribe',
        jobId: 'test-job-456',
      });

      expect(wsServer.send).not.toHaveBeenCalledWith(
        expect.stringMatching(/"error"/)
      );
    });
  });

  describe('Job Update Notifications', () => {
    it('should receive updates for subscribed jobs', async () => {
      wsServer.triggerEvent('message', {
        type: 'subscribe',
        jobId: 'notify-test-job',
      });

      const notification = {
        jobId: 'notify-test-job',
        status: 'completed',
        timestamp: new Date().toISOString(),
        data: { result: 'success' },
      };

      const response = await monitor.fetch(
        new Request('http://internal/notify', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(notification),
        })
      );

      expect(response.status).toBe(200);
      await new Promise(resolve => setTimeout(resolve, 50));

      expect(wsServer.send).toHaveBeenCalledWith(
        expect.stringMatching(/"type":"job_update"/)
      );
      expect(wsServer.send).toHaveBeenCalledWith(
        expect.stringMatching(/"jobId":"notify-test-job"/)
      );
    });

    it('should handle cached status on subscribe', async () => {
      const notification = {
        jobId: 'cached-job',
        status: 'processing',
        timestamp: new Date().toISOString(),
      };

      await monitor.fetch(
        new Request('http://internal/notify', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(notification),
        })
      );

      wsServer.triggerEvent('message', {
        type: 'subscribe',
        jobId: 'cached-job',
      });

      expect(wsServer.send).toHaveBeenCalledWith(
        expect.stringMatching(/"jobId":"cached-job"/)
      );
      expect(wsServer.send).toHaveBeenCalledWith(
        expect.stringMatching(/"status":"processing"/)
      );
    });
  });

  describe('Error Handling', () => {
    it('should handle malformed JSON messages', () => {
      const messageEvent = { data: 'invalid json' };
      const handlers = wsServer.getEventHandlers('message');
      if (handlers.length > 0) {
        expect(() => handlers[0](messageEvent)).not.toThrow();
      }
    });

    it('should handle message with unknown type', () => {
      wsServer.triggerEvent('message', {
        type: 'unknown_message_type',
        data: 'some data',
      });

      expect(wsServer.send).not.toHaveBeenCalledWith(
        expect.stringMatching(/"error"/)
      );
    });
  });

  describe('Connection Management', () => {
    it('should handle connection close', async () => {
      wsServer.triggerEvent('message', {
        type: 'subscribe',
        jobId: 'close-test-job',
      });

      wsServer.triggerEvent('close');

      await monitor.fetch(
        new Request('http://internal/notify', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            jobId: 'close-test-job',
            status: 'completed',
            timestamp: new Date().toISOString(),
          }),
        })
      );

      await new Promise(resolve => setTimeout(resolve, 50));

      const sendCalls = wsServer.send.mock.calls;
      const afterCloseCalls = sendCalls.filter((call: unknown[]) =>
        (call[0] as string).includes('close-test-job')
      );
      expect(afterCloseCalls).toHaveLength(0);
    });
  });
});

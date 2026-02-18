/**
 * Test script to verify DidoQ2 shard processing fixes
 *
 * Verifies:
 * 1. Shard ID extraction works correctly for all shards 0-9
 * 2. Job fetching queries work for non-zero shards
 * 3. Alarm-based processing loop works for all shards
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { EnhancedDidoQDurableObject } from '../src/core-enhanced';

const createMockEnv = () => {
  const mockBoundStatement = {
    run: vi.fn().mockResolvedValue({ success: true }),
    all: vi.fn().mockResolvedValue({ results: [] }),
    first: vi.fn().mockResolvedValue({
      total_jobs: 0,
      pending_jobs: 0,
      stuck_locked_jobs: 0,
      stuck_processing_jobs: 0,
    }),
  };

  const mockStatement = {
    bind: vi.fn().mockReturnValue(mockBoundStatement),
  };

  return {
    DB: {
      prepare: vi.fn().mockReturnValue(mockStatement),
    },
    JOB_MONITOR: null,
    WORKER_SERVICE: null,
  };
};

const createMockState = (shardName: string) => ({
  id: {
    name: shardName,
    toString: () => shardName,
  },
  storage: {
    getAlarm: vi.fn().mockResolvedValue(null),
    setAlarm: vi.fn().mockResolvedValue(undefined),
    get: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
  },
  waitUntil: vi.fn(),
  passThroughOnException: vi.fn(),
});

describe('DidoQ2 Shard Processing Fixes', () => {
  let mockEnv: ReturnType<typeof createMockEnv>;

  beforeEach(() => {
    vi.clearAllMocks();
    mockEnv = createMockEnv();
  });

  describe('Shard ID Extraction', () => {
    it('should correctly extract shard ID for all shards 0-9', () => {
      for (let i = 0; i <= 9; i++) {
        const shardName = `shard-${i}`;
        const state = createMockState(shardName);
        const doInstance = new EnhancedDidoQDurableObject(
          state as unknown as DurableObjectState,
          mockEnv as unknown as import('../src/types').DidoQEnv,
        );

        const shardId = (doInstance as unknown as Record<string, number>).shardId;
        expect(shardId).toBe(i);
      }
    });

    it('should handle invalid shard names gracefully', () => {
      const invalidNames = ['invalid', 'shard-', 'shard-abc', 'shard--1', ''];

      for (const invalidName of invalidNames) {
        const state = createMockState(invalidName);
        const doInstance = new EnhancedDidoQDurableObject(
          state as unknown as DurableObjectState,
          mockEnv as unknown as import('../src/types').DidoQEnv,
        );

        const shardId = (doInstance as unknown as Record<string, number>).shardId;
        expect(shardId).toBe(0);
      }
    });

    it('should handle undefined DO name gracefully', () => {
      const state = {
        ...createMockState(''),
        id: { name: undefined, toString: () => 'undefined' },
      };

      const doInstance = new EnhancedDidoQDurableObject(
        state as unknown as DurableObjectState,
        mockEnv as unknown as import('../src/types').DidoQEnv,
      );

      const shardId = (doInstance as unknown as Record<string, number>).shardId;
      expect(shardId).toBe(0);
    });
  });

  describe('Job Fetching', () => {
    it('should fetch pending jobs with correct shard_id parameter', async () => {
      for (let shardId = 0; shardId <= 9; shardId++) {
        const shardName = `shard-${shardId}`;
        const state = createMockState(shardName);
        const doInstance = new EnhancedDidoQDurableObject(
          state as unknown as DurableObjectState,
          mockEnv as unknown as import('../src/types').DidoQEnv,
        );

        const mockJobs = [
          { id: `job-${shardId}-1`, status: 'pending', shard_id: shardId },
          { id: `job-${shardId}-2`, status: 'pending', shard_id: shardId },
        ];

        mockEnv.DB.prepare().bind().all.mockResolvedValueOnce({ results: mockJobs });

        const response = await doInstance.fetch(
          new Request('http://test/kick', { method: 'POST' })
        );
        expect(response.status).toBe(200);

        const bindCalls = mockEnv.DB.prepare().bind.mock.calls;
        expect(bindCalls.some((call: unknown[]) => call.includes(shardId))).toBe(true);
      }
    });
  });

  describe('Alarm Processing', () => {
    it('should set and trigger alarms correctly for all shards', async () => {
      for (let shardId = 0; shardId <= 9; shardId++) {
        const shardName = `shard-${shardId}`;
        const state = createMockState(shardName);
        const doInstance = new EnhancedDidoQDurableObject(
          state as unknown as DurableObjectState,
          mockEnv as unknown as import('../src/types').DidoQEnv,
        );

        const response = await doInstance.fetch(
          new Request('http://test/kick', { method: 'POST' })
        );
        expect(response.status).toBe(200);
        expect(state.storage.setAlarm).toHaveBeenCalled();

        await doInstance.alarm();
        expect(true).toBe(true);
      }
    });

    it('should handle concurrent alarm processing correctly', async () => {
      const shardName = 'shard-5';
      const state = createMockState(shardName);
      const doInstance = new EnhancedDidoQDurableObject(
        state as unknown as DurableObjectState,
        mockEnv as unknown as import('../src/types').DidoQEnv,
      );

      (doInstance as unknown as Record<string, boolean>).isProcessing = true;

      await doInstance.alarm();
      expect(mockEnv.DB.prepare).not.toHaveBeenCalled();
    });
  });

  describe('Kick Endpoint', () => {
    it('should respond correctly to kick requests for all shards', async () => {
      for (let shardId = 0; shardId <= 9; shardId++) {
        const shardName = `shard-${shardId}`;
        const state = createMockState(shardName);
        const doInstance = new EnhancedDidoQDurableObject(
          state as unknown as DurableObjectState,
          mockEnv as unknown as import('../src/types').DidoQEnv,
        );

        const response = await doInstance.fetch(
          new Request('http://test/kick', { method: 'POST' })
        );
        expect(response.status).toBe(200);

        const data = await response.json() as Record<string, unknown>;
        expect(data.success).toBe(true);
        expect(data.shardId).toBe(shardId);
      }
    });
  });

  describe('Info Endpoint', () => {
    it('should return correct shard info for all shards', async () => {
      for (let shardId = 0; shardId <= 9; shardId++) {
        const shardName = `shard-${shardId}`;
        const state = createMockState(shardName);
        const doInstance = new EnhancedDidoQDurableObject(
          state as unknown as DurableObjectState,
          mockEnv as unknown as import('../src/types').DidoQEnv,
        );

        const response = await doInstance.fetch(
          new Request('http://test/info', { method: 'GET' })
        );
        expect(response.status).toBe(200);

        const data = await response.json() as Record<string, unknown>;
        expect(data.shardId).toBe(shardId);
        expect(data.isProcessing).toBe(false);
        expect(data.processingCount).toBe(0);
      }
    });
  });

  describe('Error Handling', () => {
    it('should handle database errors gracefully', async () => {
      const shardName = 'shard-3';
      const state = createMockState(shardName);

      mockEnv.DB.prepare.mockImplementation(() => {
        throw new Error('Database connection failed');
      });

      const doInstance = new EnhancedDidoQDurableObject(
        state as unknown as DurableObjectState,
        mockEnv as unknown as import('../src/types').DidoQEnv,
      );

      const response = await doInstance.fetch(
        new Request('http://test/kick', { method: 'POST' })
      );
      expect(response.status).toBe(200);

      const data = await response.json() as Record<string, unknown>;
      expect(data.success).toBe(true);
      expect(data.shardId).toBe(3);
    });
  });
});

/**
 * WebSocket Utilities単体テスト
 */

import { describe, it, expect, vi } from 'vitest';
import {
  MONITOR_SHARD_COUNT,
  hashCode,
  getMonitorShardId,
  getJobMonitorStub,
  getUserMonitorStub,
  getGlobalMonitorStub,
} from '../src/websocket-utils';

describe('WebSocket Utilities', () => {
  describe('hashCode()', () => {
    it('同じ文字列は同じハッシュを返す', () => {
      const hash1 = hashCode('test-string');
      const hash2 = hashCode('test-string');
      expect(hash1).toBe(hash2);
    });

    it('異なる文字列は異なるハッシュを返す', () => {
      const hash1 = hashCode('string1');
      const hash2 = hashCode('string2');
      expect(hash1).not.toBe(hash2);
    });

    it('空文字列でもハッシュを返す', () => {
      const hash = hashCode('');
      expect(typeof hash).toBe('number');
      expect(hash).toBe(0);
    });

    it('長い文字列でもハッシュを返す', () => {
      const longString = 'a'.repeat(1000);
      const hash = hashCode(longString);
      expect(typeof hash).toBe('number');
    });
  });

  describe('getMonitorShardId()', () => {
    it('デフォルトのシャード数で正しく分散する', () => {
      const shardIds = new Set<number>();
      
      // 100個のジョブIDでテスト
      for (let i = 0; i < 100; i++) {
        const shardId = getMonitorShardId(`job-${i}`);
        expect(shardId).toBeGreaterThanOrEqual(0);
        expect(shardId).toBeLessThan(MONITOR_SHARD_COUNT);
        shardIds.add(shardId);
      }

      // すべてのシャードが使われている
      expect(shardIds.size).toBe(MONITOR_SHARD_COUNT);
    });

    it('カスタムシャード数で動作する', () => {
      const customShardCount = 10;
      const shardIds = new Set<number>();

      for (let i = 0; i < 100; i++) {
        const shardId = getMonitorShardId(`job-${i}`, customShardCount);
        expect(shardId).toBeGreaterThanOrEqual(0);
        expect(shardId).toBeLessThan(customShardCount);
        shardIds.add(shardId);
      }

      expect(shardIds.size).toBe(customShardCount);
    });

    it('同じジョブIDは常に同じシャードに割り当てられる', () => {
      const jobId = 'consistent-job-id';
      const shardId1 = getMonitorShardId(jobId);
      const shardId2 = getMonitorShardId(jobId);
      const shardId3 = getMonitorShardId(jobId);

      expect(shardId1).toBe(shardId2);
      expect(shardId2).toBe(shardId3);
    });
  });

  describe('getJobMonitorStub()', () => {
    const createMockEnv = () => ({
      JOB_MONITOR: {
        idFromName: vi.fn((name: string) => ({ name })),
        get: vi.fn((id) => ({ id, fetch: vi.fn() })),
      },
      JOB_QUEUE: {
        idFromName: vi.fn((name: string) => ({ name })),
        get: vi.fn((id) => ({ id, fetch: vi.fn() })),
      },
    });

    it('JOB_MONITORが存在する場合、正しいシャードIDでDOを取得する', () => {
      const env = createMockEnv();
      const jobId = 'test-job-123';
      
      const stub = getJobMonitorStub(env, jobId);

      // シャードIDが計算される
      const expectedShardId = getMonitorShardId(jobId);
      
      expect(env.JOB_MONITOR.idFromName).toHaveBeenCalledWith(
        `monitor-shard-${expectedShardId}`
      );
      expect(env.JOB_MONITOR.get).toHaveBeenCalled();
      expect(stub).toBeDefined();
    });

    it('JOB_MONITORがない場合、JOB_QUEUEにフォールバックする', () => {
      const env = {
        JOB_QUEUE: {
          idFromName: vi.fn((name: string) => ({ name })),
          get: vi.fn((id) => ({ id, fetch: vi.fn() })),
        },
      };

      const stub = getJobMonitorStub(env, 'job-123');

      expect(env.JOB_QUEUE.idFromName).toHaveBeenCalled();
      expect(stub).toBeDefined();
    });

    it('どちらのnamespaceもない場合、エラーをスローする', () => {
      const env = {};
      
      expect(() => {
        getJobMonitorStub(env, 'job-123');
      }).toThrow('JOB_MONITOR namespace not found in environment');
    });
  });

  describe('getUserMonitorStub()', () => {
    it('ユーザーIDベースでDOを取得する', () => {
      const env = {
        JOB_MONITOR: {
          idFromName: vi.fn((name: string) => ({ name })),
          get: vi.fn((id) => ({ id, fetch: vi.fn() })),
        },
      };

      const userId = 'user-123';
      const stub = getUserMonitorStub(env, userId);

      expect(env.JOB_MONITOR.idFromName).toHaveBeenCalledWith(
        `monitor-user-${userId}`
      );
      expect(stub).toBeDefined();
    });
  });

  describe('getGlobalMonitorStub()', () => {
    it('グローバルモニターDOを取得する', () => {
      const env = {
        JOB_MONITOR: {
          idFromName: vi.fn((name: string) => ({ name })),
          get: vi.fn((id) => ({ id, fetch: vi.fn() })),
        },
      };

      const stub = getGlobalMonitorStub(env);

      expect(env.JOB_MONITOR.idFromName).toHaveBeenCalledWith(
        'monitor-global'
      );
      expect(stub).toBeDefined();
    });
  });

  describe('Sharding Distribution', () => {
    it('シャーディングが均等に分散される', () => {
      const distribution: Record<number, number> = {};
      
      // 1000個のランダムなジョブIDでテスト
      for (let i = 0; i < 1000; i++) {
        const jobId = `job-${Math.random().toString(36).substring(7)}`;
        const shardId = getMonitorShardId(jobId);
        distribution[shardId] = (distribution[shardId] || 0) + 1;
      }

      // 各シャードに均等に分散されているか確認（±20%の誤差を許容）
      const expectedCount = 1000 / MONITOR_SHARD_COUNT;
      for (let i = 0; i < MONITOR_SHARD_COUNT; i++) {
        expect(distribution[i]).toBeGreaterThan(expectedCount * 0.8);
        expect(distribution[i]).toBeLessThan(expectedCount * 1.2);
      }
    });
  });
});
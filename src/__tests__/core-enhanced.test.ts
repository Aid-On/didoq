/**
 * Enhanced DidoQ2 Core Tests
 *
 * Alarm-based job processing and lock rescue tests
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { EnhancedDidoQDurableObject } from '../core-enhanced';
import type { DurableObjectId } from '@cloudflare/workers-types';
import type { DidoQEnv, D1JobRecord, JobStatus } from '../types';

// Mock implementations
class MockDurableObjectState {
  private alarms: number[] = [];
  private storageMap = new Map<string, unknown>();

  id = {
    name: 'shard-0',
    toString: () => 'shard-0',
  } as DurableObjectId;

  async waitUntil(promise: Promise<unknown>): Promise<void> {
    await promise;
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

  blockConcurrencyWhile = async (fn: () => Promise<void>) => {
    await fn();
  };

  acceptWebSocket = vi.fn();
  getWebSockets = vi.fn(() => []);
}

class MockD1Database {
  private jobs: D1JobRecord[] = [];

  prepare(sql: string) {
    return {
      bind: (...params: unknown[]) => ({
        all: async <T = D1JobRecord>() => {
          if (sql.includes('UPDATE') && sql.includes('RETURNING')) {
            const shardId = params[1] as number;
            const limit = params[2] as number;

            const candidates = this.jobs
              .filter(j => {
                if (j.shard_id !== shardId) return false;
                if (j.status === 'pending') return true;
                if (j.status === 'locked' && j.locked_at) {
                  const lockTime = new Date(j.locked_at).getTime();
                  const fiveMinutesAgo = Date.now() - 5 * 60 * 1000;
                  return lockTime < fiveMinutesAgo;
                }
                if (j.status === 'processing' && j.started_at) {
                  const startTime = new Date(String(j.started_at)).getTime();
                  const tenMinutesAgo = Date.now() - 10 * 60 * 1000;
                  return startTime < tenMinutesAgo;
                }
                return false;
              })
              .slice(0, limit);

            candidates.forEach(j => {
              j.status = 'locked' as JobStatus;
              j.locked_at = params[0] as string;
            });

            return { results: candidates as unknown as T[], success: true };
          }

          if (sql.includes('GROUP BY status')) {
            const shardId = params[0] as number;
            const stats = this.jobs
              .filter(j => j.shard_id === shardId)
              .reduce((acc: Array<{ status: string; count: number }>, job) => {
                const existing = acc.find(s => s.status === job.status);
                if (existing) {
                  existing.count++;
                } else {
                  acc.push({ status: job.status, count: 1 });
                }
                return acc;
              }, []);

            return { results: stats as unknown as T[], success: true };
          }

          return { results: [] as T[], success: true };
        },
        run: async () => {
          if (sql.includes('INSERT OR REPLACE')) {
            const job: D1JobRecord = {
              id: params[0] as string,
              user_id: params[1] as string,
              job_type: params[2] as string,
              status: (params[3] as string) as JobStatus,
              shard_id: params[4] as number,
              queue_position: params[5] as number,
              input_text: params[6] as string,
              output_language: params[7] as string,
              created_at: Date.now(),
              started_at: null,
              completed_at: null,
              last_heartbeat: null,
              result_id: null,
              error_message: null,
              payload: '',
              retry_count: 0,
            };
            this.jobs.push(job);
          }

          if (sql.includes('UPDATE') && sql.includes("SET status = 'processing'")) {
            const startedAt = params[0] as number;
            const jobIds = params.slice(1) as string[];
            this.jobs.forEach(job => {
              if (jobIds.includes(job.id)) {
                job.status = 'processing';
                job.started_at = startedAt;
              }
            });
          }

          if (sql.includes('UPDATE') && sql.includes("SET status = 'completed'")) {
            const completedAt = params[0];
            const result = params[1];
            const jobId = params[2] as string;
            const job = this.jobs.find(j => j.id === jobId);
            if (job) {
              job.status = 'completed';
              job.completed_at = completedAt as number;
              job.result = result;
            }
          }

          if (sql.includes('UPDATE') && sql.includes("SET status = 'failed'")) {
            const completedAt = params[0];
            const error = params[1];
            const jobId = params[2] as string;
            const job = this.jobs.find(j => j.id === jobId);
            if (job) {
              job.status = 'failed';
              job.completed_at = completedAt as number;
              job.error = error;
              job.retry_count = (job.retry_count || 0) + 1;
            }
          }

          return { success: true };
        },
      }),
    };
  }

  addJob(job: Partial<D1JobRecord>): void {
    this.jobs.push({
      id: job.id || 'test-job',
      user_id: job.user_id || 'test-user',
      status: job.status || 'pending',
      job_type: job.job_type || 'text',
      shard_id: job.shard_id || 0,
      retry_count: 0,
      queue_position: 0,
      created_at: Date.now(),
      started_at: null,
      completed_at: null,
      last_heartbeat: null,
      result_id: null,
      error_message: null,
      payload: job.payload || '{}',
      locked_at: job.locked_at || null,
      ...job,
    } as D1JobRecord);
  }

  getJobs(): D1JobRecord[] {
    return this.jobs;
  }
}

describe('EnhancedDidoQDurableObject', () => {
  let dobj: EnhancedDidoQDurableObject;
  let state: MockDurableObjectState;
  let mockDB: MockD1Database;
  let env: DidoQEnv;

  beforeEach(() => {
    state = new MockDurableObjectState();
    mockDB = new MockD1Database();
    env = {
      DB: mockDB as unknown as D1Database,
      JOB_QUEUE: {} as unknown as DurableObjectNamespace,
      JOB_MONITOR: {} as unknown as DurableObjectNamespace,
    };

    dobj = new EnhancedDidoQDurableObject(state as unknown as DurableObjectState, env);

    const dobjInternal = dobj as unknown as Record<string, unknown>;
    dobjInternal.ctx = {
      id: state.id,
      storage: state.storage,
      waitUntil: state.waitUntil.bind(state),
      blockConcurrencyWhile: state.blockConcurrencyWhile,
      acceptWebSocket: state.acceptWebSocket,
      getWebSockets: state.getWebSockets,
    };
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe('Initialization', () => {
    it('should extract shard ID from DO name on first fetch', async () => {
      const response = await dobj.fetch(new Request('http://internal/info'));
      const info = await response.json() as Record<string, unknown>;
      expect(info.shardId).toBe(0);
    });

    it('should handle different shard IDs correctly', async () => {
      // Create a new DO with shard-5 name (shardId is extracted in constructor)
      const state5 = new MockDurableObjectState();
      (state5.id as unknown as Record<string, string>).name = 'shard-5';
      const dobj5 = new EnhancedDidoQDurableObject(state5 as unknown as DurableObjectState, env);
      const dobj5Internal = dobj5 as unknown as Record<string, unknown>;
      dobj5Internal.ctx = {
        id: state5.id,
        storage: state5.storage,
        waitUntil: state5.waitUntil.bind(state5),
        blockConcurrencyWhile: state5.blockConcurrencyWhile,
        acceptWebSocket: state5.acceptWebSocket,
        getWebSockets: state5.getWebSockets,
      };
      const response = await dobj5.fetch(new Request('http://internal/info'));
      const info = await response.json() as Record<string, unknown>;
      expect(info.shardId).toBe(5);
    });
  });

  describe('Job Enqueueing', () => {
    it('should enqueue a job successfully', async () => {
      const jobRequest = {
        jobId: 'test-123',
        userId: 'user-456',
        jobType: 'text',
        text: 'テストテキスト',
        outputLanguage: 'ja',
        shardId: 0,
      };

      const response = await dobj.fetch(
        new Request('http://internal/enqueue', {
          method: 'POST',
          body: JSON.stringify(jobRequest),
        })
      );

      const result = await response.json() as Record<string, unknown>;
      expect(result.success).toBe(true);
      expect(result.jobId).toBe('test-123');

      const jobs = mockDB.getJobs();
      expect(jobs).toHaveLength(1);
      expect(jobs[0].id).toBe('test-123');
      expect(jobs[0].status).toBe('pending');
    });
  });

  describe('Alarm-based Processing', () => {
    it('should set alarm when kick is called', async () => {
      const response = await dobj.fetch(
        new Request('http://internal/kick', { method: 'POST' })
      );

      const result = await response.json() as Record<string, unknown>;
      expect(result.success).toBe(true);
      expect(result.message).toBe('Processing started');

      const alarm = await state.storage.getAlarm();
      expect(alarm).toBeTruthy();
      // Alarm is set to Date.now() + 100 in startProcessing,
      // so it should be within a reasonable range of the current time.
      expect(alarm).toBeGreaterThan(Date.now() - 1000);
    });

    it('should process pending jobs when alarm triggers', async () => {
      mockDB.addJob({
        id: 'job-1',
        status: 'pending',
        shard_id: 0,
        job_type: 'text',
        input_text: 'test content',
      });

      await dobj.alarm();

      const jobs = mockDB.getJobs();
      const job = jobs.find(j => j.id === 'job-1');
      expect(job?.status).toBe('completed');
    });

    it('should process jobs in batches', async () => {
      for (let i = 0; i < 10; i++) {
        mockDB.addJob({
          id: `job-${i}`,
          status: 'pending',
          shard_id: 0,
          queue_position: i,
        });
      }

      await dobj.alarm();

      const jobs = mockDB.getJobs();
      const completed = jobs.filter(j => j.status === 'completed');
      expect(completed.length).toBeGreaterThanOrEqual(5);
    });
  });

  describe('Lock Rescue Mechanism', () => {
    it('should rescue stuck jobs with old locks', async () => {
      const oldTime = new Date(Date.now() - 6 * 60 * 1000).toISOString();
      mockDB.addJob({
        id: 'stuck-job',
        status: 'locked' as JobStatus,
        shard_id: 0,
        locked_at: oldTime,
      });

      mockDB.addJob({
        id: 'new-job',
        status: 'pending',
        shard_id: 0,
      });

      await dobj.alarm();

      const jobs = mockDB.getJobs();
      const stuckJob = jobs.find(j => j.id === 'stuck-job');
      const newJob = jobs.find(j => j.id === 'new-job');

      expect(stuckJob?.status).toBe('completed');
      expect(newJob?.status).toBe('completed');
    });

    it('should rescue jobs stuck in processing state', async () => {
      const oldTime = new Date(Date.now() - 11 * 60 * 1000).toISOString();
      mockDB.addJob({
        id: 'processing-stuck-job',
        status: 'processing' as JobStatus,
        shard_id: 0,
        started_at: oldTime,
      });

      await dobj.alarm();

      const jobs = mockDB.getJobs();
      const stuckJob = jobs.find(j => j.id === 'processing-stuck-job');
      expect(stuckJob?.status).toBe('completed');
    });
  });

  describe('Stats and Info', () => {
    it('should return correct shard stats', async () => {
      mockDB.addJob({ id: 'j1', status: 'pending', shard_id: 0 });
      mockDB.addJob({ id: 'j2', status: 'pending', shard_id: 0 });
      mockDB.addJob({ id: 'j3', status: 'completed', shard_id: 0 });
      mockDB.addJob({ id: 'j4', status: 'failed', shard_id: 0 });
      mockDB.addJob({ id: 'j5', status: 'pending', shard_id: 1 });

      const response = await dobj.fetch(new Request('http://internal/stats'));

      const stats = await response.json() as Record<string, unknown>;
      expect(stats.shardId).toBe(0);
      expect(stats.stats).toContainEqual({ status: 'pending', count: 2 });
      expect(stats.stats).toContainEqual({ status: 'completed', count: 1 });
      expect(stats.stats).toContainEqual({ status: 'failed', count: 1 });
    });

    it('should return queue info', async () => {
      const response = await dobj.fetch(new Request('http://internal/info'));
      const info = await response.json() as Record<string, unknown>;
      expect(info.shardId).toBe(0);
      expect(info.isProcessing).toBe(false);
      const config = info.config as Record<string, unknown>;
      expect(config.tableName).toBe('jobs');
      expect(config.shardCount).toBe(10);
      expect(config.websocketEnabled).toBe(true);
    });
  });

  describe('Error Handling', () => {
    it('should handle invalid endpoints', async () => {
      const response = await dobj.fetch(new Request('http://internal/invalid'));
      expect(response.status).toBe(404);
      const result = await response.json() as Record<string, unknown>;
      expect(result.error).toBe('Not found');
    });

    it('should handle malformed job requests', async () => {
      const response = await dobj.fetch(
        new Request('http://internal/enqueue', {
          method: 'POST',
          body: 'invalid json',
        })
      );
      expect(response.status).toBe(500);
    });
  });
});

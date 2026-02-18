/**
 * Advanced Enhanced DidoQ2 Core Tests
 *
 * Concurrency, sharding, and lock rescue mechanism tests
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { EnhancedDidoQDurableObject } from '../core-enhanced';
import type { DurableObjectId } from '@cloudflare/workers-types';
import type { DidoQEnv, D1JobRecord, JobStatus } from '../types';

// Enhanced Mock with concurrency simulation
class AdvancedMockDurableObjectState {
  private alarms: number[] = [];
  private storageMap = new Map<string, unknown>();
  private waitUntilPromises: Promise<unknown>[] = [];

  id = {
    name: 'shard-2',
    toString: () => 'shard-2',
  } as DurableObjectId;

  async waitUntil(promise: Promise<unknown>): Promise<void> {
    this.waitUntilPromises.push(promise);
    await promise;
  }

  getPendingPromises(): Promise<unknown>[] {
    return this.waitUntilPromises;
  }

  clearPendingPromises(): void {
    this.waitUntilPromises = [];
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

class AdvancedMockD1Database {
  private jobs: D1JobRecord[] = [];
  private queryLog: Array<{ sql: string; params: unknown[] }> = [];

  prepare(sql: string) {
    return {
      bind: (...params: unknown[]) => ({
        all: async <T = D1JobRecord>() => {
          this.queryLog.push({ sql, params });

          if (sql.includes('UPDATE') && sql.includes('RETURNING')) {
            const lockedAt = params[0] as string;
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
                  const startTime = typeof j.started_at === 'string'
                    ? new Date(j.started_at).getTime()
                    : j.started_at;
                  const tenMinutesAgo = Date.now() - 10 * 60 * 1000;
                  return startTime < tenMinutesAgo;
                }
                return false;
              })
              .sort((a, b) => {
                if (a.status === 'pending' && b.status !== 'pending') return -1;
                if (a.status !== 'pending' && b.status === 'pending') return 1;
                return a.queue_position - b.queue_position;
              })
              .slice(0, limit);

            candidates.forEach(j => {
              j.status = 'locked' as JobStatus;
              j.locked_at = lockedAt;
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
          this.queryLog.push({ sql, params });

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
            const startedAt = params[0];
            const jobIds = params.slice(1) as string[];
            this.jobs.forEach(job => {
              if (jobIds.includes(job.id)) {
                job.status = 'processing';
                job.started_at = startedAt as number;
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
      queue_position: job.queue_position || 0,
      created_at: Date.now(),
      started_at: job.started_at || null,
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

  getJobById(id: string): D1JobRecord | undefined {
    return this.jobs.find(j => j.id === id);
  }

  getJobsByStatus(status: string): D1JobRecord[] {
    return this.jobs.filter(j => j.status === status);
  }
}

describe('Enhanced DidoQ2 - Advanced Features', () => {
  let dobj: EnhancedDidoQDurableObject;
  let state: AdvancedMockDurableObjectState;
  let mockDB: AdvancedMockD1Database;

  beforeEach(() => {
    state = new AdvancedMockDurableObjectState();
    mockDB = new AdvancedMockD1Database();
    const env: DidoQEnv = {
      DB: mockDB as unknown as D1Database,
      JOB_QUEUE: {} as unknown as DurableObjectNamespace,
      JOB_MONITOR: {} as unknown as DurableObjectNamespace,
    };

    dobj = new EnhancedDidoQDurableObject(state as unknown as DurableObjectState, env);
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe('Concurrent Job Processing', () => {
    it('should handle multiple concurrent alarm triggers', async () => {
      for (let i = 0; i < 10; i++) {
        mockDB.addJob({
          id: `concurrent-job-${i}`,
          status: 'pending',
          shard_id: 2,
          queue_position: i,
        });
      }

      const alarmPromises = [dobj.alarm(), dobj.alarm(), dobj.alarm()];
      await Promise.allSettled(alarmPromises);

      const jobs = mockDB.getJobs();
      const completed = jobs.filter(j => j.status === 'completed');
      expect(completed.length).toBeGreaterThanOrEqual(5);

      const completedIds = completed.map(j => j.id);
      const uniqueIds = new Set(completedIds);
      expect(uniqueIds.size).toBe(completedIds.length);
    });

    it('should handle large batch processing', async () => {
      for (let i = 0; i < 50; i++) {
        mockDB.addJob({
          id: `batch-job-${i}`,
          status: 'pending',
          shard_id: 2,
          queue_position: i,
          job_type: 'text',
          input_text: `Content ${i}`,
        });
      }

      const startTime = Date.now();
      await dobj.alarm();
      const endTime = Date.now();

      const jobs = mockDB.getJobs();
      const completed = jobs.filter(j => j.status === 'completed');

      expect(completed.length).toBeGreaterThan(0);
      expect(completed.length).toBeLessThanOrEqual(100);
      expect(endTime - startTime).toBeLessThan(10000);
    });
  });

  describe('Lock Rescue - Edge Cases', () => {
    it('should rescue jobs with various lock ages', async () => {
      const now = Date.now();

      const testCases = [
        { id: 'recent-lock', age: 2 * 60 * 1000, shouldRescue: false },
        { id: 'old-lock', age: 6 * 60 * 1000, shouldRescue: true },
        { id: 'very-old-lock', age: 30 * 60 * 1000, shouldRescue: true },
      ];

      for (const testCase of testCases) {
        const lockTime = new Date(now - testCase.age).toISOString();
        mockDB.addJob({
          id: testCase.id,
          status: 'locked' as JobStatus,
          shard_id: 2,
          locked_at: lockTime,
        });
      }

      await dobj.alarm();

      for (const testCase of testCases) {
        const job = mockDB.getJobById(testCase.id);
        if (testCase.shouldRescue) {
          expect(job?.status).toBe('completed');
        } else {
          expect(job?.status).toBe('locked');
        }
      }
    });

    it('should prioritize pending jobs over stuck jobs', async () => {
      mockDB.addJob({
        id: 'stuck-1',
        status: 'locked' as JobStatus,
        shard_id: 2,
        queue_position: 0,
        locked_at: new Date(Date.now() - 6 * 60 * 1000).toISOString(),
      });

      mockDB.addJob({ id: 'pending-1', status: 'pending', shard_id: 2, queue_position: 1 });
      mockDB.addJob({ id: 'pending-2', status: 'pending', shard_id: 2, queue_position: 2 });

      await dobj.alarm();

      const jobs = mockDB.getJobs();
      const completed = jobs.filter(j => j.status === 'completed');
      expect(completed.length).toBeGreaterThan(0);

      const pendingCompleted = completed.filter(j => j.id.startsWith('pending'));
      expect(pendingCompleted.length).toBeGreaterThan(0);
    });
  });

  describe('Job Processing Error Handling', () => {
    it('should handle partial batch failures', async () => {
      for (let i = 0; i < 5; i++) {
        mockDB.addJob({
          id: `batch-job-${i}`,
          status: 'pending',
          shard_id: 2,
          job_type: i < 3 ? 'text' : 'fail-type',
        });
      }

      await dobj.alarm();

      const jobs = mockDB.getJobs();
      const completed = jobs.filter(j => j.status === 'completed');
      expect(completed.length).toBeGreaterThan(0);
    });
  });

  describe('Sharding Behavior', () => {
    it('should only process jobs for correct shard', async () => {
      mockDB.addJob({ id: 'shard-0-job', status: 'pending', shard_id: 0 });
      mockDB.addJob({ id: 'shard-2-job', status: 'pending', shard_id: 2 });
      mockDB.addJob({ id: 'shard-5-job', status: 'pending', shard_id: 5 });

      await dobj.alarm();

      const jobs = mockDB.getJobs();
      expect(jobs.find(j => j.id === 'shard-0-job')?.status).toBe('pending');
      expect(jobs.find(j => j.id === 'shard-2-job')?.status).toBe('completed');
      expect(jobs.find(j => j.id === 'shard-5-job')?.status).toBe('pending');
    });
  });

  describe('Performance and Resource Management', () => {
    it('should handle continuous processing with alarms', async () => {
      for (let i = 0; i < 15; i++) {
        mockDB.addJob({ id: `continuous-job-${i}`, status: 'pending', shard_id: 2 });
      }

      await dobj.alarm();

      const alarm = await state.storage.getAlarm();
      expect(alarm).toBeTruthy();
      expect(alarm).toBeGreaterThan(Date.now());
    });

    it('should manage processing state correctly', async () => {
      mockDB.addJob({ id: 'state-test-job', status: 'pending', shard_id: 2 });

      const infoResponse = await dobj.fetch(new Request('http://internal/info'));
      const infoBefore = await infoResponse.json() as Record<string, unknown>;
      expect(infoBefore.isProcessing).toBe(false);

      await dobj.alarm();

      const infoResponse2 = await dobj.fetch(new Request('http://internal/info'));
      const infoAfter = await infoResponse2.json() as Record<string, unknown>;
      expect(infoAfter.isProcessing).toBe(false);
    });
  });
});

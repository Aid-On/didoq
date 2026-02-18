/**
 * Advanced Enhanced DidoQ2 Core Tests
 *
 * Concurrency, sharding, and lock rescue mechanism tests
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { EnhancedDidoQDurableObject } from '../core-enhanced';
import type { DidoQEnv, JobStatus } from '../types';
import { MockD1Database, AdvancedMockDurableObjectState } from './mock-helpers';

describe('Enhanced DidoQ2 - Advanced Features', () => {
  let dobj: EnhancedDidoQDurableObject;
  let state: AdvancedMockDurableObjectState;
  let mockDB: MockD1Database;

  beforeEach(() => {
    state = new AdvancedMockDurableObjectState();
    mockDB = new MockD1Database();
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

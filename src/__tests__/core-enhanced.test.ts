/**
 * Enhanced DidoQ2 Core Tests
 *
 * Alarm-based job processing and lock rescue tests
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { EnhancedDidoQDurableObject } from '../core-enhanced';
import type { DidoQEnv, JobStatus } from '../types';
import { MockD1Database, MockDurableObjectState } from './mock-helpers';

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

      mockDB.addJob({ id: 'new-job', status: 'pending', shard_id: 0 });

      await dobj.alarm();

      const jobs = mockDB.getJobs();
      expect(jobs.find(j => j.id === 'stuck-job')?.status).toBe('completed');
      expect(jobs.find(j => j.id === 'new-job')?.status).toBe('completed');
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
      expect(jobs.find(j => j.id === 'processing-stuck-job')?.status).toBe('completed');
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

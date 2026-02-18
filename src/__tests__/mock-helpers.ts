/**
 * Shared mock helpers for DidoQ2 tests
 * Extracted to reduce file size and cognitive complexity.
 */
import { vi } from 'vitest';
import type { DurableObjectId } from '@cloudflare/workers-types';
import type { D1JobRecord, JobStatus } from '../types';

/** Check if a locked job should be rescued based on lock age */
function isLockedJobStale(job: D1JobRecord): boolean {
  if (!job.locked_at) return false;
  const lockTime = new Date(job.locked_at).getTime();
  return lockTime < Date.now() - 5 * 60 * 1000;
}

/** Check if a processing job should be rescued based on start time */
function isProcessingJobStale(job: D1JobRecord): boolean {
  if (!job.started_at) return false;
  const startTime = typeof job.started_at === 'string'
    ? new Date(job.started_at).getTime()
    : job.started_at;
  return startTime < Date.now() - 10 * 60 * 1000;
}

/** Filter candidates for job processing */
function filterCandidates(jobs: D1JobRecord[], shardId: number): D1JobRecord[] {
  return jobs.filter(j => {
    if (j.shard_id !== shardId) return false;
    if (j.status === 'pending') return true;
    if (j.status === 'locked') return isLockedJobStale(j);
    if (j.status === 'processing') return isProcessingJobStale(j);
    return false;
  });
}

/** Sort candidates: pending first, then by queue_position */
function sortCandidates(candidates: D1JobRecord[]): D1JobRecord[] {
  return candidates.sort((a, b) => {
    if (a.status === 'pending' && b.status !== 'pending') return -1;
    if (a.status !== 'pending' && b.status === 'pending') return 1;
    return a.queue_position - b.queue_position;
  });
}

/** Compute per-status counts for a shard */
function computeStats(jobs: D1JobRecord[], shardId: number): Array<{ status: string; count: number }> {
  return jobs
    .filter(j => j.shard_id === shardId)
    .reduce((acc: Array<{ status: string; count: number }>, job) => {
      const existing = acc.find(s => s.status === job.status);
      if (existing) existing.count++;
      else acc.push({ status: job.status, count: 1 });
      return acc;
    }, []);
}

/** Handle UPDATE...RETURNING (lock-and-fetch) queries */
function handleLockAndFetch<T>(jobs: D1JobRecord[], params: unknown[]): { results: T[]; success: boolean } {
  const lockedAt = params[0] as string;
  const shardId = params[1] as number;
  const limit = params[2] as number;
  const candidates = sortCandidates(filterCandidates(jobs, shardId)).slice(0, limit);
  candidates.forEach(j => { j.status = 'locked' as JobStatus; j.locked_at = lockedAt; });
  return { results: candidates as unknown as T[], success: true };
}

/** Handle GROUP BY status queries */
function handleGroupByStatus<T>(jobs: D1JobRecord[], params: unknown[]): { results: T[]; success: boolean } {
  const stats = computeStats(jobs, params[0] as number);
  return { results: stats as unknown as T[], success: true };
}

/** Handle INSERT OR REPLACE statements */
function handleInsert(jobs: D1JobRecord[], params: unknown[]): void {
  jobs.push({
    id: params[0] as string, user_id: params[1] as string,
    job_type: params[2] as string, status: (params[3] as string) as JobStatus,
    shard_id: params[4] as number, queue_position: params[5] as number,
    input_text: params[6] as string, output_language: params[7] as string,
    created_at: Date.now(), started_at: null, completed_at: null,
    last_heartbeat: null, result_id: null, error_message: null,
    payload: '', retry_count: 0,
  });
}

/** Handle UPDATE SET status = 'processing' */
function handleSetProcessing(jobs: D1JobRecord[], params: unknown[]): void {
  const startedAt = params[0] as number;
  const jobIds = params.slice(1) as string[];
  jobs.forEach(job => {
    if (jobIds.includes(job.id)) { job.status = 'processing'; job.started_at = startedAt; }
  });
}

/** Handle UPDATE SET status = 'completed' */
function handleSetCompleted(jobs: D1JobRecord[], params: unknown[]): void {
  const job = jobs.find(j => j.id === params[2] as string);
  if (job) { job.status = 'completed'; job.completed_at = params[0] as number; job.result = params[1]; }
}

/** Handle UPDATE SET status = 'failed' */
function handleSetFailed(jobs: D1JobRecord[], params: unknown[]): void {
  const job = jobs.find(j => j.id === params[2] as string);
  if (job) {
    job.status = 'failed'; job.completed_at = params[0] as number;
    job.error = params[1]; job.retry_count = (job.retry_count || 0) + 1;
  }
}

/** Create a default D1JobRecord from partial input */
function createDefaultJob(job: Partial<D1JobRecord>): D1JobRecord {
  return {
    id: job.id || 'test-job', user_id: job.user_id || 'test-user',
    status: job.status || 'pending', job_type: job.job_type || 'text',
    shard_id: job.shard_id || 0, retry_count: 0,
    queue_position: job.queue_position || 0, created_at: Date.now(),
    started_at: job.started_at || null, completed_at: null,
    last_heartbeat: null, result_id: null, error_message: null,
    payload: job.payload || '{}', locked_at: job.locked_at || null,
    ...job,
  } as D1JobRecord;
}

/** Shared mock D1 database for tests */
export class MockD1Database {
  private jobs: D1JobRecord[] = [];

  prepare(sql: string) {
    return {
      bind: (...params: unknown[]) => ({
        all: async <T = D1JobRecord>() => {
          if (sql.includes('UPDATE') && sql.includes('RETURNING')) return handleLockAndFetch<T>(this.jobs, params);
          if (sql.includes('GROUP BY status')) return handleGroupByStatus<T>(this.jobs, params);
          return { results: [] as T[], success: true };
        },
        run: async () => { this.routeRunStatement(sql, params); return { success: true }; },
      }),
    };
  }

  private routeRunStatement(sql: string, params: unknown[]): void {
    if (sql.includes('INSERT OR REPLACE')) handleInsert(this.jobs, params);
    else if (sql.includes("SET status = 'processing'")) handleSetProcessing(this.jobs, params);
    else if (sql.includes("SET status = 'completed'")) handleSetCompleted(this.jobs, params);
    else if (sql.includes("SET status = 'failed'")) handleSetFailed(this.jobs, params);
  }

  addJob(job: Partial<D1JobRecord>): void { this.jobs.push(createDefaultJob(job)); }
  getJobs(): D1JobRecord[] { return this.jobs; }
  getJobById(id: string): D1JobRecord | undefined { return this.jobs.find(j => j.id === id); }
  getJobsByStatus(status: string): D1JobRecord[] { return this.jobs.filter(j => j.status === status); }
}

/** Create a mock storage object from a shared alarms/map */
function createMockStorage(alarms: number[], storageMap: Map<string, unknown>) {
  return {
    get: async (key: string) => storageMap.get(key),
    put: async (key: string, value: unknown) => { storageMap.set(key, value); },
    delete: async (key: string) => { storageMap.delete(key); },
    getAlarm: async () => alarms[0] || null,
    setAlarm: async (time: number) => { alarms.length = 0; alarms.push(time); },
    deleteAlarm: async () => { alarms.length = 0; },
  } as unknown as DurableObjectStorage;
}

/** Basic mock DurableObjectState for tests */
export class MockDurableObjectState {
  private alarms: number[] = [];
  private storageMap = new Map<string, unknown>();
  id = { name: 'shard-0', toString: () => 'shard-0' } as DurableObjectId;

  async waitUntil(promise: Promise<unknown>): Promise<void> { await promise; }

  storage = createMockStorage(this.alarms, this.storageMap);
  blockConcurrencyWhile = async (fn: () => Promise<void>) => { await fn(); };
  acceptWebSocket = vi.fn();
  getWebSockets = vi.fn(() => []);
}

/** Advanced mock with concurrency simulation */
export class AdvancedMockDurableObjectState {
  private alarms: number[] = [];
  private storageMap = new Map<string, unknown>();
  private waitUntilPromises: Promise<unknown>[] = [];
  id = { name: 'shard-2', toString: () => 'shard-2' } as DurableObjectId;

  async waitUntil(promise: Promise<unknown>): Promise<void> {
    this.waitUntilPromises.push(promise);
    await promise;
  }
  getPendingPromises(): Promise<unknown>[] { return this.waitUntilPromises; }
  clearPendingPromises(): void { this.waitUntilPromises = []; }

  storage = createMockStorage(this.alarms, this.storageMap);
}

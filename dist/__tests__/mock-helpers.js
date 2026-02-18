/**
 * Shared mock helpers for DidoQ2 tests
 * Extracted to reduce file size and cognitive complexity.
 */
import { vi } from 'vitest';
/** Check if a locked job should be rescued based on lock age */
function isLockedJobStale(job) {
    if (!job.locked_at)
        return false;
    const lockTime = new Date(job.locked_at).getTime();
    return lockTime < Date.now() - 5 * 60 * 1000;
}
/** Check if a processing job should be rescued based on start time */
function isProcessingJobStale(job) {
    if (!job.started_at)
        return false;
    const startTime = typeof job.started_at === 'string'
        ? new Date(job.started_at).getTime()
        : job.started_at;
    return startTime < Date.now() - 10 * 60 * 1000;
}
/** Filter candidates for job processing */
function filterCandidates(jobs, shardId) {
    return jobs.filter(j => {
        if (j.shard_id !== shardId)
            return false;
        if (j.status === 'pending')
            return true;
        if (j.status === 'locked')
            return isLockedJobStale(j);
        if (j.status === 'processing')
            return isProcessingJobStale(j);
        return false;
    });
}
/** Sort candidates: pending first, then by queue_position */
function sortCandidates(candidates) {
    return candidates.sort((a, b) => {
        if (a.status === 'pending' && b.status !== 'pending')
            return -1;
        if (a.status !== 'pending' && b.status === 'pending')
            return 1;
        return a.queue_position - b.queue_position;
    });
}
/** Compute per-status counts for a shard */
function computeStats(jobs, shardId) {
    return jobs
        .filter(j => j.shard_id === shardId)
        .reduce((acc, job) => {
        const existing = acc.find(s => s.status === job.status);
        if (existing)
            existing.count++;
        else
            acc.push({ status: job.status, count: 1 });
        return acc;
    }, []);
}
/** Handle UPDATE...RETURNING (lock-and-fetch) queries */
function handleLockAndFetch(jobs, params) {
    const lockedAt = params[0];
    const shardId = params[1];
    const limit = params[2];
    const candidates = sortCandidates(filterCandidates(jobs, shardId)).slice(0, limit);
    candidates.forEach(j => { j.status = 'locked'; j.locked_at = lockedAt; });
    return { results: candidates, success: true };
}
/** Handle GROUP BY status queries */
function handleGroupByStatus(jobs, params) {
    const stats = computeStats(jobs, params[0]);
    return { results: stats, success: true };
}
/** Handle INSERT OR REPLACE statements */
function handleInsert(jobs, params) {
    jobs.push({
        id: params[0], user_id: params[1],
        job_type: params[2], status: params[3],
        shard_id: params[4], queue_position: params[5],
        input_text: params[6], output_language: params[7],
        created_at: Date.now(), started_at: null, completed_at: null,
        last_heartbeat: null, result_id: null, error_message: null,
        payload: '', retry_count: 0,
    });
}
/** Handle UPDATE SET status = 'processing' */
function handleSetProcessing(jobs, params) {
    const startedAt = params[0];
    const jobIds = params.slice(1);
    jobs.forEach(job => {
        if (jobIds.includes(job.id)) {
            job.status = 'processing';
            job.started_at = startedAt;
        }
    });
}
/** Handle UPDATE SET status = 'completed' */
function handleSetCompleted(jobs, params) {
    const job = jobs.find(j => j.id === params[2]);
    if (job) {
        job.status = 'completed';
        job.completed_at = params[0];
        job.result = params[1];
    }
}
/** Handle UPDATE SET status = 'failed' */
function handleSetFailed(jobs, params) {
    const job = jobs.find(j => j.id === params[2]);
    if (job) {
        job.status = 'failed';
        job.completed_at = params[0];
        job.error = params[1];
        job.retry_count = (job.retry_count || 0) + 1;
    }
}
/** Create a default D1JobRecord from partial input */
function createDefaultJob(job) {
    return {
        id: job.id || 'test-job', user_id: job.user_id || 'test-user',
        status: job.status || 'pending', job_type: job.job_type || 'text',
        shard_id: job.shard_id || 0, retry_count: 0,
        queue_position: job.queue_position || 0, created_at: Date.now(),
        started_at: job.started_at || null, completed_at: null,
        last_heartbeat: null, result_id: null, error_message: null,
        payload: job.payload || '{}', locked_at: job.locked_at || null,
        ...job,
    };
}
/** Shared mock D1 database for tests */
export class MockD1Database {
    jobs = [];
    prepare(sql) {
        return {
            bind: (...params) => ({
                all: async () => {
                    if (sql.includes('UPDATE') && sql.includes('RETURNING'))
                        return handleLockAndFetch(this.jobs, params);
                    if (sql.includes('GROUP BY status'))
                        return handleGroupByStatus(this.jobs, params);
                    return { results: [], success: true };
                },
                run: async () => { this.routeRunStatement(sql, params); return { success: true }; },
            }),
        };
    }
    routeRunStatement(sql, params) {
        if (sql.includes('INSERT OR REPLACE'))
            handleInsert(this.jobs, params);
        else if (sql.includes("SET status = 'processing'"))
            handleSetProcessing(this.jobs, params);
        else if (sql.includes("SET status = 'completed'"))
            handleSetCompleted(this.jobs, params);
        else if (sql.includes("SET status = 'failed'"))
            handleSetFailed(this.jobs, params);
    }
    addJob(job) { this.jobs.push(createDefaultJob(job)); }
    getJobs() { return this.jobs; }
    getJobById(id) { return this.jobs.find(j => j.id === id); }
    getJobsByStatus(status) { return this.jobs.filter(j => j.status === status); }
}
/** Create a mock storage object from a shared alarms/map */
function createMockStorage(alarms, storageMap) {
    return {
        get: async (key) => storageMap.get(key),
        put: async (key, value) => { storageMap.set(key, value); },
        delete: async (key) => { storageMap.delete(key); },
        getAlarm: async () => alarms[0] || null,
        setAlarm: async (time) => { alarms.length = 0; alarms.push(time); },
        deleteAlarm: async () => { alarms.length = 0; },
    };
}
/** Basic mock DurableObjectState for tests */
export class MockDurableObjectState {
    alarms = [];
    storageMap = new Map();
    id = { name: 'shard-0', toString: () => 'shard-0' };
    async waitUntil(promise) { await promise; }
    storage = createMockStorage(this.alarms, this.storageMap);
    blockConcurrencyWhile = async (fn) => { await fn(); };
    acceptWebSocket = vi.fn();
    getWebSockets = vi.fn(() => []);
}
/** Advanced mock with concurrency simulation */
export class AdvancedMockDurableObjectState {
    alarms = [];
    storageMap = new Map();
    waitUntilPromises = [];
    id = { name: 'shard-2', toString: () => 'shard-2' };
    async waitUntil(promise) {
        this.waitUntilPromises.push(promise);
        await promise;
    }
    getPendingPromises() { return this.waitUntilPromises; }
    clearPendingPromises() { this.waitUntilPromises = []; }
    storage = createMockStorage(this.alarms, this.storageMap);
}

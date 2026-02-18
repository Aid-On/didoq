/**
 * Enhanced DidoQ2 - Job Processing Logic
 * processJobBatch, fetchPendingJobs extracted from core-enhanced.ts
 */
import { getJobMonitorStub } from './websocket-utils';
/**
 * Process a batch of pending jobs.
 * Fetches, locks, processes, and handles results.
 */
export async function processJobBatch(opts) {
    const { env, config, shardId, ctx, processingJobs } = opts;
    const BATCH_SIZE = config.batchSize || 5;
    const jobs = await fetchPendingJobs(env, config, shardId, BATCH_SIZE);
    if (!jobs || jobs.length === 0) {
        return;
    }
    await markJobsAsProcessing(env, config, jobs.map(j => j.id), processingJobs);
    const CONCURRENT_LIMIT = 3;
    const results = [];
    for (let i = 0; i < jobs.length; i += CONCURRENT_LIMIT) {
        const batch = jobs.slice(i, i + CONCURRENT_LIMIT);
        const batchResults = await Promise.allSettled(batch.map(job => opts.processJobFn(job)));
        results.push(...batchResults);
    }
    await handleBatchResults(opts, jobs, results);
    if (jobs.length === BATCH_SIZE) {
        await ctx.storage.setAlarm(Date.now() + 100);
    }
    else if (jobs.length > 0) {
        await ctx.storage.setAlarm(Date.now() + 1000);
    }
}
/**
 * Handle results from a processed batch.
 */
async function handleBatchResults(opts, jobs, results) {
    for (let i = 0; i < results.length; i++) {
        const result = results[i];
        const job = jobs[i];
        if (result.status === 'fulfilled' && result.value.success) {
            await opts.markJobCompletedFn(job.id, result.value.data);
        }
        else {
            const error = result.status === 'rejected'
                ? result.reason
                : result.value?.error;
            await opts.markJobFailedFn(job.id, error);
        }
        if (opts.config.websocketEnabled) {
            try {
                const status = result.status === 'fulfilled' ? 'completed' : 'failed';
                await notifyJobUpdateInternal(opts.env, job.id, status, opts.shardId);
            }
            catch {
                /* WebSocket notification failed, continue processing */
            }
        }
    }
}
/**
 * Fetch pending jobs with lock rescue mechanism.
 */
export async function fetchPendingJobs(env, config, shardId, limit) {
    const result = await env.DB.prepare(`
    UPDATE ${config.tableName}
    SET status = 'locked', locked_at = ?
    WHERE id IN (
      SELECT id FROM ${config.tableName}
      WHERE shard_id = ?
        AND (
          status = 'pending'
          OR (status = 'locked' AND locked_at < datetime('now', '-5 minutes'))
          OR (status = 'processing' AND started_at < datetime('now', '-10 minutes'))
        )
      ORDER BY
        CASE
          WHEN status = 'pending' THEN 0
          ELSE 1
        END,
        queue_position ASC
      LIMIT ?
    )
    RETURNING *
  `).bind(new Date().toISOString(), shardId, limit).all();
    return result.results || [];
}
/**
 * Mark jobs as processing status.
 */
async function markJobsAsProcessing(env, config, jobIds, processingJobs) {
    if (jobIds.length === 0)
        return;
    const placeholders = jobIds.map(() => '?').join(',');
    await env.DB.prepare(`
    UPDATE ${config.tableName}
    SET status = 'processing', started_at = ?
    WHERE id IN (${placeholders})
  `).bind(new Date().toISOString(), ...jobIds).run();
    jobIds.forEach(id => processingJobs.add(id));
}
/**
 * Internal WebSocket notification helper.
 */
async function notifyJobUpdateInternal(env, jobId, status, shardId) {
    if (!env.JOB_MONITOR)
        return;
    const monitorStub = getJobMonitorStub(env, jobId);
    await monitorStub.fetch('http://internal/notify', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            jobId,
            status,
            timestamp: new Date().toISOString(),
            shardId,
        }),
    });
}

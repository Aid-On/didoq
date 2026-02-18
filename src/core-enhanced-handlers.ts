/**
 * Enhanced DidoQ2 - Job Handlers
 * processJob, processTextAnalysis, processPdfConversion,
 * markJobCompleted, markJobFailed extracted from core-enhanced.ts
 */

import type { DidoQConfig, DidoQEnv, D1JobRecord } from './types';

/** Context for job processing */
export interface JobProcessContext {
  env: DidoQEnv;
  config: DidoQConfig;
  shardId: number;
  processingJobs: Set<string>;
}

/**
 * Process an individual job.
 * Delegates to WORKER_SERVICE if available, otherwise handles inline.
 */
export async function processJob(
  ctx: JobProcessContext,
  job: D1JobRecord,
): Promise<{ success: boolean; data?: unknown; error?: string }> {
  try {
    if (ctx.env.WORKER_SERVICE) {
      return await processViaServiceBinding(ctx.env, job);
    }

    return processInline(ctx.shardId, job);
  } catch (error) {
    return {
      success: false,
      error: error instanceof Error ? error.message : 'Unknown error',
    };
  } finally {
    ctx.processingJobs.delete(job.id);
  }
}

/** Delegate to Worker via Service Binding */
async function processViaServiceBinding(
  env: DidoQEnv,
  job: D1JobRecord,
): Promise<{ success: boolean; data?: unknown; error?: string }> {
  const response = await env.WORKER_SERVICE!.fetch(
    new Request('http://internal/process-job', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jobId: job.id,
        type: job.type,
        payload: job.payload,
      }),
    })
  );

  if (!response.ok) {
    throw new Error(`Worker processing failed: ${response.statusText}`);
  }

  const result: unknown = await response.json();
  return { success: true, data: result };
}

/** Process job inline based on type */
function processInline(
  shardId: number,
  job: D1JobRecord,
): { success: boolean; data?: unknown; error?: string } {
  switch (job.job_type || job.type) {
    case 'text':
      return {
        success: true,
        data: {
          title: 'テキスト解析結果',
          summary: ['Enhanced DidoQ2で処理されました'],
          content: job.input_text || job.payload || '',
          processedAt: new Date().toISOString(),
        },
      };

    case 'text-analysis':
    case 'pdf-conversion':
      return {
        success: true,
        data: {
          type: job.job_type || job.type,
          status: 'processed',
          timestamp: new Date().toISOString(),
        },
      };

    default:
      return {
        success: true,
        data: {
          echo: job.payload,
          processedAt: new Date().toISOString(),
          processedBy: `shard-${shardId}`,
        },
      };
  }
}

/**
 * Mark a job as completed in D1.
 */
export async function markJobCompleted(
  env: DidoQEnv,
  config: DidoQConfig,
  jobId: string,
  result: unknown,
): Promise<void> {
  await env.DB.prepare(`
    UPDATE ${config.tableName}
    SET status = 'completed',
        completed_at = ?,
        result = ?
    WHERE id = ?
  `).bind(
    new Date().toISOString(),
    JSON.stringify(result),
    jobId
  ).run();
}

/**
 * Mark a job as failed in D1.
 */
export async function markJobFailed(
  env: DidoQEnv,
  config: DidoQConfig,
  jobId: string,
  error: unknown,
): Promise<void> {
  await env.DB.prepare(`
    UPDATE ${config.tableName}
    SET status = 'failed',
        completed_at = ?,
        error = ?,
        retry_count = retry_count + 1
    WHERE id = ?
  `).bind(
    new Date().toISOString(),
    JSON.stringify(error),
    jobId
  ).run();
}

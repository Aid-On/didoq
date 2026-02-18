/**
 * DidoQ v2 - Fluent API with Nagare Stream Integration
 * 
 * Next generation job queue system with intuitive fluent API
 * and reactive stream processing
 * 
 * @example
 * ```typescript
 * import { didoq } from '@aid-on/didoq2';
 * 
 * // Simple job enqueue
 * const job = await didoq(env)
 *   .job()
 *   .type('analyze')
 *   .payload({ text: 'Hello' })
 *   .enqueue();
 * 
 * // Stream-based monitoring
 * const stream = didoq(env)
 *   .job()
 *   .type('process')
 *   .payload(data)
 *   .stream();
 * 
 * for await (const event of stream) {
 *   console.log(event.type, event.jobId);
 * }
 * ```
 */

// Export fluent API
export {
  DidoQ,
  didoq,
  FluentJob,
  FluentQueue,
  type JobEvent,
} from './fluent';

// Export types
export type {
  DidoQConfig,
  DidoQEnv,
  JobStatus,
  D1JobRecord,
  JobEnqueueRequest,
  JobCompleteRequest,
  QueueInfo,
  HealthCheckResult,
  AggregatedQueueInfo,
  JobProcessor,
} from './types';

// Export core functionality
export {
  DEFAULT_CONFIG,
  DidoQDurableObject,
  getShardId,
  getJobQueueStub,
  getAllQueueInfo,
  kickAllShards,
  insertJob,
  enqueueJob,
  completeJob,
  sendHeartbeat,
} from './core';

// Export WebSocket support
export {
  JobMonitorDO,
  getJobMonitorStub,
  notifyJobUpdate,
} from './websocket';

// Export Enhanced versions (with job processing and optimizations)
export {
  EnhancedDidoQDurableObject,
} from './core-enhanced';

export {
  EnhancedJobMonitorDO,
} from './websocket-enhanced';

// Export WebSocket utilities for sharding
export {
  MONITOR_SHARD_COUNT,
  hashCode,
  getMonitorShardId,
  getJobMonitorStub as getShardedJobMonitorStub,
  getUserMonitorStub,
  getGlobalMonitorStub,
} from './websocket-utils';
/**
 * DidoQ v2 - Type definitions
 * Independent from didoq v1
 */

/**
 * DidoQ設定
 */
export interface DidoQConfig {
  /** D1テーブル名 */
  tableName: string;
  /** シャード数 (default: 10) */
  shardCount?: number;
  /** ジョブタイムアウト (ms, default: 180000 = 3分) */
  timeoutMs?: number;
  /** Heartbeat間隔 (ms, default: 30000) */
  heartbeatIntervalMs?: number;
  /** アラームチェック間隔 (ms, default: 5000) */
  alarmCheckIntervalMs?: number;
  /** 最大再試行回数 (default: 3) */
  maxRetryCount?: number;
  /** 次ジョブまでの遅延 (ms, default: 100) */
  nextJobDelayMs?: number;
  /** WebSocket通知有効化 (default: true) */
  websocketEnabled?: boolean;
  /** フラッシュ間隔 (ms, default: 5000) */
  flushInterval?: number;
  /** バッチサイズ (default: 100) */
  batchSize?: number;
  /** 最大リトライ回数 (default: 3) */
  maxRetries?: number;
}

/**
 * デフォルト設定
 */
export const DEFAULT_CONFIG: Required<DidoQConfig> = {
  tableName: 'jobs',
  shardCount: 10,
  timeoutMs: 3 * 60 * 1000,
  heartbeatIntervalMs: 30 * 1000,
  alarmCheckIntervalMs: 5000,
  maxRetryCount: 3,
  nextJobDelayMs: 100,
  websocketEnabled: true,
  flushInterval: 5000,
  batchSize: 100,
  maxRetries: 3,
};

/**
 * ジョブステータス
 */
export type JobStatus = 'pending' | 'processing' | 'completed' | 'failed' | 'locked';

/**
 * D1のジョブレコード（必須フィールド）
 */
export interface D1JobRecord {
  id: string;
  user_id: string | null;
  status: JobStatus;
  job_type: string;
  shard_id: number;
  retry_count: number;
  queue_position: number;
  created_at: number;
  started_at: number | null;
  completed_at: number | null;
  last_heartbeat: number | null;
  result_id: string | null;
  error_message: string | null;
  /** アプリ固有のペイロード (JSON文字列) */
  payload: string;
  /** ロックされた時刻 */
  locked_at?: string | null;
  /** ジョブタイプ（後方互換性） */
  type?: string;
  /** 結果データ */
  result?: unknown;
  /** エラー情報 */
  error?: unknown;
  /** Text-smash-app specific fields */
  input_text?: string;
  output_language?: string;
  filename?: string;
  image_keys?: string;
}

/**
 * ジョブリクエスト（エンキュー時）
 */
export interface JobEnqueueRequest<TPayload = unknown> {
  id: string;
  userId?: string;
  jobType: string;
  payload?: TPayload;
}

/**
 * ジョブ完了通知
 */
export interface JobCompleteRequest {
  jobId: string;
  status: 'completed' | 'failed';
  resultId?: string;
  error?: string;
}

/**
 * キュー情報
 */
export interface QueueInfo {
  shardId: number;
  pending: number;
  processing: number;
  hasAlarm: boolean;
}

/**
 * ヘルスチェック結果
 */
export interface HealthCheckResult extends QueueInfo {
  healthy: boolean;
  timedOut: number;
}

/**
 * 全シャード集計結果
 */
export interface AggregatedQueueInfo {
  totalPending: number;
  totalProcessing: number;
  shards: Array<QueueInfo & { shard: number; error?: string }>;
}

/**
 * ジョブ処理コールバック
 * アプリケーションが実装する処理ロジック
 */
export type JobProcessor<TPayload = unknown> = (
  job: D1JobRecord & { parsedPayload?: TPayload }
) => Promise<void>;

/**
 * DidoQ環境インターフェース
 * アプリのEnvがこれを実装する必要がある
 */
export interface DidoQEnv {
  DB: D1Database;
  JOB_QUEUE: DurableObjectNamespace;
  /** Monitor Durable Object Namespace (オプション) */
  JOB_MONITOR?: DurableObjectNamespace;
  /** Worker Service Binding (オプション) */
  WORKER_SERVICE?: Fetcher;
}
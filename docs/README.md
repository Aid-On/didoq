# @aid-on/didoq2

## 概要

`@aid-on/didoq2`は、DidoQの次世代版であり、nagareストリーミングとの統合された直感的なFluent APIを提供するジョブキューシステムです。Enhanced版では実際のジョブ処理ループ、WebSocketプッシュ通知、シャーディング対応、そして高度な最適化機能を実装しています。

## 主な特徴

- **Fluent API**: 直感的なチェーン可能なビルダーパターン
- **nagareストリーミング**: リアクティブなイベント駆動処理
- **Enhanced版**: 自動ジョブ処理ループとAlarm API活用
- **WebSocketプッシュ通知**: N+1問題解決済みリアルタイム通知
- **シャーディング対応**: 水平スケーリング可能な分散アーキテクチャ
- **ロック救済メカニズム**: クラッシュしたDOからのジョブ自動救済
- **パフォーマンス最適化**: インデックス最適化とバッチ処理
- **型安全性**: TypeScriptによる完全な型サポート

## アーキテクチャ

### Enhanced版の改善点

1. **実際のジョブ処理ロジック実装** - Alarm APIを使った自動処理ループ
2. **WebSocket N+1問題の解決** - ポーリング排除、プッシュ型通知
3. **シャーディング対応** - WebSocket DOの適切な分散
4. **ロック救済メカニズム** - クラッシュしたDOのジョブを自動救済
5. **パフォーマンス最適化** - 適切なインデックス、バッチ処理

### アーキテクチャ図

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Enhanced      │    │   Enhanced       │    │      D1         │
│   Client Side   │    │   Workers        │    │   Database      │
├─────────────────┤    ├──────────────────┤    ├─────────────────┤
│ Fluent API      │◄──►│ EnhancedDidoQ DO │◄──►│ Optimized       │
│ nagare Stream   │    │ (Processing)     │    │ Indexes         │
│ WebSocket Push  │    │                  │    │                 │
│                 │    │ JobMonitor DO    │    │ Job Records     │
│                 │    │ (Notifications)  │    │ (Source of      │
│                 │    │                  │    │  Truth)         │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### データフロー

```
1. Fluent Job → nagare Stream → Enhanced DO Processing → D1 Update
2. Job Events → WebSocket DO → Sharded Notifications → Client
3. Auto Recovery → Alarm API → Failed Job Detection → Retry Logic
```

## APIリファレンス

### Fluent API

```typescript
import { didoq, type JobEvent } from '@aid-on/didoq2';

// 基本的なジョブエンキュー
const job = await didoq(env)
  .job()
  .type('analyze')
  .payload({ text: 'Hello World' })
  .user('user123')
  .shard(0)
  .enqueue();

// ストリーミング監視
const stream = didoq(env)
  .job()
  .type('process')
  .payload(data)
  .stream();

for await (const event of stream) {
  console.log(`Job ${event.jobId}: ${event.type}`);
}

// イベントハンドラー登録
const jobBuilder = didoq(env)
  .job()
  .type('upload')
  .payload({ fileId: '123' })
  .onEvent('processing', (event) => {
    console.log('Processing started:', event);
  })
  .onEvent('completed', (event) => {
    console.log('Job completed:', event);
  });

await jobBuilder.enqueue();
```

### Enhanced Durable Object

```typescript
import { EnhancedDidoQDurableObject } from '@aid-on/didoq2';

export class MyEnhancedJobQueue extends EnhancedDidoQDurableObject {
  constructor(state: DurableObjectState, env: Env) {
    super(state, env);
  }

  // カスタムジョブプロセッサーを実装
  protected async processJob(job: D1JobRecord): Promise<void> {
    console.log(`Processing job ${job.id} of type ${job.job_type}`);
    
    try {
      switch (job.job_type) {
        case 'data_analysis':
          await this.analyzeData(job.payload);
          break;
        case 'file_processing':
          await this.processFile(job.payload);
          break;
        default:
          throw new Error(`Unknown job type: ${job.job_type}`);
      }
      
      // 成功時の処理
      await this.markJobCompleted(job.id, { result: 'success' });
      
    } catch (error) {
      console.error(`Job ${job.id} failed:`, error);
      await this.markJobFailed(job.id, error.message);
    }
  }

  private async analyzeData(payload: any) {
    // データ分析処理
    const result = await this.env.AI.run('@cf/meta/llama-2-7b-chat-int8', {
      messages: [{ role: 'user', content: payload.text }]
    });
    
    return result;
  }

  private async processFile(payload: any) {
    // ファイル処理
    const file = await this.env.R2_BUCKET.get(payload.fileKey);
    if (!file) throw new Error('File not found');
    
    const processed = await this.transformFile(file);
    await this.env.R2_BUCKET.put(`processed/${payload.fileKey}`, processed);
  }
}
```

### WebSocket Enhanced版

```typescript
import { EnhancedJobMonitorDO } from '@aid-on/didoq2';

// Enhanced WebSocket DO（自動エクスポート）
export { EnhancedJobMonitorDO as JobMonitorDO } from '@aid-on/didoq2';

// クライアント側 WebSocket 接続
const ws = new WebSocket('wss://your-worker.domain/ws');

ws.onmessage = (event) => {
  const jobEvent = JSON.parse(event.data);
  console.log('Job update:', jobEvent);
};

// ジョブ監視を開始
ws.send(JSON.stringify({
  type: 'subscribe',
  jobId: 'job-123'
}));
```

### Queue操作

```typescript
// キュー情報取得
const queue = didoq(env).queue('my_jobs');
const info = await queue.info();
console.log('Queue stats:', info);

// 全シャードキック
await queue.kickAll();

// 特定シャードキック
await queue.kick(shardId);

// キューのクリア（危険操作）
await queue.clear();

// ヘルスチェック
const health = await queue.healthCheck();
console.log('Queue health:', health);
```

## 使用例

### Enhanced版への移行

```typescript
// wrangler.toml の更新
/*
[[durable_objects.bindings]]
name = "JOB_QUEUE"
class_name = "EnhancedDidoQDurableObject"
script_name = "your-worker"

[[durable_objects.bindings]]
name = "JOB_MONITOR"  
class_name = "EnhancedJobMonitorDO"
script_name = "your-worker"
*/

// Worker内のDO export
export { 
  EnhancedDidoQDurableObject as DidoQDurableObject,
  EnhancedJobMonitorDO as JobMonitorDO 
} from '@aid-on/didoq2';
```

### データ処理パイプライン

```typescript
// src/data-pipeline.ts
import { didoq } from '@aid-on/didoq2';

export async function createDataPipeline(env: Env, data: any[]) {
  const pipeline = [];
  
  // データチャンク分割
  for (let i = 0; i < data.length; i += 100) {
    const chunk = data.slice(i, i + 100);
    
    // 各チャンクを並列処理ジョブとしてエンキュー
    const job = await didoq(env)
      .job()
      .type('process_chunk')
      .payload({ 
        chunk, 
        chunkIndex: i / 100,
        totalChunks: Math.ceil(data.length / 100)
      })
      .user('pipeline')
      .priority(1) // 高優先度
      .timeout(300000) // 5分タイムアウト
      .retries(3)
      .onEvent('completed', (event) => {
        console.log(`Chunk ${event.data.chunkIndex} completed`);
      })
      .onEvent('failed', (event) => {
        console.error(`Chunk failed: ${event.error}`);
      })
      .enqueue();
    
    pipeline.push(job);
  }
  
  // 全ジョブの完了を監視
  const allCompleted = await Promise.all(
    pipeline.map(job => job.waitForCompletion())
  );
  
  // 結果の集約
  const aggregatedResults = await didoq(env)
    .job()
    .type('aggregate_results')
    .payload({ resultIds: allCompleted.map(r => r.resultId) })
    .enqueue();
  
  return aggregatedResults;
}
```

### リアルタイム進捗UI

```typescript
// React component with real-time updates
import React, { useState, useEffect } from 'react';

interface JobProgressProps {
  jobId: string;
  wsEndpoint: string;
}

export function JobProgress({ jobId, wsEndpoint }: JobProgressProps) {
  const [status, setStatus] = useState('pending');
  const [progress, setProgress] = useState(0);
  const [error, setError] = useState<string | null>(null);
  
  useEffect(() => {
    const ws = new WebSocket(wsEndpoint);
    
    ws.onopen = () => {
      // ジョブ監視開始
      ws.send(JSON.stringify({
        type: 'subscribe',
        jobId
      }));
    };
    
    ws.onmessage = (event) => {
      const jobEvent = JSON.parse(event.data);
      
      if (jobEvent.jobId === jobId) {
        setStatus(jobEvent.type);
        
        if (jobEvent.data?.progress) {
          setProgress(jobEvent.data.progress);
        }
        
        if (jobEvent.type === 'failed') {
          setError(jobEvent.error || 'Unknown error');
        }
        
        if (jobEvent.type === 'completed') {
          setProgress(100);
        }
      }
    };
    
    ws.onerror = (error) => {
      console.error('WebSocket error:', error);
      setError('Connection error');
    };
    
    return () => {
      ws.close();
    };
  }, [jobId, wsEndpoint]);
  
  return (
    <div className="job-progress">
      <h3>Job: {jobId.slice(0, 8)}...</h3>
      <div className="status">
        Status: <span className={`status-${status}`}>{status}</span>
      </div>
      
      <div className="progress-bar">
        <div 
          className="progress-fill"
          style={{ width: `${progress}%` }}
        />
        <span className="progress-text">{progress}%</span>
      </div>
      
      {error && (
        <div className="error">
          Error: {error}
        </div>
      )}
    </div>
  );
}
```

### バッチ処理システム

```typescript
// src/batch-processor.ts
import { didoq } from '@aid-on/didoq2';

export class BatchProcessor {
  private env: Env;
  private batchSize = 50;
  
  constructor(env: Env) {
    this.env = env;
  }
  
  async processBatch(items: any[], batchType: string) {
    const batches = this.createBatches(items, this.batchSize);
    const batchJobs = [];
    
    // バッチジョブを並列エンキュー
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      
      const job = await didoq(this.env)
        .job()
        .type(batchType)
        .payload({
          batch,
          batchIndex: i,
          totalBatches: batches.length
        })
        .priority(2)
        .timeout(600000) // 10分
        .retries(2)
        .onEvent('processing', (event) => {
          console.log(`Batch ${i + 1}/${batches.length} processing...`);
        })
        .onEvent('completed', (event) => {
          console.log(`Batch ${i + 1}/${batches.length} completed`);
        })
        .enqueue();
      
      batchJobs.push(job);
    }
    
    // 進捗監視
    const progressStream = this.monitorBatchProgress(batchJobs);
    
    for await (const progressEvent of progressStream) {
      console.log(`Overall progress: ${progressEvent.completedCount}/${batchJobs.length}`);
      
      if (progressEvent.allCompleted) {
        break;
      }
    }
    
    return batchJobs;
  }
  
  private createBatches(items: any[], batchSize: number): any[][] {
    const batches = [];
    for (let i = 0; i < items.length; i += batchSize) {
      batches.push(items.slice(i, i + batchSize));
    }
    return batches;
  }
  
  private async* monitorBatchProgress(jobs: any[]) {
    let completed = 0;
    const jobStatuses = new Map();
    
    // 各ジョブの完了を監視
    const promises = jobs.map(async (job, index) => {
      const result = await job.waitForCompletion();
      jobStatuses.set(index, result);
      completed++;
      return { index, result };
    });
    
    // 進捗をストリーミング
    while (completed < jobs.length) {
      yield {
        completedCount: completed,
        totalCount: jobs.length,
        progress: (completed / jobs.length) * 100,
        allCompleted: false
      };
      
      // 1秒待機
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    
    // 全完了
    yield {
      completedCount: completed,
      totalCount: jobs.length,
      progress: 100,
      allCompleted: true,
      results: Array.from(jobStatuses.values())
    };
  }
}

// 使用例
const processor = new BatchProcessor(env);
await processor.processBatch(
  largeDataSet, 
  'data_transformation'
);
```

### WebSocket API エンドポイント

```typescript
// src/websocket-handler.ts
export async function handleWebSocket(request: Request, env: Env): Promise<Response> {
  const upgradeHeader = request.headers.get('Upgrade');
  if (!upgradeHeader || upgradeHeader !== 'websocket') {
    return new Response('Expected Upgrade: websocket', { status: 426 });
  }
  
  const url = new URL(request.url);
  const userId = url.searchParams.get('userId');
  const jobId = url.searchParams.get('jobId');
  
  // ユーザー固有またはジョブ固有のMonitor DO取得
  const monitorStub = userId 
    ? getUserMonitorStub(env, userId)
    : getJobMonitorStub(env, jobId || 'global');
  
  return monitorStub.handleWebSocketUpgrade(request);
}

// Pages Functions での使用
export const onRequestGet: PagesFunction<Env> = async (context) => {
  return handleWebSocket(context.request, context.env);
};
```

## 設定オプション

### Enhanced DO設定

```typescript
interface EnhancedDidoQConfig extends DidoQConfig {
  // 基本設定
  tableName: string;
  shardCount?: number;               // シャード数 (デフォルト: 10)
  
  // 処理設定
  batchSize?: number;                // バッチサイズ (デフォルト: 100)
  flushInterval?: number;            // フラッシュ間隔 (デフォルト: 5000ms)
  maxRetries?: number;               // 最大再試行 (デフォルト: 3)
  
  // WebSocket設定
  websocketEnabled?: boolean;        // WebSocket有効化 (デフォルト: true)
  monitorShardCount?: number;        // Monitor DOシャード数 (デフォルト: 5)
  
  // パフォーマンス設定
  jobTimeoutMs?: number;             // ジョブタイムアウト (デフォルト: 300000)
  heartbeatIntervalMs?: number;      // ハートビート間隔 (デフォルト: 30000)
  cleanupIntervalMs?: number;        // クリーンアップ間隔 (デフォルト: 3600000)
  
  // 救済メカニズム
  rescueIntervalMs?: number;         // 救済チェック間隔 (デフォルト: 60000)
  staleJobThresholdMs?: number;      // 失効ジョブ判定時間 (デフォルト: 300000)
}

// 使用例
const config: EnhancedDidoQConfig = {
  tableName: 'enhanced_jobs',
  shardCount: 20,
  batchSize: 200,
  websocketEnabled: true,
  maxRetries: 5,
  jobTimeoutMs: 600000, // 10分
  rescueIntervalMs: 30000, // 30秒
};
```

### パフォーマンス設定

```typescript
// 高スループット設定
const highThroughputConfig: EnhancedDidoQConfig = {
  tableName: 'high_throughput_jobs',
  shardCount: 50,
  batchSize: 500,
  flushInterval: 2000,
  websocketEnabled: true,
  monitorShardCount: 10,
};

// 低レイテンシ設定
const lowLatencyConfig: EnhancedDidoQConfig = {
  tableName: 'realtime_jobs',
  shardCount: 10,
  batchSize: 10,
  flushInterval: 500,
  websocketEnabled: true,
  heartbeatIntervalMs: 10000,
};

// 長時間実行設定
const longRunningConfig: EnhancedDidoQConfig = {
  tableName: 'long_jobs',
  shardCount: 5,
  jobTimeoutMs: 3600000, // 1時間
  heartbeatIntervalMs: 60000, // 1分
  rescueIntervalMs: 120000, // 2分
  maxRetries: 10,
};
```

## パフォーマンス考慮事項

### データベース最適化

```sql
-- Enhanced版で追加される最適化インデックス
CREATE INDEX IF NOT EXISTS idx_jobs_shard_status_priority_created 
ON jobs(shard_id, status, priority DESC, created_at ASC);

CREATE INDEX IF NOT EXISTS idx_jobs_processing_heartbeat 
ON jobs(status, last_heartbeat) 
WHERE status = 'processing';

CREATE INDEX IF NOT EXISTS idx_jobs_failed_retry_count 
ON jobs(status, retry_count, created_at) 
WHERE status = 'failed' AND retry_count < 3;

CREATE INDEX IF NOT EXISTS idx_jobs_cleanup 
ON jobs(status, completed_at) 
WHERE status IN ('completed', 'failed');
```

### シャーディング最適化

```typescript
// 効率的なシャード分散
function getOptimalShardCount(expectedJobsPerSecond: number): number {
  // 1シャードあたり最大50jobs/secを想定
  return Math.max(1, Math.ceil(expectedJobsPerSecond / 50));
}

// ホットシャード検出と負荷分散
async function detectHotShards(env: Env, config: EnhancedDidoQConfig) {
  const shardLoads = [];
  
  for (let i = 0; i < config.shardCount!; i++) {
    const load = await env.DB.prepare(`
      SELECT 
        shard_id,
        COUNT(*) as total_jobs,
        COUNT(CASE WHEN status = 'processing' THEN 1 END) as active_jobs,
        AVG(CASE WHEN completed_at IS NOT NULL 
            THEN completed_at - started_at END) as avg_duration
      FROM ${config.tableName} 
      WHERE shard_id = ? 
        AND created_at > ?
      GROUP BY shard_id
    `).bind(i, Date.now() - 3600000).first();
    
    shardLoads.push(load);
  }
  
  // ホットシャードの特定（平均より50%以上負荷が高い）
  const avgLoad = shardLoads.reduce((sum, shard) => 
    sum + (shard?.active_jobs || 0), 0) / shardLoads.length;
  
  return shardLoads.filter(shard => 
    (shard?.active_jobs || 0) > avgLoad * 1.5
  );
}
```

### WebSocket最適化

```typescript
// 効率的なWebSocket通知
class OptimizedJobMonitor extends EnhancedJobMonitorDO {
  private notificationBuffer: Map<string, any> = new Map();
  private flushTimer?: number;
  
  async notifyJobUpdate(jobId: string, event: any) {
    // バッファリングで通知頻度を制御
    this.notificationBuffer.set(jobId, event);
    
    if (!this.flushTimer) {
      this.flushTimer = setTimeout(() => {
        this.flushNotifications();
      }, 100); // 100ms後にバッチ送信
    }
  }
  
  private flushNotifications() {
    const notifications = Array.from(this.notificationBuffer.entries());
    this.notificationBuffer.clear();
    this.flushTimer = undefined;
    
    // 一括でWebSocket送信
    this.broadcastBatch(notifications);
  }
  
  private broadcastBatch(notifications: [string, any][]) {
    const message = JSON.stringify({
      type: 'batch_update',
      notifications
    });
    
    // 接続されたすべてのクライアントに送信
    this.state.getWebSockets().forEach(ws => {
      try {
        ws.send(message);
      } catch (error) {
        console.error('WebSocket send error:', error);
      }
    });
  }
}
```

### メモリ使用量最適化

- **適応的バッチサイズ**: 負荷に応じたバッチサイズ調整
- **ジョブキャッシュ**: 頻繁にアクセスされるジョブの効率的キャッシュ
- **ガベージコレクション**: 完了ジョブの適切なクリーンアップ

```typescript
// 適応的バッチサイズ制御
class AdaptiveBatchProcessor extends EnhancedDidoQDurableObject {
  private batchSize = 100;
  private lastProcessingTime = 0;
  private performanceHistory: number[] = [];
  
  protected async processPendingJobs() {
    const startTime = Date.now();
    
    // 現在のバッチサイズで処理
    await super.processPendingJobs();
    
    const processingTime = Date.now() - startTime;
    this.performanceHistory.push(processingTime);
    
    // 直近10回の平均を維持
    if (this.performanceHistory.length > 10) {
      this.performanceHistory.shift();
    }
    
    // バッチサイズの適応的調整
    this.adjustBatchSize(processingTime);
  }
  
  private adjustBatchSize(processingTime: number) {
    const avgTime = this.performanceHistory.reduce((a, b) => a + b, 0) 
      / this.performanceHistory.length;
    
    if (processingTime > avgTime * 1.2) {
      // 処理時間が長い場合、バッチサイズを削減
      this.batchSize = Math.max(10, Math.floor(this.batchSize * 0.8));
    } else if (processingTime < avgTime * 0.8) {
      // 処理時間が短い場合、バッチサイズを増加
      this.batchSize = Math.min(500, Math.floor(this.batchSize * 1.2));
    }
  }
}
```
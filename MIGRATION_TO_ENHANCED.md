# DidoQ2 Enhanced版への移行ガイド

## 🎯 Enhanced版の主な改善点

1. **実際のジョブ処理ロジック実装** - Alarm APIを使った自動処理ループ
2. **WebSocket N+1問題の解決** - ポーリング排除、プッシュ型通知
3. **シャーディング対応** - WebSocket DOの適切な分散
4. **ロック救済メカニズム** - クラッシュしたDOのジョブを自動救済
5. **パフォーマンス最適化** - 適切なインデックス、バッチ処理

## 📦 移行手順

### 1. Wrangler.tomlの更新

```toml
# Durable Objectsの設定を更新
[[durable_objects.bindings]]
name = "JOB_QUEUE"
class_name = "EnhancedDidoQDurableObject"  # Enhanced版を使用
script_name = "text-smash-do-dev"

[[durable_objects.bindings]]
name = "JOB_MONITOR"
class_name = "EnhancedJobMonitorDO"  # Enhanced版を使用
script_name = "text-smash-do-dev"
```

### 2. Worker内のDO exportを更新

```typescript
// src/worker/index.ts または _worker.ts

// Before
export { DidoQDurableObject, JobMonitorDO } from '@aid-on/didoq2';

// After - Enhanced版をエクスポート
export { 
  EnhancedDidoQDurableObject as DidoQDurableObject,
  EnhancedJobMonitorDO as JobMonitorDO 
} from '@aid-on/didoq2';
```

### 3. データベースマイグレーション実行

```bash
# インデックスの追加（必須）
wrangler d1 execute YOUR_DB_NAME --file=node_modules/@aid-on/didoq2/migrations/001_optimize_indexes.sql
```

### 4. WebSocket接続の更新

```typescript
// Before - ジョブごとにDO
const monitorStub = env.JOB_MONITOR.get(
  env.JOB_MONITOR.idFromName(`monitor-${jobId}`)
);

// After - シャードを使用
import { getJobMonitorStub } from '@aid-on/didoq2';
const monitorStub = getJobMonitorStub(env, jobId);
```

### 5. Service Binding設定（オプション）

ジョブ処理をWorkerに委譲する場合:

```toml
# wrangler.toml
[[services]]
binding = "WORKER_SERVICE"
service = "your-job-processor-worker"
```

```typescript
// job-processor worker
import { Hono } from 'hono';

const app = new Hono();

app.post('/process-job', async (c) => {
  const { type, payload } = await c.req.json();
  
  switch (type) {
    case 'text-analysis':
      // 実際の処理
      return c.json({ result: 'analyzed' });
    default:
      return c.json({ error: 'Unknown job type' }, 400);
  }
});

export default app;
```

## 🔄 後方互換性

Enhanced版は既存のAPIと完全互換:

- 同じFluent API (`didoq(env).job().type('...').enqueue()`)
- 同じ型定義 (`DidoQEnv`, `D1JobRecord` など)
- 同じテーブル構造（新しいインデックスは追加のみ）

## ⚠️ 注意事項

### DO名の形式

- **Queue DO**: `shard-0`, `shard-1`, ... `shard-9`
- **Monitor DO**: `monitor-shard-0`, ... `monitor-shard-4`

### ロック救済のタイミング

- `locked`状態: 5分後に自動救済
- `processing`状態: 10分後に自動救済

### WebSocketシャード数

デフォルトは5シャード。変更する場合:

```typescript
import { MONITOR_SHARD_COUNT } from '@aid-on/didoq2';
// デフォルト: 5

// カスタムシャード数を使う場合
const shardId = getMonitorShardId(jobId, 10); // 10シャード
```

## 🚀 パフォーマンス改善の期待値

- **ジョブ取得**: 10-100倍高速化（インデックス利用）
- **WebSocket負荷**: 95%削減（ポーリング排除）
- **DO数**: 最大90%削減（シャーディング）
- **可用性**: 99.9%以上（ロック救済メカニズム）

## 📊 モニタリング

Enhanced版の統計情報:

```typescript
// Queue統計
const response = await queueStub.fetch('http://internal/stats');
const stats = await response.json();
// { shardId, stats: [{status, count}], isProcessing, processingCount }

// Monitor統計
const response = await monitorStub.fetch('http://internal/stats');
const stats = await response.json();
// { activeSessions, subscribedJobs, cachedStatuses, queueLength }
```

## 🔍 トラブルシューティング

### ジョブが処理されない

1. Alarm APIが有効か確認
2. `/kick`エンドポイントを手動で呼び出し
3. ロックされたジョブを確認: `SELECT * FROM jobs WHERE status = 'locked'`

### WebSocket接続エラー

1. Monitor DOのシャードIDを確認
2. `getJobMonitorStub()`を使用しているか確認
3. DOの再起動: デプロイまたは手動でDOをevict

### パフォーマンス問題

1. インデックスが作成されているか確認
2. シャード数が適切か確認（負荷に応じて調整）
3. バッチサイズの調整: `config.batchSize`
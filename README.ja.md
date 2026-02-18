# @aid-on/didoq

<div align="center">

[![npm version](https://img.shields.io/npm/v/@aid-on/didoq.svg?style=flat-square&color=00DC82)](https://www.npmjs.com/package/@aid-on/didoq)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.7-3178C6?style=flat-square&logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat-square)](https://opensource.org/licenses/MIT)

<br />

<h3>
<b>didoq</b> - Cloudflare Durable Objects向け分散ジョブキュー
</h3>

<p align="center">
<b>ただのジョブキューではありません。</b><br/>
nagare Stream統合のFluent API、Cloudflare Workers + D1 + Durable Objectsにネイティブ対応。
</p>

<br/>

**日本語** | [**English**](./README.md)

<br/>

</div>

## didoqを選ぶ理由

### **1. ジョブキューのためのFluent API**
```typescript
import { didoq } from '@aid-on/didoq';

const job = await didoq(env)
  .job()
  .type('analyze')
  .payload({ text: 'Hello World' })
  .user('user123')
  .enqueue();
```

### **2. nagareによるリアクティブストリーム監視**
```typescript
const stream = didoq(env)
  .job()
  .type('process')
  .payload(data)
  .stream();

for await (const event of stream) {
  console.log(`Job ${event.jobId}: ${event.type}`);
}
```

### **3. WebSocketリアルタイムプッシュ通知**
```typescript
const ws = new WebSocket('wss://your-worker.domain/ws');

ws.send(JSON.stringify({ type: 'subscribe', jobId: 'job-123' }));

ws.onmessage = (event) => {
  const jobEvent = JSON.parse(event.data);
  console.log('ジョブ更新:', jobEvent);
};
```

## アーキテクチャ

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   クライアント    │    │   Workers        │    │      D1         │
├─────────────────┤    ├──────────────────┤    ├─────────────────┤
│ Fluent API      │◄──►│ DidoQ DO         │◄──►│ 最適化された     │
│ nagare Stream   │    │ (処理)           │    │ インデックス     │
│ WebSocket Push  │    │                  │    │                 │
│                 │    │ JobMonitor DO    │    │ ジョブレコード    │
│                 │    │ (通知)           │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## 主な特徴

- **Fluent API** - 直感的なチェーン可能なビルダーパターン
- **nagare Stream統合** - リアクティブなイベント駆動処理
- **Enhanced DO** - Alarm APIを使った自動ジョブ処理ループ
- **WebSocketプッシュ通知** - N+1問題解決済みリアルタイム通知
- **シャーディング** - 水平スケーリング可能な分散アーキテクチャ
- **ロック救済メカニズム** - クラッシュしたDOからのジョブ自動回復
- **パフォーマンス** - インデックス最適化とバッチ処理
- **型安全性** - TypeScriptによる完全な型サポート

## インストール

```bash
npm install @aid-on/didoq
```

## クイックスタート

### Durable Objectsのセットアップ

```typescript
import { DidoQDurableObject, JobMonitorDO } from '@aid-on/didoq';

export { DidoQDurableObject, JobMonitorDO };
```

### ジョブのエンキュー

```typescript
import { didoq } from '@aid-on/didoq';

const job = await didoq(env)
  .job()
  .type('data_analysis')
  .payload({ text: 'これを分析して' })
  .user('user-456')
  .enqueue();
```

### イベントハンドラー

```typescript
const job = didoq(env)
  .job()
  .type('upload')
  .payload({ fileId: '123' })
  .onEvent('processing', (event) => {
    console.log('処理開始:', event);
  })
  .onEvent('completed', (event) => {
    console.log('ジョブ完了:', event);
  });

await job.enqueue();
```

### キュー操作

```typescript
const queue = didoq(env).queue('my_jobs');

const info = await queue.info();       // キュー統計情報
await queue.kickAll();                 // 全シャードキック
const health = await queue.healthCheck(); // ヘルスチェック
```

### Enhanced DO（自動処理付き）

```typescript
import { EnhancedDidoQDurableObject } from '@aid-on/didoq';

export class MyJobQueue extends EnhancedDidoQDurableObject {
  protected async processJob(job: D1JobRecord): Promise<void> {
    switch (job.job_type) {
      case 'data_analysis':
        await this.analyzeData(job.payload);
        break;
      default:
        throw new Error(`不明なジョブタイプ: ${job.job_type}`);
    }
  }
}
```

## 設定

```typescript
const config: DidoQConfig = {
  tableName: 'jobs',         // D1テーブル名
  shardCount: 10,            // シャード数（デフォルト: 10）
  timeoutMs: 180000,         // ジョブタイムアウト（デフォルト: 3分）
  maxRetryCount: 3,          // 最大再試行回数（デフォルト: 3）
  websocketEnabled: true,    // WebSocket通知
  batchSize: 100,            // バッチサイズ
};
```

## Wrangler設定

```toml
[[durable_objects.bindings]]
name = "JOB_QUEUE"
class_name = "DidoQDurableObject"

[[durable_objects.bindings]]
name = "JOB_MONITOR"
class_name = "JobMonitorDO"

[[d1_databases]]
binding = "DB"
database_name = "your-database"
database_id = "your-database-id"
```

## ライセンス

MIT © Aid-On

---

<div align="center">

**Cloudflareのために作られ、スケールのために設計され、本番環境で実証済み。**

<br/>

[NPM](https://www.npmjs.com/package/@aid-on/didoq) •
[GitHub](https://github.com/Aid-On/didoq)

</div>

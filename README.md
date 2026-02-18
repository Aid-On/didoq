# @aid-on/didoq

<div align="center">

[![npm version](https://img.shields.io/npm/v/@aid-on/didoq.svg?style=flat-square&color=00DC82)](https://www.npmjs.com/package/@aid-on/didoq)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.7-3178C6?style=flat-square&logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat-square)](https://opensource.org/licenses/MIT)

<br />

<h3>
<b>didoq</b> - Distributed Job Queue for Cloudflare Durable Objects
</h3>

<p align="center">
<b>Not just another job queue.</b><br/>
A fluent API with nagare Stream integration, built natively for Cloudflare Workers + D1 + Durable Objects.
</p>

<br/>

[**日本語**](./README.ja.md) | **English**

<br/>

</div>

## Why didoq

### **1. Fluent API for Job Queues**
```typescript
import { didoq } from '@aid-on/didoq';

const job = await didoq(env)
  .job()
  .type('analyze')
  .payload({ text: 'Hello World' })
  .user('user123')
  .enqueue();
```

### **2. Reactive Stream Monitoring with nagare**
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

### **3. Real-time WebSocket Push Notifications**
```typescript
const ws = new WebSocket('wss://your-worker.domain/ws');

ws.send(JSON.stringify({ type: 'subscribe', jobId: 'job-123' }));

ws.onmessage = (event) => {
  const jobEvent = JSON.parse(event.data);
  console.log('Job update:', jobEvent);
};
```

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Client Side   │    │   Workers        │    │      D1         │
├─────────────────┤    ├──────────────────┤    ├─────────────────┤
│ Fluent API      │◄──►│ DidoQ DO         │◄──►│ Optimized       │
│ nagare Stream   │    │ (Processing)     │    │ Indexes         │
│ WebSocket Push  │    │                  │    │                 │
│                 │    │ JobMonitor DO    │    │ Job Records     │
│                 │    │ (Notifications)  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Key Features

- **Fluent API** - Intuitive chainable builder pattern
- **nagare Stream Integration** - Reactive event-driven processing
- **Enhanced DO** - Auto job processing loop with Alarm API
- **WebSocket Push** - Real-time notifications (N+1 problem solved)
- **Sharding** - Horizontally scalable distributed architecture
- **Lock Rescue** - Automatic recovery from crashed DOs
- **Performance** - Optimized indexes and batch processing
- **Type Safety** - Full TypeScript support

## Installation

```bash
npm install @aid-on/didoq
```

## Quick Start

### Setup Durable Objects

```typescript
import { DidoQDurableObject, JobMonitorDO } from '@aid-on/didoq';

export { DidoQDurableObject, JobMonitorDO };
```

### Enqueue a Job

```typescript
import { didoq } from '@aid-on/didoq';

const job = await didoq(env)
  .job()
  .type('data_analysis')
  .payload({ text: 'Analyze this' })
  .user('user-456')
  .enqueue();
```

### Event Handlers

```typescript
const job = didoq(env)
  .job()
  .type('upload')
  .payload({ fileId: '123' })
  .onEvent('processing', (event) => {
    console.log('Processing started:', event);
  })
  .onEvent('completed', (event) => {
    console.log('Job completed:', event);
  });

await job.enqueue();
```

### Queue Operations

```typescript
const queue = didoq(env).queue('my_jobs');

const info = await queue.info();       // Queue stats
await queue.kickAll();                 // Kick all shards
const health = await queue.healthCheck(); // Health check
```

### Enhanced DO (with Auto Processing)

```typescript
import { EnhancedDidoQDurableObject } from '@aid-on/didoq';

export class MyJobQueue extends EnhancedDidoQDurableObject {
  protected async processJob(job: D1JobRecord): Promise<void> {
    switch (job.job_type) {
      case 'data_analysis':
        await this.analyzeData(job.payload);
        break;
      default:
        throw new Error(`Unknown job type: ${job.job_type}`);
    }
  }
}
```

## Configuration

```typescript
const config: DidoQConfig = {
  tableName: 'jobs',         // D1 table name
  shardCount: 10,            // Number of shards (default: 10)
  timeoutMs: 180000,         // Job timeout (default: 3min)
  maxRetryCount: 3,          // Max retries (default: 3)
  websocketEnabled: true,    // WebSocket notifications
  batchSize: 100,            // Batch size
};
```

## Wrangler Configuration

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

## License

MIT © Aid-On

---

<div align="center">

**Built for Cloudflare. Designed for scale. Ready for production.**

<br/>

[NPM](https://www.npmjs.com/package/@aid-on/didoq) •
[GitHub](https://github.com/Aid-On/didoq)

</div>

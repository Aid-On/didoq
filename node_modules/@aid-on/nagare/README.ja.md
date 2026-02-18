# @aid-on/nagare (流れ)

[![npm version](https://img.shields.io/npm/v/@aid-on/nagare.svg)](https://www.npmjs.com/package/@aid-on/nagare)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.7-blue.svg)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**nagare**（流れ）は、Web Streams API上に構築されたユニバーサルストリーミングインターフェースです。エッジコンピューティング環境向けに設計され、モダンなTypeScriptアプリケーションのための流暢で型安全なAPIとリアクティブ拡張を提供します。

[English README is here](./README.md)

## 特徴

- 🌊 **Web Streams ネイティブ**: 標準Web Streams API上に構築
- 🚀 **エッジ対応**: Cloudflare Workers、Deno Deployなどのエッジランタイムに最適化
- 🔄 **リアクティブ拡張**: バックプレッシャーサポート付きのRxJS風オペレーター
- 🎯 **型安全**: 厳密な型付けによる完全なTypeScriptサポート
- 🔧 **合成可能**: 複雑なストリーム変換のためのオペレーターチェーン
- ⚡ **高性能**: 最小限のオーバーヘッド、ゼロ依存
- 🌐 **ユニバーサル**: ブラウザ、Node.js、Deno、エッジ環境で動作

## インストール

```bash
npm install @aid-on/nagare
```

```bash
yarn add @aid-on/nagare
```

```bash
pnpm add @aid-on/nagare
```

## クイックスタート

```typescript
import { stream } from '@aid-on/nagare';

// 配列からストリームを作成
const numbers = stream.array([1, 2, 3, 4, 5]);

// オペレーターで変換
const result = await numbers
  .map(x => x * 2)
  .filter(x => x > 4)
  .collect();

console.log(result); // [6, 8, 10]
```

## コアコンセプト

### ストリーム作成

```typescript
// 配列から
const s1 = stream.array([1, 2, 3]);

// ReadableStreamから
const s2 = stream.from(readableStream);

// 非同期ジェネレーターから
async function* generate() {
  yield 1;
  yield 2;
  yield 3;
}
const s3 = stream.from(ReadableStream.from(generate()));

// 命令型プッシュで作成（リアルタイムデータに最適）
const s4 = stream.create<number>((controller) => {
  controller.next(1);
  controller.next(2);
  controller.next(3);
  controller.complete();
});

// SSEエンドポイントから
const events = await stream.fromSSE('/api/events');
```

### ストリーム変換

```typescript
const transformed = stream
  .array([1, 2, 3, 4, 5])
  .map(x => x * 2)                    // 2倍にする
  .filter(x => x > 5)                 // 5より大きいものだけ
  .mapAsync(async x => {              // 非同期変換
    const result = await fetch(`/api/data/${x}`);
    return result.json();
  }, 3)                                // 並行度3
  .tap(data => console.log(data))     // 副作用
  .buffer(10)                          // 10個バッファリング
  .debounce(100);                      // 100msデバウンス
```

### オペレーター

#### 変換
- `map<U>(fn: (value: T) => U)` - 各値を変換
- `mapAsync<U>(fn: (value: T) => Promise<U>, concurrency?: number)` - 並行制御と**順序保証**付きの非同期変換
- `filter(predicate: (value: T) => boolean)` - 値をフィルター
- `scan<U>(fn: (acc: U, value: T) => U, initial: U)` - 値を累積
- `expand(fn: (value: T) => T[])` - 単一値を複数に展開
- `compact()` - null/undefinedを削除

#### フロー制御
- `take(count: number)` - 最初のN個を取得
- `takeUntil(predicate: (value: T) => boolean)` - 条件まで取得
- `debounce(ms: number)` - ミリ秒単位でデバウンス
- `throttle(ms: number)` - 後縁サポート付きスロットル
- `buffer(size: number)` - 配列にバッファリング
- `batch(size: number)` - bufferのエイリアス

#### 副作用
- `tap(fn: (value: T) => void, options?: { rethrow?: boolean })` - 設定可能なエラーハンドリングで副作用を実行

#### 結合
- `merge(...streams: Stream<T>[])` - 複数ストリームをマージ
- `concat(stream: Stream<T>)` - ストリームを連結

### 消費

```typescript
// すべての値を配列に収集
const array = await stream.collect();

// Responseに変換（エッジワーカー用）
const response = stream.toResponse({
  headers: { 'Content-Type': 'application/json' }
});

// SSE形式に変換
const sseStream = stream.toSSE();

// Observerパターンでサブスクライブ
const subscription = stream.subscribe({
  next: (value) => console.log(value),
  error: (err) => console.error(err),
  complete: () => console.log('完了')
});

// 後でアンサブスクライブ
subscription.unsubscribe();

// 非同期イテレーション
for await (const value of stream) {
  console.log(value);
}
```

## 高度な機能

### バックプレッシャーサポート

subscribeは自動的にバックプレッシャーを処理します：

```typescript
stream.subscribe({
  next: async (value) => {
    // ストリームは非同期処理を待機
    await processData(value);
  }
});
```

### 複数サブスクリプション

ストリームはデフォルトで単一消費者です。複数消費者には`tee()`を使用：

```typescript
const original = stream.array([1, 2, 3]);
const [stream1, stream2] = original.tee();

// Streamに変換し直す
const s1 = stream.from(stream1);
const s2 = stream.from(stream2);

s1.subscribe({ next: v => console.log('A:', v) });
s2.subscribe({ next: v => console.log('B:', v) });
```

### SSE（Server-Sent Events）

エッジワーカーからのリアルタイムストリーミングに最適：

```typescript
// クライアント
const events = await stream.fromSSE<MessageEvent>('/api/chat', {
  method: 'POST',
  body: { prompt: 'こんにちは' }
});

events.subscribe({
  next: (event) => console.log(event.data)
});

// サーバー（エッジワーカー）
export default {
  async fetch(request: Request) {
    const dataStream = stream.create<string>((controller) => {
      controller.next('Hello');
      controller.next('World');
      controller.complete();
    });

    return dataStream
      .toSSE()
      .toResponse({
        headers: {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache'
        }
      });
  }
};
```

## プラットフォームサポート

- ✅ Cloudflare Workers
- ✅ Deno / Deno Deploy
- ✅ Node.js 18+
- ✅ Bun
- ✅ モダンブラウザ (Chrome, Firefox, Safari, Edge)
- ✅ Vercel Edge Runtime
- ✅ Netlify Edge Functions

## パフォーマンス

Nagareは最小限のオーバーヘッドで設計されています：

- ゼロ依存
- 小さなバンドルサイズ（~10KB gzip圧縮後）
- ネイティブWeb Streamsによる最適なパフォーマンス
- 遅延評価 - オペレーターは消費されるまで実行されない
- 自動リソースクリーンアップ

## APIリファレンス

### メインエクスポート

```typescript
import {
  stream,           // メインストリームファクトリー
  Stream,           // Stream型
  StreamPipeOptions, // パイプオプション型
  operators,        // 低レベルオペレーター
  fromArray,        // 配列から作成
  fromReadableStream // ReadableStreamから作成
} from '@aid-on/nagare';
```

### Streamインターフェース

```typescript
interface Stream<T> {
  // ネイティブWeb Streams
  readable: ReadableStream<T>;
  pipeTo(destination: WritableStream<T>, options?: StreamPipeOptions): Promise<void>;
  pipeThrough<U>(transform: TransformStream<T, U>): Stream<U>;
  tee(): [ReadableStream<T>, ReadableStream<T>];

  // オペレーター
  map<U>(fn: (value: T) => U): Stream<U>;
  mapAsync<U>(fn: (value: T) => Promise<U>, concurrency?: number): Stream<U>;
  filter(predicate: (value: T) => boolean): Stream<T>;
  tap(fn: (value: T) => void, options?: { rethrow?: boolean }): Stream<T>;
  scan<U>(fn: (acc: U, value: T) => U, initial: U): Stream<U>;
  expand(fn: (value: T) => T[]): Stream<T>;
  compact(): Stream<NonNullable<T>>;
  buffer(size: number): Stream<T[]>;
  batch(size: number): Stream<T[]>;
  debounce(ms: number): Stream<T>;
  throttle(ms: number): Stream<T>;
  take(count: number): Stream<T>;
  takeUntil(predicate: (value: T) => boolean): Stream<T>;

  // 結合
  merge(...streams: Stream<T>[]): Stream<T>;
  concat(stream: Stream<T>): Stream<T>;

  // 消費
  collect(): Promise<T[]>;
  toArray(): Promise<T[]>;
  toResponse(init?: ResponseInit): Response;
  toSSE(formatter?: (value: T) => string): Stream<string>;
  subscribe(observer: Observer<T>): Subscription;
  [Symbol.asyncIterator](): AsyncIterableIterator<T>;
}
```

## バージョン履歴

### v0.1.0 (2025)
- ✅ 並行処理での`mapAsync`の順序保証
- ✅ SSEパーサーのクロスプラットフォーム改行サポート（`\r\n`, `\n`, `\r`）
- ✅ `tap`オペレーターの設定可能なエラーハンドリング
- ✅ `takeUntil`のリソース管理改善
- ✅ `StreamPipeOptions`によるTypeScriptエクスポート強化
- ✅ `tee()`動作の完全なドキュメント化

### v0.0.x (2024)
- コアストリーミング機能の初期リリース
- Web Streams API基盤
- 基本的なオペレーターと変換

## エコシステムでの使用例

### 他のライブラリがnagareを使用する方法

```typescript
// @aid-on/unilmp の例
export const groq = {
  instant: (apiKey: string) => ({
    stream: (prompt: string): Stream<string> => {
      // nagare Stream<T>を返す
      return stream.from(createGroqStream(prompt, apiKey));
    }
  })
};

// @aid-on/qwiks の例
export function websocket(url: string) {
  return {
    events: (): Stream<MessageEvent> => {
      return stream.from(createWebSocketStream(url));
    }
  };
}

// ユニバーサルな合成が可能に
const aiResponse = unilmp.groq.instant(key).stream("こんにちは");
const wsEvents = qwiks.websocket(url).events();

// すべてがStream<T>を返す - 完璧な合成
const merged = aiResponse
  .merge(wsEvents)
  .map(normalize)
  .filter(isImportant)
  .toResponse();
```

## 設計原則

### 1. **Web Streams ファースト**
```typescript
// ReadableStreamがStream<T>である（変換されない）
const response = await fetch(url);
const stream = fromReadableStream(response.body); // ゼロオーバーヘッド
```

### 2. **ユニバーサルコントラクト**
```typescript
// すべてのライブラリがStream<T>を返す
function myLibrary(): Stream<Data> {
  return stream.from(createDataStream());
}
```

### 3. **エッジ最適化**
```typescript
// メモリを意識した操作
stream.from(largeDataStream)
  .buffer(100)    // バッチ処理
  .take(1000)     // 総数制限
  .subscribe(handleBatch);
```

### 4. **標準互換**
```typescript
// すべてのWeb APIで動作
stream.from(response.body)           // fetch API
  .pipeTo(writable)                  // WritableStream API
  .then(() => console.log("完了"));  // Promise API
```

## 貢献

コントリビューションを歓迎します！詳細は[貢献ガイド](CONTRIBUTING.md)をご覧ください。

## ライセンス

MIT © Aid-On

## 関連プロジェクト

- [@aid-on/unilmp](https://github.com/Aid-On/aid-on-platform/tree/main/packages/unilmp) - ユニバーサルLLMプロバイダー
- [@aid-on/qwiks](https://github.com/Aid-On/aid-on-platform/tree/main/packages/qwiks) - Qwik + ストリーミング統合

---

エッジコンピューティング時代のために ❤️ で構築
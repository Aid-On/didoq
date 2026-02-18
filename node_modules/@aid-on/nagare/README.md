# @aid-on/nagare

[![npm version](https://img.shields.io/npm/v/@aid-on/nagare.svg)](https://www.npmjs.com/package/@aid-on/nagare)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.7-blue.svg)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**nagare** (流れ, "flow" in Japanese) is a universal streaming interface built on Web Streams API, designed for edge computing environments. It provides a fluent, type-safe API with reactive extensions for modern TypeScript applications.

[日本語版 README はこちら](./README.ja.md)

## Features

- 🌊 **Web Streams Native**: Built on the standard Web Streams API
- 🚀 **Edge-Ready**: Optimized for Cloudflare Workers, Deno Deploy, and other edge runtimes
- 🔄 **Reactive Extensions**: RxJS-like operators with backpressure support
- 🎯 **Type-Safe**: Full TypeScript support with strict typing
- 🔧 **Composable**: Chain operators for complex stream transformations
- ⚡ **High Performance**: Minimal overhead, zero dependencies
- 🌐 **Universal**: Works in browsers, Node.js, Deno, and edge environments

## Installation

```bash
npm install @aid-on/nagare
```

```bash
yarn add @aid-on/nagare
```

```bash
pnpm add @aid-on/nagare
```

## Quick Start

```typescript
import { stream } from '@aid-on/nagare';

// Create a stream from an array
const numbers = stream.array([1, 2, 3, 4, 5]);

// Transform with operators
const result = await numbers
  .map(x => x * 2)
  .filter(x => x > 4)
  .collect();

console.log(result); // [6, 8, 10]
```

## Core Concepts

### Stream Creation

```typescript
// From array
const s1 = stream.array([1, 2, 3]);

// From ReadableStream
const s2 = stream.from(readableStream);

// From async generator
async function* generate() {
  yield 1;
  yield 2;
  yield 3;
}
const s3 = stream.from(ReadableStream.from(generate()));

// Create with imperative push (perfect for real-time data)
const s4 = stream.create<number>((controller) => {
  controller.next(1);
  controller.next(2);
  controller.next(3);
  controller.complete();
});

// From SSE endpoint
const events = await stream.fromSSE('/api/events');
```

### Stream Transformation

```typescript
const transformed = stream
  .array([1, 2, 3, 4, 5])
  .map(x => x * 2)                    // Multiply by 2
  .filter(x => x > 5)                 // Keep only > 5
  .mapAsync(async x => {              // Async transformation
    const result = await fetch(`/api/data/${x}`);
    return result.json();
  }, 3)                                // Concurrency of 3
  .tap(data => console.log(data))     // Side effects
  .buffer(10)                          // Buffer 10 items
  .debounce(100);                      // Debounce 100ms
```

### Operators

#### Transformation
- `map<U>(fn: (value: T) => U)` - Transform each value
- `mapAsync<U>(fn: (value: T) => Promise<U>, concurrency?: number)` - Async transformation with concurrency control and **order preservation**
- `filter(predicate: (value: T) => boolean)` - Filter values
- `scan<U>(fn: (acc: U, value: T) => U, initial: U)` - Accumulate values
- `expand(fn: (value: T) => T[])` - Expand single values to multiple
- `compact()` - Remove null/undefined values

#### Flow Control
- `take(count: number)` - Take first N values
- `takeUntil(predicate: (value: T) => boolean)` - Take until condition
- `debounce(ms: number)` - Debounce by milliseconds
- `throttle(ms: number)` - Throttle with trailing edge support
- `buffer(size: number)` - Buffer into arrays
- `batch(size: number)` - Alias for buffer

#### Side Effects
- `tap(fn: (value: T) => void, options?: { rethrow?: boolean })` - Perform side effects with configurable error handling

#### Combination
- `merge(...streams: Stream<T>[])` - Merge multiple streams
- `concat(stream: Stream<T>)` - Concatenate streams

### Consumption

```typescript
// Collect all values into array
const array = await stream.collect();

// Convert to Response (for edge workers)
const response = stream.toResponse({
  headers: { 'Content-Type': 'application/json' }
});

// Convert to SSE format
const sseStream = stream.toSSE();

// Subscribe with observer pattern
const subscription = stream.subscribe({
  next: (value) => console.log(value),
  error: (err) => console.error(err),
  complete: () => console.log('Done')
});

// Later: unsubscribe
subscription.unsubscribe();

// Async iteration
for await (const value of stream) {
  console.log(value);
}
```

## Advanced Features

### Backpressure Support

Subscribe handles backpressure automatically:

```typescript
stream.subscribe({
  next: async (value) => {
    // Stream waits for async processing
    await processData(value);
  }
});
```

### Multiple Subscriptions

Streams are single-consumer by default. Use `tee()` for multiple consumers:

```typescript
const original = stream.array([1, 2, 3]);
const [stream1, stream2] = original.tee();

// Convert back to Stream
const s1 = stream.from(stream1);
const s2 = stream.from(stream2);

s1.subscribe({ next: v => console.log('A:', v) });
s2.subscribe({ next: v => console.log('B:', v) });
```

### SSE (Server-Sent Events)

Perfect for real-time streaming from edge workers:

```typescript
// Client
const events = await stream.fromSSE<MessageEvent>('/api/chat', {
  method: 'POST',
  body: { prompt: 'Hello' }
});

events.subscribe({
  next: (event) => console.log(event.data)
});

// Server (Edge Worker)
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

## Platform Support

- ✅ Cloudflare Workers
- ✅ Deno / Deno Deploy
- ✅ Node.js 18+
- ✅ Bun
- ✅ Modern Browsers (Chrome, Firefox, Safari, Edge)
- ✅ Vercel Edge Runtime
- ✅ Netlify Edge Functions

## Performance

Nagare is designed for minimal overhead:

- Zero dependencies
- Small bundle size (~10KB gzipped)
- Leverages native Web Streams for optimal performance
- Lazy evaluation - operators don't execute until consumed
- Automatic resource cleanup

## API Reference

### Main Exports

```typescript
import {
  stream,           // Main stream factory
  Stream,           // Stream type
  StreamPipeOptions, // Pipe options type
  operators,        // Low-level operators
  fromArray,        // Create from array
  fromReadableStream // Create from ReadableStream
} from '@aid-on/nagare';
```

### Stream Interface

```typescript
interface Stream<T> {
  // Native Web Streams
  readable: ReadableStream<T>;
  pipeTo(destination: WritableStream<T>, options?: StreamPipeOptions): Promise<void>;
  pipeThrough<U>(transform: TransformStream<T, U>): Stream<U>;
  tee(): [ReadableStream<T>, ReadableStream<T>];

  // Operators
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

  // Combination
  merge(...streams: Stream<T>[]): Stream<T>;
  concat(stream: Stream<T>): Stream<T>;

  // Consumption
  collect(): Promise<T[]>;
  toArray(): Promise<T[]>;
  toResponse(init?: ResponseInit): Response;
  toSSE(formatter?: (value: T) => string): Stream<string>;
  subscribe(observer: Observer<T>): Subscription;
  [Symbol.asyncIterator](): AsyncIterableIterator<T>;
}
```

## Version History

### v0.1.0 (2025)
- ✅ Order preservation in `mapAsync` with concurrency
- ✅ Cross-platform line ending support in SSE parser (`\r\n`, `\n`, `\r`)
- ✅ Configurable error handling in `tap` operator
- ✅ Improved resource management in `takeUntil`
- ✅ Enhanced TypeScript exports with `StreamPipeOptions`
- ✅ Full documentation for `tee()` behavior

### v0.0.x (2024)
- Initial release with core streaming functionality
- Web Streams API foundation
- Basic operators and transformations

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details.

## License

MIT © Aid-On

## Related Projects

- [@aid-on/unilmp](https://github.com/Aid-On/aid-on-platform/tree/main/packages/unilmp) - Universal LLM Provider
- [@aid-on/qwiks](https://github.com/Aid-On/aid-on-platform/tree/main/packages/qwiks) - Qwik + Streaming Integration

---

Built with ❤️ for the edge computing era
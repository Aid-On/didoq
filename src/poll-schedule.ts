/**
 * poll-schedule — ジョブ監視のポーリング/再接続スケジュール（純粋関数）。
 *
 * React に依存しないため、単体テスト・ベンチマークから直接検証できる。
 * client.ts（useJobMonitor）はこのスケジュールに従ってタイマーを張る。
 *
 * 設計値の根拠（text-smash の実ジョブ分布: 数秒〜数分）:
 *   - 立ち上がりは密（800ms）: 短いジョブの完了検知を速くする
 *   - 指数的に疎へ（×1.5, 上限 4s）: 長いジョブでリクエストを浪費しない
 *   - ジッタ ±20%: 多クライアントのポーリング同期（thundering herd）を散らす
 */

/** 初回ポーリング間隔 (ms) */
export const POLL_BASE_MS = 800;
/** ポーリング間隔の伸長係数 */
export const POLL_GROWTH = 1.5;
/** ポーリング間隔の上限 (ms)。長時間ジョブでも平均検知遅延はこの半分 */
export const POLL_MAX_MS = 4000;
/**
 * WebSocket 再接続遅延の上限 (ms)。
 * 旧実装は無上限の指数（1000×2^retry）で、maxRetries=15 だと HTTP ポーリングへの
 * フォールバック到達に累計 2^15 秒 ≈ 9 時間かかり、WS が恒常的に塞がれる環境
 * （企業プロキシ等）では実質フォールバック不能だった。
 */
export const WS_RECONNECT_MAX_MS = 10_000;

/**
 * n 回目（0 始まり）のポーリングまでの遅延 (ms)。
 * 指数的に伸びて上限で頭打ち。`random` はテスト用に注入可能。
 */
export function nextPollDelay(
  pollCount: number,
  random: () => number = Math.random,
): number {
  const raw = Math.min(POLL_BASE_MS * POLL_GROWTH ** pollCount, POLL_MAX_MS);
  const jitter = 0.8 + random() * 0.4; // 0.8〜1.2 倍
  return Math.round(raw * jitter);
}

/** n 回目（0 始まり）の WebSocket 再接続までの遅延 (ms)。指数バックオフ + 上限 */
export function wsReconnectDelay(retryCount: number, baseMs = 1000): number {
  return Math.min(baseMs * 2 ** retryCount, WS_RECONNECT_MAX_MS);
}

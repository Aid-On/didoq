import { describe, it, expect } from 'vitest';
import {
  nextPollDelay,
  wsReconnectDelay,
  POLL_BASE_MS,
  POLL_MAX_MS,
  WS_RECONNECT_MAX_MS,
} from '../poll-schedule';

/** ジッタ中立（×1.0）にする random */
const mid = () => 0.5;

describe('nextPollDelay', () => {
  it('初回は密（800ms）、回を追うごとに ×1.5 で疎になる', () => {
    expect(nextPollDelay(0, mid)).toBe(POLL_BASE_MS);
    expect(nextPollDelay(1, mid)).toBe(Math.round(POLL_BASE_MS * 1.5));
    expect(nextPollDelay(2, mid)).toBe(Math.round(POLL_BASE_MS * 2.25));
  });

  it('上限 4000ms で頭打ちになる', () => {
    expect(nextPollDelay(10, mid)).toBe(POLL_MAX_MS);
    expect(nextPollDelay(100, mid)).toBe(POLL_MAX_MS);
  });

  it('ジッタは ±20% の範囲に収まる', () => {
    expect(nextPollDelay(0, () => 0)).toBe(Math.round(POLL_BASE_MS * 0.8));
    expect(nextPollDelay(0, () => 1)).toBe(Math.round(POLL_BASE_MS * 1.2));
    for (let i = 0; i < 50; i++) {
      const d = nextPollDelay(3);
      const raw = POLL_BASE_MS * 1.5 ** 3;
      expect(d).toBeGreaterThanOrEqual(Math.floor(raw * 0.8));
      expect(d).toBeLessThanOrEqual(Math.ceil(raw * 1.2));
    }
  });
});

describe('wsReconnectDelay', () => {
  it('指数バックオフ（1s, 2s, 4s, 8s）', () => {
    expect(wsReconnectDelay(0)).toBe(1000);
    expect(wsReconnectDelay(1)).toBe(2000);
    expect(wsReconnectDelay(2)).toBe(4000);
    expect(wsReconnectDelay(3)).toBe(8000);
  });

  it('上限 10s で頭打ち（旧実装は 2^14 秒 ≈ 4.6 時間まで伸びた）', () => {
    expect(wsReconnectDelay(4)).toBe(WS_RECONNECT_MAX_MS);
    expect(wsReconnectDelay(14)).toBe(WS_RECONNECT_MAX_MS);
  });

  it('maxRetries=15 までの累計待機が現実的な時間に収まる', () => {
    let total = 0;
    for (let r = 0; r < 15; r++) total += wsReconnectDelay(r);
    expect(total).toBeLessThanOrEqual(125_000); // 約 2 分（旧実装: 約 9.1 時間）
  });
});

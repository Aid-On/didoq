/**
 * didoq クライアント・ベンチマーク — ポーリングスケジュールの効率を机上計算で比較する。
 *
 *   node bench/run.mjs
 *
 * ネットワークは使わない。スケジュール（固定 2s vs 適応 800ms→×1.5→上限 4s）に対し、
 * ジョブ完了時間の代表値ごとに以下を算出する:
 *   - requests   : 完了検知までの HTTP リクエスト数
 *   - detectMs   : 完了から検知までの遅延（完了直後〜次のポーリングまで）
 * あわせて「WS が塞がれた環境でポーリングに到達するまでの時間」を新旧で比較する。
 */

const POLL_BASE_MS = 800;
const POLL_GROWTH = 1.5;
const POLL_MAX_MS = 4000;

/** 旧: 固定 2000ms（初回は即時） */
function* fixedSchedule() {
  yield 0;
  while (true) yield 2000;
}

/** 新: 適応（初回は即時、ジッタ中立） */
function* adaptiveSchedule() {
  yield 0;
  let n = 0;
  while (true) yield Math.min(POLL_BASE_MS * POLL_GROWTH ** n++, POLL_MAX_MS);
}

/** ジョブが jobMs で完了するとき、検知時刻とリクエスト数を返す */
function simulate(scheduleFactory, jobMs) {
  const schedule = scheduleFactory();
  let t = 0;
  let requests = 0;
  while (true) {
    t += schedule.next().value;
    requests++;
    if (t >= jobMs) return { requests, detectMs: t - jobMs };
  }
}

const JOB_DURATIONS = [3_000, 10_000, 30_000, 120_000, 300_000];

console.log('didoq polling schedule benchmark（机上計算・ジッタ中立）');
console.log('─'.repeat(74));
console.log(
  `${'job'.padEnd(8)}${'fixed 2s: req'.padStart(14)}${'detect'.padStart(8)}${'adaptive: req'.padStart(15)}${'detect'.padStart(8)}${'req削減'.padStart(9)}`,
);
console.log('─'.repeat(74));
for (const jobMs of JOB_DURATIONS) {
  const f = simulate(fixedSchedule, jobMs);
  const a = simulate(adaptiveSchedule, jobMs);
  const cut = (((f.requests - a.requests) / f.requests) * 100).toFixed(0);
  console.log(
    `${(jobMs / 1000 + 's').padEnd(8)}${String(f.requests).padStart(14)}${(f.detectMs / 1000).toFixed(1).padStart(7)}s${String(a.requests).padStart(15)}${(a.detectMs / 1000).toFixed(1).padStart(7)}s${(cut + '%').padStart(9)}`,
  );
}
console.log('─'.repeat(74));

// WS が恒常的に塞がれた環境（企業プロキシ等）でのフォールバック到達時間
const MAX_RETRIES = 15; // text-smash の本番設定
let oldTotal = 0;
for (let r = 0; r < MAX_RETRIES; r++) oldTotal += 1000 * 2 ** r; // 旧: 無上限指数
let newTotal = 0;
for (let r = 0; r < MAX_RETRIES; r++) newTotal += Math.min(1000 * 2 ** r, 10_000); // 新: 上限 10s
console.log('WS 遮断環境でのフォールバック:');
console.log(`  旧: ポーリング開始まで ${(oldTotal / 3600_000).toFixed(1)} 時間（全リトライ消化後にのみ開始）`);
console.log(`  新: 即時（初回 WS 失敗時から安全網ポーリング併走）/ WS 再試行の累計も ${(newTotal / 1000).toFixed(0)}s に短縮`);

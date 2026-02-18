-- DidoQ2 Performance Optimization Indexes
-- レビューフィードバックに基づくインデックス最適化

-- 1. メインジョブテーブルの作成（存在しない場合）
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  shard_id INTEGER NOT NULL,
  queue_position INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  type TEXT,
  user_id TEXT,
  payload TEXT,
  result TEXT,
  error TEXT,
  retry_count INTEGER DEFAULT 0,
  max_retries INTEGER DEFAULT 3,
  created_at TEXT DEFAULT (datetime('now')),
  started_at TEXT,
  completed_at TEXT,
  locked_at TEXT,
  CONSTRAINT status_check CHECK (status IN ('pending', 'locked', 'processing', 'completed', 'failed', 'cancelled'))
);

-- 2. 高頻度クエリ用の複合インデックス

-- シャードごとの未処理ジョブ取得用（最重要）
CREATE INDEX IF NOT EXISTS idx_jobs_shard_status_queue 
ON jobs (shard_id, status, queue_position)
WHERE status IN ('pending', 'locked');

-- ユーザーごとのジョブ一覧用
CREATE INDEX IF NOT EXISTS idx_jobs_user_status_created
ON jobs (user_id, status, created_at DESC);

-- ステータス別の集計用
CREATE INDEX IF NOT EXISTS idx_jobs_status_created
ON jobs (status, created_at DESC);

-- リトライ管理用
CREATE INDEX IF NOT EXISTS idx_jobs_retry
ON jobs (status, retry_count)
WHERE status = 'failed' AND retry_count < max_retries;

-- ロック解放用（タイムアウト処理）
CREATE INDEX IF NOT EXISTS idx_jobs_locked_timeout
ON jobs (status, locked_at)
WHERE status = 'locked';

-- 3. WebSocket通知用テーブル
CREATE TABLE IF NOT EXISTS job_notifications (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT NOT NULL,
  status TEXT NOT NULL,
  data TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  delivered_at TEXT,
  FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
);

-- 通知配信用インデックス
CREATE INDEX IF NOT EXISTS idx_notifications_delivered
ON job_notifications (delivered_at)
WHERE delivered_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_notifications_job_id
ON job_notifications (job_id, created_at DESC);

-- 4. メトリクステーブル（パフォーマンス監視用）
CREATE TABLE IF NOT EXISTS job_metrics (
  shard_id INTEGER NOT NULL,
  hour TEXT NOT NULL,
  total_jobs INTEGER DEFAULT 0,
  completed_jobs INTEGER DEFAULT 0,
  failed_jobs INTEGER DEFAULT 0,
  avg_processing_time_ms INTEGER,
  max_processing_time_ms INTEGER,
  PRIMARY KEY (shard_id, hour)
);

-- メトリクス集計用インデックス
CREATE INDEX IF NOT EXISTS idx_metrics_hour
ON job_metrics (hour DESC);

-- 5. デッドレタージョブテーブル（失敗ジョブの永続化）
CREATE TABLE IF NOT EXISTS dead_letter_jobs (
  id TEXT PRIMARY KEY,
  original_job_id TEXT NOT NULL,
  shard_id INTEGER NOT NULL,
  type TEXT,
  user_id TEXT,
  payload TEXT,
  error TEXT,
  retry_count INTEGER,
  created_at TEXT,
  failed_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_dead_letter_user
ON dead_letter_jobs (user_id, failed_at DESC);

-- 6. アーカイブテーブル（完了ジョブの長期保管）
CREATE TABLE IF NOT EXISTS jobs_archive (
  id TEXT PRIMARY KEY,
  shard_id INTEGER NOT NULL,
  status TEXT NOT NULL,
  type TEXT,
  user_id TEXT,
  payload TEXT,
  result TEXT,
  created_at TEXT,
  completed_at TEXT,
  archived_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_archive_user_completed
ON jobs_archive (user_id, completed_at DESC);

-- 7. ビューの作成（よく使うクエリの最適化）

-- シャードごとの統計ビュー
CREATE VIEW IF NOT EXISTS v_shard_stats AS
SELECT 
  shard_id,
  status,
  COUNT(*) as count,
  MIN(created_at) as oldest_job,
  MAX(created_at) as newest_job
FROM jobs
GROUP BY shard_id, status;

-- ユーザーごとのジョブサマリービュー
CREATE VIEW IF NOT EXISTS v_user_job_summary AS
SELECT 
  user_id,
  COUNT(*) as total_jobs,
  SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_jobs,
  SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_jobs,
  SUM(CASE WHEN status IN ('pending', 'processing') THEN 1 ELSE 0 END) as active_jobs
FROM jobs
GROUP BY user_id;

-- 8. トリガー（自動アーカイブとメトリクス更新）

-- 完了ジョブの自動アーカイブ（30日後）
CREATE TRIGGER IF NOT EXISTS archive_old_jobs
AFTER UPDATE OF status ON jobs
WHEN NEW.status IN ('completed', 'cancelled')
BEGIN
  INSERT INTO jobs_archive 
  SELECT 
    id, shard_id, status, type, user_id, 
    payload, result, created_at, completed_at, datetime('now')
  FROM jobs
  WHERE id = NEW.id
    AND datetime(completed_at) < datetime('now', '-30 days');
  
  DELETE FROM jobs
  WHERE id = NEW.id
    AND datetime(completed_at) < datetime('now', '-30 days');
END;

-- メトリクス自動更新
CREATE TRIGGER IF NOT EXISTS update_metrics
AFTER UPDATE OF status ON jobs
WHEN NEW.status IN ('completed', 'failed')
BEGIN
  INSERT OR REPLACE INTO job_metrics (
    shard_id,
    hour,
    total_jobs,
    completed_jobs,
    failed_jobs,
    avg_processing_time_ms
  )
  SELECT 
    NEW.shard_id,
    strftime('%Y-%m-%d %H:00:00', 'now'),
    COALESCE((SELECT total_jobs FROM job_metrics 
              WHERE shard_id = NEW.shard_id 
              AND hour = strftime('%Y-%m-%d %H:00:00', 'now')), 0) + 1,
    COALESCE((SELECT completed_jobs FROM job_metrics 
              WHERE shard_id = NEW.shard_id 
              AND hour = strftime('%Y-%m-%d %H:00:00', 'now')), 0) + 
    CASE WHEN NEW.status = 'completed' THEN 1 ELSE 0 END,
    COALESCE((SELECT failed_jobs FROM job_metrics 
              WHERE shard_id = NEW.shard_id 
              AND hour = strftime('%Y-%m-%d %H:00:00', 'now')), 0) + 
    CASE WHEN NEW.status = 'failed' THEN 1 ELSE 0 END,
    CASE 
      WHEN NEW.started_at IS NOT NULL AND NEW.completed_at IS NOT NULL
      THEN (julianday(NEW.completed_at) - julianday(NEW.started_at)) * 86400000
      ELSE NULL
    END;
END;

-- 9. パフォーマンス分析用のEXPLAINクエリ（コメント）
/*
-- 以下のクエリで実行計画を確認できます：

-- シャード取得クエリの実行計画
EXPLAIN QUERY PLAN
SELECT * FROM jobs 
WHERE status = 'pending' AND shard_id = 0
ORDER BY queue_position ASC 
LIMIT 10;

-- ユーザージョブ取得の実行計画
EXPLAIN QUERY PLAN
SELECT * FROM jobs 
WHERE user_id = 'user123' AND status = 'processing'
ORDER BY created_at DESC;

-- 統計集計の実行計画
EXPLAIN QUERY PLAN
SELECT status, COUNT(*) as count
FROM jobs
WHERE shard_id = 0
GROUP BY status;
*/
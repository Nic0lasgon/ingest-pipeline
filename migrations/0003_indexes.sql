-- Migration 0003: Performance indexes
-- Covers the query patterns described in the PRD and original D1 spec.

-- raw_articles: main query paths
CREATE INDEX idx_raw_articles_source_id          ON raw_articles(source_id);
CREATE INDEX idx_raw_articles_url                ON raw_articles(url);
CREATE INDEX idx_raw_articles_processing_status  ON raw_articles(processing_status);
CREATE INDEX idx_raw_articles_quality_status     ON raw_articles(quality_status);
CREATE INDEX idx_raw_articles_duplicate_status   ON raw_articles(duplicate_status);
CREATE INDEX idx_raw_articles_pub_date           ON raw_articles(pub_date DESC);
CREATE INDEX idx_raw_articles_created_at         ON raw_articles(created_at DESC);

-- pipeline_step_runs: lookup by run and step
CREATE INDEX idx_step_runs_run_id     ON pipeline_step_runs(run_id);
CREATE INDEX idx_step_runs_step_name  ON pipeline_step_runs(step_name);
CREATE INDEX idx_step_runs_status     ON pipeline_step_runs(status);

-- rejected_articles: lookup by article, source, reason
CREATE INDEX idx_rejected_articles_article_id ON rejected_articles(article_id);
CREATE INDEX idx_rejected_articles_source_id  ON rejected_articles(source_id);
CREATE INDEX idx_rejected_articles_reason     ON rejected_articles(reason);

-- jobs: SKIP LOCKED polling, lock owner tracking, type filtering
CREATE INDEX idx_jobs_status_run_at  ON jobs(status, run_at);
CREATE INDEX idx_jobs_locked_by      ON jobs(locked_by);
CREATE INDEX idx_jobs_job_type       ON jobs(job_type);
CREATE INDEX idx_jobs_priority       ON jobs(priority DESC);

-- feed_sources: active feed discovery
CREATE INDEX idx_feed_sources_enabled      ON feed_sources(enabled);
CREATE INDEX idx_feed_sources_tier         ON feed_sources(tier);
CREATE INDEX idx_feed_sources_fetch_status ON feed_sources(fetch_status);

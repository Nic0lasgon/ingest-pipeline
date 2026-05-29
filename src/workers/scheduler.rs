use anyhow::Result;
use chrono::{DateTime, Timelike, Utc};
use sqlx::PgPool;
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};

use crate::db::article_queries::cleanup_old_duplicates;
use crate::db::feed_queries::{get_active_feeds, get_active_tier1_feeds};
use crate::db::run_queries::{
    clean_orphaned_articles_from_failed_runs, mark_zombie_runs_failed, start_run, start_run_step,
};
use crate::db::schema::{RunTriggerType, StepName};
use crate::queue::jobs::create_job;

const DEFAULT_CRON_HOURS: [u32; 4] = [2, 6, 14, 20];

pub async fn run_scheduler(pool: PgPool) {
    info!("Scheduler started");

    loop {
        let next_run = get_next_cron_time();
        let wait_duration = next_run - Utc::now();
        let wait_secs = wait_duration.num_seconds().max(0) as u64;

        info!(
            "Next scheduled run at {} (waiting {}s)",
            next_run, wait_secs
        );
        sleep(Duration::from_secs(wait_secs)).await;

        if let Err(e) = handle_scheduled_run(&pool).await {
            error!("Scheduled run failed: {}", e);
        }
    }
}

async fn handle_scheduled_run(pool: &PgPool) -> Result<()> {
    info!("Starting scheduled pipeline run");

    // 1. Marquer les runs zombies comme failed (> 2h sans activité)
    let zombie_count = mark_zombie_runs_failed(pool, 2).await?;
    if zombie_count > 0 {
        warn!("Marked {} zombie runs as failed", zombie_count);
    }

    // 2. Nettoyer les articles orphelins
    clean_orphaned_articles_from_failed_runs(pool).await?;

    // 3. Guard : vérifier qu'aucun run n'est déjà en cours
    let active_runs: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM pipeline_runs WHERE status = 'running'")
            .fetch_one(pool)
            .await?;

    if active_runs > 0 {
        warn!("Pipeline run already in progress, skipping");
        return Ok(());
    }

    // 4. Récupérer feeds actifs tier1_keep (fallback: tous les actifs)
    let feeds = get_active_tier1_feeds(pool).await?;
    let feeds = if feeds.is_empty() {
        get_active_feeds(pool).await?
    } else {
        feeds
    };
    let feeds_count = feeds.len() as i32;

    // 5. Créer un nouveau pipeline_run
    let run = start_run(pool, RunTriggerType::Scheduled, Some(feeds_count)).await?;
    info!(run_id = %run.id, feeds_count, "Pipeline run started");

    // 6. Créer pipeline_step_run pour 'ingest'
    let _step = start_run_step(pool, run.id, StepName::Ingest, Some(feeds_count)).await?;

    // 7. Créer des jobs fetch_feed (un par feed)
    for feed in &feeds {
        let payload = serde_json::json!({
            "feed_id": feed.id,
            "run_id": run.id.to_string(),
        });
        create_job(pool, "fetch_feed", payload, feed.priority).await?;
        info!(feed_id = %feed.id, "Created fetch_feed job");
    }

    // 8. Nettoyer doublons anciens (> 7 jours)
    let cleaned = cleanup_old_duplicates(pool, 7).await?;
    if cleaned > 0 {
        info!("Cleaned {} old duplicates", cleaned);
    }

    info!(
        run_id = %run.id,
        feeds_count = feeds.len(),
        "Scheduled run completed"
    );
    Ok(())
}

fn get_next_cron_time() -> DateTime<Utc> {
    let override_expr = std::env::var("RUN_SCHEDULE").ok();
    if let Some(ref expr) = override_expr {
        if let Some(dt) = parse_cron_next(expr) {
            return dt;
        }
        warn!("Invalid RUN_SCHEDULE '{}', falling back to default", expr);
    }

    let now = Utc::now();

    for hour in DEFAULT_CRON_HOURS {
        let candidate = now.date_naive().and_hms_opt(hour, 0, 0).unwrap().and_utc();
        if candidate > now {
            return candidate;
        }
    }

    now.date_naive()
        .succ_opt()
        .unwrap()
        .and_hms_opt(DEFAULT_CRON_HOURS[0], 0, 0)
        .unwrap()
        .and_utc()
}

fn parse_cron_next(expr: &str) -> Option<DateTime<Utc>> {
    let parts: Vec<&str> = expr.split_whitespace().collect();
    if parts.len() != 5 {
        return None;
    }

    let now = Utc::now();

    // Every N minutes: */N * * * *
    if parts[0].starts_with("*/") {
        let minutes: u32 = parts[0].trim_start_matches("*/").parse().ok()?;
        if minutes == 0 {
            return None;
        }
        let current_min = now.minute();
        let next_minute = ((current_min / minutes) + 1) * minutes;
        if next_minute < 60 {
            return now
                .date_naive()
                .and_hms_opt(now.hour(), next_minute, 0)
                .map(|naive| naive.and_utc());
        }
        let next_hour = (now.hour() + 1) % 24;
        let date = if next_hour == 0 {
            now.date_naive().succ_opt()?
        } else {
            now.date_naive()
        };
        return date
            .and_hms_opt(next_hour, next_minute % 60, 0)
            .map(|naive| naive.and_utc());
    }

    // Specific hours: 0 H1,H2,... * * *
    if parts[0] == "0" && parts[1].contains(',') {
        let hours: Vec<u32> = parts[1].split(',').filter_map(|h| h.parse().ok()).collect();
        for hour in &hours {
            let candidate = now.date_naive().and_hms_opt(*hour, 0, 0).unwrap().and_utc();
            if candidate > now {
                return Some(candidate);
            }
        }
        let first = *hours.first()?;
        return now
            .date_naive()
            .succ_opt()?
            .and_hms_opt(first, 0, 0)
            .map(|naive| naive.and_utc());
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Timelike;
    use std::env;
    use std::sync::LazyLock;
    use tokio::sync::Mutex;

    static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    #[test]
    fn test_get_next_cron_time_returns_future() {
        let next = get_next_cron_time();
        let now = Utc::now();
        assert!(next > now, "next cron time should be in the future");
    }

    #[test]
    fn test_get_next_cron_time_is_at_valid_hour() {
        let next = get_next_cron_time();
        let hour = next.hour();
        assert!(
            DEFAULT_CRON_HOURS.contains(&hour),
            "next cron hour {} should be one of {:?}",
            hour,
            DEFAULT_CRON_HOURS
        );
        assert_eq!(next.minute(), 0);
        assert_eq!(next.second(), 0);
    }

    #[test]
    fn test_parse_cron_every_minute() {
        let result = parse_cron_next("*/1 * * * *");
        assert!(result.is_some(), "*/1 should parse successfully");
        let next = result.unwrap();
        let now = Utc::now();
        let diff = (next - now).num_seconds();
        assert!(
            (0..=120).contains(&diff),
            "next minute cron should be within 120s, got {}s",
            diff
        );
    }

    #[test]
    fn test_parse_cron_specific_hours() {
        let result = parse_cron_next("0 2,6,14,20 * * *");
        assert!(result.is_some(), "specific hours cron should parse");
        let next = result.unwrap();
        assert!(next > Utc::now());
        assert!(DEFAULT_CRON_HOURS.contains(&next.hour()));
    }

    #[test]
    fn test_parse_cron_invalid_format() {
        assert!(parse_cron_next("invalid").is_none());
        assert!(parse_cron_next("* * *").is_none());
        assert!(parse_cron_next("*/0 * * * *").is_none());
    }

    async fn setup_test_pool() -> Option<PgPool> {
        let url = env::var("DATABASE_URL").ok()?;
        let pool = PgPool::connect(&url).await.ok()?;

        let _ = sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS feed_sources (
                id                      TEXT PRIMARY KEY,
                feed_url                TEXT NOT NULL UNIQUE,
                name                    TEXT NOT NULL,
                category                TEXT,
                description             TEXT,
                logo                    TEXT,
                priority                INTEGER NOT NULL DEFAULT 0,
                tier                    TEXT NOT NULL DEFAULT 'free',
                fetch_status            TEXT NOT NULL DEFAULT 'pending',
                last_fetch_error        TEXT,
                last_fetch_at           TIMESTAMPTZ,
                last_ingested_pub_date  TIMESTAMPTZ,
                enabled                 BOOLEAN NOT NULL DEFAULT true,
                created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
            )"#,
        )
        .execute(&pool)
        .await;

        let _ = sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS pipeline_runs (
                id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                status              TEXT NOT NULL DEFAULT 'running',
                trigger_type        TEXT NOT NULL DEFAULT 'scheduled',
                started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
                completed_at        TIMESTAMPTZ,
                error_message       TEXT,
                feeds_count         INTEGER,
                articles_ingested   INTEGER NOT NULL DEFAULT 0,
                articles_qualified  INTEGER NOT NULL DEFAULT 0,
                articles_rejected   INTEGER NOT NULL DEFAULT 0,
                articles_duplicate  INTEGER NOT NULL DEFAULT 0,
                created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
            )"#,
        )
        .execute(&pool)
        .await;

        let _ = sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS pipeline_step_runs (
                id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                run_id          UUID NOT NULL REFERENCES pipeline_runs(id),
                step_name       TEXT NOT NULL,
                status          TEXT NOT NULL DEFAULT 'running',
                started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
                completed_at    TIMESTAMPTZ,
                error_message   TEXT,
                items_count     INTEGER NOT NULL DEFAULT 0,
                items_processed INTEGER NOT NULL DEFAULT 0,
                items_failed    INTEGER NOT NULL DEFAULT 0,
                created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
            )"#,
        )
        .execute(&pool)
        .await;

        let _ = sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS raw_articles (
                id                          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                source_id                   TEXT NOT NULL,
                title                       TEXT NOT NULL,
                url                         TEXT NOT NULL,
                description                 TEXT,
                image_url                   TEXT,
                author                      TEXT,
                pub_date                    TIMESTAMPTZ,
                content                     TEXT,
                content_length              INTEGER,
                content_hash                TEXT,
                title_clean                 TEXT,
                canonical_url               TEXT,
                processing_status           TEXT NOT NULL DEFAULT 'ingested',
                quality_status              TEXT NOT NULL DEFAULT 'pending',
                duplicate_status            TEXT NOT NULL DEFAULT 'pending',
                duplicate_of                UUID,
                preferred_extraction_method TEXT,
                extraction_attempts         INTEGER NOT NULL DEFAULT 0,
                last_extraction_error       TEXT,
                last_extraction_at          TIMESTAMPTZ,
                created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
                CONSTRAINT unique_raw_articles_url_source UNIQUE (url, source_id)
            )"#,
        )
        .execute(&pool)
        .await;

        let _ = sqlx::query(
            "DO $$ BEGIN
                CREATE TYPE job_status AS ENUM ('pending', 'running', 'completed', 'failed', 'dead');
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$",
        )
        .execute(&pool)
        .await;

        let _ = sqlx::query(
            "CREATE TABLE IF NOT EXISTS jobs (
                id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                job_type     TEXT NOT NULL,
                payload      JSONB NOT NULL,
                status       job_status NOT NULL DEFAULT 'pending',
                priority     INTEGER NOT NULL DEFAULT 0,
                attempts     INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 3,
                run_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
                locked_at    TIMESTAMPTZ,
                locked_by    TEXT,
                last_error   TEXT,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
        )
        .execute(&pool)
        .await;

        sqlx::query("DELETE FROM pipeline_step_runs")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM pipeline_runs")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM raw_articles")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM jobs")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM feed_sources")
            .execute(&pool)
            .await
            .unwrap();

        Some(pool)
    }

    #[tokio::test]
    async fn test_handle_scheduled_run_guard_skips_when_run_active() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        sqlx::query(
            r#"INSERT INTO pipeline_runs (status, trigger_type)
               VALUES ('running', 'scheduled')"#,
        )
        .execute(&pool)
        .await
        .unwrap();

        let result = handle_scheduled_run(&pool).await;
        assert!(result.is_ok(), "should succeed even when skipping");

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pipeline_runs")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 1, "should not have created a second run");

        sqlx::query("DELETE FROM pipeline_step_runs")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM pipeline_runs")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM jobs")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM feed_sources")
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_handle_scheduled_run_creates_jobs() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        for i in 1..=2 {
            sqlx::query(
                r#"INSERT INTO feed_sources (id, feed_url, name, enabled, tier, priority)
                   VALUES ($1, $2, $3, true, 'tier1_keep', $4)
                   ON CONFLICT (id) DO NOTHING"#,
            )
            .bind(format!("feed-{}", i))
            .bind(format!("https://example.com/feed{}", i))
            .bind(format!("Feed {}", i))
            .bind(i)
            .execute(&pool)
            .await
            .unwrap();
        }

        let result = handle_scheduled_run(&pool).await;
        assert!(result.is_ok(), "handle_scheduled_run should succeed");

        let run_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pipeline_runs")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(run_count, 1);

        let job_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE job_type = 'fetch_feed'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(job_count, 2);

        let step_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pipeline_step_runs")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(step_count, 1);

        sqlx::query("DELETE FROM pipeline_step_runs")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM pipeline_runs")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM jobs")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM feed_sources")
            .execute(&pool)
            .await
            .unwrap();
    }
}

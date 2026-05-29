-- Migration 0001: Initial schema
-- Creates all core pipeline tables: feed_sources, raw_articles, pipeline_runs,
-- pipeline_step_runs, and rejected_articles.

CREATE TABLE feed_sources (
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
);

CREATE TABLE raw_articles (
    id                          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id                   TEXT NOT NULL REFERENCES feed_sources(id),
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
    duplicate_of                UUID REFERENCES raw_articles(id),
    preferred_extraction_method TEXT,
    extraction_attempts         INTEGER NOT NULL DEFAULT 0,
    last_extraction_error       TEXT,
    last_extraction_at          TIMESTAMPTZ,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE pipeline_runs (
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
);

CREATE TABLE pipeline_step_runs (
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
);

CREATE TABLE rejected_articles (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    article_id  UUID NOT NULL REFERENCES raw_articles(id),
    source_id   TEXT NOT NULL,
    title       TEXT NOT NULL,
    url         TEXT NOT NULL,
    reason      TEXT NOT NULL,
    details     TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

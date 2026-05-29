-- Migration 0005: Add unique constraint on (url, source_id) for idempotent batch inserts
DO $$ BEGIN
    ALTER TABLE raw_articles ADD CONSTRAINT unique_raw_articles_url_source UNIQUE (url, source_id);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

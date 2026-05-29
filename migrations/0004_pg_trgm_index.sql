-- Extension trigram (idempotente)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Index GIN pour recherche de similarité sur title_clean
CREATE INDEX IF NOT EXISTS idx_raw_articles_title_trgm
ON raw_articles USING gin(title_clean gin_trgm_ops);

-- Index GIN sur url pour les recherches exactes rapides
CREATE INDEX IF NOT EXISTS idx_raw_articles_url_trgm
ON raw_articles USING gin(url gin_trgm_ops);

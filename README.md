# ingest-pipeline — Pipeline d'ingestion RSS

Pipeline d'ingestion RSS/Atom/JSON Feed pour VoxPod, écrit en Rust. Récupère les flux, extrait le contenu HTML des articles, détecte les doublons, et qualifie les articles pour le traitement aval par **Vox\_rag** (embedding + topics).

## Stack technique

| Composant | Technologie |
|---|---|
| Runtime | Rust + Tokio |
| HTTP | Axum |
| Base de données | PostgreSQL + sqlx (runtime queries, pas de compile-time macros) |
| Job queue | PostgreSQL (`SELECT FOR UPDATE SKIP LOCKED` + `LISTEN/NOTIFY`) |
| Extraction HTML | rs-trafilatura (primaire) + regex (fallback) |
| Déploiement | Docker Compose (local), VPS ou Railway/Fly.io (prod) |

## Architecture

### Pipeline de données

```
Scheduler (cron 4h)
  └─> Crée un pipeline_run
      └─> Crée des jobs fetch_feed (un par source RSS)
          └─> Worker ingère le flux RSS → parse → insert articles
              └─> Crée des jobs process_article
                  └─> Worker fetch HTML → rs-trafilatura → validation (≥300 chars, ≥350 mots) → dédup → qualifié
```

### Modes de lancement

```bash
cargo run -- api        # Serveur HTTP Axum uniquement
cargo run -- worker     # Workers de jobs uniquement
cargo run -- scheduler  # Scheduler des tâches récurrentes uniquement
cargo run -- all        # API + scheduler + worker ensemble (dev local)
```

## Structure du projet

```
ingest-pipeline/
├── src/
│   ├── main.rs              # Entry point + CLI (clap)
│   ├── config.rs            # Configuration par variables d'environnement
│   ├── lib.rs               # Exports pour les tests
│   ├── db/
│   │   ├── schema.rs        # Types Rust ↔ PostgreSQL
│   │   ├── feed_queries.rs  # Queries feed_sources
│   │   ├── article_queries.rs # Queries raw_articles
│   │   ├── run_queries.rs   # Queries pipeline_runs / step_runs
│   │   ├── job_queries.rs   # Queries jobs
│   │   └── rejected_queries.rs # Queries rejected_articles
│   ├── queue/
│   │   ├── jobs.rs          # CRUD jobs + SKIP LOCKED
│   │   └── worker.rs        # Boucle worker (poll + dispatch + retry)
│   ├── pipeline/
│   │   ├── ingest_step.rs   # Fetch RSS → parse → insert
│   │   └── content_step.rs  # Fetch HTML → extract → validate → qualify
│   ├── utils/
│   │   ├── rss_parser.rs    # Parser RSS/Atom/JSON (sans dépendance externe)
│   │   ├── text_extract.rs  # Extraction texte (rs-trafilatura + regex fallback)
│   │   ├── dedup.rs         # Déduplication URL + Jaccard titre
│   │   ├── url_resolver.rs  # Résolution URLs raccourcies
│   │   ├── word_count.rs    # Compteur de mots
│   │   └── shared.rs        # HTML entities + parse JSON safe
│   ├── workers/
│   │   ├── ingest_worker.rs # Handler fetch_feed
│   │   ├── content_worker.rs # Handler process_article
│   │   └── scheduler.rs     # Cron scheduler
│   └── api/
│       └── mod.rs           # Routes Axum (/health)
├── migrations/              # Migrations sqlx
│   ├── 0001_initial_schema.sql
│   ├── 0002_jobs_table.sql
│   ├── 0003_indexes.sql
│   ├── 0004_pg_trgm_index.sql    # Index GIN trigram pour dédup
│   └── 0005_unique_url_source.sql # Contrainte UNIQUE (url, source_id)
├── benches/                 # Benchmarks Criterion
├── tests/                   # Tests d'intégration + fixtures
├── Cargo.toml
├── docker-compose.yml
├── Dockerfile
└── .env.example
```

## Démarrage rapide

### Avec Docker Compose

```bash
cp .env.example .env
docker compose up -d
curl http://localhost:3000/health   # → {"status":"ok"}
```

### Sans Docker

```bash
createdb mypod_pipeline
export DATABASE_URL="postgres://user:password@localhost:5432/mypod_pipeline"
cargo sqlx migrate run
cargo run -- all
```

### Tests et quality gates

```bash
cargo test --lib                  # Tests unitaires (sans DB)
cargo test --all-targets          # Tous les tests (avec DB)
cargo test -- --ignored           # Tests E2E (vrais flux RSS)

# Quality gates (à passer avant chaque commit)
cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test --all-targets
```

## Variables d'environnement

Toutes dans `.env` (`.gitignore`). Jamais de clés en dur dans le code.

| Variable | Obligatoire | Défaut | Description |
|---|---|---|---|
| `DATABASE_URL` | Oui | — | URL PostgreSQL (port 5432) |
| `PORT` | Non | 3000 | Port API HTTP |
| `LOG_LEVEL` | Non | info | Niveau de log (trace/debug/info/warn/error) |
| `HETZNER_EXTRACT_URL` | Non | — | URL du service d'extraction Scrapling Hetzner |
| `HETZNER_EXTRACT_SECRET` | Non | — | Clé API Scrapling |
| `PIPELINE_API_SECRET` | Non | — | Secret pour l'API interne |
| `BFF_CRON_SECRET` | Non | — | Secret pour les endpoints cron |
| `RUN_SCHEDULE` | Non | `0 2,6,14,20 * * *` | Expression cron du scheduler |

## Schéma base de données

### feed_sources

Sources RSS configurées. Champs : `id`, `feed_url`, `name`, `category`, `tier`, `fetch_status`, `last_ingested_pub_date`, `enabled`.

### raw_articles

Table principale des articles ingérés. Champs clés :

| Colonne | Type | Description |
|---|---|---|
| `id` | UUID | Identifiant unique |
| `source_id` | TEXT → feed_sources | Source RSS d'origine |
| `title` / `title_clean` | TEXT | Titre brut et nettoyé |
| `url` / `canonical_url` | TEXT | URL article et URL canonique |
| `content` | TEXT | Texte extrait (clean) |
| `content_legacy` | TEXT | Ancienne extraction regex (référence, à nettoyer après validation) |
| `content_length` | INTEGER | Longueur du texte extrait |
| `processing_status` | TEXT | ingested → extracted → pending_qualification → qualified / rejected |
| `quality_status` | TEXT | pending / qualified / rejected |
| `duplicate_status` | TEXT | pending / distinct / duplicate / near_duplicate |
| `duplicate_of` | UUID | Référence vers l'article original si doublon |
| `extraction_attempts` | INTEGER | Nombre de tentatives d'extraction |
| `preferred_extraction_method` | TEXT | `hetzner_fallback` si le site nécessite JavaScript |

### jobs

Job queue PostgreSQL : `id`, `job_type`, `payload` (JSONB), `status`, `priority`, `attempts`, `max_attempts`, `run_at`, `locked_at`, `locked_by`.

Mécanisme : `SELECT FOR UPDATE SKIP LOCKED` pour éviter les conflits entre workers, `NOTIFY jobs_channel` pour réveiller les workers immédiatement. Backoff exponentiel (`2^attempts * 1s`), max 3 tentatives avant `dead`.

### rejected_articles

Articles rejetés avec raison : `article_id`, `source_id`, `title`, `url`, `reason`, `details`.

## Extraction de contenu

### Approche duale : rs-trafilatura + regex fallback

Le pipeline utilise **rs-trafilatura** (crate Rust, F1=0.859 au benchmark WCXB sur 2 008 pages) en première intention, avec le regex comme filet de sécurité. Le flux est :

```
Fetch HTML → rs-trafilatura → si échec ou extraction_quality < 0.3 → regex (fallback)
```

rs-trafilatura **nettoie le HTML, ne le télécharge pas**. Le fetch HTML (5 stratégies HTTP + fallback Hetzner) reste en amont.

### Problématique résolue

Les extractions regex seules incluaient du bruit qui polluait les embeddings en aval :

- Commentaires HTML (`<!-- Header + Social -->`) non filtrés
- Breadcrumbs de navigation ("News", "Gaming", "Big Tech")
- Catégories en bloc, sidebars, articles connexes
- Métadonnées résiduelles ("By Steve Dent", "May 7, 2026")
- Sur MarkTechPost : regex extrait **2.4× trop de contenu** (17 658 mots vs 1 657 attendus), causant 92% de singletons dans Vox\_rag

### Options rs-trafilatura pour VoxPod

| Option | Valeur | Raison |
|---|---|---|
| `favor_precision` | `true` | Seuils plus stricts, moins de bruit dans les embeddings |
| `deduplicate` | `true` | Supprime les paragraphes répétés (articles connexes, sidebars) |
| `max_link_density` | `0.5` | Plus agressif contre les sections de navigation (défaut = 0.8) |
| `url` | passé | Meilleure extraction metadata (hostname, canonical URL) |
| `include_tables` | `true` | Conserve les tableaux de données |
| `include_comments` | `false` | Pas de commentaires HTML |
| `include_images` | `false` | Pas d'images |
| `include_links` | `false` | Pas de liens |
| `include_formatting` | `false` | Texte brut uniquement |
| `use_fallback_extraction` | `true` | JSON-LD + structural fallback si extraction principale insuffisante |

La metadata (titre, canonical URL) est extraite en priorité par rs-trafilatura (JSON-LD, OG tags, Dublin Core). Si rs-trafilatura échoue, le regex prend le relais.

### Résultats de la comparaison (v1.1)

| Source | Résultat |
|---|---|
| **Engadget** (20 articles) | Les deux méthodes marchent. Regex inclut les breadcrumbs ("News", "Gaming"). Trafilatura commence directement par le contenu. |
| **MarkTechPost** (10 articles) | Différence massive. Regex capture sidebars, articles connexes, blocs de code (jusqu'à 17 000 mots). Trafilatura extrait uniquement l'article (~1 500 mots). |
| **TestingCatalog** (17 articles) | Les deux sont proches (HTML propre). Trafilatura inclut les tweets embeds — bruit négligeable. |
| **The Decoder** (10 articles) | Regex laisse passer les commentaires HTML. Trafilatura produit un texte propre avec "Key Points" en début. |

### Points d'attention

1. **Ne jamais supprimer le fallback regex** — rs-trafilatura peut échouer sur certains sites. Le regex garantit qu'on ne perd jamais de données.
2. **`extraction_quality < 0.3` = fallback** — Le score de confiance ML détecte les extractions douteuses. Ne pas monter ce seuil trop haut.
3. **Ne pas modifier `favor_precision` sans re-tester** — Ce réglage est critique pour les embeddings. `favor_recall = true` remettrait du bruit.
4. **`max_link_density: 0.5`** — Ne pas remonter à 0.8 (défaut). La valeur 0.5 est plus agressive contre les sections de navigation.
5. **Les tweets embeds passent à travers** — rs-trafilatura conserve les tweets incorporés. Bruit mineur, à surveiller.
6. **rs-trafilatura ne remplace pas le fetch HTML** — Le fetch (stratégies HTTP + Hetzner) reste en amont.
7. **La colonne `content_legacy`** existe dans `raw_articles`. Ne pas la supprimer, elle sert de référence pour comparer. Peut être nettoyée après validation définitive.

### Stratégies de fetch HTTP

5 stratégies essayées dans l'ordre (timeout 15s) :

1. User-Agent Googlebot
2. Referer Google
3. Referer Twitter (t.co)
4. Version AMP (`?amp=1`)
5. Referer Facebook

Si toutes échouent → fallback **Scrapling Hetzner** (extraction JavaScript-rendered pour les sites comme Euronews, RFI).

### Seuils de validation

- Contenu ≥ 300 caractères
- Nombre de mots ≥ 350
- En dessous → article rejeté (`content_too_short`)

## Bugs connus

### BUG-001 : Migration 0005 `IF NOT EXISTS` invalide — **FIXÉ**

- **Cause** : `ALTER TABLE ... ADD CONSTRAINT IF NOT EXISTS` n'existe pas en PostgreSQL
- **Fix** : Remplacement par un bloc `DO $$ BEGIN ... EXCEPTION WHEN duplicate_object THEN null; END $$;`
- **Statut** : Corrigé dans `migrations/0005_unique_url_source.sql`

### BUG-003 : `quality_status` jamais mis à `qualified` — **NON FIXÉ** (BLOQUANT)

- **Cause** : `content_step.rs` met `processing_status = 'PendingQualification'` mais ne définit pas `quality_status = 'qualified'`. L'étape de qualification est un placeholder commenté dans `content_worker.rs:46`.
- **Impact** : Les articles extraits avec succès restent en `quality_status = 'pending'` et ne sont pas importables par Vox\_rag.
- **Workaround** :
  ```sql
  UPDATE raw_articles SET quality_status = 'qualified'
  WHERE processing_status IN ('pending_qualification')
    AND content IS NOT NULL AND length(content) > 300;
  ```
- **Fix nécessaire** : Ajouter `quality_status = 'qualified'` dans `content_step.rs` après extraction réussie.

## Intégration avec Vox\_rag

L'`ingest-pipeline` est la **première moitié** du pipeline VoxPod. Il produit des articles qualifiés qui sont ensuite importés dans **Vox\_rag** pour la seconde moitié du traitement.

### Flux complet VoxPod

```
RSS/Atom/JSON Feed
  ↓
ingest-pipeline : fetch → parse → extract (rs-trafilatura) → dedup → qualify
  ↓
raw_articles (quality_status = 'qualified')
  ↓
import vers Vox_rag (scripts/import_from_ingest.sh)
  ↓
Vox_rag : embedding Octen → similarité pgvector → topic clustering → LLM (DeepSeek)
```

### Ce que Vox\_rag fait avec les articles

Vox\_rag prend les articles qualifiés et :

1. Génère des **embeddings vectoriels** (modèle `octen-embedding-8b`, 4096 dimensions) via l'API Octen
2. Calcule la **similarité cosinus** entre articles via pgvector
3. Regroupe les articles en **topics** selon des seuils de similarité :
   - `topic_strong ≥ 0.84` : même topic, fusion automatique
   - `topic_ambiguous 0.76-0.84` : zone grise → LLM DeepSeek pour arbitrage
   - `topic_weak 0.70` : sujet potentiellement lié
4. Maintient une **timeline** par topic et génère des résumés multi-sources

### Comment les articles sont importés

Le script `Vox_rag/scripts/import_from_ingest.sh` copie les articles depuis la base `ingest-pipeline` (port 5432) vers la base `Vox_rag` (port 5433), en filtrant sur `quality_status = 'qualified'`.

### Séparation des bases

| Base | Port | Contenu |
|---|---|---|
| ingest-pipeline | 5432 | Sources RSS, articles bruts, jobs, logs de pipeline |
| Vox\_rag | 5433 | Articles qualifiés, embeddings, topics, timelines |

### Variables d'environnement partagées

| Variable | ingest-pipeline | Vox\_rag |
|---|---|---|
| `DATABASE_URL` | Port 5432 | Port 5433 |
| `HETZNER_EXTRACT_URL` / `HETZNER_EXTRACT_SECRET` | Oui | Non |
| `OCTEN_API_KEY` | Non | Oui |
| `LLM_API_KEY` / `LLM_MODEL` | Non | Oui |

### Impact de la qualité d'extraction sur Vox\_rag

Un texte bruité (regex seul) dilue les embeddings et empêche le topic clustering : **92% de singletons** constatés lors de la validation du 2026-05-29. L'intégration rs-trafilatura est la correction principale pour réduire ce taux et permettre à Vox\_rag de grouper correctement les articles.

## Conventions de code

- Rust edition 2021, `anyhow::Result`, `tracing` pour les logs
- Pas de `.unwrap()` ou `.expect()` dans le code de production (autorisé dans les tests uniquement)
- `sqlx::query_as::<_, T>()` avec `.bind()` — pas de macros compile-time
- `SKIP LOCKED` pour la job queue — jamais de simple `UPDATE ... WHERE status='pending'`
- `LazyLock` pour les regex compilées une seule fois
- Quality gates avant chaque commit :
  ```bash
  cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test --all-targets
  ```
- Tests : 196 tests (unitaires + intégration). `httpmock` pour les requêtes HTTP, fixtures locales pour le HTML/RSS.

## Performances

### Benchmarks mesurés (Criterion, MacBook Pro, PostgreSQL Docker)

| Opération | Temps | Notes |
|---|---|---|
| Batch insert 30 articles (UNNEST) | 587 µs | 6.5× plus rapide que l'insert individuel |
| Dédup Jaccard (100 articles) | 290 µs | Scaling linéaire, index pg\_trgm en base |
| Extraction texte (regex) | ~4.5 ms | Regex compilées via LazyLock |
| rs-trafilatura | ~44 ms/page | Moyenne constatée |
| Ingestion 3 articles (mock RSS) | ~22 ms | Sans latence réseau |

### Optimisations v1.1

- **mimalloc** (Linux) : allocateur mémoire optimisé, +5-15% throughput
- **HTTP/2 + connection pooling** : réutilisation des connexions reqwest
- **Index pg\_trgm** GIN sur `title_clean` : dédup <10ms à 1M+ lignes
- **Batch inserts UNNEST** : 6.5× plus rapide

## Déploiement

```bash
docker compose up -d          # Local
docker build -t ingest-pipeline .  # Build image
```

En production : une seule instance du scheduler (pas de multi-instance sans coordination). Les workers peuvent être multipliés (SKIP LOCKED garantit l'absence de conflits).

---

**Version** : v1.1.0 | **Dernière mise à jour** : 2026-05-29 | **Tests** : 196

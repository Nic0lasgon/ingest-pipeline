# MyPod Ingestion Pipeline — Rust Rewrite

Pipeline d'ingestion RSS pour MyPod, réécrit en Rust. Remplace la stack Cloudflare Workers/D1/Queues par une architecture **Axum + PostgreSQL + Job Queue maison**.

## Vue d'ensemble

Ce projet ingère des flux RSS/Atom/JSON Feed, extrait le contenu HTML des articles, détecte les doublons, et les qualifie pour l'étape éditoriale (hors scope v1).

**Stack technique** :
- **Runtime** : Rust + Tokio
- **Web** : Axum
- **Database** : PostgreSQL + sqlx
- **Queue** : PostgreSQL (`SELECT FOR UPDATE SKIP LOCKED` + `LISTEN/NOTIFY`)
- **Déploiement** : Docker Compose (local) → VPS/Railway/Fly.io (prod)

## Architecture

### Modes de lancement

```bash
cargo run -- api        # Serveur HTTP Axum uniquement
cargo run -- worker     # Workers de jobs uniquement
cargo run -- scheduler  # Scheduler des tâches récurrentes uniquement
cargo run -- all        # API + scheduler + worker ensemble (dev local)
```

### Flux de données

```
Scheduler (cron 4h)
  └─> Crée un pipeline_run
      └─> Crée des jobs fetch_feed (un par source RSS)
          └─> Worker ingère le flux RSS
              └─> Crée des jobs process_article
                  └─> Worker extrait le contenu HTML
                      └─> Article qualifié (pending_qualification)
```

### Structure du projet

```
ingest-pipeline/
├── src/
│   ├── main.rs              # Entry point + CLI (clap) + mode dispatch
│   ├── config.rs            # Configuration par variables d'environnement
│   ├── lib.rs               # Exports pour les tests
│   ├── db/                  # Couche d'accès aux données
│   │   ├── schema.rs        # Types Rust ↔ PostgreSQL (structs + enums)
│   │   ├── feed_queries.rs  # Queries pour feed_sources
│   │   ├── article_queries.rs # Queries pour raw_articles
│   │   ├── run_queries.rs   # Queries pour pipeline_runs / step_runs
│   │   └── rejected_queries.rs # Queries pour rejected_articles
│   ├── queue/               # Job Queue PostgreSQL
│   │   ├── jobs.rs          # CRUD jobs + SKIP LOCKED
│   │   └── worker.rs        # Boucle worker (poll + dispatch + retry)
│   ├── pipeline/            # Logique métier du pipeline
│   │   ├── ingest_step.rs   # Fetch RSS → parse → insert articles
│   │   └── content_step.rs  # Extract HTML → validate → qualify
│   ├── utils/               # Utilitaires purs (pas de DB)
│   │   ├── rss_parser.rs    # Parser RSS/Atom/JSON sans dépendances externes
│   │   ├── text_extract.rs  # Extraction texte HTML sans DOM
│   │   ├── dedup.rs         # Déduplication par URL + Jaccard 80%
│   │   ├── url_resolver.rs  # Résolution des URLs raccourcies
│   │   ├── word_count.rs    # Compteur de mots
│   │   └── shared.rs        # Décodage HTML entities + parse JSON safe
│   ├── workers/             # Handlers de jobs
│   │   ├── ingest_worker.rs # Handler fetch_feed
│   │   ├── content_worker.rs # Handler process_article
│   │   └── scheduler.rs     # Cron scheduler (4h)
│   └── api/                 # HTTP API
│       └── mod.rs           # Routes Axum (/health)
├── migrations/              # Migrations sqlx
│   ├── 0001_initial_schema.sql
│   ├── 0002_jobs_table.sql
│   └── 0003_indexes.sql
├── tests/                   # Tests d'intégration + fixtures
│   ├── fixtures/
│   │   ├── rss/            # Flux RSS de test
│   │   └── html/           # Pages HTML de test
│   └── *_tests.rs          # Tests par module
├── Cargo.toml
├── docker-compose.yml
├── Dockerfile
└── .env.example
```

## Démarrage rapide

### Prérequis

- Rust 1.88+ (pour compiler)
- PostgreSQL 16+ (ou Docker)
- Docker & Docker Compose (optionnel, recommandé)

### Avec Docker Compose (recommandé)

```bash
# 1. Copier la config
cp .env.example .env

# 2. Lancer le stack complet
docker compose up -d

# 3. Vérifier le health check
curl http://localhost:3000/health
# → {"status":"ok"}

# 4. Voir les logs
docker compose logs -f backend

# 5. Arrêter
docker compose down -v
```

### Sans Docker (dev local)

```bash
# 1. Créer la base de données PostgreSQL
createdb mypod_pipeline

# 2. Configurer les variables d'environnement
export DATABASE_URL="postgres://user:password@localhost:5432/mypod_pipeline"
export PORT=3000
export LOG_LEVEL=info

# 3. Lancer les migrations
cargo sqlx migrate run

# 4. Lancer le pipeline complet
cargo run -- all
```

### Lancer les tests

```bash
# Tests rapides (sans DB)
cargo test --lib

# Tous les tests (nécessite une DB PostgreSQL)
export DATABASE_URL="postgres://..."
cargo test --all-targets --all-features

# Tests d'intégration avec vrais flux RSS (nécessite internet + DB)
cargo test -- --ignored

# Quality gates (à passer avant chaque commit)
cargo fmt --check && cargo clippy --all-targets --all-features -- -D warnings && cargo test --all-targets --all-features
```

## Composants clés

### 1. Job Queue (`src/queue/`)

**Table `jobs`** :
- `id`, `job_type`, `payload` (JSONB), `status` (pending|running|completed|failed|dead)
- `priority`, `attempts`, `max_attempts`, `run_at`, `locked_at`, `locked_by`

**Mécanisme** :
1. `create_job()` insère un job + envoie `NOTIFY jobs_channel`
2. `pick_jobs()` utilise `SELECT FOR UPDATE SKIP LOCKED` pour éviter les conflits entre workers
3. `Worker::run()` écoute `LISTEN jobs_channel` (ou poll toutes les secondes en fallback)
4. `fail_job()` applique un backoff exponentiel (`2^attempts * 1s`), marque `dead` après 3 échecs

**⚠️ Point d'attention** : Les workers concurrents utilisent `SKIP LOCKED` pour ne jamais verrouiller la table entière. Ne jamais remplacer par un simple `UPDATE ... WHERE status='pending'`.

### 2. RSS Parser (`src/utils/rss_parser.rs`)

**Sans dépendance externe** : parsing XML par string/regex.

Supporte :
- RSS 2.0 (`<item>`)
- Atom (`<entry>`)
- JSON Feed (`.items`)

Fonctionnalités :
- CDATA unwrap
- HTML entities decode
- URLs relatives normalisées avec `base_url`
- Strip HTML des titles/descriptions
- Extraction des images (media:content, enclosure, thumbnail, `<img>` dans description)
- Parsing des dates (RFC 822, ISO 8601)

**⚠️ Point d'attention** : Le parser est volontairement léger (pas de crate `rss` ou `xml-rs`). Si tu ajoutes un format complexe, vérifie que les fixtures de test couvrent les edge cases (CDATA, namespaces, encodage).

### 3. Extraction HTML (`src/utils/text_extract.rs`)

**Sans DOM** : extraction par regex.

Algorithme :
1. Extrait metadata (`canonical_url`, `title` via og:title ou `<title>`)
2. Extrait contenu principal : `<article>` → `<main>` → `<body>`
3. Supprime 24+ éléments non-content (script, nav, footer, pub, etc.)
4. Remplace les block tags par `\n`, les inline tags par espace
5. Collapse whitespace
6. Nettoie le titre (split sur `" - "`, `" | "`, `" — "`, etc.)

**⚠️ Point d'attention** : Cet extracteur est optimisé pour les articles de presse. Si tu changes la liste des tags supprimés, teste avec les fixtures `article_noisy.html` et `article_clean.html`.

### 4. Déduplication (`src/utils/dedup.rs`)

**Deux niveaux** :
1. **URL exacte** : normalise l'URL (protocol+host+path, lowercase, sans query/fragment) et compare
2. **Jaccard titre** : si pas de match URL, compare les titres (stop words FR/EN filtrés, threshold 80%)

**⚠️ Point d'attention** :
- Le pré-dédup (ingest step) ne compare que les URLs (pas encore de `title_clean`)
- Le dédup final (content step) utilise `title_clean` après extraction HTML
- Ne jamais comparer les titres bruts sans normaliser (stop words + ponctuation)

### 5. URL Resolver (`src/utils/url_resolver.rs`)

Résout les URLs raccourcies (t.co, bit.ly, news.google.com, etc.) vers l'URL source finale.

Stratégies (dans l'ordre) :
1. HEAD request (follow redirects, timeout 5s)
2. GET request (follow redirects, timeout 5s) + parse HTML si pas de redirect
3. Meta refresh (`<meta http-equiv="refresh" content="0; url=...">`)
4. Link canonical (`<link rel="canonical" href="...">`)

**⚠️ Point d'attention** : Le resolver ne fait de requête QUE pour les domains raccourcisseurs (liste codée en dur). Pour les URLs normales, il retourne `None` immédiatement.

### 6. Ingest Step (`src/pipeline/ingest_step.rs`)

Algorithme :
1. Récupère le feed source
2. Fetch le flux RSS (timeout 15s, user-agent custom)
3. Parse les items
4. Trie par date décroissante, garde les 30 plus récents
5. Pour chaque item : résout l'URL, vérifie doublon par URL, vérifie cutoff date, insère

**⚠️ Point d'attention critique** : Le curseur `last_ingested_pub_date` N'EST PAS avancé dans le step. C'est le **Ingest Worker** qui l'avance UNIQUEMENT après avoir créé tous les jobs `process_article` avec succès. Cela garantit l'idempotence.

### 7. Content Step (`src/pipeline/content_step.rs`)

Algorithme :
1. Guard : vérifie que l'article est en statut `ingested` ou `extracted`
2. Pré-dédup par URL avec les 500 derniers articles comparables
3. Extraction texte : essaie 5 stratégies HTTP + fallback Hetzner
4. Validation : min 300 caractères, min 350 mots
5. Dédup finale par titre (Jaccard)
6. Met à jour l'article : `quality_status='qualified'`, `processing_status='pending_qualification'`

**Stratégies HTTP** (dans l'ordre) :
1. User-Agent Googlebot
2. Referer Google
3. Referer Twitter (t.co)
4. Version AMP (`?amp=1`)
5. Referer Facebook

**⚠️ Point d'attention** : Si l'article a `preferred_extraction_method='hetzner_fallback'`, Hetzner est essayé en PREMIER (avant les 5 stratégies).

### 8. Scheduler (`src/workers/scheduler.rs`)

Cron : `0 2,6,14,20 * * *` (tous les jours à 2h, 6h, 14h, 20h)

Override via `RUN_SCHEDULE` (ex: `*/1 * * * *` pour tester)

Algorithme `handle_scheduled_run` :
1. Marque les runs zombies (> 2h) comme failed
2. Nettoie les articles orphelins
3. Guard : skip si un run est déjà en cours
4. Crée un `pipeline_run`
5. Récupère les feeds actifs (tier1_keep en priorité)
6. Crée un `pipeline_step_run` pour 'ingest'
7. Crée des jobs `fetch_feed` (un par feed)
8. Nettoie les doublons anciens (> 7 jours)

**⚠️ Point d'attention** : Le guard empêche les runs en double. Si le scheduler crash et redémarre, il ne créera pas de run concurrent grâce à la vérification `SELECT COUNT(*) FROM pipeline_runs WHERE status='running'`.

## Types DB ↔ Rust

### Enums PostgreSQL

| PostgreSQL | Rust | Valeurs |
|-----------|------|---------|
| `job_status` | `JobStatus` | pending, running, completed, failed, dead |

### Enums TEXT (autres)

Toutes les autres enums sont stockées en `TEXT` dans PostgreSQL et mappées via `impl_text_enum!` :

| PostgreSQL | Rust | Valeurs |
|-----------|------|---------|
| `feed_fetch_status` | `FeedFetchStatus` | Pending, Fetching, Success, Failed, Disabled |
| `quality_status` | `QualityStatus` | Pending, Qualified, Rejected, PendingQualification |
| `duplicate_status` | `DuplicateStatus` | Pending, Distinct, Duplicate, NearDuplicate |
| `processing_status` | `ProcessingStatus` | Ingested, Extracted, ExtractionFailed, PendingQualification, Qualified, Rejected |
| `run_status` | `RunStatus` | Running, Completed, Failed |
| `run_trigger_type` | `RunTriggerType` | Scheduled, Manual, Test |
| `step_name` | `StepName` | Ingest, Content, Qualification, Audio |
| `step_status` | `StepStatus` | Running, Completed, Failed |

**⚠️ Point d'attention** : `JobStatus` utilise `#[derive(sqlx::Type)]` (vrai ENUM PostgreSQL). Les autres utilisent `impl_text_enum!` qui fait le mapping TEXT ↔ Rust via serde. **Ne mélange jamais les deux approches pour le même enum.**

## Points d'attention pour les développeurs

### Idempotence

Le pipeline est conçu pour être idempotent :
- Ré-exécuter un `fetch_feed` ne crée pas de doublons (vérifie `get_by_url` avant insert)
- Ré-exécuter un `process_article` sur un article déjà qualifié est un no-op (guard dans content_step)
- Le scheduler a un guard contre les runs en double

**Régression à éviter** : Ne jamais supprimer le check `get_by_url()` avant l'insertion dans l'ingest step.

### Gestion des erreurs

- `anyhow::Result` pour le code applicatif (erreurs non structurées)
- `thiserror` pour les erreurs métier (si besoin dans le futur)
- `anyhow::Context` sur chaque requête SQL pour des messages explicites

**Régression à éviter** : Ne pas utiliser `.unwrap()` ou `.expect()` dans le code de production (hors `main.rs`). Toujours propager les erreurs avec `?`.

### Transactions

Pour l'instant, chaque opération est autonome (pas de transactions explicites multi-requêtes). Si tu ajoutes une logique transactionnelle complexe :

1. Utilise `sqlx::Transaction` ou `pool.begin()`
2. Gère le rollback en cas d'erreur
3. Attention aux locks `FOR UPDATE` qui bloquent les autres workers

### Performance

- Le worker poll toutes les secondes (configurable via `with_poll_interval`)
- `LISTEN/NOTIFY` réveille le worker immédiatement quand un job est créé
- Le batch size par défaut est 10 jobs
- L'extraction HTML utilise des regex compilées une seule fois via `LazyLock`

**Régression à éviter** : Ne pas diminuer le poll interval en dessous de 500ms (charge inutile sur PostgreSQL).

### Tests

- **Tests unitaires** : dans `src/xxx.rs` sous `#[cfg(test)]`, utilisent des mocks et des fixtures
- **Tests d'intégration** : dans `tests/xxx_tests.rs`, utilisent `#[sqlx::test]` avec une DB PostgreSQL
- **Tests E2E** : dans `tests/integration_tests.rs`, marqués `#[ignore]`, utilisent de vrais flux RSS

**Régression à éviter** : Ne jamais faire de requêtes HTTP réelles dans les tests non-ignorés. Utiliser `httpmock` ou des fixtures locales.

## Guide de contribution (pour les agents)

### Avant de commencer

1. Lire le PRD : `Documentation/INGEST_PIPELINE_PRD.md`
2. Lancer les quality gates :
   ```bash
   cargo fmt --check && cargo clippy --all-targets --all-features -- -D warnings && cargo test --all-targets --all-features
   ```
3. Vérifier que `cargo sqlx prepare --check` passe (si sqlx offline mode activé)

### Ajouter une user story

1. Créer une branche : `git checkout -b feature/US-XXX`
2. Implémenter les changements
3. Ajouter des tests (coverage > 80% sur les utilitaires)
4. Vérifier les quality gates
5. Commiter avec message conventionnel :
   ```
   feat: US-XXX description
   
   - Changement 1
   - Changement 2
   ```

### Modifier un utilitaire

Les utilitaires dans `src/utils/` sont **purs** (pas de DB, pas d'IO). Si tu modifies :
- `rss_parser.rs` → ajoute des fixtures dans `tests/fixtures/rss/`
- `text_extract.rs` → ajoute des fixtures dans `tests/fixtures/html/`
- `dedup.rs` → ajoute des cas de test dans `tests/dedup_tests.rs`

### Modifier une query DB

1. Vérifier que la struct dans `schema.rs` correspond à la requête
2. Utiliser `sqlx::query_as!` ou `sqlx::query!` (pas de `query` non typé)
3. Ajouter un test avec `#[sqlx::test]`
4. Si tu changes le schéma SQL, créer une nouvelle migration dans `migrations/`

### Ajouter un mode de lancement

1. Ajouter la variante dans `Mode` (src/main.rs)
2. Implémenter la logique dans `match cli.mode`
3. Mettre à jour ce README
4. Vérifier que `cargo run -- <mode>` compile

## Variables d'environnement

| Variable | Obligatoire | Défaut | Description |
|----------|-------------|--------|-------------|
| `DATABASE_URL` | Oui | — | URL PostgreSQL |
| `PORT` | Non | 3000 | Port API HTTP |
| `LOG_LEVEL` | Non | info | Niveau de log (trace/debug/info/warn/error) |
| `HETZNER_EXTRACT_URL` | Non | — | URL du service d'extraction Hetzner |
| `HETZNER_EXTRACT_SECRET` | Non | — | Clé API Hetzner |
| `PIPELINE_API_SECRET` | Non | — | Secret pour l'API interne |
| `BFF_CRON_SECRET` | Non | — | Secret pour les endpoints cron |
| `RUN_SCHEDULE` | Non | `0 2,6,14,20 * * *` | Expression cron du scheduler |

## Déploiement

### Local (Docker Compose)

```bash
docker compose up -d
```

### Production (ex: Railway, Fly.io, VPS)

1. Builder l'image Docker :
   ```bash
   docker build -t ingest-pipeline .
   ```

2. Pousser sur le registry du provider

3. Configurer les variables d'environnement (voir ci-dessus)

4. Lancer avec `cargo run -- all` (ou séparer les services)

**⚠️ Point d'attention production** :
- Toujours utiliser PostgreSQL en production (pas de SQLite)
- Configurer des health checks (endpoint `/health`)
- Surveiller les jobs `dead` (indiquent des échecs répétés)
- Le scheduler doit tourner sur une seule instance (pas de multi-instance sans coordination)

## Dépannage

### `cargo check` échoue avec des erreurs sqlx

```bash
# Si la DB n'est pas accessible, générer le cache offline
cargo sqlx prepare

# Ou vérifier que DATABASE_URL est correct
export DATABASE_URL="postgres://user:password@host:5432/db"
```

### Les tests sqlx échouent

```bash
# Vérifier que PostgreSQL est accessible
psql $DATABASE_URL -c "SELECT 1"

# Vérifier que les migrations sont appliquées
cargo sqlx migrate run
```

### Le worker ne traite pas les jobs

1. Vérifier que le worker est lancé : `cargo run -- worker`
2. Vérifier les logs : les jobs sont-ils en statut `pending` ?
3. Vérifier la connexion PostgreSQL : `LISTEN/NOTIFY` fonctionne-t-il ?
4. Vérifier qu'il n'y a pas de deadlock : `SELECT * FROM pg_locks WHERE NOT granted;`

### Docker Compose ne démarre pas

1. Vérifier que le port 5432 n'est pas déjà utilisé
2. Vérifier les logs PostgreSQL : `docker compose logs postgres`
3. Vérifier que le backend attend bien PostgreSQL : `depends_on` + `condition: service_healthy`

## Ressources

- **PRD** : `Documentation/INGEST_PIPELINE_PRD.md` (spécifications complètes)
- **Spec originale** : `Documentation/INGEST_PIPELINE_SPEC.md` (logique TypeScript originale)
- **Repository** : https://github.com/Nic0lasgon/ingest-pipeline

## Changelog

### v1.0.0 (initial)
- Pipeline complet RSS → qualification
- Job queue PostgreSQL avec retries
- 196 tests (95 unitaires + 101 intégration)
- Docker Compose fonctionnel
- Testé avec Le Monde RSS en local

---

**Dernière mise à jour** : 2026-05-29
**Auteur** : Équipe d'agents IA (deepseek-v4-pro, mimo-v25-pro, kimi-2.6, glm-5.1, mimo-v25, deepseek-v4-flash)

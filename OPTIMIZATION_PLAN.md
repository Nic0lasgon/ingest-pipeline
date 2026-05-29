# Plan d'Optimisation — Pipeline d'Ingestion v1.1

> **Date** : 29 mai 2026
> **Objectif** : 4 optimisations rapides (~40% gain de perf) pour ~2h de travail
> **Statut** : Prêt pour implémentation

---

## Vue d'ensemble

| Optimisation | Effort | Impact estimé | Fichiers touchés |
|-------------|--------|---------------|------------------|
| 1. mimalloc allocator | 5 min | +5-15% throughput | `Cargo.toml`, `src/main.rs` |
| 2. HTTP/2 + pooling reqwest | 30 min | -20-40% latence HTTP | `src/pipeline/content_step.rs`, `src/pipeline/ingest_step.rs` |
| 3. Index pg_trgm (dédup) | 15 min | <10ms dédup même à 1M rows | `migrations/0004_pg_trgm_index.sql` |
| 4. Batch inserts articles | 45 min | +10-50x ingestion | `src/pipeline/ingest_step.rs`, `src/db/article_queries.rs` |

**Gain cumulé attendu** : ~40% de latence en moins, +200% de débit

---

## 1. mimalloc — Allocator Global

### Problème
L'allocateur système par défaut de Rust n'est pas optimisé pour les workloads intensifs en allocations (parsing XML, strings, etc.)

### Solution
Remplacer par `mimalloc` en une ligne.

### Implémentation

**Fichier : `Cargo.toml`**
```toml
[target.'cfg(target_os = "linux")'.dependencies]
mimalloc = "0.1"
```

**Fichier : `src/main.rs`** (avant `fn main()`)
```rust
#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
```

### Vérification
- [ ] `cargo check` passe
- [ ] `cargo test` passe
- [ ] Pas de régression mémoire (vérifier avec `valgrind` ou `dhat` si dispo)

### Risques
- **Très faible** : mimalloc est mature et utilisé en production par Microsoft, Blender, etc.
- Sur macOS, l'allocateur système est déjà performant → pas besoin de mimalloc (d'où le `cfg(target_os = "linux")`)

---

## 2. HTTP/2 + Connection Pooling (reqwest)

### Problème
Le code crée un nouveau `reqwest::Client` à chaque requête HTTP (dans `fetch_with_strategy`, `process_ingest_step`, `resolve_source_url`). Chaque création = nouveau handshake TLS + nouvelle connexion TCP. Sur 5 stratégies HTTP, on fait 5 handshakes TLS pour le même article.

### Solution
Créer un `reqwest::Client` global réutilisé (pool de connexions) avec HTTP/2 activé.

### Implémentation

**Fichier : `src/config.rs`** (ajout d'un client HTTP partagé)
```rust
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;

pub struct HttpClient {
    pub client: Arc<Client>,
}

impl HttpClient {
    pub fn new() -> Self {
        let client = Client::builder()
            .pool_idle_timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(10)
            .http2_adaptive_window(true)
            .http2_keep_alive_interval(Duration::from_secs(30))
            .tcp_nodelay(true)
            .tcp_keepalive(Duration::from_secs(60))
            .gzip(true)
            .brotli(true)
            .build()
            .expect("Failed to build HTTP client");
        
        Self {
            client: Arc::new(client),
        }
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}
```

**Fichier : `src/config.rs`** (ajouter à la struct Config)
```rust
pub struct Config {
    // ... champs existants ...
    pub http_client: HttpClient,
}
```

**Fichier : `src/pipeline/content_step.rs`**
Remplacer :
```rust
let client = reqwest::Client::builder()
    .timeout(Duration::from_millis(CONTENT_FETCH_TIMEOUT_MS))
    .build()?;
```
Par :
```rust
let client = config.http_client.client.clone();
// ou passer le client en paramètre
```

**Fichier : `src/pipeline/ingest_step.rs`**
Remplacer la création du client par un client passé en paramètre ou stocké dans Config.

**Fichier : `src/utils/url_resolver.rs`**
Idem : utiliser le client partagé au lieu d'en créer un nouveau.

### Vérification
- [ ] `cargo check` passe
- [ ] `cargo test` passe
- [ ] Les tests avec `httpmock` fonctionnent toujours

### Risques
- **Faible** : reqwest gère automatiquement le pooling. Le risque principal est un changement de signature de fonction (passer le client en paramètre).
- Les tests avec `httpmock` utilisent leur propre mock server, donc pas impactés par le pooling.

---

## 3. Index pg_trgm (Déduplication)

### Problème
La dédup actuelle (`check_duplicate`) charge 500 articles en mémoire et compare un par un. Complexité O(n) en mémoire. À 10k articles, ça devient lent.

### Solution
Créer un index GIN trigram sur `raw_articles.title_clean` pour faire la recherche de similarité directement en PostgreSQL.

### Implémentation

**Fichier : `migrations/0004_pg_trgm_index.sql`**
```sql
-- Extension trigram (idempotente)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Index GIN pour recherche de similarité sur title_clean
CREATE INDEX IF NOT EXISTS idx_raw_articles_title_trgm 
ON raw_articles USING gin(title_clean gin_trgm_ops);

-- Index GIN sur url (pour les recherches exactes rapides)
CREATE INDEX IF NOT EXISTS idx_raw_articles_url_trgm 
ON raw_articles USING gin(url gin_trgm_ops);
```

**Fichier : `src/db/article_queries.rs`** (nouvelle fonction)
```rust
/// Recherche d'articles similaires par trigram similarity
/// Retourne les articles avec un score de similarité > 0.6
pub async fn find_similar_articles(
    pool: &PgPool,
    title_clean: &str,
    exclude_id: Uuid,
    limit: i64,
) -> Result<Vec<ComparableArticle>> {
    sqlx::query_as!(
        ComparableArticle,
        r#"
        SELECT id, url, canonical_url, title, title_clean
        FROM raw_articles
        WHERE id != $1
          AND title_clean % $2
          AND processing_status IN ('ingested', 'extracted', 'pending_qualification', 'qualified')
        ORDER BY similarity(title_clean, $2) DESC
        LIMIT $3
        "#,
        exclude_id,
        title_clean,
        limit
    )
    .fetch_all(pool)
    .await
    .context("Failed to find similar articles")
}
```

**Fichier : `src/pipeline/content_step.rs`** (utilisation)
Dans `process_content_step`, remplacer :
```rust
let comparable = get_comparable_articles(pool, article_id, 500).await?;
```
Par :
```rust
let comparable = if let Some(ref title_clean) = extraction.title_clean {
    find_similar_articles(pool, title_clean, article_id, 10).await?
} else {
    get_comparable_articles(pool, article_id, 500).await?
};
```

### Vérification
- [ ] Migration s'applique sans erreur
- [ ] `cargo test` passe
- [ ] Test de performance : insérer 1000 articles, mesurer le temps de dédup

### Risques
- **Faible** : pg_trgm est une extension standard de PostgreSQL.
- L'opérateur `%` retourne les lignes avec similarity > 0.3 par défaut. On peut ajuster via `set_limit(0.6)` si besoin.
- **Rollback** : Si la requête trigram est trop lente, on garde le fallback `get_comparable_articles`.

---

## 4. Batch Inserts Articles

### Problème
L'ingest step insère les articles un par un (boucle `for item in items`). 30 articles = 30 requêtes INSERT. C'est le bottleneck principal de l'ingestion.

### Solution
Utiliser `INSERT ... VALUES (), (), ()` ou `UNNEST()` pour insérer tous les articles en une seule requête.

### Implémentation

**Fichier : `src/db/article_queries.rs`** (nouvelle fonction)
```rust
/// Insert multiple articles in a single query (batch insert)
pub async fn insert_batch(
    pool: &PgPool,
    articles: &[RawArticle],
) -> Result<usize> {
    if articles.is_empty() {
        return Ok(0);
    }
    
    // Build the VALUES clause
    let mut query = String::from(
        "INSERT INTO raw_articles (
            id, source_id, title, url, description, image_url, 
            author, pub_date, processing_status, quality_status, 
            duplicate_status, created_at, updated_at
        ) VALUES "
    );
    
    let mut args = Vec::new();
    let now = chrono::Utc::now();
    
    for (i, article) in articles.iter().enumerate() {
        if i > 0 {
            query.push_str(", ");
        }
        query.push_str(&format!(
            "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
            i * 13 + 1, i * 13 + 2, i * 13 + 3, i * 13 + 4,
            i * 13 + 5, i * 13 + 6, i * 13 + 7, i * 13 + 8,
            i * 13 + 9, i * 13 + 10, i * 13 + 11, i * 13 + 12, i * 13 + 13
        ));
        
        args.push(article.id as _);
        args.push(article.source_id.clone() as _);
        args.push(article.title.clone() as _);
        args.push(article.url.clone() as _);
        args.push(article.description.clone() as _);
        args.push(article.image_url.clone() as _);
        args.push(article.author.clone() as _);
        args.push(article.pub_date as _);
        args.push(article.processing_status.clone() as _);
        args.push(article.quality_status.clone() as _);
        args.push(article.duplicate_status.clone() as _);
        args.push(now as _);
        args.push(now as _);
    }
    
    query.push_str(" ON CONFLICT (url, source_id) DO NOTHING");
    
    let rows_affected = sqlx::query(&query)
        .execute(pool)
        .await
        .context("Failed to batch insert articles")?
        .rows_affected();
    
    Ok(rows_affected as usize)
}
```

**Alternative avec UNNEST** (plus propre, mais nécessite que les enums implémentent `PgTypeInfo`) :
```rust
pub async fn insert_batch_unnest(
    pool: &PgPool,
    articles: &[RawArticle],
) -> Result<usize> {
    if articles.is_empty() {
        return Ok(0);
    }
    
    let ids: Vec<Uuid> = articles.iter().map(|a| a.id).collect();
    let source_ids: Vec<String> = articles.iter().map(|a| a.source_id.clone()).collect();
    let titles: Vec<String> = articles.iter().map(|a| a.title.clone()).collect();
    let urls: Vec<String> = articles.iter().map(|a| a.url.clone()).collect();
    let descriptions: Vec<Option<String>> = articles.iter().map(|a| a.description.clone()).collect();
    let image_urls: Vec<Option<String>> = articles.iter().map(|a| a.image_url.clone()).collect();
    let authors: Vec<Option<String>> = articles.iter().map(|a| a.author.clone()).collect();
    let pub_dates: Vec<Option<chrono::DateTime<chrono::Utc>>> = articles.iter().map(|a| a.pub_date).collect();
    let processing_statuses: Vec<String> = articles.iter().map(|a| format!("{:?}", a.processing_status)).collect();
    // ... etc
    
    let rows_affected = sqlx::query(
        "INSERT INTO raw_articles (id, source_id, title, url, description, image_url, author, pub_date, processing_status, quality_status, duplicate_status, created_at, updated_at)
        SELECT * FROM UNNEST($1, $2, $3, $4, $5, $6, $7, $8, $9::text[], $10::text[], $11::text[], $12, $13)
        ON CONFLICT (url, source_id) DO NOTHING"
    )
    .bind(&ids)
    .bind(&source_ids)
    .bind(&titles)
    .bind(&urls)
    .bind(&descriptions)
    .bind(&image_urls)
    .bind(&authors)
    .bind(&pub_dates)
    .bind(&processing_statuses)
    // ... etc
    .execute(pool)
    .await?
    .rows_affected();
    
    Ok(rows_affected as usize)
}
```

**Fichier : `src/pipeline/ingest_step.rs`** (utilisation)
Dans `process_ingest_step`, remplacer la boucle d'insertion individuelle par :
```rust
// Collecter tous les articles à insérer
let articles_to_insert: Vec<RawArticle> = new_items
    .into_iter()
    .map(|item| RawArticle {
        id: Uuid::new_v4(),
        source_id: feed_id.to_string(),
        title: item.title.unwrap_or_default(),
        url: item.link.unwrap_or_default(),
        description: item.description,
        image_url: item.image_url,
        author: item.author,
        pub_date: item.pub_date,
        processing_status: ProcessingStatus::Ingested,
        quality_status: QualityStatus::Pending,
        duplicate_status: DuplicateStatus::Pending,
        // ... autres champs par défaut
        ..Default::default() // si RawArticle implémente Default
    })
    .collect();

// Batch insert
let inserted_count = insert_batch(pool, &articles_to_insert).await?;
let new_article_ids: Vec<Uuid> = articles_to_insert.iter().map(|a| a.id).collect();
```

### Vérification
- [ ] `cargo check` passe
- [ ] `cargo test` passe
- [ ] Test `test_ingest_step_success` vérifie que les articles sont toujours insérés
- [ ] Test d'idempotence : ré-exécuter → 0 insertions (ON CONFLICT DO NOTHING)

### Risques
- **Moyen** : La requête batch est plus complexe. Il faut bien gérer le mapping des types (surtout les enums).
- **Fallback** : Si le batch échoue, on peut fallback sur l'insert individuel (garder les deux fonctions).
- **Taille limite** : PostgreSQL a une limite de 32767 paramètres par requête. À 13 colonnes, on peut insérer max ~2500 articles par batch (largement suffisant pour 30 articles RSS).

---

## Ordre d'implémentation recommandé

```
Étape 1 (5 min)  : mimalloc
Étape 2 (15 min) : Index pg_trgm (migration SQL)
Étape 3 (30 min) : HTTP/2 + pooling reqwest
Étape 4 (45 min) : Batch inserts
```

**Total** : ~1h35 (moins que les 2h estimées si tout se passe bien)

---

## Tests de validation

Après chaque optimisation, lancer :
```bash
cargo fmt --check && cargo clippy --all-targets --all-features -- -D warnings && cargo test --all-targets --all-features
```

**Test de performance** (optionnel mais recommandé) :
```bash
# Build release
cargo build --release

# Lancer le pipeline avec un vrai flux (Le Monde)
cargo run -- all

# Mesurer le temps d'ingestion de 30 articles
time curl -X POST http://localhost:3000/health  # ou endpoint de test
```

---

## Checklist pré-merge

- [ ] mimalloc compilé et actif sur Linux
- [ ] reqwest Client partagé utilisé partout
- [ ] Migration 0004 appliquée et index pg_trgm créé
- [ ] Batch insert fonctionne avec ON CONFLICT DO NOTHING
- [ ] Tous les tests passent (95+ tests)
- [ ] Aucun warning clippy
- [ ] Code formaté (`cargo fmt`)
- [ ] README mis à jour si nécessaire
- [ ] `cargo sqlx prepare` mis à jour (si offline mode)

---

## Annexes

### Références

- **mimalloc** : https://github.com/microsoft/mimalloc
- **reqwest HTTP/2** : https://docs.rs/reqwest/latest/reqwest/struct.ClientBuilder.html
- **pg_trgm** : https://www.postgresql.org/docs/current/pgtrgm.html
- **sqlx batch insert** : https://docs.rs/sqlx/latest/sqlx/struct.QueryBuilder.html

### Notes

- Ces optimisations sont **retro-compatibles** : aucun changement de schéma breaking
- Le pipeline reste fonctionnel même si une optimisation est désactivée
- Les gains sont cumulatifs mais pas multiplicatifs (ex: mimalloc + HTTP/2 = ~+30% au lieu de +55%)

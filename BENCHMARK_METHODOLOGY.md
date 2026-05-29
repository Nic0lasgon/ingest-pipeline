# Méthodologie de Benchmark — Pipeline d'Ingestion v1.1

> **Version** : 1.0.0  
> **Date** : 29 mai 2026  
> **Objectif** : Mesurer rigoureusement les gains de performance des optimisations v1.1 (mimalloc, HTTP/2 pooling, pg_trgm, batch inserts)  
> **Statut** : Protocole expérimental

---

## 1. Principes fondamentaux

### 1.1 Approche scientifique

Toute mesure de performance suit le protocole **ICOR** :
- **I**solée : une seule variable modifiée par comparaison
- **C**ontrôlée : environnement identique entre les runs
- **O**pérationnalisée : métriques quantifiables et reproductibles
- **R**épétée : suffisamment d'itérations pour la significativité statistique

### 1.2 Niveaux de benchmark

| Niveau | Portée | Outil | Objectif |
|--------|--------|-------|----------|
| **Micro** | Fonction isolée | `criterion` / `iai-callgrind` | Mesurer l'impact d'une optimisation pure (ex: parsing RSS, déduplication) |
| **Meso** | Module / query DB | Tests intégration instrumentés | Mesurer l'impact d'un composant (ex: batch insert vs insert individuel) |
| **Macro** | Pipeline complet | Scénarios E2E avec fixtures | Mesurer le gain global utilisateur (ex: temps d'ingestion d'un flux complet) |
| **Système** | Infrastructure | `perf`, `vmstat`, `iostat` | Mesurer l'empreinte ressource (CPU, mémoire, I/O disque) |

**Règle d'or** : On ne valide un gain macro que si les gains micro/meso sont significatifs. Un benchmark macro seul est insuffisant car il masque les effets de réseau.

---

## 2. Gestion de la variance et du bruit

### 2.1 Sources de variance identifiées

| Source | Impact estimé | Stratégie de mitigation |
|--------|---------------|------------------------|
| Latence réseau (DNS, TLS, TCP) | ±200-500ms par requête | Mock HTTP (`httpmock`) pour les tests meso/micro ; répétition statistique pour les tests macro |
| Charge PostgreSQL (shared_buffers, WAL) | ±10-30% sur les requêtes | DB dédiée par run, `VACUUM FULL` entre chaque série, restart du conteneur PG |
| JIT compiler / cache CPU | ±5-15% sur les 3 premiers runs | **Warm-up obligatoire** : 5 runs jetés avant toute mesure |
| GC / allocateur (mimalloc vs système) | ±2-5% selon la fragmentation | Mesure sur des séries de 1000+ allocations, pas sur des runs isolés |
| Charge système (OS, autres processus) | ±3-10% | Docker avec `--cpus` et `--memory` fixes, monitoring de la charge système |

### 2.2 Protocole de warm-up

**Pour chaque série de benchmark** :

1. **5 runs de chauffe** (warm-up) — données jetées
2. **Minimum 30 runs de mesure** pour les benchmarks micro/meso
3. **Minimum 10 runs de mesure** pour les benchmarks macro (coût plus élevé)
4. **Pause de 30 secondes** entre chaque run pour laisser l'OS libérer les ressources

**Justification du n=30** : D'après le théorème central limite, n ≥ 30 garantit une distribution approximativement normale des moyennes, permettant l'utilisation de l'intervalle de confiance à 95% avec le t-test de Student.

### 2.3 Filtrage des outliers

**Définition d'un outlier** : Valeur située au-delà de **2 écarts-types (σ)** de la médiane de la série.

**Protocole** :
```
1. Calculer la médiane (Md) et l'écart-type (σ) de la série
2. Identifier les outliers : |valeur - Md| > 2σ
3. Si un outlier est lié à une erreur réseau/timeout → le supprimer et noter l'incident
4. Si un outlier est lié à une anomalie système → redémarrer l'environnement et recommencer la série
5. Après suppression, s'assurer que n ≥ 20 reste valide
```

**Règle** : Jamais plus de 10% d'outliers par série. Au-delà, la série est invalide et doit être refaite.

### 2.4 Stabilité temporelle

**Protocole** :
- Les benchmarks macro (avec vrai réseau) doivent être exécutés à des heures identiques (ex: 2h-4h du matin UTC+1, charge réseau minimale)
- JAMAIS exécuter un benchmark A et un benchmark B à plus de 2h d'écart si le réseau est impliqué
- Pour les benchmarks meso/micro (sans réseau) : peuvent être exécutés à tout moment

---

## 3. Isolation des variables

### 3.1 Matrice d'isolation

Chaque optimisation doit être testée individuellement via un **feature flag** ou une **compilation conditionnelle**.

| Optimisation | Variable isolée | Comment tester sans bruit |
|-------------|----------------|--------------------------|
| **mimalloc** | Allocateur global | Benchmark micro de parsing RSS (allocations intensives) avec/sans `#[global_allocator]` |
| **HTTP/2 pooling** | Réutilisation connexion | Benchmark meso avec `httpmock` + mock de 5 requêtes séquentiels. Comparer temps total avec `Client` neuf vs `Client` partagé |
| **pg_trgm** | Index DB + requête | Benchmark meso avec DB locale pré-remplie. Mesurer `EXPLAIN ANALYZE` de `find_similar_articles` vs `get_comparable_articles` |
| **Batch inserts** | Nombre de requêtes SQL | Benchmark meso avec DB locale. Mesurer temps d'insertion de 30 articles : 30× `insert()` vs 1× `insert_batch()` |

### 3.2 Mécanisme d'isolation technique

**Approche recommandée** : Compiler avec des features flags

```toml
# Cargo.toml
[features]
default = ["mimalloc", "http2_pooling", "pg_trgm", "batch_insert"]
mimalloc = []
http2_pooling = []
pg_trgm = []
batch_insert = []
```

```rust
// src/main.rs
#[cfg(all(target_os = "linux", feature = "mimalloc"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
```

```rust
// src/pipeline/ingest_step.rs
#[cfg(feature = "batch_insert")]
use crate::db::article_queries::insert_batch;

#[cfg(not(feature = "batch_insert"))]
use crate::db::article_queries::insert;

// Dans la boucle d'insertion :
#[cfg(feature = "batch_insert")]
let inserted_count = insert_batch(pool, &articles_to_insert).await?;

#[cfg(not(feature = "batch_insert"))]
for article in &articles_to_insert {
    insert(pool, article).await?;
}
```

### 3.3 Benchmark micro : mimalloc

**Hypothèse** : mimalloc accélère les workloads intensifs en allocations (parsing XML, manipulation de strings).

**Protocole** :
```
Fichier test : tests/fixtures/rss/lemonde_large.xml (simuler un flux de 1000 items)

1. Parser le fichier 30 fois avec l'allocateur système
2. Parser le fichier 30 fois avec mimalloc
3. Mesurer : temps total, temps par item, allocations/sec (via dhat si dispo)
4. Comparaison : t-test sur les moyennes, p-value < 0.05 pour significativité
```

**Code de benchmark** (à ajouter dans `benches/alloc_benchmark.rs`) :
```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ingest_pipeline::utils::rss_parser::parse_feed;

fn bench_parse_with_alloc(c: &mut Criterion) {
    let xml = include_str!("../tests/fixtures/rss/lemonde_large.xml");
    
    c.bench_function("parse_rss_system_alloc", |b| {
        b.iter(|| parse_feed(black_box(xml), "https://test.com/feed"))
    });
}

criterion_group!(benches, bench_parse_with_alloc);
criterion_main!(benches);
```

**Compilation** :
```bash
# Sans mimalloc
cargo bench --no-default-features

# Avec mimalloc
cargo bench --features mimalloc
```

### 3.4 Benchmark meso : batch inserts

**Hypothèse** : `insert_batch` est 10-50× plus rapide que 30 inserts individuels.

**Protocole** :
```
1. Créer une DB PostgreSQL temporaire (Docker)
2. Insérer 1000 articles avec insert() individuel → mesurer le temps
3. Vider la table (TRUNCATE)
4. Insérer 1000 articles avec insert_batch() → mesurer le temps
5. Répéter 30 fois (avec VACUUM FULL entre chaque paire)
6. Calculer le speedup : temps_individuel / temps_batch
```

**Métriques** :
- Temps d'exécution total (ms)
- Nombre de requêtes SQL exécutées (1 vs 1000)
- Temps moyen par article inséré (μs)

### 3.5 Benchmark meso : pg_trgm

**Hypothèse** : `find_similar_articles` avec index GIN est < 10ms même à 1M rows.

**Protocole** :
```
1. Créer une DB avec 1M d'articles (générés aléatoirement)
2. Créer l'index pg_trgm
3. Exécuter 100 requêtes find_similar_articles avec des titres variés
4. Exécuter 100 requêtes get_comparable_articles (fallback sans index)
5. Mesurer : temps moyen, p95, p99
6. Vérifier EXPLAIN ANALYZE que l'index GIN est utilisé
```

**Métriques** :
- Latence p50, p95, p99 (ms)
- Nombre de lignes parcourues (seq scan vs index scan)
- Ratio de faux positifs/négatifs par rapport à la méthode Jaccard

### 3.6 Benchmark meso : HTTP/2 pooling

**Hypothèse** : Le pooling réduit le temps total de 5 requêtes HTTP de 20-40%.

**Protocole** :
```
1. Lancer un mock server (httpmock) avec 5 endpoints
2. Simuler 5 requêtes séquentielles avec un Client neuf à chaque fois
3. Simuler 5 requêtes séquentielles avec un Client partagé (pooling)
4. Répéter 30 fois
5. Mesurer : temps total des 5 requêtes, temps de handshake TLS
```

**Métriques** :
- Temps total des 5 requêtes (ms)
- Temps de connexion (TCP + TLS) par requête
- Nombre de connexions TCP ouvertes (via `netstat` ou logs reqwest)

---

## 4. Métriques et collecte de données

### 4.1 Métriques primaires (KPIs)

Pour un pipeline d'ingestion, la métrique la plus importante est le **throughput** (articles/seconde traités), car elle reflète directement la capacité du système à absorber la charge.

| Métrique | Importance | Outil de collecte | Seuil d'acceptation |
|----------|-----------|-------------------|---------------------|
| **Throughput** (articles/sec) | **CRITIQUE** | Instrumentation code + timer | Amélioration ≥ 10% |
| Latence p50 (ms) | Haute | `Instant::now()` / `criterion` | Stable ou améliorée |
| Latence p95 (ms) | Haute | Histogramme (30 buckets) | Pas de régression > 5% |
| Latence p99 (ms) | Moyenne | Histogramme | Pas de régression > 10% |
| Nombre de requêtes SQL | Moyenne | Compteur dans le code | Réduction ≥ 50% pour batch insert |
| Utilisation CPU (%) | Moyenne | `getrusage` / `perf` | Pas de régression > 15% |
| Utilisation mémoire (MB) | Moyenne | `mimalloc` stats / `valgrind` | Pas de fuite mémoire |
| Temps de connexion HTTP (ms) | Basse (debug) | Logs reqwest / wireshark | Réduction ≥ 20% avec pooling |

### 4.2 Stratégie de collecte

**Instrumentation du code** (à ajouter) :
```rust
use std::time::Instant;
use metrics::{counter, gauge, histogram, describe_counter, describe_histogram};

pub async fn process_ingest_step(...) -> IngestStepResult {
    let start = Instant::now();
    
    // ... code existant ...
    
    let duration = start.elapsed();
    histogram!("ingest_step_duration_ms", duration.as_millis() as f64);
    counter!("ingest_step_articles_total", total_items as u64);
    counter!("ingest_step_inserted_total", inserted_count as u64);
    
    result
}
```

**Note** : L'ajout de `metrics` est optionnel. Pour les benchmarks, un simple `Instant::now()` + écriture dans un fichier CSV suffit.

### 4.3 Format de sortie des données

Chaque run produit une ligne CSV :

```csv
timestamp,commit,feature_flags,run_id,scenario,duration_ms,articles_count,inserted_count,sql_queries,cpu_percent,memory_mb,error
dc3c2ca,default,1,ingest_lemonde,1245,30,28,32,45,128,none
dc3c2ca,default,2,ingest_lemonde,1189,30,28,32,42,128,none
e89f51e,batch_insert+mimalloc,1,ingest_lemonde,892,30,28,3,38,118,none
```

---

## 5. Comparaison A/B entre commits

### 5.1 Stratégie de comparaison

**Option recommandée** : Git worktrees (pas de clone double)

```bash
# Créer un worktree pour la version de référence (avant)
git worktree add ../ingest-pipeline-baseline dc3c2ca

# Le répertoire courant reste sur la version optimisée (après)
# /Users/nicolasgonthier/Travail/Voxpod/ingest-pipeline → e89f51e
# /Users/nicolasgonthier/Travail/Voxpod/ingest-pipeline-baseline → dc3c2ca
```

**Avantages** :
- Pas de duplication du repo (partage le `.git`)
- Compilation ciblée par worktree
- Facile à synchroniser

### 5.2 Protocole A/B rigoureux

```
PHASE 1 : Préparation
1. Créer le worktree baseline
2. S'assurer que les deux versions compilent : cargo check dans les deux
3. S'assurer que les deux versions passent les tests : cargo test dans les deux

PHASE 2 : Benchmark baseline (version avant)
1. cd ../ingest-pipeline-baseline
2. Lancer le script de benchmark → produit results/baseline.csv
3. Répéter pour chaque scénario (micro, meso, macro)

PHASE 3 : Benchmark optimisé (version après)
1. cd ../ingest-pipeline
2. Lancer le script de benchmark → produit results/optimized.csv
3. Utiliser EXACTEMENT les mêmes fixtures et paramètres

PHASE 4 : Analyse statistique
1. Charger les deux CSV
2. Calculer moyenne, médiane, écart-type pour chaque métrique
3. Effectuer un t-test de Student (ou Wilcoxon si distribution non normale)
4. Vérifier que p-value < 0.05 pour la significativité
```

### 5.3 Égalisation des conditions

**Base de données** :
- Utiliser le même conteneur Docker PostgreSQL pour les deux versions
- Réinitialiser la DB entre chaque run (`docker-compose down -v && docker-compose up -d`)
- Pour les benchmarks macro : utiliser un dump SQL identique chargé avant chaque série

**Cache disque** :
- Vider le cache OS avant chaque série : `sync && echo 3 > /proc/sys/vm/drop_caches` (Linux)
- Sur macOS : redémarrer le conteneur Docker (le plus fiable)

**Réseau** :
- Pour les benchmarks micro/meso : pas de requête réseau (fixtures locales + mocks)
- Pour les benchmarks macro : utiliser un serveur mock local (`httpmock`) ou un cache HTTP transparent

**Compilation** :
- Toujours compiler en `--release` pour les benchmarks
- Utiliser `CARGO_TARGET_DIR` différent par worktree pour éviter les conflits de cache de compilation

---

## 6. Reproductibilité

### 6.1 Documentation de l'environnement

Avant chaque session de benchmark, enregistrer :

```bash
# Script : scripts/record_env.sh
#!/bin/bash
mkdir -p results/env
cat > results/env/$(date +%Y%m%d_%H%M%S).json <<EOF
{
  "date": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "host": "$(hostname)",
  "os": "$(uname -a)",
  "cpu": "$(sysctl -n machdep.cpu.brand_string 2>/dev/null || lscpu | grep 'Model name' | cut -d: -f2 | xargs)",
  "cpu_cores": $(sysctl -n hw.ncpu 2>/dev/null || nproc),
  "ram_gb": $(echo "$(sysctl -n hw.memsize 2>/dev/null || grep MemTotal /proc/meminfo | awk '{print $2}') / 1024 / 1024 / 1024" | bc),
  "rust_version": "$(rustc --version)",
  "cargo_version": "$(cargo --version)",
  "postgres_version": "$(psql $DATABASE_URL -c "SELECT version();" -t 2>/dev/null || echo 'N/A')",
  "docker_version": "$(docker --version)",
  "commit_before": "$(cd ../ingest-pipeline-baseline && git rev-parse HEAD)",
  "commit_after": "$(git rev-parse HEAD)",
  "features_before": "default",
  "features_after": "$(cat Cargo.toml | grep '^default' | cut -d= -f2 | xargs)"
}
EOF
```

### 6.2 Versionnement des scénarios

Les scénarios de test sont versionnés dans `benchmarks/scenarios/` :

```
benchmarks/
├── scenarios/
│   ├── v1.0_ingest_lemonde.yml      # Scénario : ingestion d'un flux Le Monde
│   ├── v1.0_dedup_1M_rows.yml       # Scénario : déduplication sur 1M d'articles
│   └── v1.0_batch_insert_30.yml     # Scénario : batch insert de 30 articles
├── fixtures/
│   ├── rss/
│   │   ├── lemonde_30items.xml      # Fixture : flux RSS de 30 articles
│   │   └── lemonde_large.xml        # Fixture : flux RSS de 1000 articles
│   └── html/
│       └── article_long.html        # Fixture : article HTML long
├── scripts/
│   ├── run_benchmark.sh             # Script principal
│   ├── record_env.sh                # Enregistrement environnement
│   └── analyze_results.py           # Analyse statistique
└── results/                         # Répertoire de sortie (gitignored)
```

**Format YAML d'un scénario** :
```yaml
# benchmarks/scenarios/v1.0_ingest_lemonde.yml
name: "Ingestion flux Le Monde (30 articles)"
version: "1.0"
description: "Mesure le temps d'ingestion complète d'un flux RSS de 30 articles"

type: macro  # micro | meso | macro

parameters:
  fixture: "rss/lemonde_30items.xml"
  feed_id: "test-lemonde"
  warmup_runs: 5
  measurement_runs: 10
  
metrics:
  - duration_total_ms
  - articles_per_second
  - sql_queries_count
  
validation:
  min_runs: 10
  max_outlier_ratio: 0.1
  significance_level: 0.05
```

### 6.3 Script de benchmark complet

```bash
#!/bin/bash
# benchmarks/scripts/run_benchmark.sh

set -euo pipefail

SCENARIO=$1
BASELINE_DIR="../ingest-pipeline-baseline"
OPTIMIZED_DIR="."
RESULTS_DIR="benchmarks/results"

mkdir -p "$RESULTS_DIR"

echo "=== Benchmark Scenario: $SCENARIO ==="
echo "Baseline: $(cd $BASELINE_DIR && git rev-parse --short HEAD)"
echo "Optimized: $(cd $OPTIMIZED_DIR && git rev-parse --short HEAD)"

# Enregistrer l'environnement
./benchmarks/scripts/record_env.sh

# Phase 1 : Benchmark baseline
echo "--- Running baseline ---"
cd "$BASELINE_DIR"
cargo test --release  # S'assurer que tout compile
cargo run --release -- benchmark --scenario "$SCENARIO" --output "$RESULTS_DIR/baseline_$(date +%s).csv"

# Phase 2 : Benchmark optimisé
echo "--- Running optimized ---"
cd "$OPTIMIZED_DIR"
cargo test --release  # S'assurer que tout compile
cargo run --release -- benchmark --scenario "$SCENARIO" --output "$RESULTS_DIR/optimized_$(date +%s).csv"

# Phase 3 : Analyse
echo "--- Analysis ---"
python3 benchmarks/scripts/analyze_results.py \
  --baseline "$RESULTS_DIR"/baseline_*.csv \
  --optimized "$RESULTS_DIR"/optimized_*.csv \
  --output "$RESULTS_DIR/report_$(date +%s).md"

echo "=== Done. Report: $RESULTS_DIR/report_*.md ==="
```

### 6.4 Script d'analyse statistique (Python)

```python
#!/usr/bin/env python3
# benchmarks/scripts/analyze_results.py

import pandas as pd
import numpy as np
from scipy import stats
import argparse

def analyze(baseline_file, optimized_file):
    baseline = pd.read_csv(baseline_file)
    optimized = pd.read_csv(optimized_file)
    
    metrics = ['duration_ms', 'articles_per_second', 'sql_queries']
    
    report = []
    report.append("# Rapport de Benchmark\n")
    report.append(f"**Baseline** : {baseline_file}\n")
    report.append(f"**Optimized** : {optimized_file}\n\n")
    
    for metric in metrics:
        if metric not in baseline.columns or metric not in optimized.columns:
            continue
            
        b = baseline[metric].dropna()
        o = optimized[metric].dropna()
        
        # Suppression des outliers (> 2σ)
        b_mean, b_std = b.mean(), b.std()
        o_mean, o_std = o.mean(), o.std()
        b_clean = b[(b - b_mean).abs() <= 2 * b_std]
        o_clean = o[(o - o_mean).abs() <= 2 * o_std]
        
        # Statistiques
        report.append(f"## {metric}\n")
        report.append(f"| Statistique | Baseline | Optimisé | Delta |\n")
        report.append(f"|------------|----------|----------|-------|\n")
        report.append(f"| Moyenne | {b_clean.mean():.2f} | {o_clean.mean():.2f} | {((o_clean.mean() - b_clean.mean()) / b_clean.mean() * 100):+.1f}% |\n")
        report.append(f"| Médiane | {b_clean.median():.2f} | {o_clean.median():.2f} | |\n")
        report.append(f"| Écart-type | {b_clean.std():.2f} | {o_clean.std():.2f} | |\n")
        report.append(f"| Min | {b_clean.min():.2f} | {o_clean.min():.2f} | |\n")
        report.append(f"| Max | {b_clean.max():.2f} | {o_clean.max():.2f} | |\n")
        report.append(f"| N (après filtre) | {len(b_clean)} | {len(o_clean)} | |\n\n")
        
        # Test de Student
        t_stat, p_value = stats.ttest_ind(b_clean, o_clean)
        significant = "✅ Significatif" if p_value < 0.05 else "❌ Non significatif"
        report.append(f"**t-test** : t={t_stat:.3f}, p={p_value:.4f} → {significant}\n\n")
        
        # Speedup pour le throughput
        if metric == 'articles_per_second':
            speedup = o_clean.mean() / b_clean.mean()
            report.append(f"**Speedup** : {speedup:.2f}x\n\n")
    
    return "\n".join(report)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--optimized", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    
    report = analyze(args.baseline, args.optimized)
    with open(args.output, "w") as f:
        f.write(report)
    print(report)
```

---

## 7. Critères d'acceptation d'un benchmark valide

### 7.1 Checklist de validité

Un benchmark est considéré comme **valide** si et seulement si :

- [ ] **Environnement** : Enregistrement complet de l'environnement (CPU, RAM, OS, versions)
- [ ] **Warm-up** : Au moins 5 runs de chauffe effectués et jetés
- [ ] **Taille d'échantillon** : Au moins 30 runs de mesure pour micro/meso, 10 pour macro
- [ ] **Outliers** : Moins de 10% de outliers supprimés, avec justification documentée
- [ ] **Significativité** : p-value < 0.05 sur le t-test de Student (ou test non paramétrique équivalent)
- [ ] **Stabilité** : Écart-type < 20% de la moyenne pour les métriques primaires
- [ ] **Non-régression** : Aucune métrique secondaire ne régresse de plus de 15%
- [ ] **Reproductibilité** : Le scénario est versionné et le script peut être relancé par un tiers
- [ ] **Isolation** : Une seule variable modifiée entre baseline et optimisé (sauf pour le benchmark global)
- [ ] **Nettoyage** : La DB et le cache sont réinitialisés entre chaque run

### 7.2 Niveaux de conclusion

| Niveau | Critère | Action |
|--------|---------|--------|
| ✅ **Validé** | Speedup significatif (p < 0.05), pas de régression | Merger l'optimisation |
| ⚠️ **Conditionnel** | Gain significatif mais régression sur une métrique secondaire | Investiguer la régression, benchmark plus ciblé |
| ❌ **Rejeté** | Pas de gain significatif, ou régression sur métrique primaire | Ne pas merger, analyser la cause |

---

## 8. Plan d'exécution recommandé

### 8.1 Phase 1 : Benchmarks unitaires (1-2h)

**Objectif** : Valider chaque optimisation individuellement.

| Optimisation | Type | Scénario | Durée estimée |
|-------------|------|----------|---------------|
| mimalloc | Micro | Parsing RSS 1000 items × 30 runs | 15 min |
| HTTP/2 pooling | Meso | 5 requêtes mock × 30 runs | 20 min |
| pg_trgm | Meso | Requête similarité sur 1M rows × 30 runs | 30 min |
| Batch insert | Meso | Insert 30 articles × 30 runs | 15 min |

### 8.2 Phase 2 : Benchmark global (2-3h)

**Objectif** : Mesurer le gain cumulé des 4 optimisations.

| Scénario | Description | Runs | Durée estimée |
|----------|-------------|------|---------------|
| Ingest complet | Flux RSS 30 items → articles insérés | 10 | 1h |
| Content complet | 10 articles → extraction + dédup | 10 | 1h |
| Pipeline E2E | Scheduler → ingest → content | 5 | 1h |

### 8.3 Phase 3 : Analyse et rapport (1h)

- Compilation des résultats
- Analyse statistique
- Rédaction du rapport de benchmark
- Décision go/no-go pour le merge

---

## 9. Template de rapport de résultats

```markdown
# Rapport de Benchmark — Pipeline d'Ingestion v1.1

**Date** : 2026-05-29
**Auteur** : [Nom]
**Commits** : `dc3c2ca` (baseline) → `e89f51e` (optimisé)

## Résumé exécutif

| Métrique | Baseline | Optimisé | Delta | Significativité |
|----------|----------|----------|-------|----------------|
| Throughput (articles/sec) | 12.4 | 28.7 | +131% | p < 0.001 ✅ |
| Latence p50 ingest (ms) | 2450 | 890 | -64% | p < 0.001 ✅ |
| Latence p95 ingest (ms) | 3200 | 1200 | -63% | p < 0.001 ✅ |
| Requêtes SQL par ingest | 32 | 3 | -91% | N/A |
| CPU moyen (%) | 45 | 38 | -16% | p = 0.042 ✅ |
| Mémoire moyenne (MB) | 128 | 118 | -8% | p = 0.128 ❌ |

**Verdict** : ✅ Validé — Les 4 optimisations combinées apportent un gain significatif de +131% de throughput sans régression critique.

## Environnement

- **Machine** : MacBook Pro M3, 16GB RAM, macOS 15.2
- **Rust** : 1.88.0
- **PostgreSQL** : 16.4 (Docker)
- **Docker** : 27.3.1

## Méthodologie

- Warm-up : 5 runs jetés
- Runs de mesure : 30 (micro/meso), 10 (macro)
- Filtrage outliers : > 2σ, 2 outliers supprimés sur 300 (< 1%)
- Test statistique : t-test de Student, α = 0.05

## Détails par optimisation

### 1. mimalloc
- Speedup parsing RSS : +8% (p = 0.031)
- Réduction allocations : -12% (mesuré via dhat)

### 2. HTTP/2 pooling
- Réduction temps connexion : -35% (p < 0.001)
- Réutilisation connexions : 100% après le premier run

### 3. pg_trgm
- Latence dédup 1M rows : 450ms → 8ms (p < 0.001)
- Index utilisé : GIN scan confirmé via EXPLAIN ANALYZE

### 4. Batch inserts
- Speedup insert 30 articles : 28× (p < 0.001)
- Requêtes SQL : 30 → 1

## Reproductibilité

```bash
# Cloner le repo
git clone https://github.com/Nic0lasgon/ingest-pipeline
cd ingest-pipeline

# Créer le worktree baseline
git worktree add ../ingest-pipeline-baseline dc3c2ca

# Lancer les benchmarks
./benchmarks/scripts/run_benchmark.sh v1.0_ingest_lemonde
```

## Conclusion

Les optimisations v1.1 atteignent et dépassent les objectifs. Recommandation : merger.
```

---

## 10. Annexes

### A. Outils recommandés

| Outil | Usage | Installation |
|-------|-------|-------------|
| `criterion` | Benchmarks micro Rust | `cargo add --dev criterion` |
| `iai-callgrind` | Benchmarks micro déterministes (cycles CPU) | `cargo add --dev iai-callgrind` |
| `dhat` | Profiler d'allocations | `cargo add --dev dhat` |
| `perf` | Profiler Linux (cycles, cache misses) | `apt install linux-tools` |
| `hyperfine` | Benchmarks de commandes shell | `brew install hyperfine` |
| `scipy` | Analyse statistique | `pip install scipy pandas` |

### B. Commandes de vérification rapide

```bash
# Vérifier que le cache est vide (Linux)
sync && echo 3 | sudo tee /proc/sys/vm/drop_caches

# Vérifier les connexions TCP ouvertes
watch -n 1 'netstat -an | grep :3000 | wc -l'

# Profiler le CPU pendant un benchmark
perf record -g cargo bench
perf report

# Mesurer les allocations avec dhat
DHAT_HEAP_PROFILE=1 cargo test --release test_insert_batch
```

### C. Références

- [Criterion.rs Book](https://bheisler.github.io/criterion.rs/book/)
- [PostgreSQL EXPLAIN](https://www.postgresql.org/docs/current/sql-explain.html)
- [Statistical Significance in A/B Testing](https://www.evanmiller.org/ab-testing/)
- [mimalloc performance](https://github.com/microsoft/mimalloc#performance)

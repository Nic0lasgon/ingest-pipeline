#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_ROOT"

echo "=== Voxpod Ingest Pipeline Benchmark Runner ==="
echo ""

DB_READY=false
if command -v pg_isready &>/dev/null; then
    if pg_isready -h localhost -p 5432 -U mypod &>/dev/null 2>&1; then
        DB_READY=true
    fi
fi

if [ "$DB_READY" = false ]; then
    echo "PostgreSQL not reachable. Starting via docker compose..."
    docker compose up -d postgres
    echo "Waiting for postgres to be healthy..."
    for i in $(seq 1 30); do
        if pg_isready -h localhost -p 5432 -U mypod &>/dev/null 2>&1; then
            DB_READY=true
            break
        fi
        sleep 1
    done
    if [ "$DB_READY" = false ]; then
        echo "ERROR: PostgreSQL failed to start. Aborting."
        exit 1
    fi
    echo "PostgreSQL is ready."
fi

export DATABASE_URL="${DATABASE_URL:-postgres://mypod:mypod_password@localhost:5432/mypod_pipeline}"

echo "DATABASE_URL=$DATABASE_URL"
echo ""

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="$PROJECT_ROOT/target/criterion"
BASELINE_NAME="${1:-baseline_$TIMESTAMP}"

echo "=== Running CPU-only benchmarks (no DB needed) ==="
echo ""

echo "--- Dedup Benchmark ---"
cargo bench --bench bench_dedup -- --save-baseline "$BASELINE_NAME" 2>&1 | grep -E "(Benchmarking|time:)" || true
echo ""

echo "--- HTML Extraction Benchmark ---"
cargo bench --bench bench_extract -- --save-baseline "$BASELINE_NAME" 2>&1 | grep -E "(Benchmarking|time:)" || true
echo ""

echo "=== Running DB benchmarks ==="
echo ""

echo "--- Insert Comparison (Individual vs Batch) ---"
cargo bench --bench bench_insert -- --save-baseline "$BASELINE_NAME" 2>&1 | grep -E "(Benchmarking|time:|SKIP)" || true
echo ""

echo "--- Full Ingest Pipeline ---"
cargo bench --bench bench_ingest -- --save-baseline "$BASELINE_NAME" 2>&1 | grep -E "(Benchmarking|time:|SKIP)" || true
echo ""

echo "=== Benchmark Results ==="
echo ""
echo "Baseline saved as: $BASELINE_NAME"
echo "Results directory: $RESULTS_DIR"
echo "HTML reports:      $RESULTS_DIR/*/report/index.html"
echo ""
echo "=== How to Compare Before/After ==="
echo ""
echo "1. On the OLD code, run:"
echo "   $0 before_optimization"
echo ""
echo "2. Switch to NEW code, then run:"
echo "   $0 after_optimization"
echo ""
echo "3. Compare (on NEW code):"
echo "   cargo bench -- --baseline before_optimization"
echo ""
echo "Criterion will show % change for each benchmark."
echo ""
echo "=== Quick Summary ==="

for bench_name in bench_dedup bench_extract bench_insert bench_ingest; do
    latest=$(ls -td "$RESULTS_DIR/$bench_name"*/  2>/dev/null | head -1)
    if [ -n "$latest" ] && [ -d "$latest" ]; then
        echo "  $bench_name: $latest"
    fi
done

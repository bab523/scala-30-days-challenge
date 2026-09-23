cat > day12/README.md << 'EOF'
# Day 12 - Cache and Persist

## Topics covered

- Cache an RDD reused by multiple actions
- Compare cache and persist
- Experiment with different storage levels
- Explain when caching hurts performance
- Scenario: reuse a cleaned transaction dataset in three reports

## Key notes

- `cache()` is the same as `persist(StorageLevel.MEMORY_ONLY)` for RDDs.
- `persist()` lets you choose the storage level.
- Without cache, every action recomputes the whole RDD lineage. An accumulator showed about 400000 computations without cache and about 200000 with cache.

## Storage levels tried

MEMORY_ONLY, MEMORY_AND_DISK, MEMORY_ONLY_SER, MEMORY_AND_DISK_SER, DISK_ONLY, MEMORY_ONLY_2

## When caching hurts

- The RDD is used only once (extra cost, no benefit).
- The data is larger than available memory.
- The transformation is cheap to recompute.
- `unpersist()` is forgotten, so memory stays occupied.

## Scenario result

Raw

# Vector Bench

这个工具用于复现第二阶段要求的 `exact / IVFFlat / HNSW` 对比实验。

构建：

```bash
cmake --build build --target vector-bench -j
```

运行：

```bash
build/bin/bustub-vector-bench
build/bin/bustub-vector-bench --dataset-size 5000 --queries 500 --dim 32 --metric l2
build/bin/bustub-vector-bench --dataset-size 5000 --queries 500 --dim 32 --metric cosine
```

输出格式：

```text
algo,top_k,search_budget,avg_recall,avg_latency_us
```

说明：

- `exact` 的 `search_budget` 固定输出为 `0`，作为 baseline。
- `ivfflat` 的 `search_budget` 对应 `nprobe`。
- `hnsw` 的 `search_budget` 对应 `ef_search`。
- 默认会输出 `k=1/10/50` 三组结果，便于直接观察 recall-latency tradeoff。

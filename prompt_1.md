# 第一阶段：补全非索引 exact KNN 执行链

## 背景
当前向量 exact KNN 查询在没有可用向量索引时，会沿用普通 `Sort + Limit -> TopN` 路径。
仓库中已经存在 `VectorKnnScanPlanNode` / `VectorKnnScanExecutor` 的骨架，但尚未真正接入 optimizer 主路径。

## 目标
把“非索引 exact KNN”的主执行路径补完整：
当查询满足向量 KNN 模式、且无法使用向量索引时，在 optimizer 中将原计划改写为 `VectorKnnScanPlanNode`，
并由 `VectorKnnScanExecutor` 执行，保证结果与原 `Sort + Limit` 语义一致，并支持 MVCC 可见性。

## 本阶段只做
1. 识别 `Limit(Sort(child))` 模式。
2. 仅支持单个 `ORDER BY` 键。
3. 该排序键必须是 `l2_distance(...)` / `cosine_distance(...)` / `ip_distance(...)`。
4. 若存在可匹配的向量索引，则不要走本阶段路径，保持索引改写逻辑优先。
5. 若不存在可匹配索引，则改写为 `VectorKnnScanPlanNode`。
6. 打通 `optimizer -> plan node -> execution factory -> executor` 整条链路。
7. `VectorKnnScanExecutor` 的结果必须满足 MVCC 可见性。
8. 返回结果顺序必须与原始 `ORDER BY distance ASC LIMIT k` 一致。

## 本阶段不做
1. 不实现 HNSW。
2. 不修改 IVFFlat 的索引结构与召回策略。
3. 不扩展 ANN recall / benchmark。
4. 不处理多表 join 场景。
5. 不支持多列 `ORDER BY` 的向量改写。
6. 不处理 `VectorIndexScan` 的“过滤后不足 k 个补候选”问题，这属于下一阶段。

## rewrite 条件
仅在以下条件全部满足时改写为 `VectorKnnScanPlanNode`：
1. 根节点是 `Limit`。
2. 子节点是 `Sort`。
3. `Sort` 只有一个 `ORDER BY`。
4. 排序方向为 `ASC` 或默认升序。
5. 排序表达式是向量距离表达式。
6. 当前没有可匹配的向量索引可供 `VectorIndexScan` 使用。

否则必须 fallback 到原计划，不得报错。

## 失败路径与边界
1. 向量维度不一致：保持现有异常语义。
2. 类型不匹配：保持现有异常语义。
3. 空表：返回空结果。
4. `LIMIT 0`：返回空结果。
5. 不满足 rewrite 条件：回退到原计划。
6. 不能因为本阶段的改动破坏已有 `TopN` 和 `VectorIndexScan` 路径。

## 建议优先查看 / 修改的文件
1. `src/optimizer/vector_knn_scan.cpp`
2. `src/include/execution/plans/vector_knn_scan_plan.h`
3. `src/execution/vector_knn_scan_executor.cpp`
4. `src/execution/executor_factory.cpp`
5. 如有必要：`src/execution/execution_common.cpp`

## 测试要求
至少覆盖以下场景：
1. 无索引 exact KNN 查询被正确改写为 `VectorKnnScanPlanNode`。
2. 查询结果与原 `Sort + Limit` 完全一致。
3. 空表场景。
4. 向量维度不匹配场景。
5. MVCC 可见性场景。
6. 存在向量索引时，不能误走本阶段路径。

## 验收标准
1. `EXPLAIN` 或 optimizer 输出中可以看到 `VectorKnnScan`。
2. 对无索引的向量 KNN SQL，结果与原 `Sort + Limit` 完全一致。
3. 不破坏已有 `VectorIndexScan` 路径。
4. 相关 SQLLogicTest / gtest 新增并通过。
5. 提交结果时说明仍然存在的限制。

## 输出要求
完成后请汇报：
1. 修改了哪些文件。
2. 为什么这样改。
3. 新增了哪些测试。
4. 还有哪些已知限制。


# 第二阶段：增强 VectorIndexScan 对 Filter 的支持

## 背景
当前 `VectorIndexScan` 已能支持基本的 `ORDER BY distance LIMIT k` 索引改写，
但对 `Filter + ORDER BY distance + LIMIT k` 的支持还不完整。
尤其是在索引先返回一批候选、再做可见性检查和过滤后，可能出现结果不足 `k` 的问题。

## 目标
增强 `VectorIndexScan`，使其能够正确支持：
`Filter + ORDER BY distance + LIMIT k`
并解决“过滤后不足 k 个”的候选补充问题。

## 本阶段只做
1. 支持带 `filter_predicate` 的向量索引扫描。
2. 在候选被 MVCC / filter 过滤后，能够继续补足候选，尽量返回 `k` 条结果。
3. 保持结果顺序与距离排序一致。
4. 不破坏当前 IVFFlat 的已有行为。

## 本阶段不做
1. 不在本阶段实现 HNSW。
2. 不要求一开始就做复杂 cost model。
3. 不要求支持 join 场景下的向量索引改写。

## 需要明确的实现问题
1. `SearchKnn` 是一次只返回 `k` 个，还是支持“过取”更多候选。
2. 若第一次候选不足，如何继续扩大搜索范围。
3. 在 IVFFlat 下，补候选是扩大 `nprobe`、扩大返回上限，还是由 executor 反复请求更多候选。
4. 最终返回的结果必须满足 MVCC 可见性和 filter 条件。

## 测试要求
至少覆盖以下场景：
1. `WHERE/Filter + ORDER BY distance + LIMIT k` 基本正确性。
2. 过滤后原始候选不足 `k` 时，仍能继续补候选。
3. 无可见 tuple 时返回空结果。
4. 不带 filter 的旧路径不回归。

## 验收标准
1. `VectorIndexScan` 能正确支持 `Filter + ORDER BY distance + LIMIT k`。
2. 结果顺序和条数符合预期。
3. 相关测试通过。
4. 说明当前候选补充策略的限制。



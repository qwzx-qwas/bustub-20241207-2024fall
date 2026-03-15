# 第一阶段：维度校验 + CRUD 一致性维护 + 脏条目清理/重建策略

## 背景
当前代码库已经具备以下能力：

1. `VECTOR(n)` 类型、数组字面量、距离函数已经打通。
2. 精确 KNN 以及专门的 `PlanNode` / `Executor` 已经完成。
3. IVFFlat 近似检索 MVP 已经完成。
4. `CREATE INDEX ... USING ivfflat`、`nlist/nprobe/metric`、从已有表构建 IVF、后续插入增量维护都已经有了。
5. 近似检索执行器会做 MVCC 可见性重建、过滤后补候选。

本阶段不追求一步到位实现完整向量数据库，只做第一阶段最值得补齐的闭环：

1. 写入时的维度校验。
2. 向量索引在 `INSERT/DELETE/UPDATE` 下的一致性维护。
3. IVFFlat 中 stale entry 的最小可用清理 / 重建策略。
4. 将目前偏 IVFFlat 私有的 ANN 查询接口，抽象成更通用的接口，为后续 HNSW 留好扩展点。

## 总目标
完成后，这一阶段应达到以下效果：

1. 向量列在写入路径上严格校验维度，错误路径明确报错。
2. 向量索引在 `INSERT/DELETE/UPDATE` 后查询结果仍然正确。
3. 即使索引内部允许暂时存在 stale 条目，执行器也必须过滤不可见或失效候选，保证结果正确。
4. stale 条目积累到阈值后，可以触发一次 IVFFlat rebuild，并在 rebuild 后恢复较干净的索引状态。
5. ANN 查询接口不再直接暴露 `nprobe` 这种 IVFFlat 特定术语。

## 本阶段只做
1. 在写入 SQL 路径上做维度校验，至少覆盖 `INSERT VALUES`、`INSERT ... SELECT`、`UPDATE`。
2. 支持 `INSERT/DELETE/UPDATE` 对向量索引的一致性维护。
3. 本阶段先做 IVFFlat 的 stale 标记、查询过滤、阈值触发 rebuild；不要求后台线程 vacuum。
4. 将 `SearchKnnWithProbe()` 这一类暴露 IVFFlat 细节的接口，改写为更通用的 ANN 搜索接口。
5. 新接口建议采用统一参数对象，例如 `VectorSearchOptions` 或 `AnnSearchOptions`。
6. 统一参数对象至少包含：
   - `top_k`
   - `candidate_budget`
   - `search_budget`
7. IVFFlat 将 `search_budget` 解释为 `nprobe`。
8. 未来 HNSW 将 `search_budget` 解释为 `ef_search`。
9. `Executor` 不直接感知 `nprobe/ef_search` 这类索引私有术语。

## 本阶段不做
1. 不实现 HNSW。
2. 不要求引入复杂 cost model。
3. 不要求支持 join 场景下的向量索引改写。
4. 不要求支持多个向量列联合索引或复合向量索引。
5. 不要求做 WAL / 崩溃恢复后的向量索引恢复闭环。
6. 不要求做异步 rebuild、后台 vacuum、并发安全切换。

## 脏条目清理以及重建策略
建议采用“查询期过滤 + 阈值触发 rebuild”的最小闭环：

1. `DELETE/UPDATE` 时，不要求立刻从 ANN 索引中硬删除旧条目。
2. 旧 RID 可以先被视为 stale / tombstone。
3. 查询时继续沿用当前执行器的可见性复查逻辑。
4. 若索引返回的候选中包含 stale / 不可见 RID，执行器必须跳过它们，并补充更多候选。
5. 维护一个触发 rebuild 的阈值，示例：
   - stale 条目占比超过阈值，例如 `20%`
   - 或者查询时补候选次数持续偏高
6. 一旦触发 rebuild，允许同步执行：
   - 全表扫描
   - 仅收集当前应被索引看到的 tuple
   - 重新训练 IVFFlat centroid
   - 重建 list
   - 最后原子替换旧索引状态
7. rebuild 失败时不得破坏旧索引的可读性。

## 实现约束
1. 本阶段优先保证“结果正确性”，其次才是性能。
2. 允许 IVFFlat 内部暂时保留 stale 条目，但不允许把错误结果返回给用户。
3. rebuild 可以是同步、阻塞式实现，只要逻辑清楚、测试完整即可。
4. 若需要新增索引内部状态统计，请保持接口最小化，不要把实现细节泄露到优化器或执行器之外。
5. 对外接口应优先表达“搜索预算”语义，而不是某一种 ANN 实现的内部参数名。

## 失败路径与边界
1. 维度校验失败时，`INSERT/UPDATE` 必须直接报错，不能补零、截断或悄悄跳过。
2. 至少要覆盖以下入口的维度校验：
   - `INSERT VALUES`
   - `INSERT ... SELECT`
   - `UPDATE`
3. 若当前实现不打算支持向量列为 `NULL`，必须在 prompt 实现中明确拒绝并加测试。
4. 删除后旧索引项可以残留，但查询结果必须正确，即使性能退化。
5. rebuild 失败时不能破坏旧索引，必须保持旧索引仍然可读。
6. 本阶段 rebuild 不要求并发安全切换。
7. 本阶段不要求处理进程重启后自动恢复向量索引状态。
8. 本阶段不要求支持多向量列、复合向量索引、join 改写、HNSW。

## 建议优先查看 / 修改的文件
1. SQL 类型与维度：
   - `/mnt/d/cmu15445/bustub-20241207-2024fall/src/binder/bind_create.cpp`
   - `/mnt/d/cmu15445/bustub-20241207-2024fall/src/storage/table/tuple.cpp`
   - `/mnt/d/cmu15445/bustub-20241207-2024fall/src/type/value.cpp`
2. 向量索引实现：
   - `/mnt/d/cmu15445/bustub-20241207-2024fall/src/include/storage/index/ivfflat_index.h`
   - `/mnt/d/cmu15445/bustub-20241207-2024fall/src/include/storage/index/index.h`
3. 执行器维护索引：
   - `/mnt/d/cmu15445/bustub-20241207-2024fall/src/execution/insert_executor.cpp`
   - `/mnt/d/cmu15445/bustub-20241207-2024fall/src/execution/delete_executor.cpp`
   - `/mnt/d/cmu15445/bustub-20241207-2024fall/src/execution/update_executor.cpp`
4. 查询路径：
   - `/mnt/d/cmu15445/bustub-20241207-2024fall/src/execution/vector_index_scan_executor.cpp`
   - `/mnt/d/cmu15445/bustub-20241207-2024fall/src/optimizer/vector_knn_scan.cpp`
5. DDL / catalog：
   - `/mnt/d/cmu15445/bustub-20241207-2024fall/src/common/bustub_ddl.cpp`
   - `/mnt/d/cmu15445/bustub-20241207-2024fall/src/include/catalog/catalog.h`

## 测试要求
至少补齐以下测试：

1. 维度不匹配的 `INSERT` 失败。
2. 维度不匹配的 `UPDATE` 失败。
3. `INSERT ... SELECT` 导入错误维度时失败。
4. `DELETE` 后查询结果仍正确，且能观察到 stale 候选被过滤。
5. `UPDATE` 改变向量值后，旧位置不会错误命中，新位置能命中。
6. stale 比例超过阈值后触发 rebuild，rebuild 前后查询结果一致。
7. 通用 ANN 搜索参数接口下，IVFFlat 仍能正确映射到 `nprobe`。
8. 原有 `/mnt/d/cmu15445/bustub-20241207-2024fall/test/sql/vector.slt` 和
   `/mnt/d/cmu15445/bustub-20241207-2024fall/test/sql/vector_index_scan.slt` 不能回归。
9. 扩展 `/mnt/d/cmu15445/bustub-20241207-2024fall/test/storage/ivfflat_index_test.cpp`，
   覆盖 CRUD + rebuild 场景。

## 验收标准
满足以下条件即可认为本阶段完成：

1. 向量列写入时严格校验维度，错误路径有测试覆盖。
2. `INSERT/DELETE/UPDATE` 后向量索引查询结果正确。
3. 允许索引中存在 stale 条目，但执行器必须过滤掉不可见或已失效 RID。
4. stale 达到阈值后可触发一次完整 IVFFlat rebuild，且 rebuild 后查询结果不变。
5. `SearchKnnWithProbe` 不再作为执行器依赖接口，改为通用 ANN 搜索参数接口。
6. 本阶段不引入 HNSW 实现，不改变现有 exact KNN 行为。

## 输出要求
完成后请汇报：

1. 修改了哪些文件。
2. 为什么这样改。
3. 新增了哪些测试。
4. 还有哪些已知限制。

## 可参考资料
1. `/mnt/d/cmu15445/bustub-20241207-2024fall/prompt_1.md`
2. `/mnt/d/cmu15445/bustub-20241207-2024fall/learning_prompt.md`
3. `/mnt/d/cmu15445/bustub-20241207-2024fall/learning_QA.md`

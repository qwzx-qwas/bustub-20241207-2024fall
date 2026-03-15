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




# 第二阶段：实现 HNSW + IVFFlat / HNSW 对比实验

## 背景
第二阶段建立在第一阶段已经完成的基础上：

1. `/mnt/d/cmu15445/bustub-20241207-2024fall/prompt_1.md`
2. `/mnt/d/cmu15445/bustub-20241207-2024fall/learning_prompt.md`
3. `/mnt/d/cmu15445/bustub-20241207-2024fall/learning_QA.md`
4. 第一阶段中与向量查询相关的基础能力已完成：
   - 通用 ANN 搜索接口已经就位
   - `VECTOR(n)`、距离函数、精确 KNN 已打通
   - IVFFlat 的查询路径已经是“优化器 rewrite -> 向量索引计划节点 -> executor 回表 + MVCC + filter”

这一阶段的重点不是再扩展 SQL 能力，而是：

1. 把 HNSW 接到现有执行链条里。
2. 让系统具备在 HNSW 和 IVFFlat 之间做选择的能力。
3. 给出一组像样的对比实验，而不是只停留在功能可跑。

## 总目标
完成后，这一阶段应达到以下效果：

1. HNSW 作为新的向量索引类型接入现有系统。
2. 优化器能够识别查询是否可由 HNSW 加速，并在必要时选择 HNSW path。
3. Executor 能通过 HNSW 做 ANN 搜索，并继续沿用回表、MVCC 可见性检查、filter 复查的执行模式。
4. 系统可以在 IVFFlat 和 HNSW 之间做最小可用的路径选择。
5. 能拿 exact KNN 作为 baseline，对 IVFFlat 和 HNSW 做 recall-latency tradeoff 对比。

## 本阶段只做
1. HNSW 索引的数据结构与构建逻辑。
2. HNSW 查询逻辑及其接入现有 ANN 搜索接口。
3. HNSW 对应的 DDL / catalog / index type 扩展。
4. optimizer 对 HNSW path 的识别与最小可用路径选择。
5. IVFFlat 与 HNSW 的对比实验。
6. 允许使用简单、可解释的代价估计或启发式规则来选择 IVFFlat / HNSW，不要求复杂统计模型。

## 本阶段不做
1. 不要求实现 HNSW 的后台压缩、删除重连、复杂图修复。
2. 不要求做页级持久化 HNSW 图结构。
3. 不要求做 WAL / 崩溃恢复后的 HNSW 状态恢复。
4. 不要求支持 join 场景下的 HNSW 改写。
5. 不要求引入非常复杂的 cost model；简单启发式可接受。
6. 不要求支持多向量列联合索引或复合向量索引。
7. 不要求做高度工程化的 benchmark 框架，只要实验可重复、结果清楚。

## HNSW 设计要求
1. 沿用第一阶段通用 ANN 接口，HNSW 将 `search_budget` 解释为 `ef_search`。
2. 索引构建期，需要把数据组织成 HNSW 多层图结构：
   - 每个向量对应一个图节点
   - 每个节点有一个最高层 `level`
   - 节点在每层与若干近邻建立边
   - 底层最稠密，高层更稀疏
3. 查询时采用“从高层入口点下降到底层”的典型 HNSW 搜索流程。
4. 必须支持至少一组常见 HNSW 参数，并明确它们的语义，建议至少包含：
   - `M`
   - `ef_construction`
   - `ef_search`
5. 规划期需要识别查询是否属于可由 HNSW 加速的 KNN 形态，并判断是否存在匹配的 HNSW 索引。
6. 执行期如果选中了 HNSW path，则进入 HNSW 索引扫描逻辑，在图上做 ANN 搜索并返回候选结果。
7. HNSW 返回候选后，仍然要经过当前系统已有的回表、MVCC、filter 复查逻辑。

## 实现约束
1. 距离函数必须和已有距离表达式逻辑保持一致，索引 metric 与查询 distance function 必须匹配。
2. 每层边数不能无限增长，必须受 `M` 或等价参数约束。
3. 插入新节点时必须做邻居裁剪，不能简单连所有近邻。
4. 邻居裁剪策略必须稳定、可解释，避免图局部退化。
5. 不能把过多节点放到高层，也不能让高层过于稀薄到失去“远程跳转”意义。
6. `level` 生成策略必须明确，且足够稳定，便于测试与复现实验。
7. 搜索时必须有 `visited set` 或等价机制，避免重复遍历。
8. `visited set` 的空间成本必须可控，不能让访问标记本身成为主要瓶颈。
9. 候选集与结果集必须有界，不能无限扩张。
10. HNSW 的实现应尽量复用第一阶段 ANN 接口与执行器框架，不要额外发明一套独立查询通路。

## 路径选择要求
本阶段允许使用简单启发式，而不是复杂 cost model。可接受的方案示例：

1. 若查询是小 `top_k`、较高 `search_budget`、数据规模较大，则优先尝试 HNSW。
2. 若索引 metric 不匹配，则该索引不可用。
3. 若存在多个可用向量索引，可以用简单规则比较：
   - 索引类型偏好
   - 参数规模
   - 表规模
   - 查询 `top_k`
4. 若无法可靠选择，允许保守回退到已有 path，而不是强行使用 HNSW。

## 对比实验要求
实验部分至少回答下面几个问题：

1. 与 exact KNN 相比，IVFFlat 和 HNSW 的 recall@k 分别如何。
2. 在相同数据集与相同 metric 下，IVFFlat 和 HNSW 的查询延迟分别如何。
3. 改变搜索预算时：
   - IVFFlat 的 `nprobe`
   - HNSW 的 `ef_search`
   它们的 recall / latency 如何变化。
4. 至少选一组具有代表性的 `top_k` 设置，例如 `k=1/10/50`。
5. 尽量控制实验变量一致：相同数据、相同 metric、相同 baseline、相同硬件环境。

## 失败路径与边界
1. 如果查询形态无法 rewrite 为 HNSW path，必须安全 fallback 到已有 path。
2. 如果 HNSW 索引不存在、metric 不匹配、参数非法，也必须 fallback，而不是返回错误结果。
3. 如果 HNSW 搜索返回的候选不足，允许继续用现有 executor 补救逻辑，但不能返回错误结果。
4. 如果路径选择逻辑拿不准，允许保守选择 exact KNN 或已有 IVFFlat path。
5. 本阶段不要求处理 HNSW 删除后的图修复难题；若暂不支持删除修复，必须在输出里明确限制。

## 建议优先查看 / 修改的文件
1. 通用 ANN 接口：
   - `/mnt/d/cmu15445/bustub-20241207-2024fall/src/include/storage/index/index.h`
2. 现有向量索引实现：
   - `/mnt/d/cmu15445/bustub-20241207-2024fall/src/include/storage/index/ivfflat_index.h`
   - `/mnt/d/cmu15445/bustub-20241207-2024fall/src/include/catalog/catalog.h`
3. 现有向量执行路径：
   - `/mnt/d/cmu15445/bustub-20241207-2024fall/src/execution/vector_index_scan_executor.cpp`
   - `/mnt/d/cmu15445/bustub-20241207-2024fall/src/optimizer/vector_knn_scan.cpp`
   - `/mnt/d/cmu15445/bustub-20241207-2024fall/src/include/execution/plans/vector_index_scan_plan.h`
4. DDL / catalog 扩展：
   - `/mnt/d/cmu15445/bustub-20241207-2024fall/src/common/bustub_ddl.cpp`
   - `/mnt/d/cmu15445/bustub-20241207-2024fall/src/include/catalog/catalog.h`
5. 建议新增：
   - HNSW 对应的 index 头文件 / 实现文件
   - HNSW 测试文件

## 测试要求
至少补齐以下测试：

1. HNSW 索引可创建、可构建、可查询。
2. HNSW 查询结果在小规模数据上与 exact KNN 高度一致，至少能验证 recall 不为 0 且结果合理。
3. optimizer 能在存在 HNSW 索引时识别可用 path。
4. metric 不匹配时不会错误使用 HNSW。
5. `search_budget -> ef_search` 的映射正确，调大 `ef_search` 后 recall 不应明显下降。
6. HNSW 查询仍然经过 MVCC / filter 复查，结果正确。
7. 路径无法 rewrite 为 HNSW 时可以正确 fallback。
8. 原有 IVFFlat 路径不能回归。
9. 至少补一组实验脚本或实验说明，能够重复跑出 IVFFlat / HNSW / exact 的对比结果。

## 验收标准
满足以下条件即可认为第二阶段完成：

1. HNSW 已作为新的向量索引类型接入系统。
2. HNSW 查询复用了现有执行链条，而不是另起一套绕过 MVCC / filter 的逻辑。
3. 系统可以在 HNSW、IVFFlat、exact KNN 之间做最小可用的路径选择或 fallback。
4. 至少有一组对比实验展示 exact / IVFFlat / HNSW 的 recall-latency tradeoff。
5. 原有第一阶段功能没有回归。
6. 输出中清楚说明当前 HNSW 实现的已知限制，例如删除修复、持久化、恢复、复杂 cost model 等未完成项。

## 输出要求
完成后请汇报：

1. 修改了哪些文件。
2. HNSW 的核心数据结构与搜索流程是怎么设计的。
3. 为什么路径选择逻辑这样设计。
4. 新增了哪些测试。
5. 对比实验怎么做的，结果如何。
6. 还有哪些已知限制。


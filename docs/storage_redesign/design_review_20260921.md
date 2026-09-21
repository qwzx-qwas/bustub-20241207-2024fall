# 2026-09-21 改进方案覆盖、冲突与项目兼容审查

[主方案](README.md) · [下一步 F00](modules/storage-contracts.md) · [实际测试记录](testing_execution_20260921.md)

## 1. 本轮范围与结论

按用户要求将最近讨论的改进写入总方案及对应子方案：查询执行链复用、A 记录/页空间管理、extent 连续性、F35 事件推进与背压、F26 页并发、日志批量/组提交、既有共享快照方向及测试比较边界。

文档中的职责、阶段和正确性约束已收录；具体接口、页/日志格式、参数及当轮验收仍按模块冻结。生产模块都保持未完成，prompt 未启用。本次不修改生产/测试代码，不重新运行已完成的性能测试。

审查结果：已修正下表中的过时状态、职责歧义及与当前代码不相容的直接套用方式；未发现未说明的模块归属/阶段冲突。此结论是设计与代码静态核对，不是运行验证或证明所有并发/恢复细节已解决。

## 2. 对话内容覆盖表

| 对话议题 | 总方案位置 | 详细方案及处理 |
| --- | --- | --- |
| BusTub 是否已有候选行读取链 | §9.5、§11 | [F35 §6](modules/raft-pipeline.md)：复用 SELECT 的优化器/执行器/BeginReadAt；只执行写计划的读取子树，完整过滤与旧版本薄适配 |
| 索引规则与剩余谓词是否可直接使用 | §9.5 | F35 核对等值/OR 与 AND 差异、IndexScan 剩余谓词、RID/元数据和输出 schema；保留扫描与原逻辑命令 |
| HOT 是否就是不改索引、直接覆盖 | §10.1 | [F36 §6.2](modules/table-space-management.md)：保 RID 原地更新、HOT 同页版本链、当前新 RID 路径分别说明；非首轮默认实现 |
| F36 的实现顺序 | §13、§13.1、§14 | F36 在 F26/S9.1 后实施 S9.2；有完整安全条件才实施 S9.3；S11/S12/S13 分别协作恢复根、预算和新并发 |
| 页内整理与跨页减少页数的区别 | §10.1 | F36 §6.1：整理碎片、复用抑制增长、跨页搬迁分开；不引入每记录 page 配额或跨页大记录 |
| extent 与逻辑/物理连续性 | §4.1、§5.2 | [F12](modules/allocator.md)、[F13](modules/object-mapping.md)：单段物理连续，对象多段；连续布局是择优目标，不保证减少逻辑读取 |
| event loop/worker/pending/队满 | §11 | F35 §6.3–6.4、[F31](modules/resource-budget.md)：协议不等 worker 腾位；全链路数量/字节预算、完成预留、通知合并、公平推进与退出 |
| 异步任务次序与失败 | §11 | F35 保留 Prepare 视图/依赖、日志/HardState 顺序、按序 Apply 和 ReadIndex；不因切任期盲丢已提交结果 |
| BufferPool 怎样支持并行 IO | §9.4、§10.2 | [F26](modules/page-storage-adapter.md)、[F02](modules/object-io.md)、[F01](modules/block-device.md)：Loading/同页等待、短映射锁、帧代次、稳定写回版本、淘汰/错误及定位 IO |
| 缓存职责及代码页重排启发 | §10.2 | [F32](modules/cache-policy.md)/F31：A/B/工作缓冲总预算，区分三种缺页/未命中，不建立重复 FS 干净业务页缓存或假定启动重排提升 DB 性能 |
| batch 是否合并多次日志写入 | §6、§9.1 | [F23](modules/raft-log-store.md)、[F07](modules/journal-service.md)：写入合并与组提交分开，不扩大原子性，不把 durable 当多数派提交，不重复存 entry 正文 |
| 日志回收与 A 空间清理边界 | §10.1 | [F22](modules/gc-service.md)、[F29](modules/log-segment-cleaner.md)、F36：所有者判断，统一安排执行，不取混合时间戳最小值；整段和部分有效段分别处理 |
| 快照共享是否已有 | §9.3–9.4 | [F25](modules/snapshot-store.md)、[F28](modules/business-checkpoint.md)、[F14](modules/reference-manager.md)：沿用原 S11 方向，补充 RocksDB 依据，不新增同职责模块 |
| 开源机制的落点、差异和约束 | 各相应段落 | 每个采用参考的子方案保留直接来源、原问题、所借机制、本项目落点与采用差异；不能用引用代替本地恢复证明 |
| 测试原则、旧结果限制及性能归因 | §1.4、§13.1、§15 | [测试方案](testing_plan.md)：复用已审查内容，保持真实链路/独立预期/生产隔离，记录 FS、F36、SQL 路径和并发版本边界；不宣称旧 P3 稳定性已覆盖 |

## 3. 冲突与兼容处理

| 发现的问题 | 已写入的修正或实施约束 |
| --- | --- |
| 总方案/测试入口还说性能尚未运行，而末尾已有完成报告 | 统一入口到 2026-09-21 结果：本轮约定观测结束，P2 超时保留、P3 稳定性未覆盖；历史资格失败保留历史身份 |
| F35 旧句“不增加 F36”容易被理解为禁止独立 A 模块 | 明确它仅禁止为会话/提案重复建模块；F36 是记录/页空间管理，主目录现有 F00–F36 共 37 个入口 |
| 直接给 Prepare 加 Optimizer 就假设可执行 | 当前 MutationFilter 限定计划形状；改为只读子计划适配。IndexScan 并非通用完整谓词检查，结果收集也没有独立元数据，均列入实现前提 |
| BeginReadAt 被误当作自动保存历史版本 | 保留生命周期/可见性保护；移到 worker 不等于取消保护；并发 Apply 前另证明读取视图 |
| 只外包 event loop，但 Propose/状态查询仍可同步阻塞 | F35 要核对同步业务校验、持久化及状态机锁，事件完成推进进度，不能在协议回调中重新等待 SQL/IO |
| BufferPool 只解锁会暴露未加载页、复用中的帧或清掉新脏版本 | F26 同时设计加载状态、pin/代次、稳定输入、版本比较、同页顺序、错误唤醒和淘汰协议 |
| A/B 共用 BufferPool/PageGuard，后期改 A 可能破坏先实现的 B | [F08](modules/metadata-engine.md) 和 F26 双向说明私有页/WAL 先行/恢复回归；F36 不负责 B 页存活性 |
| 页内整理被说成必然减少页面数 | 区分页内碎片、已有页复用和跨页合并；只有满足引用条件的空页才释放 |
| 当前换 RID 更新若省索引操作会保留旧指向 | 说明 HOT 所需版本链/入口与原地更新的区别；检查全部相关索引和旧版本需求，不能仅凭主键值不变跳过更新 |
| A 页内空闲摘要与设备 bitmap 重复维护 | F36 管可重建字节摘要、锁页重查；F12/B 管设备分配权威，F13 管对象映射 |
| 一个对象包含多个页，删一个页被误当成删对象 | F26/F36/F13/F22/F12 明确局部范围、最小分配单位和共享引用，不删除其他活页 |
| S9 页复用若依赖完整 S11 checkpoint 会形成顺序歧义 | S9 保留当前快照+Raft 重建并先证明清理安全；不满足条件时暂缓整页释放；S11 再扩大到持久业务根和共享页 |
| 新 group commit 导致把 Raft 正文再写一次 Journal | F23 正文仍在最终段；F07 管 FS 恢复批次，F02 共用 IO 不等于正文二次持久化 |
| 共享快照被当成新模块或可直接硬链接 BusTub 文件 | 保留 F25/F28/F14 原职责，明确不可变版本、一致根、canonical 格式差异及接收端基础验证 |

## 4. 当前代码核对入口

- 查询链：[BusTubInstance](../../src/common/bustub_instance.cpp)、[分布式读取/准备](../../src/distributed/raft_state_machine.cpp)、[SqlCommandPreparer](../../src/distributed/sql_command_preparer.cpp)、[索引优化](../../src/optimizer/seqscan_as_indexscan.cpp)、[IndexScan](../../src/execution/index_scan_executor.cpp)、[Filter](../../src/execution/filter_executor.cpp)、[ExecutionEngine](../../src/include/execution/execution_engine.h)。
- IO 生命周期：[BufferPool](../../src/buffer/buffer_pool_manager.cpp)、[PageGuard](../../src/storage/page/page_guard.cpp)、[DiskScheduler](../../src/storage/disk/disk_scheduler.cpp)、[DiskManager](../../src/storage/disk/disk_manager.cpp)。
- 空间/恢复：[ApplyUpdate](../../src/distributed/bustub_state_machine.cpp)、[TableHeap](../../src/storage/table/table_heap.cpp)、[TablePage](../../src/storage/page/table_page.cpp)、[事务 GC](../../src/concurrency/transaction_manager.cpp)、[canonical 快照](../../src/recovery/canonical_snapshot.cpp)。
- 协议与持久化：[RaftNode](../../src/raft/raft_node.cpp)、[DistributedNode](../../src/distributed/node.cpp)、[DurableStorage](../../src/include/recovery/durable_storage.h)。

这些是核对依据和后续阅读入口，不是本轮代码修改清单。当前项目既有 Raft durable 文件接口，工作数据库页的 fstream flush 不等于整个系统未提供持久化。

## 5. 第一轮文档验证与范围

- 检查修改文档的相对链接目标、代码围栏、尾部空白、必需 prompt/代码/测试约束、未完成标记和 F00–F36 目录唯一性。
- 对照模块阶段表、主顺序与跨阶段使用关系，避免把协作模块的全部未来能力倒置为前置依赖。
- 对照修改前快照核对生产/测试文件内容哈希，确认本轮没有改动生产或测试内容；保留用户已有工作区改动。
- 本次仅文档改动，未编译、未运行数据库/E2E/性能测试。文档检查不充当模块验收。

实际检查结果：本轮修改或新增 21 份 Markdown；相对链接及标题锚点、代码围栏、重复标题、尾部空白和必需约束检查通过；37 个模块目录及阶段表一致；`git diff --check -- docs/storage_redesign` 通过。`src/` 与 `test/` 下 566 个文件的内容哈希和文件清单与本轮开始时一致。

## 6. 仍需后续冻结的内容与下一步

F35 候选计划范围/视图/完成额度、F26 写回完成/帧代次/稳定缓冲选择、F36 页格式/回收判据/旧句柄失效、S11 共享格式和增量基点仍是显式待定项。它们不是本次漏写后由实现者随意补齐的默认值。

下一步启动 [S0/F00](modules/storage-contracts.md) 的详细讨论：对象和范围身份 → 完成语义 → 原子/顺序 → 资源/错误 → 回收/兼容 → 必要验证与实现 prompt。用“调用成功后能做什么、掉电后保留什么、何时可以释放缓冲/空间”逐项解释；契约冻结后再实施必要共享类型，继而进入 S1 后端与 IO 基础。

## 7. 第二轮复核与补充

用户要求再次审查，并先讲解 S0、等待认可后才完成实现方案及编写运行测试。本轮只更新文档；F00 §6.5–6.12 为建议评审稿，prompt 未启用，生产实现未完成。

### 7.1 本轮新发现的缺口与处理

| 缺口/歧义 | 处理及归属 |
| --- | --- |
| 完成项列在一起，容易被理解为每个请求都必须串行经历的全局状态 | F00/F01/F02/F15/F20 区分缓冲归还、IO、durable、发布和 Raft 结果；只定义相关依赖，F00 不成为运行服务 |
| 批次和 LSN 容易被实现成两份必需持久记录 | F00/F07/F19 明确语义不同、表示可复用；保留原子边界但不强制重复计数器 |
| 只给 Raft 段强调了尾块保护，FS Journal 未具体说明 | F06/F07 补齐旧 durable 前缀保护，不能用损坏后截尾抹去此前确认的提交 |
| 小 Deferred redo 容易遗漏物理 RMW 单元中其他有效字节 | F18/F27/F00 明确按故障模型保护所有可能受损的有效字节；无法证明则不能启用该原地路径；具体编码待冻结 |
| 混合 Common/Deferred 可能各自成功后误当整体原子 | F07/F15/F19 明确先满足 Common 依赖，再同批记录元数据及 Deferred 正文，统一发布 |
| B 的可靠页访问可能误依赖 S9 的 BufferPool 改造 | F08/F09/F26 明确私有页、受控写回和 WAL 先行在 S4/S5 建立；S9 在此基础上扩展并发并保护 A/B |
| checkpoint 暂停提交后等待 Deferred，存在相互等待可能 | F10/F27/F31 明确保留完成资源/通路，checkpoint 不无条件等待全部落位；实际暂停边界仍需冻结 |
| 旧 Superblock 校验成功不等于回退安全 | F03/F10/F11 要求有效恢复链保留已确认提交；缺失时失败上报，不能静默回退已承诺内容 |
| 缺少工作库重建的对象身份说明 | 当前 InitializeEmpty 清工作目录、InstallSnapshotFile 建候选库；F00/F13/F26/F34 增加新工作集身份/切换/回收，防止页号重用串到旧数据 |
| 文件接口和新异步接口的成功语义可能降级 | F00/F24：当前 Append/Update 返回即 durable，S8 保留同步契约，S13 再改协议异步；异常/bool 不能无依据转成 durable 成功 |
| read 缺失范围与设备短读易被统称为补零 | F00/F01/F02/F15/F21/F26 区分初始化、格式明示空洞、EOF/不存在、短 IO/损坏；具体 API 在所属阶段冻结 |
| 节点状态只列名字，未定义服务含义 | F33 补 phase 解释、conditions 叠加与按操作 capabilities；Serving 不等于 Leader 可写，F33 不管理每次请求/范围 |
| 线性一致性只讲进度，未明说实时顺序及当前读接口限制 | 主方案/F35 补 `<H ⊆ <S` 和当前 ExecuteReadSql 的精确发布视图要求；不把任意较旧 ReadIndex 当历史快照 |
| 模块模板仍写“先完成总体测试设计” | 更新为沿用已讨论的共同测试、仅补模块新增验证；不把旧系统观测重新置为未执行 |
| 子模块模板还写 36 个模块 | 同步到 F00–F36 共 37 个；主目录和阶段表再次交叉核对 |

上表中的安全要求已进入职责；“具体格式/方法待冻结”仍是进入相关实现阶段的前置讨论项，不能被实现者自行补齐。本轮没有声称已证明所有恢复与并发协议。

### 7.2 更早对话的覆盖与取舍

| 议题 | 当前落点/明确取舍 |
| --- | --- |
| 保留 Raft/SQL，FS 优化本地设备管理 | 主方案 §2–5；A 保留 SQL，B 复用同仓库存储算法但运行上下文独立，普通正文不穿 B 的 value |
| page/slot 与下层通用容器隔离 | F26/F13；上层仍整页，slot 由 A 管；细粒度脏范围尚需上层信息，不凭 FS 接入自动获得 |
| log/data/metadata/引导是否统一 IO | F02；普通映射、基础区域与固定引导三类寻址，共用范围执行，不以自寻址造成恢复循环 |
| extent、虚拟内存类比、空间分类 | F12/F13/F26；逻辑多段映射可恢复，不强制按 OSD 类型隔离，连续布局不保证减少逻辑读页 |
| bitmap 分层、坏块和区域分配 | F12/F04/F05；高层搜索摘要可重建，坏块/隔离另表达，B 基础空间不依赖普通映射 |
| BlueFS 职责、B+Tree/WAL、FULL/PATCH | F03–F11；直接页后端代替文件接口依赖；页头和 Create/Open 适配；周期首改 FULL，不默认每次整页日志；B WAL 与 FS Journal 共用恢复基础 |
| Common/Deferred、RMW/COW、redo | F18/F19/F27；两类提交路径平级，RMW 可与 COW 结合，运行写回与崩溃重放分开，阈值待定 |
| 去掉冗余正文、对象引用 | F07/F14/F23/F25；引用共用正文，必要恢复信息、临时交接及容错副本不承诺只写一次，不新增全盘哈希去重 |
| memtable/SSTable、日志段轮转与合并 | F23/F29；选分段追加/内存索引/整理，不默认 LSM；保留仍需重放的 entry 语义，不引入多 Raft group 或集中到 MON 存权威日志 |
| HardState、快照、压缩和轻恢复 | F24/F25/F28；保留现有 term/vote/commit 恢复保证；先可靠发布，后共享一致页版本；压缩不能替代一致捕获或自动消除扫描 |
| 缓存 70/30、重复 data cache、函数重排启发 | F31/F32；先按职责和总预算分配，默认不再缓存一份 FS 干净业务页；比例未冻结，代码缺页与数据库缺页区分 |
| 统一 GC 与各层判断 | F22/F36/F12；所有者判断可清理性、统一安排执行，页内碎片整理/页复用/跨页搬迁分开 |
| 全局可叠加状态与优先级 | F33；节点内统一视图与异步动作，具体模块保留必要局部安全状态，普通 IO 不等待状态机许可 |
| 副本/EC Pool、PG、MON/OSDMap、peering/recovery/backfill | 主方案 §2 明确另一条架构方向；已有 Raft 承担复制权威、增量追赶和快照安装，不等于实现全部 Pool/EC/故障域/坏块修复职责 |
| SQL 准备扫描、HOT、页回收、并发和快照已有范围 | 第一轮表及 F35/F36/F26/F25/F28/F14；没有新增同职责组件，阶段安排保留 |
| 真实测试、三层隔离、资源/故障边界和 `t+` | testing_plan 与已归档运行记录；新模块只加项目承诺的必要验证，S0 尚未获准实施/写测试 |

Ceph 对照来源：[OSD 与对象](https://docs.ceph.com/en/latest/architecture/storage-cluster/)、[Pool/放置/peering](https://docs.ceph.com/en/latest/architecture/dynamic-cluster-management/)、[recovery/backfill 状态](https://docs.ceph.com/en/latest/rados/operations/pg-states/)。这些用于说明取舍：本项目采用对象间接寻址和分阶段恢复的分工，保留 Raft 作为复制权威，不引入第二层对象副本/EC 协议。MON 提供集群图的权威信息，不是集中存储全部对象业务日志的替代品。核对：2026-09-21。

### 7.3 本轮代码核对与验证范围

重新检查了 DurableStorage/PosixDurableStorage、LogStore/StableStore、DiskScheduler/DiskManager/BufferPool、B+Tree 页头与构造、LogManager/CheckpointManager、SQL 准备/查询执行、Raft 提案与工作库恢复路径。新增判断的代码入口均可由第 4 节及 F00 阅读入口定位。

第二轮验证：修改 39 份 Markdown（其中部分模块仅统一测试入口的状态措辞）；相对链接/标题锚点、围栏、重复标题、尾部空白、必需约束检查通过。37 个模块的总目录、阶段顺序、模块阶段声明/进度表一致；`git diff --check -- docs/storage_redesign` 通过。`src/`、`test/` 的 566 个文件内容与清单相对本轮起点未变。审查临时文件完成核对后清理。

生产/测试实现和运行验证均未执行，用户认可前不启用 F00 prompt。本次结论限定为：已把发现的职责/阶段歧义和项目接入风险写清，未发现额外未记录的同类冲突；磁盘格式、并发/崩溃协议的完整证明与具体测试仍待所属模块讨论和验证。

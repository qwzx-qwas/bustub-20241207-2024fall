# S12.1：共享额度、后台回收与热路径精简

2026-10-07。用户已授权落实近期讨论及测试，已确认 F26 乐观短读采用“已发布内存正文不可变”的路线 B。本轮 S12.1 已完成，实际改动及八项测试审查见 [执行记录](s12_execution_20261007.md)。本文是当前共同协议；下方模块旧日期的待讨论记录保留历史，不撤销本轮授权。

## 1. 范围和顺序

1. F31 两级额度基础，接入 F02 自有 IO 缓冲、B 私有/已发布页和 F19 输入/组装、F27 写回正文。
2. F02 为必要完成工作保留操作/缓冲名额，F07 沿用已实现的完成记录及 checkpoint 空间预留。
3. F22 把已有对象退役扫描迁出提交角色；单独有界执行、游标继续、受阻项不挡住后续候选；保留 F14/F12 唯一判定/执行路径。
4. F26/F32 合并驻留命中的 LRU-K 访问与首次 pin 更新，保留每次访问历史；原 Guard、翻译内存寿命与正文保护保持。
5. 先审生产逻辑，再编写真实消费者的阶段测试、执行八项测试审查并压缩归档。

本轮是 S12.1 接入，不将完整 S12、F31/F22 跨阶段职责标为完成。A FrameArena/目录、快照 CPU/网络正文等已有局部限额继续有效，纳入同一节点总额度及调节属于后续 F31/F32 接续；当前受管字节不能称作整个进程 RSS。F29 整理、F30 scrub、CLI/共同 E2E 性能对照仍待后续。

## 2. 额度、接口和寿命

`ResourceBudget` 持有总字节数、成批授予大小和进展保留；`ResourceAccount` 持有某个所有者的 grant/charged。节点通过显式 `NodeStorageOptions::memory_budget_` 组装同一个预算，原独立组件/未配置的部署保留局部限制。`IOExecutorOptions` 的进展操作/缓冲额度为正式部署参数，零保留原独立部署行为，不为测试改变原参数的含义。

约束：charged <= grant；普通授予不占用进展保留；总授予不超过总额。预算授予不是分配正文。组件只在局部额度不足时拿节点短锁，释放时归还超过局部缓存的整量子空闲部分；每类保留不足两个量子的空闲额度以减少反复申请。节点不足时，在不持节点锁的情况下回收账户闲置额度（包括申请者另一类别的闲置额度）后再试；最后所有者释放则归还全部。锁序为组件账本→节点账本，节点不反调组件，不持这些锁等待 IO。初期采用释放时归还和压力时收回，无另外的额度维护线程。

IO 的内存容量按实际对齐余量计算；外部帧许可只算保留压力及操作名额，不能重复算一份帧 RAM。F19 接纳的借用输入同理：局部在途压力仍保留原容量限制，节点实际内存预算只预留拥有的输入及组装/Deferred 复制上界。`ResourceCharge` 随真实所有者归还；等待超时不等于释放，B 旧视图继续计账，F02 自有读取结果直到最后所有者释放才归还。

`ProgressWork` 只标记同步准备调用所处的工作类别；IO 接纳时保存类别，后续跨线程完成不读取提交线程的 TLS。它不给任务永久成功承诺，也不提供业务权限。F27、F22、B checkpoint/writeback/完成记录使用进展类别；普通 IO 不能借走这部分额度。

ObjectIO 预检和 NodeStorage 页 IO 批量能力同步扣除普通请求不可使用的进展保留。超过配置永久容量的请求明确报错；暂时不足可返回 Full。普通对象尚未提交时遇到共享内存耗尽，允许报告 NotCommitted 并归还临时资源，不能占着输入无限等待本身占住的内存。已经 durable 的 Deferred 任务仍持久保留并重试；不能改成丢弃。关闭时停止新回收批次，排空已认领的实际工作；持久未完成任务按原 S10 恢复接续。

拒绝原因随本次 `IOPreparation` 返回：`Full + shared_memory_limited` 表示共享 RAM 暂时不足；其他 Full 仍是局部容量争用。ObjectIO 将前者转换为正式 ResourceUnavailable，让尚未提交的流水线归还输入；后者保留原 ObjectIOBusy 等待重试。节点 `Pressure()` 只作状态提示，不参与某个请求的失败分类，避免其他线程的成功/失败改变本次决策。单次 Reserve 超过该工作类别的共享总容量，在 ResourceAccount 统一报参数错误；不能伪装成总有机会成功的 Full。

Deferred 的最终 Data 写入及 Flush 成功后，先销毁已不用的解码正文并归还其内存额度，再尝试提交完成记录；完成记录暂时缺资源时不能继续占着这份无用 RAM。F02 已完成的 Data 批次同样退出。这里归还的是临时内存，Journal 正文及任务的持久保留仍按完成记录和旧读者协议退出。

这些额度为资源推进提供条件，不声称单靠预留就能释放被永久读者保留的 B 页或 extent。维护 IO 上限和 B max_live_pages 仍须能容纳合法的完成批次；本轮不会通过无限重试隐藏配置错误。

## 3. Journal 计算沿用原实现

U 为运行配置的单元字节，格式开销为单元头 64、校验 4、片段头 16、提交正文 24。实际提交 D 由 `Plan(records)` 编码得到，不能只用正文/U。

当前完成上界：B=min(B.max_batch_bytes, Journal.max_batch_bytes)，R=Journal.max_records_per_batch，C=1+ceil((B+16R)/(U-84))。普通追加检查 D+max(旧完成保留, 提交后 Payload 数×C)+1 <= AvailableUnits。最后的 1 是 checkpoint；新保留包含旧责任，不能两份相加。落位完成 durable 后更新额度，字节回收仍受 checkpoint/正文/读者保留约束。

示例参数 U=4096、B=16384、R=8 得 C=6。记录长度 100/6000/2000 经编码得到 D=3，Payload 由 3 增为 5，需 3+30+1=34 个可用单元。仅作演算，禁止写死这些运行参数。新段头和对齐缓冲另由 Layout/F02 计算。保留现有单份 Journal，不引入额外提交副本。

## 4. F22 当前执行协议

NodeStorage 所有的 ObjectTransactionPipeline 增加独立回收执行角色，删除 CommitLoop 的周期扫描责任，不再保留两个周期 GC。没有新回收数据库、物理 bitmap 或第二个设备执行器。

按现有 max_query_spans 有界发现候选，仅保存对象/分配身份与游标；每轮处理一个候选，F14 的 max_scan_entries/max_reclaim_ranges/max_reclaim_bytes 继续限制内部工作。游标对受阻项也前进；完整遍历后可重新发现仍持久存在的退役事实。不持有旧 B snapshot 跨轮等待。MetadataCommitBusy/视图冲突为后续再试；损坏及真实 IO 失败上报已有节点错误链。

F14 的并发调用名额暂满由准入处报告现有 ObjectReferenceError::Busy，F22 下轮重试，F19 沿原 Busy 路径退让；不把这种短暂竞争升级为节点错误。单次扫描、范围数量或编码超限仍保留原资源错误，不能通过统一捕获所有 ResourceUnavailable 来无限重试不可完成的工作。

当前已有单一对象退役来源，因此不先造泛化插件注册表/多层队列。F36/Store 仍负责退出逻辑引用，事实到达原目录后被此角色发现。后续 F29/F30 接入多来源时才增加轮转状态；调度与 B/F02 的在途限额需共同约束，不能承诺迁出提交线程就无竞争。

节点现有 `StorageMaintenanceLoop → RaftObjectStorage::Collect` 仍负责 Store 对象的逻辑退役、传输 lease 和分步删除；本轮 `GarbageLoop → F14/F12` 负责随后物理范围的安全复用。两者消费的事实和产出不同，不能删掉其中一个代替统一调度；它们的节奏还未由完整 F22 合并。本轮唯一被替代并删除的是 CommitLoop 中同一物理退役目录的周期扫描。

## 5. F26 后续协议已确定的方向

- 状态与事件分开：pins 0→1 才需取消可淘汰，1→0 才可重新淘汰；每次 RecordAccess 是独立事件，保持 LRU-K 语义。此次合并同次命中的两个替换器临界区。
- 驻留命中后续可改成条目原子读取→帧锁→核对身份/映射/状态→pin；帧锁内不能反向等待条目独占锁。加载/淘汰/关闭全部服从同一交接。
- 目录周期 Maintain、生命周期 calls 的批量登记、组级 RCU/epoch 与 PathCache 代次一起接续 F26，当前未据此移除 shared_lock。稳定描述符停止发布旧组，旧访问退出后才能析构和打洞；不能在目录短访问范围等待 IO。
- **用户已选择路线 B，归 S13：已发布内存正文不可变；写者修改私有版本后发布，旧版本等读者退出才复用。** 短读返回独立值，不让页指针/未校验副作用逃逸；普通 PageGuard 保留。路线 A“原地正文原子访问”作为备选研究，不再与已选路线并列待选择。
- 目录寿命、帧身份与正文版本分别保护；现有映射版本不能证明正文未改。BufferPool 协议不自动证明 B+Tree 分裂/合并时查找一致；真实索引消费者须一并验证。内存 COW 不等于已经实现的 FS COW。
- FrameArena 实际持有匿名映射及稳定帧地址；容量、对齐余量、实际 RSS/大页观察分开，不因 F02 借用而重复记 RAM。

## 6. 开源来源与内化边界

| 来源 | 原机制/问题 | 本项目落点 |
| --- | --- | --- |
| [PostgreSQL buffer README](https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/storage/buffer/README)、[bufmgr.c](https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/storage/buffer/bufmgr.c) | 映射、pin、正文分工；本地重复 pin 减少共享更新 | F26 状态交接分析；未照搬跨进程私有计数到可跨线程 Guard |
| [Linux RCU](https://docs.kernel.org/RCU/whatisRCU.html) | 移除与安全回收分离 | F26 组/PathCache/打洞后续，不直接移植内核 API |
| [Calico §4.4](https://arxiv.org/html/2604.00423v1)、[P1478R6](https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2020/p1478r6.html) | 乐观短读/版本与 C++ 正文访问合法性 | F26/S13 不可变内存版本；不把提案当可直接调用的标准库 API |
| [RocksDB WriteBufferManager](https://github.com/facebook/rocksdb/wiki/Write-Buffer-Manager) | 共享额度/所有者记账 | F31 两级额度，无 dummy 正文复制；本地量子算法为本项目设计 |
| [XFS Logging Design](https://docs.kernel.org/filesystems/xfs/xfs-delayed-logging-design.html) | 事务修改前保留所需日志空间 | 复用 F07 已有完成上界/预留，不复制 XFS grant-head 协议 |
| [RocksDB Thread Pool](https://github.com/facebook/rocksdb/wiki/Thread-Pool)、[Ceph mClock](https://docs.ceph.com/en/latest/rados/configuration/mclock-config-ref/)、[下游队列限制](https://docs.ceph.com/en/latest/rados/configuration/osd-config-ref/#caveats) | flush 不被大整理占满；份额/权重/上限与下游在途协调 | 独立有界回收角色和进展资源；不是完整 mClock 或不受 IO 竞争的承诺 |

## 7. 实现 prompt 与测试约束

按 §1 顺序实现当前 S12.1 生产消费者，复用原局部预算、IO、恢复、回收与状态路径。后续职责更新唯一模块文档，不增加同职责模块。执行完更新真实进度，不以框架接口存在宣布全节点资源管理完成。

> 当遇到方案中未提到的部分时，先不要自作主张去写,而是汇报当前遇到的问题（注意先不要堆砌专有词汇，用我能看的懂的话去讲当前情况，有需要时再用专有词汇）。用户本轮已认可的对话内容直接执行，不重复请求批准。

> 代码要考虑复用性，间接性，安全性，不要破坏别的模块和层次的production级代码；接续任务完成前删除失去用途的旧代码和未用接口。不要过多纠结于边界条件和防御性编程，防止可能出现的问题被掩盖；先从代码逻辑分析，确认无已知问题后再编写测试。

> 测试应该保护“这个项目自己承诺的行为和风险边界”，而不是给每一层实现细节、第三方库行为和历史重构痕迹都建立一套测试。

验证共享预算拒绝/归还与跨所有者争用、保留 IO/完成通道、旧视图/结果寿命、受阻回收仍推进其他候选、关闭与重开正文、原 BufferPool 内容与替换契约。结果根据明确输入/实际读取和恢复判断，不检查私有计数代替行为。复用 F27/F26/NodeStorage 已有场景；新阶段源码仅压缩归档，同一模块包更新不留旧错误源码包。执行八项测试设计审查：全景、目标/输入/路径/oracle/失败含义、生产及接口污染、重复、变异能力、稳定性与矩阵。共同 C/P 基线不改变。

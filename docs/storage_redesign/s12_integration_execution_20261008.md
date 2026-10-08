# S12.2 现有消费者整合：实施与测试设计审查（2026-10-08）

依据：[共同协议 §8](s12_resource_gc.md#8-s122-现有消费者整合2026-10-08已授权实施)。用户本轮授权先整合现有资源消费者与后台来源，F29 打洞/搬迁仍在讨论，本次没有实施。先审生产逻辑、再写测试，运行后按发现的问题再次复核。

**已完成本轮范围：82 项正常场景通过（8 新增、74 复用），13 个指定变异被检出。** Clang14 Debug / AddressSanitizer；LSan 关闭，未跑 TSan。

## 1. 具体改动与职责去重

| 位置 | 原问题 | 本次改变 |
| --- | --- | --- |
| `frame_arena.cpp`、`buffer_pool_manager.cpp` | A 的帧正文只有局部额度 | 正式对象页部署从 NodeStorage 取得同一 ResourceBudget；Arena 创建前计完整映射容量，Guard/IO 保留真实帧期间持续计账；命中路径不申请新额度 |
| `translation_directory.cpp` | 目录与 PathCache 只有局部额度 | 原局部 Budget 同时取得共享额度；保留原物化/打洞计量。线程缓存退出晚于目录时仍持有其账户，没有保活整个目录 |
| `BufferPoolManager::CapturePages` | checkpoint 复制脏页只有调用上限 | PageCapture 持有副本额度，独立于缓存寿命；临时排序页号数组单独计量并及时归还 |
| `snapshot_tasks.cpp`、`raft_node.cpp` | worker 名额结束即难以约束后续结果保留 | 任务接纳前预留块工作上界；结果以共享所有者连同额度交接至 Raft 重试状态；直到最后持有者退出才归还。Decode 先接纳再复制正文，保留原 worker 和队列 |
| `tcp_transport.cpp` | 排队帧及编码/解码临时正文只有本地队列上限 | 同一预算限制编码峰值、接收解码与待发送 frame；编码后缩减为实际 frame.capacity。Full 沿既有网络丢包/重试语义退让，不持协议锁等待资源 |
| `DistributedNode::Initialize/Start/Stop/TickLoop` | 节点另有 Store 周期线程，错误只在其循环发布 | 注入同一根预算；注册 Store 的 `Collect(1)` 到原 GC 角色；删除 StorageMaintenanceLoop 与线程；Tick 接续真实存储错误 |
| `ObjectTransactionPipeline::GarbageLoop` | 只安排物理范围回收 | 每轮一个有界 Store 步骤、一个物理候选；保持原 Store/F14/F12 权威事实，受阻后仍轮转；注册锁不跨 IO |
| `Submit/Drive/CommitLoop` | 后台调用线程的类别不能自动跨 worker | 接纳时存进展类别、异步角色恢复类别；一个额外维护事务名额，普通持有票据不占它；局部字节/操作上限和根预算仍有效 |
| `RaftObjectStorage::Commit/Collect`、B/ObjectIO | 暂满可能被普通异常升级为节点故障 | 未接纳 Full 与明确暂时内存占用用 MetadataCommitBusy；Collect 遇到它保留当前候选、结束本轮。损坏、未知提交和真实 IO 失败仍上报 |
| `NodeStorage::Close/SetStoreMaintenance` | 关闭先降 readiness 可能使已认领维护访问失败；故障后也必须能解除注册 | 先停止、排空 Store 来源，再降低对象 API readiness；空注册在错误/关闭状态仍可执行，保留上下文直到退出 |

没有另建资源管理器、物理分配器、回收清单、快照格式或通用线程池。`SetStoreMaintenance` 是节点组装与原 GC 执行角色的连接，不负责判断对象/物理范围是否无用。B checkpoint、Deferred 和快照 CPU 原执行角色继续存在，它们不是重复的物理 GC。

**边界：**这是所列资源所有者的容量/工作上界预算，不是整个进程 RSS 上限；线程栈、通用容器附加成本等没有逐字节统一管理。空配置保留既有独立/文件部署。Store 单步仍会等自身提交，可能延迟这一轮物理回收；没有宣称所有后台任务同时并行或已实现完整 mClock。没有改变 LRU-K、实现 S13 乐观短读或运行性能对比。

开源内化沿用共同协议 §6 的 [RocksDB WriteBufferManager](https://github.com/facebook/rocksdb/wiki/Write-Buffer-Manager) 思路：同一根额度由真实所有者计量，资源仍由各组件分配。借鉴后台工作分类与下游限额协调，没有复制其代码/接口，也没有引入新的第三方库。

## 2. 逻辑审查与发现的问题

- 所有权：Arena 先解除映射再退账；目录物化成功/失败与共享额度同步；TLS PathCache 比目录活得久时账户仍有效。快照结果的额度不能随 worker 或 future 提前消失。复制的 checkpoint 脏页不能只算原 FrameArena 一份。网络发送队列拥有自己的编码帧，与原压缩正文不同。
- 锁序：账本沿 S12.1 的局部→根顺序；不在根锁回调组件。Store 取出回调后释放注册锁再执行。关闭不持 NodeStorage mutex 等 Store，因为 Store 会重新进入 NodeStorage。未新增热页访问时的根预算锁。
- 关闭：NodeStorage 先设 closing 阻止重复关闭/新注册，仍允许既有 Store 操作完成；停止来源后再 Drain 原调用。故障状态不妨碍解除注册。新线程没有增加，原节点维护线程已删除。
- 结果分类：Full 明确尚未接纳；MetadataCommitBusy 明确暂时占用/未提交；Indeterminate 沿原错误链停止。未把所有异常改成可重试；物理 GC 不再笼统吞掉 ResourceUnavailable。
- 快照组合复验发现真实兼容问题：单个维护名额与 `Collect(limit>1)` 连续提交相遇，上一笔虽已完成，内部持有者仍可能短暂占位。修正为 **Collect 保留当前持久候选，返回本轮已完成数量，下轮继续**；没有扩大保留数量、阻塞重试或修改旧业务预期。单候选真实 IO 错误仍到节点错误状态。
- 本轮阶段测试不能替代端到端资源性能测量；不能从额度断言推导“高并发性能提高”。

## 3. 测试全景

新增 8 项，全部经真实 production 组件，是 integration，同时保护回归风险；不冒充完整 SQL/Raft E2E。源码仅在 `test/archives/F31-F22-resource-gc.tar.gz` 中：前五项及 I8 在 `tests/integration_test.cpp`，I6/I7 在 `tests/maintenance_test.cpp`。旧七项风险继续引用 [S12.1 审查](s12_execution_20261007.md)，不另写镜像套件。

| 编号 / 测试（S12Integration） | invariant / 防止的 bug |
| --- | --- |
| I1 ObjectPageFramesShareBudgetAndGuardsKeepTheirMemory | 帧正文与其他所有者共用总额；缓存销毁后活 Guard 不能提前退账 |
| I2 SparseDirectoryCompetesWithIOAndReturnsMemoryAfterLastAccess | 稀疏翻译物化受根预算限制；旧访问结束前不能将其内存额度再授给 IO |
| I3 SnapshotResultRetainsCreditsAfterWorkerAndExecutorExit | worker 已结束不代表快照结果/重试正文无人持有；防止提前归还额度 |
| I4 TCPPressureDropsWorkThenDeliversTheOriginalSnapshotBytes | 网络额度暂满可以退让，退出压力后同一非空正文必须正确送达 |
| I5 StoreCollectionWaitsForItsSlotAndUsesProgressMemoryAcrossThreads | 暂满有界退让；普通票据/缓冲不能占尽维护能力；进展类别跨提交角色仍有效 |
| I6 CloseDrainsAcceptedStoreIOBeforeDisablingItsStorageCalls | 关闭必须等待已认领维护 IO；不能先关闭 API 导致半途 NotReady |
| I7 RealMaintenanceDurabilityFailureIsPublishedAndCanStillDetach | 真实 EIO 不能伪装成暂满重试；错误后仍可以解除维护来源 |
| I8 CapturedDirtyPagesKeepTheirOwnCreditsAfterCacheClose | checkpoint 私有脏页副本在缓存关闭后仍正确且占额度；退出后额度可用 |

复用范围：原 S12 套件 39 项（其中旧 7 项＋F27/F19/F26/LRU-K 32 项）；F25 归档 runner 的 35 项（S8 8、F28 7、共享 6、增量 9、压缩 5）。后者保留 SQL、会话结果、恢复和 TCP 判据，只适配部署/接口：对象 Harness 统一 64 MiB 根预算，正式 DistributedNode 组装传至 A/worker/TCP；部分手工 Raft 夹具仍使用原独立部署，作为兼容验证，不能称其每个网络任务都测了根预算。F25 原已被共享场景接管而排除的历史 F28 场景继续排除，未恢复重复执行。

## 4. 逐项：目标 → 输入 → 路径 → Oracle → 失败含义

### I1 帧所有者

- 目标：仍活的帧不允许被第二个缓存重复消费额度。
- 输入：2 MiB 总额、512 KiB 进展保留，256 帧；真实对象页写入确定的非零整页，保留 ReadGuard 后关闭并销毁缓存。
- 路径：NodeStorage→ObjectPageStorage→BufferPool/FrameArena→Common/F02/Direct；Guard 退出后重建缓存读取相同页。
- Oracle：两次第二缓存构造因额度拒绝；旧 Guard 正文等于独立输入；Guard 退出后重建成功、从存储取回相同正文。不检查私有计数/帧位置。
- 失败：Arena 漏记、缓存销毁提前退账、额度未归还或正文路径错误。

### I2 翻译目录

- 目标：稀疏访问实际建立的翻译组受共享额度限制，退出后真正归还。
- 输入：128 KiB 根预算、1 MiB 目录局部预算；最多 32 个不同非零 prefix，同一 suffix；正常设置不同帧值，保留一个 TranslationAccess 后销毁目录。
- 路径：真实目录 Access/物化/所有权→共享账户；与真实 Direct IOExecutor 的缓冲申请竞争，再写入/读回非零正文。
- Oracle：先受根额度拒绝；已创建条目仍是各自输入值；持有访问时 IO Full、退出后可写并读回。没有限定“恰好第几个组满”。帧编号为组件级输入，因此它是阶段集成证据，不能代替真实 SQL 翻译性能。
- 失败：目录只执行局部限额、提前归还或错误销毁活条目。

### I3 快照结果

- 目标：任务结束/执行器销毁不改变结果拥有正文的事实。
- 输入：384 KiB 根预算；任务从真实 Direct 文件读确定非空字节，返回 InstallSnapshotRequest；持有其结果后销毁 SnapshotTasks。
- 路径：SnapshotTasks 接纳→原 worker→实际 IO→shared result→新执行器接纳。
- Oracle：结果字节等于独立输入；持有结果时另一任务拒绝，退出后新任务实际执行并读回原字节。没有直接读 used/granted，不仅断言 shared_ptr 存在。
- 失败：额度只跟 worker 生命周期、提前退账或最后退出未归还。

### I4 TCP 压力

- 目标：可重试的网络压力不丢失协议语义，不转为本地永久错误。
- 输入：真实 IO 自有缓冲占住 2 MiB 根预算；发送 64 KiB 确定正文，释放竞争缓冲后重发相同请求。
- 路径：TcpRaftTransport Send→原编码→实际 loopback socket→Decode→接收回调。
- Oracle：第一次同步 admission 的 dropped 增加；第二次接收正文逐字等于输入。不是只测试 codec 自己 round-trip，也不依赖 CRC oracle。
- 失败：遗漏网络预算、Full 行为变为阻塞/异常或真实传输破坏正文。只保证这次可用环境中恢复推进，不模拟所有丢包分布。

### I5 Store 进展

- 目标：单一维护来源不被普通请求占光名额/内存；未提交的 Busy 不丢弃候选。
- 输入：真实跨两段的 1500 字节日志退役；持有全部普通完成票据及维护票据，先直接调用有界 Collect，再交给原 GarbageLoop。普通 Direct 缓冲占满其共享份额后释放维护票据。
- 路径：RaftObjectStorage→Collect→NodeStorage Submit→Drive→Commit→原 B/F14/F12；完成后追加另一条日志、Close/Open。
- Oracle：占位时不抛异常、不删除；释放维护名额后两段对象完成逻辑删除，节点无错误；新的日志正文和重开正文与输入相同。物理复用由旧 T5（有限设备写入新对象）验证，不把 Collect 返回数冒充物理可复用。
- 失败：普通抢占保留、进展类别在异步角色丢失、暂满被误报 fatal，或候选游标丢失。再次审查增加直接 Collect 的 Busy 断言，覆盖旧多步调用者。

### I6 关闭排空

- 目标：已接受维护实际 IO 没结束前不能宣布关闭完成；之后正常恢复。
- 输入：真实 Store 退役，测试链接器暂停第一笔 Direct fdatasync；异步调用 Close。
- 路径：正式维护注册→真实对象提交/系统调用→NodeStorage Close/排空→重开日志追加读取。
- Oracle：先由门确认实际 fdatasync 已进入，再要求关闭 future 尚未完成；释放门后关闭完成且重开正文正确。20 ms 检查只用于“不提前完成”，10 秒作为后续进展截止。极端调度延迟下前一负断言单独证明力有限，须结合 gate、恢复结果和逻辑锁序审查。
- 失败：关闭提前停用 API、提前返回或死锁。它验证 Store 来源这条新交接，不重复普通 IO 的全套关闭测试。

### I7 真实故障

- 目标：EIO 与短暂预算不足具有不同处理。
- 输入：相同真实退役路径，Direct fdatasync 返回 EIO；仍使用实际文件和此前非空日志。
- 路径：BlockDevice→Journal/对象提交→原节点错误发布→维护解除注册→关闭。
- Oracle：NodeView 的 `error_` 最终持有 std::system_error 且 errno=EIO，解除注册不抛异常。不是检查错误文本片段或把任意失败计为成功。
- 失败：真实错误被重试隐藏、错误类型丢失或故障后无法退出。

### I8 checkpoint 私有副本

- 目标：独立复制出的正文不能只计原缓存一份。
- 输入：768 KiB 根预算，128 帧；八张页各写不同非零内容，输入模型按 NewPage 返回的页号记录。Capture 后销毁缓存，再申请接近总额的实际 IO 缓冲。
- 路径：真实 BufferPool/FilePageStorage/CapturePages→PageCapture→独立 Direct IO 缓冲竞争。
- Oracle：每个捕获正文等于输入模型；捕获仍活时大 IO 拒绝，退出后可实际写入并读回。FilePageStorage 是生产兼容后端，不将其 Flush 称作新增 durable 保证；F28 业务 checkpoint 由复用场景验证。
- 失败：脏页副本漏记、过早退账、捕获错误或释放后额度不可用。

## 5. Production / interface pollution

| 变化 | 正式调用者 / 删除测试后是否仍需存在 | 是否仅为测试 |
| --- | --- | --- |
| NodeStorage::MemoryBudget | DistributedNode、A 工作空间/恢复候选组装取得同一根；不是读取私有 used 计数 | 否 |
| NodeStorage::SetStoreMaintenance / 内部 StopStoreMaintenance | 正式节点注册原 Store；关闭排空及错误退出 | 否 |
| Arena/目录/BufferPool/RaftConfig/TCP 的预算依赖 | 生产共享部署；空值维持原独立部署，不改变原显式参数为默认参数 | 否 |
| SnapshotTasks 显式预算、共享 Result、Decode | 实际 Raft 接纳、输入复制和 ACK/取消所有权 | 否；内部接口，无测试 getter |
| ResourceCharge::ShrinkTo | 网络编码结束及时归还峰值多余额度，队列仅计成品帧容量 | 否 |
| PageCapture 的 ResourceCharge | F28 捕获副本生存期；随 move 交接 | 否 |
| 进展类别、单维护名额、暂满退出 | 正式 GC 持续推进；真实错误处理不回退 | 否 |

没有 test-only hook、测试宏分支、默认路径更改、内部状态 getter 或没有正式调用者的转发层。链接器 fdatasync 包装、端口夹具和变异只在归档测试二进制；production 不反向依赖它们。外部缓冲原接口继续用于生产，没为方便测试改成自动分配。

## 6. 重复、杀伤力与演进

- I1/I8 都看缓存关闭后的内存，但分别覆盖 **原帧正文** 和 **独立脏页副本**，漏记点不同；I3 则覆盖 worker→协议重试结果的另一条交接。共享预算类型本身不再逐字段测试。
- I2 是实际稀疏目录所有权接入；不再复制原 F26 位编码、PathCache 命中次数或 hole punching syscall 测试。后续真实 SQL/SS/RS/PL/GT 若能覆盖相同低预算生命期，可合并/替换阶段案例。
- I5 验证 Store 逻辑退役及类别交接；旧 S12 T5 验证物理回收后的可分配性，二者不能相互冒充。I6 关闭、I7 真错误与 I5 资源暂满为不同 failure mode。
- 既有 F25、F28、S8 的内容和恢复判据仅复用一次；不另抄入此包。适配接口与部署配置不改变请求和 expected rows。共同 C/P 不变。

本轮新增最小 mutation：Arena 不接根预算→I1；目录不接根预算→I2；快照结果 deleter 不保留 charge→I3；CommitLoop 丢弃进展类别→I5；Collect 将 Busy 重新抛出→I5。旧八个 mutation 继续复用。runner 检查指定用例、退出码及目标断言，编译失败或任意崩溃不算检出。

I4 跳过网络 Reserve 会使首次 dropped 判据失败；I7 把真实错误吞掉会使 EIO 判据超时；I8 删除副本计账会让大 IO 提前成功。这三项为逻辑分析，未声称已单独运行相应 mutation。I6 的时间负断言存在上述调度限制，不声称任何关闭错误都一定被发现。

## 7. 稳定性与诊断归属

- 测试文件独立 mkstemp，正文确定生成，无随机 seed 漂移；Direct 对齐由实际设备查询。夹具资源限制不适合更大 IO 单位时明确失败，不把跳过当通过。
- TCP 使用本机动态端口和 RAII Stop；已启动回调必须先停止再销毁被捕获的栈变量。Store 注册同样有解除保护。fdatasync 门在失败/退出时释放，避免 future 析构永远等待。
- 轮询只判断真实进展，2 ms 轮询和 10 秒截止不是性能指标；极慢/过载机器可能超时，应诊断而非直接宣布生产错误。测试按默认串行执行；故障门是可执行文件内全局夹具，不支持在同进程并行这些用例。
- 首轮沙箱禁止本机端口，按授权在允许 loopback 的执行环境复验。LeakSanitizer 受环境限制关闭，AddressSanitizer 启用；没有 TSan、裸设备、真实断电或性能结论。
- 开发诊断明确分开：变异编译缺 LZ4 include 是 runner 缺项；最后一个 mutation 已触发正确异常，但 runner 文本匹配过窄，改为指定异常类型、定向复跑并复核全部 XML；一次手工定向调用漏设 S5_WORKDIR，按夹具要求报错后显式设置重跑，不计通过；故障测试最初误等 `object_error_`，真实 IO 错误应观察 `error_`，已纠正 oracle；低名额夹具先改为持有全部普通完成票据以稳定构造压力，之后旧 F25 连续 Collect 暴露的真实 Busy 交接问题已在 production 修复。失败均不计通过，不保留这些错误版本的测试压缩包。

## 8. 最终矩阵与证据

| Test | invariant | Oracle | 新 failure mode | 与其他测试重复 | Production pollution | 建议 |
| --- | --- | --- | --- | --- | --- | --- |
| I1 | 原帧最后所有者才退账 | 构造拒绝＋原正文＋恢复读取 | Guard 越过缓存寿命 | 原 F26 未覆盖根预算竞争 | 无 | 保留 |
| I2 | 物化目录/访问寿命计量 | 稀疏拒绝＋映射输入＋真实 IO | 目录只受局部限制 | 不重测编码细节 | 无 | 保留 |
| I3 | 结果保留额度 | 新任务拒绝/再接纳＋原正文 | worker 结束提前退账 | 原 worker 用例只限制工作名额 | 无 | 保留 |
| I4 | 网络压力退让后可交付 | dropped＋接收正文 | 网络绕过共享额度 | 原 codec 测试不能覆盖 | 无 | 保留 |
| I5 | 维护进展类别/Busy 有界退出 | 暂满零删除、解除后删除、重开正文 | 异步类别丢失/维护名额过早复用假设 | 与旧物理回收判据不同 | 无 | 保留 |
| I6 | 已认领 Store IO 先退出 | 实际 IO 门＋关闭/重开 | 新生命周期交接顺序 | 复用底层门，不复制完整关闭套件 | 无 | 保留 |
| I7 | 真错误不上升为“继续重试” | EIO 类型＋可解除来源 | 吞掉持久化故障/错误态无法解除 | 与 Busy 正常退让相反 | 无 | 保留 |
| I8 | 捕获副本独立计量 | 输入模型＋IO 准入/读回 | 原缓存关闭后漏记私有副本 | 不重复帧本体 | 无 | 保留 |
| 旧 39 项 | 原资源、对象、Deferred、页、替换契约 | 原请求与 oracle | 本轮跨模块回归 | 复用原归档 | 无新增 | 保留 |
| 原 35 项 | Store/SQL/checkpoint/共享/增量/压缩 | 原 SQL、会话、恢复和 TCP | 新预算部署破坏已有业务路径 | 没有复制套件 | 仅部署/接口适配 | 保留 |

“保留”指当前模块压缩证据，不注册常驻细节测试。长期主线仍是共同生产 E2E 正确性/性能套件，未来接管同一风险后替换阶段测试，不能简单把通过一次等于永不回归。

当前执行结果以 [结果入口](../../test-results/storage-s12-resource-gc-20261007/README.md) 的最终 XML、命令、mutation 清单及源摘要为准。源包原位替换，其他模块仍有效的依赖包保留，不是应删除的旧版本。

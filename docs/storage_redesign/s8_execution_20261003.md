# S8 执行与测试设计审查（2026-10-03；2026-10-04 复验）

基线 `3a69338`。授权和设计见 [S8 共同方案](s8_raft_object_stores.md)，主方案、F23/F24/F25 及相关交接文档共同引用该协议。本文记录本轮真实实现、验证与限制，不沿用 S7 的通过数。

## 1. 实现及先行逻辑审查

### 1.1 正式调用路径

```text
DistributedNodeConfig.object_storage（显式已格式化设备/空间）
  → NodeStorage.Open：B / allocator / mapping / references / Common
  → RaftObjectStorage.Open + 成员身份绑定
  → RecoverRaftPersistentState
     ├─ LogStore.OpenObjects → 段对象 + B 的 LogManifest
     ├─ StableStore.OpenObjects → B 的 HardState 控制记录
     └─ SnapshotStore.OpenObjects → 候选/正式正文 + B 的 SnapshotManifest
  → 真正的 RaftNode / TCP / BusTubRaftStateMachine
```

普通正文使用原 S7→ObjectIO→F02→F01。B 清单和所有权同批提交，沿原 B/F07；不另建 Manifest 文件、第二份正文 WAL、OSD 集群或设备 IO 池。旧文件后端仍有明确部署消费者，保留；没有自动迁移/格式化/失败回退。

### 1.2 代码逻辑先于测试确认的重点

- [ObjectLogStore::Replace/Publish](../../src/raft/object_log_store.cpp)：保护已提交前缀，候选正文先写，最终增量清单一次提交，RAM 目录提交后交换。尾部追加只在实际对象末尾等于清单末尾时复用；有未发布残留或冲突替换则使用新对象。目录允许对象编号间隙，拼接不猜测编号加一。
- [RaftObjectStorage::Commit/Candidate/Collect](../../src/raft/object_storage.cpp)：Accepted 不是成功；等待 Durable。Indeterminate 保留异常并阻止后续提交。候选创建与编号推进原子提交；重启后的未发布候选退役。维护有检查行数、所有者总数上限，避免一直扫描全部历史。
- [ObjectSnapshotStore::Publish/Stage/Source](../../src/raft/object_snapshot_store.cpp)：完整候选与正式快照分离，发布同一正文对象；完成后重复块仍比较字节。传输源保留整次使用许可，回收检查该许可，单次 IO 再由 F14 保护。
- [RecoverRaftPersistentState](../../src/raft/persistent_state.cpp)：保留 effective_commit、快照 index/term、旧恢复基点和桥接约束。只有完整业务快照覆盖所需提交时，才允许按原 verified-snapshot 路径重建日志基点。对象后端不扫描未发布候选补救损坏。
- [SnapshotInput / stream](../../src/include/recovery/snapshot_stream.h) 和 [业务状态机](../../src/distributed/raft_state_machine.cpp)：原 bundle 格式不变；流读取独立于文件位置或对象位置。消除整份 bundle 的导出/发布复制；真实 A 工作数据库和 canonical 构建仍使用文件，归 S9/S11 接续。
- [DistributedNode](../../src/distributed/node.cpp)：先打开存储和恢复再服务；后台所有权清理不持节点锁等待 IO，异常传播到现有 fatal 状态。停止先结束服务和维护调用，最后由持有关系关闭底层。当前 Raft 同步等待仍在，F35 不提前宣称完成。

审查中还修正了：提交后返回值可能分配导致误走候选清理、维护只限制删除数而未限制扫描量、缺失所有者总量预算、启动线程失败时持节点锁 join、对象部署缺少事务配置校验。删去没有使用者的 enable_shared_from_this 继承；没有增加广泛 catch-and-continue 或自动重试掩盖错误。

## 2. 测试全景与逐项 Oracle

统一源码位于 `test/archives/S8-raft-object-stores.tar.gz` 内 `S8-raft-object-stores/tests/object_stores_test.cpp`，前缀均为 `S8Objects.`。输入经正式 Store / NodeStorage / Direct 文件；不是手写存储模拟器。测试结果及命令见 `test-results/storage-s8-20261003/S8-results.tar.gz`。

### T1 CrossSegmentReplacementAndRecoveryKeepExactHistory — integration / regression

- **目标：** 跨段 entry 完整且历史连续；未提交后缀替换不得修改已提交前缀或让旧尾复活；HardState 单调及同任期投票不变。
- **输入：** 177/2741/555/1850 字节差异正文、1 KiB 段；替换为 3317/73/1981 字节的另一任期内容；非零片段起点、跨多个段、保留段中间前缀。显式提交 2 后拒绝替换 2，再提交 3 并裁剪到快照 2。
- **执行：** LogStore、StableStore→对象事务→B/设备→重开→读取/前缀裁剪→再次重开。
- **Oracle：** 测试输入序列按明确业务边界截取、拼接，逐条比较完整 ReplicatedLogEntry；term/vote/commit 是字面预期。不是拿生产目录或编码器的计算当 expected。
- **失败含义：** 可定位到历史/定位/提交约束；组合测试内断言区分重启前后。无需再为每个片段类型创建机械单测。

### T2 FinalManifestBudgetRejectsBeforeCreatingCandidates — integration

- **目标：** 已知最终原子清单必然超限时，在创建正文候选前拒绝，且原历史不变。
- **输入：** 每块能写，但一次 8700 字节 entry 所需九个段超过最终四操作预算；所有者总预算为十个，足以让无预检的错误版本开始准备整个超限请求，避免被另一项容量检查提前挡住。先有非空前缀；拒绝之后不运行 Collect，连续追加四条不同的合法 700 字节 entry，并重开。
- **执行：** 正式 NodeStorage 配置限制→LogStore.Append→预检；没有改变生产默认参数。
- **Oracle：** 超限请求必须提前拒绝；旧 entry 保持；后续合法请求必须仍能使用未被拒绝请求消耗的候选额度，并在重开后读出准确历史。预期来自公开配置与输入，不读取 Control 根、对象编号或私有计数，也不先清理以掩盖候选消耗。
- **失败含义：** 最后一步才发现永久超限、提前创建候选或改变有效历史。

### T3 CompletedUnpublishedCandidateDoesNotBecomeLatestAfterProcessExit — integration / crash-recovery

- **目标：** 完整且校验正确的新候选也不能成为 Latest；进程退出后从零接收，遗留候选可清理。
- **输入：** 已发布 index 1 的真实 KV 快照，子进程接收完整 index 2 新内容，随后 `_exit`，没有调用析构取消；子进程退出码必须证明接收确实完成。
- **执行：** 真实 FSM→StageChunk→Common durable→子进程退出→Open→LoadSnapshot / Collect；没有以修改私有 manifest 模拟发布状态。
- **Oracle：** Latest 仍是原已发布身份，恢复键值是字面旧值；非零 offset 续传拒绝、孤儿清理数符合一个候选。不是只比较 checksum 自洽。
- **失败含义：** 将“存在完整正文”误当发布，错误恢复续传进度，或缺持久清理归属。属于进程崩溃，不能推论断电/驱动缓存丢失。

### T4 CompleteRetriesCompareBytesAndTransferSurvivesRetirement — integration / regression

- **目标：** 完成后冲突重发仍拒绝，准确重发幂等；整次多块读取结束前不能回收已退役正文。
- **输入：** 旧快照值超过 3300 字节，块 IO 为 1024；读取前 1301 字节后暂停使用，发布另一个多块接收的新快照并清理，再读剩余正文。新候选完成后重发同 metadata/checksum 但第 7 字节不同的最后块，随后重发相同最后/较早块。同一输入/Oracle 还送入保留的文件部署，覆盖它独立的 complete 分支；旧文件回归只覆盖接收中冲突，不能替代这个检查。
- **执行：** SnapshotInput→多次真实 ObjectIO→发布/Collect→继续读取→释放许可→Collect→重开加载新值。
- **Oracle：** 冲突必须拒绝，精确重发必须 DUPLICATE_COMPLETE；读取拼接等于生成时保存的原始字节，新业务值为字面预期。SnapshotBytes 是生产编码，仅用作交付原字节的记录，编码合法性另由既有独立 golden 和真实 KV 值断言覆盖。
- **失败含义：** 完成分支跳过内容验证，或者只保护一个 IO、在两次读之间删除了源。无需 sleep 碰竞态；主动在读间调用 GC。

### T5 DeviceErrorsPreserveAcknowledgedPrefixAndFenceUncertainCommit — integration / fault

- **目标：** 正文写失败不得发布半条日志；Flush 失败不得报成功，不确定提交后必须停止新提交。
- **输入：** 非空 committed entry 后，测试链接器对真实 Direct Data pwrite 注入一次 EIO；重开后再给 HardState 的 fdatasync 注入一次 EIO。
- **执行：** 测试侧 `--wrap`→真实正式设备路径；除指定一次错误外都调用原 syscall，无 production hook。
- **Oracle：** 确定未提交的追加只允许旧 prefix，绝不宽泛接受新条目；不确定 HardState 只允许完整旧或新 term/vote 对且 commit 不变。两次失败调用和后续受阻更新都不能成功。
- **失败含义：** 已确认前缀损坏、丢失故障、提交结果分类错误或错误继续服务。它不直接证明电源故障下设备屏障实现。

### T6 RealSqlSnapshotAndCommittedSuffixRestoreThroughObjectBackend — integration

- **目标：** 真实 BusTub 快照包含数据库/catalog/session，加载快照后还必须重放已提交后缀，重试识别保持。
- **输入：** CREATE/INSERT(id=37)/UPDATE 三个连续请求，在前两条之后快照；第三条保留为日志后缀。
- **执行：** PrepareSql→真实 Apply→对象日志/HardState→canonical stream→快照发布→重开共同恢复→Apply 后缀→SQL 查询与会话分类。
- **Oracle：** 重开并应用后缀后的查询行必须直接等于字面 `real-after`；最近响应须与该请求原响应一致，同一 request/fingerprint 判为 RETRY_LAST。删除不增加失败边界的重启前 SELECT，不再将其输出作为恢复查询的 expected。
- **失败含义：** stream section 偏移或恢复顺序错误、遗漏后缀/session。该测试直接驱动业务状态机，没有 TCP，明确不称 E2E。

### T7 TcpThreeNodesCommitSnapshotAndRestartFromObjectStores — E2E

- **目标：** 真实节点按对象部署完成 SQL 写入、落后节点 InstallSnapshot 追赶、全节点重启、线性读和最后请求重试。
- **输入：** 三份独立已格式化 Direct 文件，通过 C++ 生产配置组装节点，真实回环 TCP；非空 inventory SQL。停止一个 follower，持续不同更新直到 leader snapshot base 超过离线节点原 last_applied，再重新启动 follower。
- **执行：** TCP Client→DistributedNode→Raft 复制/对象三 Store/真实 BusTub；通过已有 status 协议观察追赶和 snapshot base；全节点停止再打开。
- **Oracle：** 必须 COMMITTED；leader 确实裁掉落后节点缺少的前缀，follower 安装快照并应用后缀后查询字面 `tcp-after-9`；重启的线性读同值，重试返回原响应。
- **失败含义：** 正式节点装配、消息路径、快照发送/安装和多层恢复中的组合错误；定位精度低于 T1–T6，保留状态和异常日志定位。三个节点同一进程但通信走 TCP，各自存储独立；不等同跨机故障域。

### T8 RetiredMultiRangeBodyReclaimsIncrementallyAcrossRestart — integration / regression

- **目标：** 可以逐块写成的大快照必须能在同一元数据预算内逐步回收；回收到一半重启不能丢失清理责任；真实后台范围回收必须让旧空间可再次分配，正式快照不受影响。
- **输入：** Data 区显式限制为 64 个分配单位 u；u 由现有设备约束配置取得。旧快照值为 10u+13 字节再加非空前缀，经 StageChunk 每次接收 1 KiB；Raft IO/段上限 64 KiB、F13 单批变更预算 16。创建小的新快照并退役旧版本。随后经正式对象事务每次写 4u，总共保留 56u 字节的不同内容；断言旧快照正文与新正文之和超过 Data 容量，不依赖底层布局推算。
- **执行：** 正式 StageChunk/Common → ValidateSnapshot/PublishStaged → Capture 新版本 → RetainOnlyLatest → Collect(1) → 关闭并重新 Open → 有限次 Collect(1) → S7 自带后台 GC/F14/F12 → SubmitObjects 写新对象 → 再次重开 → ReadObject 与快照 LoadSnapshot。测试不直接调用 ReclaimObject、不查位图或私有对象号。
- **Oracle：** 第一步不能报整个旧正文已删完；重开后恰好删除一个旧正文。更关键的是：在固定容量不变时，56u 新写必须最终 Durable，重开后完整读取等于原输入，仍保留快照的业务值也必须正确。仅 NoSpace 且 NotCommitted 允许在 10 秒窗口内等待后台释放后重试；该窗口不打断单次票据 Wait，runner 对整个场景进程另设 240 秒超时，超时记失败。其他错误、不确定、拒绝准入直接失败。成功依据是实际可用容量、持久内容与存活数据，不是 Collect 返回数或固定 sleep。
- **失败含义：** 整删超限、进度/归属丢失、Store 与后台物理释放未接通，或复用覆盖了存活数据。覆盖的是跨模块空间复用行为，F14 的细粒度引用/位图不重测；不能因此推论长期满盘稳定性或真实掉电。

## 3. Production pollution / 接口审查

| 新增/修改项 | 正式消费者及价值 | 删除测试后是否仍需要 |
| --- | --- | --- |
| RaftObjectStorage Create/Open、显式 options、EnsureIdentity、Collect | 初始化部署、DistributedNode、三个 Store 的同一空间/寿命/回收，设备 namespace 防误接成员 | 是；不是测试 wrapper |
| LogStore OpenObjects/ProbeObjects/RebuildObjects，StableStore/SnapshotStore OpenObjects | 共同恢复选择正式后端；已有 verified-snapshot gate 调用显式重建 | 是；未把私有状态 getter 暴露给测试 |
| SnapshotInput/SnapshotAppend，状态机 stream 虚接口，bundle Read/Write | RaftNode 的发送、接收验证、安装与采集，从路径 IO 解耦 | 是；真实 object 路径无法仅用旧文件 API 完成 |
| SnapshotStore Capture/Input/StagedInput/PublishStaged | RaftNode 使用完整读取许可和同正文发布；文件部署由正式适配保留 | 是；旧 file 方法仍有兼容部署消费者 |
| ObjectMappingSnapshot.PlanTailTrim | Store 有界退役的逻辑裁剪位置，复用 B 前驱查询，正向 Resolve 不能高效给出原尾部边界 | 是；不暴露物理位置或测试状态 |
| ObjectMappingSnapshot.Controls | 正式 B 控制目录恢复和有界清理 | 是；按前缀查范围，没有测试专用 getter |
| NodeStorage.CheckControlBatch | LogManifest/SnapshotManifest 在大正文准备前验证最终提交的静态容量 | 是；仅提交后发现超限不满足既定契约 |
| DistributedNodeConfig.object_storage_（nullopt 保留文件部署） | 显式选择既有设备/空间的正式节点组装 | 是；未改变旧默认路径来迁就测试 |
| 文件 StageChunk 完成分支与 SnapshotStore file 使用许可 | 修正真实重复内容检查，保护既有 Raft 文件传输源 | 是；不是测试专用控制流 |

没有新增 `#ifdef TEST`、暂停点、内部状态 getter、故障开关或 production 对测试的依赖。test 的 socket、fork、syscall wrap 全在隔离源码中。私有 ObjectLogStore/ObjectSnapshotStore 头在 `src/raft`，不把新磁盘编码函数加入公开 API。旧文件兼容方法复用同一 stream codec，未保留两套新 bundle 编码。

## 4. 重复与价值

T1–T5 各覆盖跨段历史、永久预算、未发布候选、整次寿命/重试、真实设备错误；T6 指定 snapshot+suffix 精确边界及 session，T7 覆盖正式网络安装/启动。T7 不注入那些失败，不能替代 T1–T5。T8 增加大正文与小元数据批次之间的容量风险、部分回收重启及受限容量内的物理复用，T4 检查使用许可与冲突重试，二者的失败边界不同。T6 可在共同 C 测试明确覆盖相同恢复边界后合并，现阶段保留在压缩归档，不加入长期常驻 suite。

旧文件 Store 和 RaftNode 回归验证仍支持的后端及共同恢复协议，没有再复制一份同义新测试。片段格式未建立逐字段机械 roundtrip 测试；CRC、std::vector、第三方数据库行为不单独测。测试数不是覆盖率指标。

## 5. 杀伤力与稳定性

变异只复制生产源码到隔离目录、替换一个语句后链接测试；仓库生产文件不被来回改写。必须正常退出码 1 且 XML 中出现目标测试 failure，崩溃、编译失败、超时不能记作“抓住 bug”。逐项结果登记在结果包 `mutations.json`。

| 最小变异 | 对应场景 / Oracle |
| --- | --- |
| entry 首片 offset 改为 0 | T1 非零位置完整字节 / 记录边界 |
| 后缀切点 +1 | T1 替换后重开合法历史 |
| 删除写正文前最终清单预算检查 | T2 提前拒绝及后续合法请求的候选额度 |
| 收完候选就 Publish | T3 未发布候选不能成为 Latest |
| 对象后端跳过完成后重发字节比较 | T4 冲突必须拒绝 |
| 文件后端恢复 complete 分支提前返回 | T4 同一冲突输入在旧部署也必须拒绝 |
| GC 忽略整次使用计数 | T4 两块之间清理后仍可读完整旧正文 |
| 把不确定提交当成功 | T5 Flush 错误不得成功 |
| 恢复为只按 io_chunk_bytes 裁剪，忽略映射边界 | T8 小块接收形成的多映射，不能因 IO 上限较大而单批整删超限 |
| S7 后台跳过 F14 Reclaim，仍允许 Store 完成逻辑删除 | T8 有限 Data 容量内的新写不能成功，证明并未只检查删除计数 |

稳定性约束：输入生成固定；每例唯一临时路径，RAII 关闭线程/节点并清理文件；EIO 标志重置且当前 runner 串行执行。T3 在所有存储线程关闭后 fork，子进程显式退出码+父进程 waitpid，不带继承中的运行 IO。T4 以顺序事件制造窗口，不靠 sleep。T7 预留动态回环端口，最终启动前释放，仍存在外部程序抢端口的环境风险；30 秒状态截止和 socket 超时用来终止并报告失败，短 sleep 仅控制状态轮询频率，不作为成功依据。需要可用 O_DIRECT 文件系统、本机 TCP；不静默回退为 buffered IO 或跳过测试。

首轮有一个测试自身 setup 错误（全新 KV 状态机直接 Apply index 2），修正为从 1 连续应用后重跑；未改生产连续性约束。沙箱 LeakSanitizer 无法完成进程检查，改在授权的本机环境运行；没有将失败记录记成通过。原 PIE 的一个 legacy sanitizer 二进制在测试输出前 -11，空日志；统一与既有阶段 runner 相同的 `-no-pie` 后重验，不能仅凭该现象证明具体根因。最终证据只对应修正后的源码/构建配置；失败现象在此保留原因与处置，不保留错误测试压缩包。

## 6. 最终矩阵

| Test | invariant / Oracle | 新 failure mode | 重复 | Production pollution | 建议 |
| --- | --- | --- | --- | --- | --- |
| T1 | 输入历史精确一致、投票/commit 单调 | 非零位置、跨段、旧尾复活 | 文件回归不是新后端 | 无 | 保留：阶段归档 |
| T2 | 永久超限提前拒绝，合法后继仍可写入并恢复 | 被拒绝请求消耗候选额度 | 无 | 无；已删除控制根观察 | 保留：阶段归档 |
| T3 | 未发布候选不晋升、字面业务旧值 | 进程退出错误选择候选 | T7 不制造该崩溃点 | 无 | 保留：阶段归档 |
| T4 | 冲突拒绝、整次读取完成 | 完成分支漏校验、读间回收 | 无 | 无 | 保留：阶段归档 |
| T5 | 已确认历史不损坏、不确定不早报 | Data EIO、Flush EIO | 复用 S7 保证但增加 Store 边界 | 无 | 保留：阶段归档 |
| T6 | SQL 字面结果 + 后缀/session | bundle 正式流适配 | T7 部分重叠，精确恢复点不同 | 无 | 保留：已简化重复查询，覆盖相同风险后再迁入共同 C 测试 |
| T7 | 三节点真实 SQL/安装/重启结果 | 部署、网络快照、跨层恢复 | 共同 C 可接管 | 无 | 保留：长期目标接入共同 E2E |
| T8 | 有限容量内新写成功，重开新正文及存活快照正确 | 分步回收重启、跨层物理空间未释放 | T4 不覆盖容量复用 | 无 | 保留：阶段归档 |

## 7. 证据、归档与范围

2026-10-04 本轮最终实测：S8 8/8（42.242 秒）；旧文件 SnapshotStore 9/9、业务快照 codec/FSM 4/4、LogStore 10/10、StableStore 4/4、RaftNode 31/31，共 58 个相邻回归；10/10 变异由目标测试 failure 检出。clang 14 Debug + ASan/UBSan/LeakSanitizer，测试可执行文件使用 -no-pie；无跳过项。正式 build、命令、XML、mutations、源码 hash 与资源配置绑定；压缩包不包括设备镜像、二进制、对象文件或解压副本。C1–C5/P1–P4 场景及性能口径保持不变；没有本轮性能结论。

本轮未验证：真实块设备/掉电、长时间满盘与 checkpoint/GC 争用、S9 数组 BufferPool、S10 Deferred、S11 共享/压缩/增量、S12 完整预算/整理、S13 协议异步流水线。CLI 未增加设备初始化/选择参数，已实现并验证的是正式 C++ 节点配置与 TCP 服务。

配置的临时资源为每设备 64 MiB 逻辑文件、4 MiB 元数据区域、8 MiB Journal、32 MiB Data；每个业务 BufferPool 64 页，三个节点各自独立设备；T8 单独将 Data 区限制为 64u，以容量约束证明复用。设备内存/偏移对齐由 F01 实时查询；这些是测试显式预算，不是生产默认或实测峰值内存。源码/恢复说明见 [测试归档索引](../../test/archives/README.md)，结果和校验见 [结果目录](../../test-results/storage-s8-20261003/README.md)。

## 8. 2026-10-03 复审：大正文回收的实际缺口

本节为 2026-10-03 的历史修正；按字节切分尚有缺口，当前协议以 §9 为准。

按用户要求先分析代码：原 `RaftObjectStorage::Collect(limit)` 虽限制检查对象数量，却一次提交整个对象的 Remove。F13 对每段映射生成删除和待回收记录，正文可分批写成，整对象删除却可能超过 `max_update_entries`。节点后台维护会把此异常上报为 fatal；已有小快照场景无法证明大正文可回收。

修正使用已有 Resize/Remove：确认退役且无整次使用许可后，每步裁掉至多一个配置块，保留 Retired 归属；最后才在同批提交中删除正文和归属。持久长度天然保存进度，重启无需新日志/新字段。每步释放的旧范围继续通过 S7/F14/F12 的引用和 IO 保护回收。只是完成部分工作时 Collect 返回 0，调用方不能据此断言没有进展。

T8 在完成逻辑修正后编写；配置小预算并经真实生产写入建立大正文，避免手工构造 B/私有映射。新增“恢复整对象删除”的最小变异验证它能检出原问题。另核对旧文件 StageChunk：旧回归只检查接收中的冲突，未覆盖完成后的分支；T4 以同一输入/Oracle 检查两个后端，并通过恢复文件 complete 提前返回的变异确认杀伤力。本轮没有新增 production public API、默认参数、getter、故障开关或测试控制流。资源上限不代表任意配置组合都保证成功，未加吞异常、无限重试或自动放大预算。

复验和归档结果以本页 §7 及结果目录为准；旧七场景证据不再作为修正后代码的验证，修正后 8 个阶段场景、58 个相邻回归和 9 个变异全部重新执行；当前源码包/结果包原位更新，不保留旧 S8 压缩包。其他有效模块包及共同 C/P 内容保持。

文档复审同时修正 F33/F34 页首仍称 Raft 未接入、F23 仍称当前代码未实现、F25 指向旧标题的锚点；保留明确标为历史的早期状态。当前状态以 S8 当轮完成和 S9–S13 未完成边界为准。


## 9. 2026-10-04 复审：按字节分步仍不能限制映射数量

本节记录同日上一轮修改和实测；当前测试、通过数及归档以 §7、§10 为准。历史 9 个变异及当时 T2/T8 的限制不代表当前版本。

### 9.1 本次新发现及修正

先检查生产逻辑后发现：上轮以 `io_chunk_bytes` 为裁剪大小，但它只限制单次 IO 的字节上限，实际 StageChunk 可以每次只带很小的正文。许多小接收块会形成多条映射；一个 64 KiB 的裁剪范围可能包含十多条映射，仍超过 16 条元数据变更预算。旧 T8 把接收/写块与 IO 上限都设为 1 KiB，避开了这个组合，不能据此宣称任意合法小块接收都可有界回收。

本次生产修改：

1. **F13 新增 PlanTailTrim。** 读取对象长度，复用 B 的前驱查询找最后一条映射，计算最多删除一个尾部范围、同时不超过字节上限的新长度。尾部为空洞时先处理空洞。只输出逻辑长度，不输出物理地址，不进行 IO/释放，不扫描整个对象。
2. **Collect 使用规划位置。** 仍先检查 Retired 与整次使用许可；非零剩余长度提交原 Resize，并保持 Retired 归属；最后 Remove 和归属退出同批提交。进度仍来自持久长度，没有新增格式、GC Journal 或持久游标。
3. **测试替换同一风险的弱输入。** T8 改用真实 StageChunk 分批接收大正文，IO 上限大于正文，再在小元数据预算下回收并中途重开。测试数量仍为 8，没有添加机械组合单测。定向变异恢复上轮“只按字节裁剪”的算法，要求目标测试以实际失败检出。

### 9.2 当时的测试和接口准则复核（T2/T8 已由 §10 接续）

- 新接口由生产回收器使用，删除测试仍需存在；不为测试新增 getter、默认参数、故障开关或条件编译。Harness 的可变 Raft 配置只在测试归档中。
- T8 的业务值和“恰好完成一个退役正文”的预期来自输入/生命周期，不读取私有游标、硬编码对象号或用 PlanTailTrim 自己生成 expected。T4 原有小 IO 配置仍检查一个映射跨多次裁剪，以及许可阻止回收的行为。
- T8 中途为正常关闭重开；T3 才使用 `_exit` 模拟进程退出。二者都不等同于真实断电。T8 不直接证明物理空闲单位已复用，仍沿 F14/S7 的既有证据；不能把 Collect 的完成数当作设备已擦除。
- T2 的控制根比较仍带格式耦合，保留为阶段预检副作用检查，长期共同 E2E 接管后简化。T6 和 T7 部分路径重叠，但前者精确验证快照后缀/session，后者验证真实 TCP 追赶和节点组装，现阶段各有不同失败边界。
- 本次复审不改共同 C/P 的数据和成功口径；完整掉电、满盘长期运行、S9–S13 的测试仍在对应后续阶段。新最终证据需重新执行当前源码，不能沿用昨日哈希通过数冒充本次实测。


### 9.3 本次实际验证与归档

2026-10-04：修正版 8/8、相邻回归 58/58 全部重新执行通过；9/9 定向变异由目标测试 failure 检出。恢复上轮按字节裁剪的 `bytes_only_retired_trim` 变异实际抛出 `object update exceeds metadata budget`，没有把编译失败、崩溃或超时算作命中。ASan/UBSan/LeakSanitizer 开启，无跳过；S8 场景 39.157 秒只记录测试耗时。

源码/结果包原位替换为本次版本，489 个生产及回归源码哈希与当前文件绑定；旧 S8 包不保留备份，其余有效阶段包保留为各自版本的历史证据。F13 新查询的当前验证由 S8 T8 负责，未将历史 F13 通过数当成本次新增能力的测试。确认归档后删除本轮构建、设备镜像、展开源码与变异产物；未 commit。

## 10. 2026-10-04 测试契约接续：现在补齐，不推迟给未来测试

### 10.1 先行逻辑分析及修改范围

用户授权解决复审指出的测试缺口。先确认生产路径：候选容量由正式 RaftObjectOptions 限制；永久清单预算在创建候选前检查；Collect 只退出对象映射/归属；S7 CommitLoop 独立调用 F14 Reclaim，再由 F12 释放物理单位。明确 NoSpace 来自预留失败、结果为 NotCommitted，允许测试在后台回收期间有限重试；不确定提交和其他错误不允许重试。

本轮不修改生产代码/API/默认路径。T2 删除控制根观察，改为公开额度下的后续合法追加；T8 在同一场景内增加实际物理复用及重开内容判断，没有复制一套底层 allocator 测试。T6 删除重复的重启前查询，恢复值直接与字面预期比较。T6/T7 仍分别维护精确 suffix/session 恢复和 TCP 快照追赶；不能因为经过相同函数就删除其中一个。

### 10.2 八项审查与长期接管

测试全景、输入/路径/Oracle/失败含义见更新后的 §2；接口污染仍按 §3，无新增 production 测试接口。§4/§6 区分场景重叠与同一风险重复。新增跳过物理回收的隔离变异，必须在 T8 容量断言失败；编译失败、崩溃、超时仍不算检出。T2 原有去掉预检的变异继续运行，验证不读取内部根仍能发现错误。

T8 后台回收等待以正式操作成功为条件，仅对明确未提交的 NoSpace 在 10 秒窗口内轮询；单次票据 Wait 不受此窗口中断，整个场景进程由 runner 的 240 秒超时限制，超时不计通过或变异检出。sleep 仅控制尝试频率，不是正确性依据。上层 Store 删除和下层范围释放分别通过不足以证明连接正确，本轮通过真实调用补齐这一点；不扩展为无限循环压力测试，也不改变共同 C/P 的输入、成功与性能口径。

未来共同 E2E 接管时，必须同时覆盖同一触发条件、Oracle 和故障边界，才可退出对应阶段案例。阶段测试继续仅压缩归档，不新增长期常驻细节测试；T2/T8 当前缺口不能用未来承诺抵销。

### 10.3 首次失败归因与复验

首次补强测试将 56u 全部作为一次事务写入，在后台尚有碎片时实际分为 8 个 extent，映射/分配/所有权记录超过 16 项。gdb 栈定位在 ReplaceChanges→Changes::Put；失败属于测试把“大总量复用”误设成“小元数据预算下的大单批事务”，不是回收未运行。诊断 gdb 运行不计验证通过。修正为每次 4u、累计保留 56u；固定总容量、旧+新超过容量的断言及原 16 项预算保持不变。未吞掉 ResourceUnavailable，也未放宽生产限制。首次失败不归档为最终通过证据。

第一次 T2 公开额度输入只允许两个对象，删除 Manifest 预检的变异仍通过：更早的所有者预算检查已将请求拒绝，测试没有到达目标分支。先分析 Replace 的检查顺序后，将公开额度设为十个，足够准备该请求；随后连续四个合法小追加必须成功且重开历史准确。这样不读取内部根，同时隔离“清单预算检查过晚”这一风险。变异存活不计为检出，最终需重新执行。

修正后实测：8/8 阶段场景（42.242 秒）、58/58 相邻回归、10/10 定向变异全部通过相应判据，ASan/UBSan/LeakSanitizer 开启且无跳过。`skip_physical_reclaim` 实际只能写入 81920/229376 字节，触发容量断言；去掉清单预检也仍由新版 T2 检出。生产源码与原 489 项绑定哈希完全一致，本轮没有生产修改。

最终源码/结果包原位替换，包内包含命令、XML、变异源码/结果、哈希和源码快照；只保留当前版本，没有旧 S8 备份。首次失败的原因在本节保留，gdb 诊断单独标识、不计通过。完成逐项校验后删除本轮构建、失败/成功的展开目录、设备镜像及变异二进制。共同 C/P 未改，不注册常驻阶段测试，未 commit。

## 11. 2026-10-04 再审：一致性修正与证据复核

本次先复核生产逻辑：F23 在 Candidate 前预检最终清单；Collect 保留退役责任并分步裁剪，实际范围释放仍由 S7 CommitLoop→F14→F12 执行。T2 的十个所有者额度足以到达清单预检，四个合法后继追加验证拒绝未消耗额度；T8 以 64u 容量、累计 56u 新正文、旧+新大于容量和重开后内容比对验证真实复用，无私有 bitmap/Control 判断。

按主方案八项要求复核：测试全景及逐项 Oracle 沿 §2；本次没有生产/API/测试执行代码改动；T6/T7 的恢复后缀与 TCP 追赶触发不同；T2/T8 的两个关键变异均实际命中目标断言；固定输入、独立设备/临时路径和 cleanup 保持，TCP 端口竞争与执行时限仍属于环境限制。风险接管仍须等价触发、Oracle 和故障范围，不能用经过同一函数推定替代。

本次实际修正的是文档：主方案页首及 F23/F24/F25 残留的 9 个变异更新为 10；主方案更新时间更新；明确 §9 是历史版本；澄清 T8 的 10 秒是 NoSpace 重试窗口而非强制取消 IO，runner 另有 240 秒进程时限。归档 RESTORE 和结果说明同步，不增加生产取消接口或额外测试。

核对 489 项生产/回归源码哈希、测试源码/runner 哈希、8 项场景和 58 项回归 XML、10 项变异的指定失败。测试和生产未变，因此本次复用 §10 已有实测证据，没有重新执行或冒称新通过。源码包只修改说明及对应哈希，结果包更新文档快照和校验；执行日志/XML 保持原字节。仍只保留最终 S8 两个压缩包，无构建/展开测试/设备镜像，未 commit。


## 12. 2026-10-04 提交前复核

先复核 F23 最终清单预检与提交后目录切换、F25 候选发布及整次读取许可、HardState Durable 确认、节点关闭/维护，以及 Collect→PlanTailTrim→S7/F14/F12 的分步回收。未发现需要再次修改生产逻辑的阻断问题。

本次实际改动限于文档与归档说明：主方案风险接管表中的变异数由 9 修正为 10；阶段说明明确 S8 已接入，A 页及业务恢复点分别仍待 S9/S11；F26 不再写 S4/S5 尚未实现，并明确当前 A 使用原 BufferPoolManager、B 使用独立版本页访问，共享的是 B+Tree 算法。数组子方案的共享示意是待冻结的目标，不能当作 B 已使用 A 缓存或自动迁移的证据。未执行 S9 方案。

再次核对八项测试审查：T2 通过公开预算和后继合法请求判断，T8 通过有限容量的新写与重开正文判断，均不读取内部布局作为 expected；T6/T7 的后缀/session 与 TCP 安装故障范围仍有区别。无新增 production 测试接口或常驻阶段测试。489 项生产/原回归及可执行测试/runner 哈希与 §10 实测版本相同，复用 8/8、58/58、10/10 的原始执行证据，没有重新运行或计入新通过数。

源码包与结果包逐成员校验，文档快照同步；执行日志、XML 和变异源码保持原字节。当前只保留一份 S8 源码包和一份 S8 结果包，不保留旧 S8 包、展开测试、构建和设备镜像；其他有效模块包保留。结果包依现有 .gitignore 留在本地，Git 提交包含源码包、生产改动、方案和结果摘要。

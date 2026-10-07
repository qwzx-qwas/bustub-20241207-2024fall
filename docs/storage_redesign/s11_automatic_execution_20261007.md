# S11 / F25 自动增量追赶与重复工作整改（2026-10-07）

基线：`3a298eb`。本轮由用户授权修订方案、实现、测试与审查，未授权本轮提交或推送。共同协议见 [增量方案 §7](s11_incremental_snapshot.md#7-本轮自动追赶与去重接续已授权)。先检查生产控制流、持久引用和失败顺序，再增加测试；本记录不把旧轮实测当作新代码结果。

## 1. 实际改动

| 位置 | 改动及原因 |
| --- | --- |
| ObjectSnapshotStore::Publish / Open / Encode | 清单 v2 增加可选传输基础；兼容读取 v1。只保留可用于增量的 native 基础，canonical/KV 不额外保留无用正文。基础随下一次发布轮换，不参与恢复日志下界。原子更新清单与对象所有权；运行期 lease 继续保护已退出角色的传输 |
| OfferDelta / DeltaPlanCache | 先确认基础、后计算映射差异；同一目标共享一份不可变计划。按正文节省、描述及额外消息成本筛选，净节省至少 20%；这是工程初值，未声明预测实际延迟 |
| RaftNode::SendSnapshot / CreateSnapshot | 固定开始时的 latest；缓存一个未确认块避免重读。新 checkpoint 本身不重置会话；正常日志下界越过目标才取消，避免无限保留 bridge log |
| RaftStateMachine::PrepareSnapshot / PreparedSnapshot | 生产接收路径只建立一次候选，业务验证成功后保有候选，发布/HardState/log 边界完成后 Install。取消不改活动状态；准备阶段已分配状态机，安装阶段交换所有权 |
| WriteSharedSnapshot / Crc32cCombine | 一遍正文读取同时得到内部/外部校验；组合 CRC 不改变已有 native 格式。全量接收只累计首次接纳正文，不把重复块重复计入 CRC |
| BuildSnapshotState / SnapshotInput::ReadInto | 读入页时同时累计内部校验，复用 F02/F26 缓冲许可写入候选帧；文件输入保留兼容路径。不是所有来源都零拷贝，也不把物理活动页改写成本说成已消除 |
| CatalogSnapshotCodec::Restore | 创建索引结构后，一次表扫描进行时间戳/删除检查并填充所有索引；保留主键唯一性，向量索引保留批量 Build 行为。Catalog 的恢复开关为私有，普通 CreateIndex 行为不变 |
| NodeStorage::ShareObjectRangeBatched | Clone、快照生成、增量完成统一使用有界范围引用提交；规划容量拒绝才缩小批次，不重试持久化结果不明确的操作 |
| Clone / WriteSharedSnapshot | 从现有映射跳过空洞、枚举有效范围，减少高水位页号逐个查询；相邻可共享范围合批，不另建地址表 |

**保留的工作不是遗漏：**发送端与接收端分别检查自己使用的内容；恢复清单与运行期 lease 职责不同；已发布恢复内容在 Open 的完整性检查仍保留。当前 snapshot native 页面进入活动工作空间仍可能写出新物理页，ReadInto 只减少中间缓冲复制。F35 才负责通用协议异步化/多在途日志，S12 才负责通用后台预算；没有借本次测试提前实施这些阶段。

## 2. 方案一致性和逻辑审查

- 日志足够仍优先 AppendEntries；新传输选择当时最新的完整快照，已经开始的传输固定其目标，直到完成或按日志下界规则取消；增量不逐代安装旧 checkpoint。
- `previous` 是恢复入口，`transfer_base` 仅保留正文；`OldestRetained()` 不返回后者。清单持久引用可跨重启，传输 lease 不承担重启恢复。
- 最多一个额外 native 基础，受既有快照大小/所有权额度限制；没有后台长期复制三份正文。v2 不支持旧二进制降级打开。
- Offer 被接受但随后收益不足时，全量首块沿现有取消路径退役未用候选，不把未发布内容当恢复点。
- 先构建/验证候选，再发布与推进持久边界，再安装同一候选；发布后失败仍 fail-stop，通过已有恢复链重新进入。
- 部分范围引用提交可能留下候选前缀；候选所有权负责取消/重启清理，不伪称整个大共享请求是单事务。
- 数据页内容、索引和 Session 的原约束保留；内部 CRC 与外层 payload CRC 覆盖范围不同，仅合并遍历，不混用值。
- 未增加生产测试分支、默认测试路径或为方便测试放宽额度。

## 3. 测试全景和 Oracle

现有 F25 6 项与增量旧 5 项保留；本次新增 4 项。F28 7、S8 8、F27 12 从独立有效包复用；已有 Catalog 5 项直接复验，不复制源码或重复归档。

| 本次 F25 测试 | invariant / 独立判据 | 类型与建议 |
| --- | --- | --- |
| SharedPagesSurviveRootRetirementAndRestartThenInstallWithSessionsAndSuffix | 实际输入的 SQL 行、Session 结果、后缀和新表在 GC/重启后仍正确；正文不能被额外复制 | integration，保留 |
| TransferLeaseOutlivesSnapshotAndCheckpointRetirement | 四代轮换后，清单已不持有旧基础，已有传输仍可读；退出后观察到对象回收，具体物理释放沿用相邻验证 | integration，调整到跨越新传输角色，保留 |
| CorruptNativeBodyAndWrongBoundaryCannotReplacePublishedState | 故意损坏正文/错误边界必须失败，原业务仍可查询 | regression，保留；故障输入覆盖其全部读取合同 |
| CanonicalCompatibilityAndNativeInstallUseExistingFileDeployment | canonical 及 native 格式均能恢复独立已知业务值 | integration，保留 |
| DeferredPageSharingKeepsBytesThroughSourceLocationSwitch | Journal→Data 来源切换不改变固定快照字节 | integration，保留 |
| ReclaimOnlyFencesDeadUnitsWhileSharedMiddleRemainsReadable | 存活共享中间范围可读，已无用范围可以释放 | integration，保留，不复制底层位图测试 |
| FullAndDeltaRestoreSameSqlSessionSuffixWithLargeAllocationAndRetiredBase | 全量和增量都符合独立 SQL/Session 预期，删除不复活，大分配单位下不复制 REUSE 正文 | integration，保留；全量对照不是唯一 oracle |
| WrongBaseConflictingRetryAndDamagedReuseNeverPublish | 错误基础/冲突重发/损坏复用不能发布，也不能替换当前业务 | regression，保留 |
| UnpublishableDeltaRejectsBeforeAllocationAndFullStillWorks | 已知提交预算不够时不先耗尽对象容量，随后合法全量仍可成功 | integration，保留；不查询内部分配计数 |
| InterruptedCandidateRestartsFromBaseAndRejectsOldSession | 重启只选择已发布基础，旧 session 不能接着写新候选 | integration，保留 |
| TcpOfferAndDeltaInstallThroughRealRaftFollower | 正式 RPC 下的旧消息/续期/新 term 隔离，安装后接续日志 | TCP integration，保留；与自动选择测试的故障边界不同 |
| PreparedCandidateIsBuiltOnceAndCancellationKeepsActiveDatabase（新） | 取消候选不改旧表；正文只读取一遍；准备后禁止源读取仍可安装并查询独立预期 | integration，保留 |
| AutomaticLeaderCatchupUsesRetiredBaseAndKeepsTargetAcrossPublication（新） | 真实 Leader/两 Follower/TCP 自动选择旧基础；日志已裁剪；传输中发布新代仍保持原目标；追加真实业务命令后重启，由 Raft 构造恢复后缀及 Session | TCP/数据库 integration，保留；不手造 Offer/Install/vote/Append |
| LowBenefitNegotiationFallsBackToCompleteLatestImage（新） | 有少量共享也拒绝低收益计划；无基础不接受增量；已接受候选可退出并全量恢复 latest；v1 清单可读取 | integration，保留；v1 编码样本仅作为阶段格式兼容证据 |
| RestoreBuildsScalarAndVectorIndexesAndSkipsDeletedRows（新） | 真实 SQL 创建主键/次级/IVF/HNSW，恢复后删行不复活，标量及两种向量查询返回独立已知行 | integration，保留，覆盖合并扫描新增风险 |

### 新场景的输入 → 路径 → 失败含义

1. **候选复用**：用 180 行稳定内容和修改/删除/新增记录生成真实快照；接收端先有另一张表。经 `PrepareSnapshot→BuildSnapshotState→BufferPool/F02/F01→Catalog Restore`。输入包装只观察真实读取并在准备后禁止再次读源，不替换数据库。行结果为独立预期；读取字节数针对本项目“合并正文遍历”的承诺。失败可定位重复读取、提前替换或安装错误。
2. **自动追赶**：真实三节点，初始化夹具准备已有数据、基础及日志缺口，随后测试控制离线链路和逻辑时钟。生产逻辑自行发 Offer/Install/Append。观察正式网络消息的目标、DATA/REUSE 字节，真实 Propose 业务后缀，重启由 Raft 构造应用；最终检查 SQL、进度及 Session。发送字节减少证明该场景用了增量，不是通用性能结论。
3. **低收益回退**：两个小表只有少量固定页可共享，另一个发生更新。真实 Store 协商后计划拒绝增量，已有候选退出后发布完整 latest。检查业务结果与恢复身份；兼容夹具按历史 v1 编码构造，不用新解码器计算 expected。
4. **索引恢复**：一个含标量和两列向量的表，建立多种索引后删除/更新。经生产 SQL、固定快照、准备与安装、查询。答案来自显式插入的行及距离关系；主键重复拒绝另外复用已有 Catalog 测试。

## 4. Production pollution / 接口审查

- `PrepareSnapshot/PreparedSnapshot`：Raft 发布前准备、发布后安装的真实寿命交接；删除测试仍需要。旧 Validate/Load 继续服务兼容和启动调用。
- `OfferDelta`：实际生产先协商后规划，持有固定使用权；计划缓存在 Store，不是测试缓存。旧 PlanDelta 两层转发已无生产调用方，删除，测试改用正式协商接口。
- `ReadInto`：实际 native 恢复借用 BufferPool 帧并持有保护，非暴露帧内部状态；回退适配属于文件后端兼容。
- `ShareObjectRangeBatched`：三个生产消费者复用的有界共享构建，承担分批/失败处理，不是空转发层。
- `CatalogSnapshotCodec::Restore` 的验证回调：本地 checkpoint 和快照恢复共用单次扫描；恢复标志私有，未把内部函数改 public。
- `SharedSnapshotResult/Crc32cCombine`：生产校验遍历去重所需的返回结果与计算；没有测试 getter、测试参数、`#ifdef TEST`、生产路径替换或关闭校验。

## 5. 重复、有效性和稳定性审查

- 自动追赶覆盖选择和三节点日志缺口；原手工发送 TCP 场景覆盖迟到 Offer、旧会话、续期、新 term，不互相替代。
- 候选复用检测已准备内容的生命周期及重复读；多索引场景检测另一条新增风险，不按同一输入重复命名。
- 仅为满足收益筛选而扩大的稳定正文，使用固定、可区分的真实记录。按 30 行分批写入，发送端日志段使用合法 16 KiB 配置，避免课程夹具的 1 KiB 小段迫使批量裁剪超出既有 32 项额度。未修改生产上限。
- 一次兼容夹具把版本号写成小端，导致 unknown manifest；按项目大端格式修正测试。原样保留诊断，未改生产以迁就错误测试。
- TCP 使用动态端口、条件变量和有界等待；选举随机源固定；每个 fixture 独立设备/目录；RAII 停网络、清候选。存在本机端口获取后再监听的微小竞争及负载下超时风险，不能宣称跨机/真实断电/TSan 已验证。
- 阶段 `pwrite` 包装、输入故障包装和网络观察均只在测试侧。真实设备和数据库路径未被空参数或假结果替代。
- 常驻共同 C/P 内容与旧性能基线不变。全部模块测试只保留最终压缩包，下一阶段由共同 E2E 接管时删除重复阶段断言。

## 6. 实测与清理

测试配置为 Clang 14 Debug、ASan/UBSan/LSan、Direct 普通文件及 loopback TCP。最终默认阶段复验 **42 项通过**（`runs/clean-final/deferred.xml` 12 项、`checkpoint.xml` 30 项）；本轮直接复用已有 Catalog **5 项通过**（`runs/catalog.xml`），共 **47 个不同场景**。上轮脚本记录 18 个隔离变异失败（`runs/mutations`），其中更换目标一项实际依靠进度超时，证据不足由下文 §7 替代；删除无用 PlanDelta 转发后，另复验其中 5 个相关变异（`runs/mutations-final`），不重复计数。Catalog 测试不经过该删除接口，无须为接口清理重复运行。

最终源码只在 `test/archives/F25-shared-snapshot.tar.gz`；结果入口为 [README](../../test-results/storage-f25-s11-20261006/README.md)。同名旧 F25 源码/结果包原位替换，无旧包备份；有效的其他模块依赖包保持不变。结果包保存最终日志/XML、命令、变异及过程诊断、当前生产源码和 diff，不保存执行文件、设备镜像或对象文件。本轮 `/tmp/f25-next` 在归档核验后删除；结果 tar 沿现有 gitignore 本地保存，没有新增常驻小测试或默认生产路径。本轮未 commit/push。

变异覆盖：跳过 native 校验、发布额度预检、Session 结果、内容身份、无复制共享、增量重建 CRC、基础 lease、旧 session 拒绝、重复正文比较、旧 Offer 拒绝、旧全量隔离、协商续期、新 term 编号重置、独立基础保留、收益阈值、固定传输目标、候选安装、合并正文遍历。要求隔离编译和链接成功，指定场景正常报告对应失败；进程崩溃或超时不能替代判据。上轮更换目标一项未满足直接判据，本轮按 §7 修正。

限制：缓存重传减少实际设备读取、引用合批对尾延迟的影响和成本阈值优劣仍需要正式性能评测。输入包装统计的是快照逻辑正文读取次数，不冒称实际设备 IO 次数。

## 7. 再次设计审查：真实业务后缀与直接失败判据（2026-10-07）

### 7.1 先审逻辑发现的两项证据不足

1. **原自动追赶场景的恢复后缀主要是选举屏障，重启后由测试手工 Apply。** 这能证明内容接续，不能独立证明生产节点会自动重放实际业务后缀。正式路径是 `RecoverRaftPersistentState → RaftNode 构造 → ApplyCommitted`；生产代码已具备该路径，本次不修改它。
2. **原更换目标变异依靠进度等待超时才失败。** 上轮脚本确认了 GTest 非零退出，但失败内容是 `real Raft exchange made progress`。因此上轮“18 项均为直接断言、没有超时判据”的说法不准确；其中这一项旧证据不足，本次用直接身份判据替代。进一步沿控制流核对发现，原变异只扩大会话重建条件，却仍沿前一行取旧目标，实际反复重建旧会话；本次同时改坏目标选择和会话重建，才真实模拟中途切换到最新目标。其余异常型失败分别是重建校验不符、旧对象已删除、Apply 不连续，与对应破坏有关，不是进程崩溃。

### 7.2 实际改动与风险归属

只修改归档中的 `AutomaticLeaderCatchupUsesRetiredBaseAndKeepsTargetAcrossPublication` 及变异 runner：

- 原有三节点追赶成功后，经真实 Leader `Propose` 提交更新，将业务值 112 改为 193，等待正常多数派提交与 Follower 应用。
- 停止节点并重开真实存储；明确此时只恢复快照边界，再构造 RaftNode，由它自动重放已提交后缀。测试删除手工 Apply 循环，检查应用进度、值 193 和客户端重试返回 `RETRY_LAST`。
- 在测试观察到正式 Offer 和 InstallSnapshot 消息时立即检查目标身份，移除末尾重复检查及收集所有目标 ID 的 vector。变异同步改坏目标选择与会话重建。故意中途换目标必须报出 `snapshot target changed during ongoing transfer`，不接受普通进度超时为检出。
- 增加一个隔离变异：跳过 RaftNode 构造时的 ApplyCommitted。要求恢复进度断言失败，检查测试能发现“已提交但启动没有应用”的真实风险。

目标 → 输入 → 路径 → Oracle → 失败含义：给真实数据库提交非空 SQL，业务答案 193 来自测试输入；请求 89/1 的去重结果来自一次已提交请求的约定。路径经过现有 SQL 准备、Raft Propose/TCP、多数派提交、对象存储、重启恢复和 Raft 构造。失败能区分中途换目标、未应用后缀、业务结果或会话状态错误。未引入测试专用生产接口，也没有改变默认路径、容量或算法。正常测试函数数量不变；新增变异不另增重复正常场景。

### 7.3 复验和范围

本次增强后的 **1 项正常场景通过、4 个定向变异检出**，仍使用 Clang 14 Debug、ASan/UBSan/LSan、Direct 文件及真实 loopback TCP。证据位于结果包 `runs/design-review-20261007/normal` 和 `mutations`，保存实际命令、XML 和失败内容：

| 变异 | 本次失败依据 |
| --- | --- |
| 不保留 transfer_base | 协商基础不是预期旧版本，REUSE 为零，发送量不小于完整快照 |
| 中途替换传输目标 | `snapshot target changed during ongoing transfer`，约 10.5 秒内明确失败，不依靠 30 秒进度超时 |
| 跳过 prepared Install | 接续 Apply 不连续，说明已发布但未安装候选 |
| 跳过启动 ApplyCommitted（新增） | 恢复进度 13 而预期 15，值停在 112 而预期 193，Session 未进入 RETRY_LAST |

生产 460 个文件、7 个有效依赖包及复用 Catalog 测试的哈希与上轮一致。本次没有修改 production 或新增测试用 API；其余 46 个正常场景沿用未改动时的有效结果。当前证据合集为 **47 个正常场景、19 个不同变异**：正常场景 46 旧＋1 新，变异 15 旧＋4 本次；不是本次全套重跑。原目标变异及两个未获直接判据的尝试只保留为诊断，不计成功。

源码/结果包均原位更新为唯一版本，旧包不保留备份。完成哈希核验后清除 `/tmp/f25-review-20261007` 的构建、展开测试、执行文件和设备镜像。本次未 commit/push。

需要保留的证据边界：此场景使用真实 RaftNode/TCP 和数据库，但初始化通过已有 Business 夹具准备，未经过 CLI/客户端网关；低收益回退目前是 Store 组合测试，不等同低收益策略在真实 Leader 下的网络 E2E。合批减少多少 IO、20% 阈值是否合算，仍待性能评测。没有为了本次审查增加第三方库行为或位图内部布局测试。

## 8. 接续复审：所有变异必须有明确失败依据（2026-10-07）

### 8.1 先检查生产逻辑

本次未修改生产代码，逐项核对以下实际路径：

| 方案约束 | 代码依据与结论 |
| --- | --- |
| 额外基础不拖住恢复日志 | `ObjectSnapshotStore::Oldest` 只选 previous/latest；Publish 把退出的角色与 Manifest 同批更新，Source 的 lease 覆盖读取间隙 |
| 开始时选 latest，进行中固定目标 | `RaftNode::SendSnapshot` 复用会话目标和未确认块；`CreateSnapshot` 只在恢复日志下界越过目标时取消。修正本记录 §2“目标始终最新”的歧义 |
| 失败候选不替换活动业务 | `PrepareSnapshot → PublishStaged/HardState/log → Prepared::Install`；发布前失败取消候选，发布后失败 fail-stop，保持原恢复协议 |
| 索引去重不绕开验证 | `CatalogSnapshotCodec::Restore` 一次扫描先检查行时间戳，再跳过删除行并填充各索引；主键重复仍报错，IVF/HNSW 保留 BuildFromEntries |
| 借用帧不丢寿命保护 | `SnapshotInput::ReadInto → ObjectSnapshotStore::Source → NodeStorage::ReadObjectInto` 传入 owner；BuildSnapshotState 的 WritePageGuard 保持到实际读取与校验完成 |
| 共享批次只重试未提交拒绝 | `ShareObjectRangeBatched` 对规划容量拒绝缩小批次；不把失败或不确定的持久化当成功。三个生产消费者沿同一路径调用 |

未发现需要新增生产接口或改变本轮存储协议的问题。`PrepareSnapshot`、`ReadInto`、批量共享及 CRC 组合都有实际生产调用方；正常业务创建索引不进入恢复模式。上轮新增索引查询会经过现有标量/向量优化与执行路径，不靠假数据库返回预期结果。

### 8.2 新发现与实际修改

归档的 `run_mutations.py` 仍有 **11 项 marker=None**，另外三项只匹配通用 `throws nothing`。前者会接受同一场景中的任意失败；例如网络进度超时或无关夹具异常也可能被误记为检出。脚本还只检查“有一个测试运行”，未核对它是否是指定测试。现存原始结果没有因此失效，但未来运行的判据不符合本项目要求。

本次只修改该脚本：

1. 19 项变异全部指定与各自 bug 对应的失败内容；填补 11 项空判据，收紧 3 项通用判据。
2. `check_mutation_result` 同时核对测试全名、确实运行、没有 skip/error，并要求命中该项失败依据。
3. 外部进程超时/异常退出继续由原 runner 拒绝；场景内部的 `real Raft exchange made progress` 超时也不能被算作检出，即使它之前已有其他失败。
4. 没有新增 C++ 场景、变异种类、生产 hook、默认参数或常驻小测试。上轮实际业务后缀和直接目标核对保持不变。

这不是保证任意错误都能发现；它保证“编译/链接成功后，指定场景因预期风险失败”才能记入本阶段的变异证据。格式兼容输入、正文读取次数等阶段断言仍按原计划在共同 E2E 接管后去重。

### 8.3 验证与证据边界

**本次没有重编译或重跑 C++ 场景/变异。** 使用修改后 runner 的同一个判定函数，重新读取结果包中当前选定的 19 份原始 XML，全部符合更严格判据；同时用 10 种无效报告输入及已废弃的真实目标超时 XML 检查误报，11 项均被拒绝。输入包含错误测试名/套件、未运行、skip/error、无失败、无关异常、进度超时以及多余测试；这些是临时的判据核验，不能计作新增数据库测试。

证据为结果包 `runs/verdict-review-20261007/verdict-check.json` 和其核验脚本。当前仍是 47 个正常场景、19 个变异的既有有效证据；§7 的 1＋4 是上一次实际运行。本次核对生产/依赖/既有 Catalog 测试 468 个哈希，并确认两份 C++ 阶段源码及正常 runner 未变。没有新的性能、真实掉电、裸设备或 CLI E2E 结论。

源码/结果包再次原位替换，旧同模块包无备份；临时展开目录核验后清理。未 commit/push。

## 9. 提交前复核（2026-10-07）

用户本轮已授权：复核无误后提交当前工作，下一阶段先讲解、等待确认。本节接续上述历史审查；不新增生产实现或阶段测试。

- 先核对实际逻辑：传输基础不参与恢复日志下界；每次传输固定目标和使用权；候选只构建一次，发布与 Raft 持久化完成后安装；失败保持既有取消/停止服务与重启恢复边界。Catalog 合并扫描仍验证记录并重建各类索引，读取帧的持有范围覆盖实际 IO。本次未发现新增生产缺陷。
- 修正主方案 §0 的过时进度摘要：跨节点共享、增量接收和自动追赶已经完成，剩余压缩及 S12/S13 不能一起笼统标为“跨节点优化未完成”。有日期的历史结论保留原范围。
- 核对当前代码、依赖及归档测试共 473 个哈希和结果包 815 项校验；当前源码与证据对应。原始 XML 中 47 个不同正常场景全部通过；再次使用归档 runner 的判定函数核验 19 个变异，均由指定场景命中对应失败依据。本次不重编译、不重跑 C++，没有新增性能或设备故障结论。
- 测试仍保护发布、保留、重试、接续恢复及去重承诺；没有新增测试专用生产接口或常驻细节测试。真实 TCP 自动追赶、Store 低收益回退、候选寿命与索引恢复的失败边界不同；其证据范围、阶段实现耦合和共同 E2E 接管要求保持 §3–§8 的说明。
- 同模块源码与结果仅各一份当前压缩包，旧包没有备份；此前三个临时目录均不存在。有效依赖包继续保留。结果包沿现有 gitignore 本地保存，测试源码包、校验清单和审查说明随代码提交。

本次仅修正文档现状与提交前核验记录，并更新结果归档中的对应说明。下一步仍为 S11/F25 压缩协议讨论；不在本次授权中执行。提交身份以 Git 记录为准，不把提交自身的哈希写入待提交文件；不 push。

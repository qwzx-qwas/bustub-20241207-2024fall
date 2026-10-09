# S12 正式对象部署：实现与测试设计审查（2026-10-09）

## 1. 实施范围与结论

执行协议为 [S12 正式对象部署](s12_node_deployment.md)，基线提交 `87f4357`。本轮没有实现 S13，也没有修改原共同场景的业务输入、内容模型、故障或重试判据。节点入口和共同部署适配已经接通；验收存在失败，不能将 S12 标为全部完成。

### 实际修改

| 修改 | 原问题及现在的行为 |
|---|---|
| `tools/bustub-node/storage_config.*`、`bustub-node.cpp` | CLI 以前没有对象部署入口；现在解析显式配置，调用现有 NodeStorage/RaftObjectStorage。initialize 成功即退出，默认 Open，不创建或截断设备。inode 文件锁贯穿整个节点寿命。 |
| `ObjectTransactionPipeline::GarbageLoop`、`ObjectMappingAccess::MaintainMetadata` | 正式节点此前没有周期 B 写回/checkpoint；现有后台角色现在按配置批次写回、按轮次 checkpoint，不另建线程池。 |
| `MetadataEngine::Writeback/Checkpoint` | 已有维护竞争用 MetadataCommitBusy（继承原 ResourceUnavailable）表达；没有新提交时跳过重复 checkpoint。真实持久化错误保持原有语义。 |
| `ObjectTransactionPipeline::CommitLoop` | C1 首次执行发现 checkpoint 暂时排除提交被误当作失败；保留已完成的数据依赖，只重试未接纳的元数据发布。 |
| `ObjectPageStorage::Create`、CLI 初始化 | 长快照追赶发现直接分配工作空间绕开上述队列；对未接纳的空间申请补上同样的 Busy 等待，成功后不重复分配。 |
| `DataAllocator::Usage`、`MetadataEngine::CacheStatus`、`NodeStorage::Usage` | 提供正式运维统计，直接读取原位图/缓存所有者，不另存分配真相。 |
| 客户端协议、节点 HandleStatus | storage STATUS 使用新请求 type 5；普通 type 3 和旧客户端空正文语义保持。节点锁外采集组件统计；致命错误保留原异常原因。 |
| 原共同 runner、cluster/harness、report | 从模板为三个节点准备独立设备；仅第一次 initialize，重启只 Open。空间统计改用 Data 分配量，缺失不补零，固定 B/Journal 区单独解释。 |
| `MetadataSnapshot::Scan`、`ObjectMappingSnapshot::Resolve` | 低缓存运行后定位到先取 257 条再过滤的过量读取；新增明确上界，复用一个遍历实现，在读值前停止；同样接续写入裁剪、Deferred/控制/退役枚举和范围 GC，删除已经由 Scan 保证的重复边界过滤。 |
| 原快照 proxy / Network.catchup | C5 原观察器只认 type 5，漏掉实际完成的压缩传输；沿现有解析器适配 type 9/12，用逻辑长度判断块边界，统一匹配 type 6 响应。短追赶仍禁止快照，长追赶仍要求非 stale 完成响应及目标业务验证。 |

先审查了组件所有权、恢复入口、后台维护与关闭顺序，再写部署测试。真实共同测试暴露问题后先沿调用链分析，再修复、复验；没有先堆叠模拟回归来猜测生产行为。

## 2. 测试全景与 Oracle

新阶段测试是归档中的 `deployment/deployment_test.py`，同一个真实 CLI 场景中包含下列八项检查。它不实现假数据库。

| 名称 | invariant / 防止的 bug | 分类 |
|---|---|---|
| unknown-field | 错误配置不能开始格式化，未知字段不可静默忽略 | integration |
| open-blank | 普通启动不能把空设备当作已授权创建 | integration/regression |
| initialize | 显式初始化建立可打开的状态，完成后退出 | integration |
| refuse-reinitialize | 重复创建不能覆盖已有引导/数据 | integration/regression |
| exclusive-device | 同一 inode 即使换路径也不能由两个本工具实例同时打开 | integration |
| wrong-identity | 配置身份不符不能以另一节点身份打开/重建 | integration |
| open-status-close-1 | 初始化结果可由真实节点打开；新运维请求与旧 STATUS 并存 | E2E（节点启动/协议） |
| open-status-close-2 | 正常关闭后可从同一设备重开，锁和生命周期没有残留 | E2E（节点重启） |

另运行既有 `client_protocol_test` 三项：`EveryV1RequestResponseAndQueryKindMatchesFixedGoldenBytes`、`RejectsCorruptionDirectionAndInvalidFields`、`DistributedClientRequiresResponseRequestCorrelation`，检查原固定 wire golden、损坏/方向/字段拒绝、请求响应关联。原 C1–C5、P1–P4 继续由共同套件维护，不复制为本模块新测试。B 范围接口只在本轮 F34 组合包保存一份场景，F08/F13 引用同一份证据，不各自复制。运行结果见 §8。

## 3. 逐项：目标 → 输入 → 执行 → Oracle → 失败含义

| 检查 | 关键输入及执行路径 | 判断依据与独立性 | 失败含义/局限 |
|---|---|---|---|
| unknown-field | 新的定容零文件；模板增加未知字段；真实 CLI initialize | 独立 Python SHA256 比较整个设备前后相同，非零退出且错误指向未知字段 | 配置被吞掉或拒绝前写入。SHA 用于测试比较，不向生产新增 SHA。 |
| open-blank | 同一个空设备，省略 action → 默认 Open → Bootstrap | 非零退出且整设备字节不变 | 自动创建或异常写入；单独检查退出不能证明设备未改，因此保留字节比较。 |
| initialize | 完整配置 → NodeStorage::Create → 对象空间 → Raft 控制根/身份 → Close | 正常退出，设备不再全零，后续真实 Open 成功 | 单靠“字节变了”很弱，与后续打开共同验证恢复入口；不声称覆盖初始化掉电。 |
| refuse-reinitialize | 对已完成初始化的同设备再次 initialize | 非零退出、整设备 SHA 不变 | 覆盖已有存储。与 open-blank 分别防止默认动作和显式动作的破坏。 |
| exclusive-device | Python 持有真实 flock，CLI 通过软链接访问同 inode | 指定所有权错误、设备 SHA 不变 | 若删锁后只被 Bootstrap 拒绝，不能假装锁测试通过；定向变异验证了此区别。 |
| wrong-identity | 仅改变 storage identity，其他配置不变 → 正常 Open | 拒绝且设备 SHA 不变；恢复原配置后仍可打开 | 错误实例混入或失败后重建；它不是完整的所有配置排列测试。 |
| open-status-close-1 | 真节点 TCP，真实 bustub-client，独立固定 type-5 请求字节 | capacity 来自输入布局，初始 committed+free=capacity；旧 STATUS 无正文；固定 wire 返回新摘要 | 独立 wire 避免编码器/解码器共同错编号仍通过。初始空间恒等式只适用于此无预留/隔离 fixture。 |
| open-status-close-2 | 同设备第二次 Open/STATUS/SIGTERM/等待退出 | 同样公开判据，进程退出码为零 | 生命周期、锁释放或持久身份失效；不证明有业务数据的恢复，交给 C4/C5。 |

共同测试仍使用独立 SQL 内容模型、Porcupine 历史检查、实际故障/代理事件和恢复后业务结果。部署测试的初始容量不能代替长期空间复用；缓存换入/淘汰计数只能说明路径发生，不能自己证明业务正确。

### 新增范围查询契约验证

`deployment/metadata_range.cpp` 经真实 NodeStorage/B/Journal/Direct IO 写入 259 条独立输入；256 条在同一 owner，逻辑起点从 1000 按 13 递增，另有相邻 owner/category。对 8 个范围在 checkpoint 重开前后核对，计为一个场景，不把 16 次断言算成 16 项。覆盖非零未命中起点、键处边界、跨叶、跨 owner、返回 limit、空范围和空结果；预期来自输入 vector 的原生 uint64 tuple 比较，不调用生产编码器/扫描器。旧无上界 Scan 的总数也保持。

删除上界检查的最小变异必须在指定数量断言失败。该测试验证范围 API 的结果契约；不能单独证明上界检查发生在值读取之前，这一顺序由代码审查确认。性能收益仍需真实组件计数和业务运行，不能用此功能通过冒充。没有测试专用 production 接口。

### 快照观察器的契约验证

`deployment/observer_test.py` 是一项 unit/regression，覆盖本项目共同测试适配器，不声称经过生产数据库。四个固定 wire 输入分别是原文、增量 DATA、REUSE、LZ4 消息；预期逻辑范围均为 [40,64)，REUSE 无正文，压缩消息只有 3 个 wire 字节。测试不实现压缩/解压。执行原 proxy 解析与 Network.catchup，检查正确请求与响应关联、后到的 stale 响应不会抹掉安装证据、只有 stale 响应不能算安装、短追赶不得使用任何快照编码。预期来自固定协议字段及独立设定的范围，不从生产 codec 生成。

这里的受控事件输入只验证观察器，真实 C5 继续承担数据库与网络证据。恢复旧的 `kind = request["type"]` 匹配方式，会在指定的“observer missed completed installation”断言失败；编译/导入失败不能计为检出。该检查不是第三方 LZ4 行为测试，也没有为它新增生产接口。

## 4. production / 接口污染审查

| 新增项 | 正式调用者与必要性 | 删除测试后是否保留 |
|---|---|---|
| CLI 配置、DeviceLock | 真实部署需要绑定设备、身份、预算和显式创建；适配器私有，不复制组件校验 | 是 |
| metadata_writeback_pages_/metadata_checkpoint_rounds_ | 正式节点维护参数；追加到原配置末尾，零保留已有手工组件语义，CLI 要求显式非零 | 是；并非为了测试给原必传参数加默认值 |
| Usage/CacheStatus 三个只读接口 | 运维需要观察分配与缓存压力；NodeStorage 汇总所有者数据，节点对外发布 | 是；没有可修改内部引用或测试 getter |
| ClientStatusRequestV1::storage_ 默认 false | 保持既有 STATUS 调用与 wire 语义，新运维查询显式选入 | 是；是协议兼容，不是测试便利参数 |
| 有上界 Scan 重载 | Resolve 确实需要限制元数据读取范围；与旧无上界调用共用遍历，无第二套树 | 是 |
| 私有 MaintainMetadata | 将后台所有者连接到已有 B 写回/checkpoint，不暴露另一套队列或通用抽象 | 是 |

未新增 test-only hook、`#ifdef TEST`、为了检查而改变的生产分支。测试专用定容文件及随机身份只出现在测试部署器中；生产 CLI 不创建/truncate 设备。状态新增少量实际换入/淘汰计数，代价在慢路径；位图采样持组件锁，未宣称零成本或精确进程 RSS。各组件统计分别采集，不是跨组件原子业务视图。

## 5. 重复测试与跨模块设计审查

- 两次 Open 保留在同一生命周期场景中，不各建一套 fixture；第二次专查 Close 后重开。未知字段不扩成所有字段组合。
- 旧 F34 五项阶段场景覆盖本地编排/故障边界，本轮不重复计为已重跑；原有效内容仍在同一个 F34 包，新增部署测试也归该包。
- C2 与 C3 共用线性一致性模型；C3 的杀进程、隔离、丢响应覆盖不同故障，不能因使用同一 oracle 就删除。
- C4/C5 提供实际有数据的恢复，CLI 空实例重开不能替代它们。已有共同测试接管的业务内容不另写小型模拟案例。
- `NodeStorage` 继续唯一组装底层组件；`MetadataEngine` 继续独占 B 缓存与恢复；GC 角色复用原维护来源，不新增维护线程。A/B 复用 CLOCK-Pro 代码但状态独立，这不是重复缓存。
- `Usage` 汇总不是第二份分配器；STATUS 不是新控制目录。旧文件部署仅是兼容/历史基线适配器，不能与新对象引擎同时管理同一实例的数据。
- 尚存的性能限制：SQL 执行仍持节点锁；B checkpoint 仍按既有协议排除新提交；位图采样和完整检查点有扫描成本。没有把这些说成已消除的冗余，也没有借验收之名悄悄实现 S13。

## 6. 变异与稳定性

两个 CLI 变异在隔离构建中运行，不修改工作区 production；另有上文范围查询和观察器变异，共四个：

1. 把 Open 分支改成 initialize：`open-blank` 因错误成功退出而失败。
2. 删除设备 flock：所有权测试必须得到 lock 错误；变异穿过锁到达 Bootstrap 的另一种拒绝，指定断言失败。变异 runner 对照 `AssertionError` 的具体测试名，编译失败或任意非零退出均不算检出。

初始化/重新打开使用相同设备是该测试的显式顺序；没有跨测试套件共享设备。所有子进程在 finally 终止并 wait，超过关闭期限会 kill 且使测试失败。就绪轮询检查实际服务，sleep 仅为轮询退让；固定端口要求测试串行，端口冲突应报环境失败。UUID 只隔离存储身份，记录在配置中，不影响业务模型种子。

真实 E2E 的选主、磁盘延迟和 WSL 调度会影响结果。不能把时序相关失败默认归为“flaky”；C1 的服务失败保留证据。所有生产 socket 场景实际在允许 loopback 的执行环境重跑；初次沙箱 socket 拒绝不计代码失败，也不计通过。

## 7. 测试矩阵

| Test | 验证 invariant | Oracle | 新 failure mode / 重复 | Production pollution | 建议 |
|---|---|---|---|---|---|
| unknown-field | 拒绝错误配置且不写盘 | 整设备字节比较+错误 | 配置静默忽略 | 无 | 保留 |
| open-blank | Open 不格式化 | 字节比较+退出 | 默认路径破坏 | 无 | 保留 |
| initialize | 持久入口可用 | 成功退出+后续真实 Open | CLI 创建组装 | 无 | 保留 |
| refuse-reinitialize | 不覆盖已有设备 | 字节比较+拒绝 | 显式重复创建 | 无 | 保留 |
| exclusive-device | 一个工具所有者 | OS 锁+指定错误 | inode 别名；已变异 | 无 | 保留 |
| wrong-identity | 实例隔离 | 拒绝+原身份重开 | 打开错误设备 | 无 | 保留 |
| open-status-close-1/2 | wire兼容、关闭重开 | 固定请求字节+公开容量+退出 | 同 fixture 中两次生命周期 | 正式运维接口 | 合并（已在同一脚本） |
| BoundedMetadataRange | 范围、limit、重开不变 | 独立输入 tuple/正文 | 跨叶与范围外值；已指定变异 | 正常范围 API | 保留（仅阶段归档） |
| SnapshotObserver | 新旧编码都能证明安装，stale 不算成功 | 固定 wire/事件＋真实 C5 | 防止漏判压缩与误判短追赶；已指定变异 | 无，测试适配器 | 保留（仅阶段归档） |
| 原协议三项 | 原 wire/correlation 不变 | 固定 golden/故障服务器 | 兼容边界，非业务 E2E 重复 | 无 | 保留 |
| C1 | 更新/删插/长度变化的业务结果 | 原独立内容模型 | 实际换主导致失败 | 无 | 需要补充（修复协议阻塞后复验） |
| C2/C3 | 并发及故障线性一致 | 原 Porcupine 模型 | 三种故障边界不同 | 无 | 保留 |
| C4/C5 | 持久恢复及两种追赶 | 原模型+真实传输事件 | 重启/日志/快照组合 | 无 | 保留 |
| P1–P4/低缓存 | 实际成本、压力和长期推进 | 原工作量/模型+窗口指标 | 未进入窗口不得出性能结论 | 正式统计 | 需要补充（以 §8 实际覆盖为准） |

核心保护是显式初始化不覆盖、真实身份/所有权、旧协议兼容，以及共同业务恢复。八项 CLI 是阶段证据，长期保留压缩源码；不作为又一套常驻业务套件。缺口包括真实断电/裸设备、完整性能矩阵、SS/RS/PL/GT 分离测量、S13 锁外 SQL 与协议接续。

## 8. 执行证据

环境：WSL2 Linux 6.18、GCC 16 Release、实际 Direct 定容文件、三节点 TCP；Porcupine v1.3.0。没有运行本轮 ASan/TSan、裸设备或物理掉电。完整配置、命令、每轮源差异与原始历史保存在 [结果目录](../../test-results/storage-s12-node-20261009/README.md)。

- CLI 8 项、B 范围场景 1 项、快照观察器 1 项通过；已有协议 3 项通过；4 个变异均匹配指定断言。不同输入及重复复验不叠加测试数。
- `verified-C2`（38.281 s）、`verified-C4`（54.215 s）在最终 C++ 源码通过。C3 kill/isolate/drop 在本轮此前修复阶段分别通过；这些运行带各自源码快照，没有在后续有界 Scan 调用方修改后全部重跑，也不冒充最终版本完整矩阵。
- C5 最终 `observer-C5-long` 71.306 s、`observer-C5-short` 45.813 s 均通过，两个场景关闭退出码均为 0。长追赶实际 type 12 的末块逻辑长度 53,248 B、压缩正文 5,880 B，匹配 type 6 非 stale 完成响应，目标从 L=34 追到 K=291，业务验证通过后再完成 8 次写入和最终校验；这不是完整压缩性能测量。旧观察器的 `verified-C5` deadline 作为诊断保留：其网络历史确有非 stale 完成响应，但未解析对应 type 12 请求。它不是旧的正确通过证据；修复后重新跑真实长短追赶。
- `verified-C1` 失败（51.842 s）：更新后校验收到 NOT_LEADER；旧文件后端相同 C1 通过。没有增加健康场景重试或调整选主超时。约 584 ms 的早先读请求是线索，不能当作独立锁持有时间测量；锁内 SQL 路径由代码确认。
- `verified-P2`：B 正文 64 页、共享预算 64 MiB、20,000 行；准备预算 300 s、校验 180 s、请求 30 s、计划测量 10 s / 4 clients，属于明确配置的资格诊断，不是默认 1,800/600/60 s 基线。准备后的校验只完成 125 个读取，后继操作未确认；总计 362.969 s，未进入测量窗口。关闭超时，框架最终 SIGKILL（cluster_exit=-9），**关闭排空也未通过**。没有从这些数据报告吞吐或性能改进。
- 低缓存有效采样中，各节点 B live_bodies 最大均为 64、描述数最高 524，换入/淘汰计数实际增长；303 次采样中 46 次不完整，保留缺失。只证明换入换出路径发生及观测上界，不证明压力正确性、性能收益或进程总 RAM 上界。
- P1/P3/P4、完整基线与 SS/RS/PL/GT 未完成。不能把本轮功能及组件计数当作 Calico/CLOCK-Pro 性能证据。

因此：正式入口、维护和观测已接入；S12 总体验收未完成。锁外交接仍需讨论 S13 的 ReadIndex/业务可见边界、SQL/Apply/同步 Store 依赖；但不能把全部关闭问题归因于 S13，局部提交/维护进展缺陷已在下文 §10 修复。C1 与低缓存 P2 原场景仍待复验，不能仅删锁或放宽测试。

## 9. 归档与退出

F34 源码包原位替换，新增阶段源码只在该压缩包中保存；原共同 E2E 继续常驻，未新增 CMake 阶段目标。旧的本轮展开源码、隔离变异构建、节点定容设备及下载的临时工具链在结果封装后删除；已有 `/tmp/bustub-gcc16` 工具链保持。有效的其他模块/历史结果不是错误测试版本，不随本轮删除。

源码包 MANIFEST 区分旧 F34 历史指纹与 deployment/ 当前指纹；结果包保存失败诊断和每轮源码，不把早期通过混作最终测试矩阵。已核对本轮节点、驱动和代理均退出；具体文件清理见结果目录 cleanup-verification.json。未 commit/push。

## 10. 再次复查：提交进展不能依赖正在等待它的维护角色

### 10.1 本次发现与实际修改

先读代码，发现上一轮新增的 Busy 重试存在明确的等待环：

1. B 正文缓存中都是脏版本，`MetadataCache::NewImage()` 无法淘汰，抛出 `MetadataCommitBusy`。
2. `CommitLoop()` 保留已完成的 Data IO，等待 B 有资源再发布。
3. `GarbageLoop()` 是周期 B 写回的执行者，但可能正在 `StoreRound()` 同步等待上述提交；Close 也会在排空提交前停止周期维护。
4. 因此“等待一会儿再试”不保证有人实际让出 B 缓存，既可卡住普通 Store 清理，也可卡住关闭。

现在 `CommitLoop()` 的 Busy 分支在启用自动维护时调用已有 `MaintainMetadata(pages, false)`，协助 B 写回一个配置上限内的批次，然后重试元数据发布。`false` 明确不触发 checkpoint；原周期 GC/checkpoint 仍由维护角色执行。B 自己的写回/checkpoint 门控避免同时覆盖；门控繁忙退让，真实写回异常记入节点错误并退出请求。未重做 Data IO、未新增后台线程或另一份写回队列，也未为测试扩充 API。

最初考虑让关闭期间的周期 B 维护延后退出；进一步发现 Store 在正常运行时也能等住该角色，因此最终采用上述协助方式，关闭顺序保持原样。这不是把周期 GC 移回提交线程。配置页数为零的手工维护组件继续由原所有者安排进展。

这是从代码推导并被隔离变异证实的缺陷；没有足够证据认定它就是上轮 P2 超时的全部原因。P2 仍未通过，不能用本轮局部修复替代原场景验收。

### 10.2 新测试全景与逐项设计

两个场景共用归档 `deployment/close_pressure.cpp`、`close_check.py`，不增加常驻 CMake 测试目标。

| Test | 目标 / 输入 / 生产路径 | Oracle 与失败含义 | 分类 |
|---|---|---|---|
| ClosePressure | B 正文预算 64 页；先暂停已进入的 Store 回调，提交非空 12,000 B 元数据直到明确 Busy（本机 19 次成功），再提交 16 KiB 对象正文及 12,000 B 控制记录；在真实 Data pwrite 边界暂停，确认 NodeStorage 进入 Draining 后释放 IO | 40 s 内得到 Durable、Close 完成、Open 后逐字节等于独立输入；缺少协助时明确报 accepted publication starved after Close stopped admission，定位为接纳后关闭进展失败 | integration / regression |
| StorePressure | 复用同一压力装置，但由既有维护回调同步 SubmitObjects/Wait，正是 RaftObjectStorage 的提交等待契约；该角色无法回到下一轮周期写回 | 40 s 内回调获得 Durable，关闭重开后正文及控制记录相同；失败报 Store publication starved on its own maintenance role，定位正常运行中的自等待 | integration / regression |

**目标 → 输入 → 执行 → Oracle → 失败含义：**两项均要求“已到真实 Data IO 的操作仍可完成元数据发布，不能只依赖已停止/正在等待它的周期执行者”。所有数据库、缓存、分配、Journal、写回均调用正式组件。测试仅用链接器 `--wrap=pwrite` 暂停一次 Data 系统调用，随后调用真实 pwrite；Store 回调在公开的生产注册入口登记，不替换 B、IO 或 Raft 协议，不宣称它是完整 Raft Store E2E。输入正文为确定的非零字节模式；重开预期直接来自输入 vector，不调用被测映射或编码器计算。协议推进由票据和公开生命周期状态判定，计数只确认压力 setup 成立，不代替内容 oracle。

### 10.3 重复、污染、变异、稳定性

- 两项的不同 failure mode 是“关闭停掉进展来源”和“正常回调占住进展来源”，共用装置而非复制两套 fixture；前者通过不能代替后者。
- 旧 F31/F22 的 `CloseDrainsAcceptedStoreIOBeforeDisablingItsStorageCalls` 保护已接纳 Store IO 的寿命；这里是 B 脏页无法腾出后的提交进展，不是重复同一判据。旧 F34 的空实例 CLI 关闭也没有制造此依赖。原 C4 继续承担三节点业务恢复。
- 新增 public API/default/getter/hook/wrapper/configuration/ABI/测试分支均为 **0**。链接包装器只存在阶段测试，不进入 production；没有 `#ifdef TEST`。本次仍复用 B 唯一写回列表、缓存门控及既有 F22 执行角色，不新增同职责模块。
- 一处最小变异：删除 Busy 分支中的有界 `MaintainMetadata(..., false)` 调用，保留其他正常行为。两个场景分别在预定的进展断言失败；编译失败、任意崩溃和外层超时均不计检出。为同一个变异的两个验证，不宣称是两个不同 mutation。
- 每个场景使用新定容设备、固定字节模式；用条件变量和真实 IO/Draining 事件安排时序，不靠 sleep 猜测。10/40 s 是失败上限；极慢设备仍可能环境性超时，不能因此放宽断言自动重跑。测试进程在进展失败后 `_Exit(1)`，避免析构等待原缺陷而掩盖指定失败；退出后 OS 清理线程/FD，runner 保留诊断，最终删除测试设备。
- 未验证所有共享 RAM 长期被合法读者占住的情况；没有声称解决任何预算配置都必定进展。真实掉电、裸设备、ASan/TSan、S13 协议锁交接仍未覆盖。

### 10.4 本次执行与矩阵

GCC 16 Release 实际重建 `build-storage-e2e`。ClosePressure 14.008 s、StorePressure 13.908 s 通过；同一缺陷变异分别在指定 40 s 进展期限失败。原共同 C4 在相同最终源码复跑通过（54.215 s）：三节点 SIGKILL、恢复已确认边界 82、同身份请求重放去重、恢复后 8 次写入与业务校验，cluster_exit=0。C4 使用原工作量/判据，只设置资格外层 180 s watchdog，不冒充性能测量或真实掉电。

| Test | invariant | Oracle | 新 failure mode / 重复 | Production pollution | 建议 |
|---|---|---|---|---|---|
| ClosePressure | 停止接纳后已执行 IO 的提交可完成 | Durable＋Close＋重开独立字节比较 | 周期写回停止；非空实例关闭遗漏 | 无 | 保留：阶段归档，完整压力 E2E 接管后再退出 |
| StorePressure | 同步维护不等待自身下一轮 | 回调完成＋重开独立字节比较 | 正常运行自等待；与关闭场景共用 fixture | 无 | 保留：同包、同装置 |
| 原 C4 | 已确认业务与会话恢复、继续写入 | 原业务模型及去重语义 | 正式三节点，覆盖范围大于组件测试 | 无 | 保留 |
| 原 C1 / 低缓存 P2 | 健康请求与压力推进 | 原共同判据 | 上轮失败，本次未重跑 | 无 | 需要补充：局部修复不能替代验收 |

§8 的 C2/C3/C5 等保留当时源码和结果，是历史证据；本次仅重跑上表明确的 C4，不把旧运行自动升级为本次全通过。阶段源码仍只留 F34 唯一压缩包，结果原位补充；不留错误旧包或展开阶段文件。未 commit/push。

## 11. 再次复查：B 页 IO 暂满、永久超限与真实错误

### 11.1 代码逻辑先行：发现与修正

上轮只验证了 B 脏页缓存压力，尚未验证协助写回所需 F02 名额同时暂满的组合。审查 `MetadataPageWriter::Write → CheckMetadataAdmission → MaintainMetadata → CommitLoop/GarbageLoop` 后确认：F02 Full 被转换为普通 ResourceUnavailable，上层只让 MetadataCommitBusy 退让，因此正常竞争可变成节点故障。

但直接把全部 Full 改成 Busy 也不正确：F02 的 Full 还包括请求数或缓冲超过配置总容量，永远等待不会变好。现在 `metadata_engine.cpp` 增加私有 `PrepareMetadataPages`，复用 F02 已有的 `AccumulateReadBudget`，在实际准入前检查当前资源类别、操作名额、正文和实际内存对齐填充：

| 情况 | 结果 |
|---|---|
| 单批本身超过总容量 | 明确 ResourceUnavailable，不能当可重试 Busy |
| 单批可容纳，额度暂被其他所有者占用 | MetadataCommitBusy；自动维护让出、提交可重试 |
| 执行器停止 | 原 NotReady |
| 已提交 pwrite/Flush 失败 | 原实际异常及失败结果，不改成 Busy |

换入、写回、B 校验扫描和 checkpoint 恢复读取四处共用该入口；移除扫描中单独重复判断 Full 的分支。没有新增公共 API、错误类型、默认参数、配置、线程或预算账本。私有函数承担一致的容量/错误策略，不是测试便利包装器；节点运行时需要它。既有 F02 容量判定仍是唯一算法，页大小仍是 B 格式单位、内存对齐来自设备能力。

### 11.2 新测试全景与逐项设计

仅新增一个组合场景 `MetadataAdmission`（integration/regression），源码在 F34 的 `deployment/metadata_admission.cpp`，扩展原 `close_check.py` 支持 `--scenario admission` 和三个指定变异，未复制一套 runner。

- **目标：**能放下的 B IO 请求临时未获准，不得损坏节点可用性；单批永远装不下不得无限重试；真正的已提交 IO 错误不得被吞掉。
- **输入：**实际 NodeStorage/Direct 文件；F02 总 64 名额，其中 2 个进展名额，1 MiB 缓冲；写入 30 组非零 12,000 B 元数据，使待写回页数超过 64，但仍在 B 的 512 页正文预算内。先暂停已进入的维护回调，再通过正式 `WriteObjectData` 完成并持有 64 个结果，占住真实 F02 名额；最后两个使用既有进展类别。结果已完成仍持有资源是 F02 的实际寿命约定，不是虚构计数器。
- **被测路径：**NodeStorage → B 页写回/周期 MaintainMetadata → MetadataBackend/F04 → F02/F01；错误注入只在链接器包装的实际 Metadata 区 pwrite 返回一次 EIO，其余调用均执行真实 pwrite。未运行假数据库或替代队列。
- **Oracle：**`Writeback(1)` 必须抛 Busy；周期角色在同样压力后还能进入下一轮回调，且节点仍 Serving。释放真实结果后 `Writeback(1)` 必须 Durable。名额空闲时 `Writeback(65)` 必须明确拒绝且不能抛 Busy，之后小批次仍可写回，已发布键值与独立输入字节相等。最后放行带一次真实 EIO 的周期写回，节点必须 Failed，保留 BlockDeviceIOError、EIO 错误号及 Metadata 区物理范围。
- **失败含义：**分别定位“暂满误归类”“永久超限误重试”“释放资源后仍无进展”“拒绝改变已发布数据/丢掉待写回工作”“真实错误被隐藏”。预期来自显式名额配置、独立字节输入和内核错误号，不用生产编码/预算公式计算 expected。

### 11.3 重复、接口污染、杀伤力和稳定性

- 上轮 ClosePressure/StorePressure 验证的是提交与维护的执行依赖；本项验证 **IO 准入原因和实际 IO 失败的区别**。新场景没有重做关闭压力装置，不复制 F02 内部队列测试。
- 三个最小变异分别恢复普通 ResourceUnavailable、删除固定容量预检、将真实写回失败变成 Busy。必须分别匹配 `temporary metadata IO Full must be Busy`、`oversized metadata batch must not retry`、`real metadata EIO was hidden`。编译/链接失败、任意非零退出或 runner 外层超时不能算检出。
- 使用同一场景的三个变异，不把内部多次断言拆成额外测试数。没有测试专用 production hook/getter/分支，syscall 包装仅在阶段链接中生效；原公共 Store 注册入口仅协调角色交接，不生成数据库结果。
- 每次独立设备和固定字节模式；由条件变量、真实结果持有和下一轮回调确定压力存在及维护推进，不用 sleep 猜时序。10 s 是错误发现/角色交接上限，慢环境仍可能超时。失败时进程明确退出防止析构等住缺陷，日志保留，测试设备随后清理。
- 未在这一场景单独制造冷页读取/扫描压力；四处统一路径由代码检查，重启路径由原 C4 和两项重开验证接续。不能据此宣称全部 IO 压力组合、真实断电、裸设备或 sanitizer 已验证。

### 11.4 本次执行与测试矩阵

GCC 16 Release 重建 `build-storage-e2e`。本次新场景 6.836 s 通过；三个变异命中各自指定断言。原 ClosePressure（14.160 s）、StorePressure（13.807 s）在本次源码复验通过；原共同 C4 55.222 s 通过，恢复确认边界 82、恢复后 8 次写入、cluster_exit=0。没有改原业务输入、模型、选主超时或重试规则。

| Test | invariant | Oracle | 新 failure mode / 重复 | Production pollution | 建议 |
|---|---|---|---|---|---|
| MetadataAdmission | 暂满退让、超限拒绝、真实错误上报 | 名额配置＋实际释放/回调＋独立字节＋EIO | 上轮未覆盖的三类错误分界；一个组合场景 | 无 | 保留：阶段归档 |
| ClosePressure / StorePressure | 提交进展不只依赖周期角色 | Durable、关闭/回调、重开正文 | 两个既有场景原样复用 | 无 | 保留 |
| 原 C4 | 已确认业务与会话恢复并接续 | 原业务模型、去重与真实进程恢复 | 正式三节点路径 | 无 | 保留 |
| 原 C1 / 低缓存 P2 / 完整性能矩阵 | 完整部署的正确性与成本 | 原共同判据 | 本次未运行，旧失败/缺口仍保留 | 无 | 需要补充 |

本次核心新增保护是错误归类，未增加低价值参数排列。阶段源码和所有变异复用同一 F34 包，唯一源码/结果包原位更新；本次证据在 `review2/`，此前 `review/` 和原运行保留各自源码归属。总方案及 B、写回、GC 子方案同步，未 commit/push。

## 12. 第三次复查：checkpoint 的 Journal 准入与 Flush 合计容量

### 12.1 先审代码：两处问题与修复

§11 只统一了 B 页 IO。继续沿 `Checkpoint → AppendCheckpoint → JournalService::Append` 检查，发现 checkpoint 仍把所有未接纳统一抛普通资源错误，F22 会把 Journal 的临时 Full 记为节点失败；同文件普通 `Publish` 却已把 Full 当 Busy，规则重复且不一致。现在两处共用私有 `CheckMetadataJournalAdmission`：Accepted 继续、Full 退让、NoSpace 明确资源不足、Stopped/Faulted 不可用。原 checkpoint 的 RAII 门控继续在拒绝后释放；尚未成功不能推进 checkpoint 边界或裁剪旧日志。

同时检查了 F07 的 Full 来源。F07 先准备所有段写，再另取一个 Flush 名额；此前即使数据能单独装下，加上 Flush 后永远超过固定总容量，仍会返回 Full。现在实际准备之前用既有 F02 `AccumulateReadBudget` 联合计算段请求、对齐缓冲和 Flush 名额，永久超限抛既有 RequestTooLarge。`Layout` 是只读规划；失败不会改变追加游标、段身份或排队状态。暂时占用仍可 Full，真实写入失败不改成可重试。

本轮修改限定上述两处交接及公开注释，无新 public API/默认参数/getter/配置/测试 hook/ABI/线程。没有新增缓存、分配器或调度器。B 的页 IO 与 Journal IO 有不同资源形状，前者 Flush 合在数据批次，后者另占一个名额；复用 F02 算法，不误将两者统一成错误公式。

### 12.2 测试全景与设计

| Test | 分类 | 目标 → 输入 → production 路径 → Oracle → 失败含义 |
|---|---|---|
| MetadataAdmission（扩展原场景） | integration/regression | 页暂满/永久超限检查后，先通过真实 Writeback 排空脏页，再持有 64 个已完成 IO 结果；NodeStorage::Checkpoint → F10/F07/F02，此时应在 Journal 准入返回 Busy。释放结果后 checkpoint 必须 Durable，键值与独立输入相等，并可继续 Commit；故意拒绝后的门控或资源泄漏会阻止这些步骤。最后仍执行原实际 EIO 上报检查。 |
| JournalAdmission（新增一个场景） | integration/regression | 真实 Bootstrap/F07/F02/Direct 文件，两个总 IO 名额，实际对齐推导日志单元、每段四单元；一个正文需要两个段写，再加独立 Flush 共三个名额，必须抛 RequestTooLarge。随后持有一个真实 Flush 的完成结果，小正文只因暂时占用返回 Full；释放后成功追加、Close/Open 重放必须恰好等于独立的小正文，拒绝的正文不得出现。 |

Expected 来自显式名额和独立输入，不调用生产计划器/编码器生成正确答案。JournalAdmission 不解析私有磁盘偏移或 CRC；分段大小只是制造“写可装下、Flush 装不下”的公开配置，原场景维持非零正文。上层 checkpoint 检查先确认写回 Clean，避免误把页 IO Busy 当作 Journal Busy。

### 12.3 重复、生产污染、变异与稳定性

- 扩展 MetadataAdmission，复用原压力装置与 runner，未复制一个 checkpoint fixture。JournalAdmission 保护 F07 的跨段写加 Flush 的永久容量判定，区别于前者的后台维护错误分类；不用新增名额 getter 来观察内部状态。
- 历史 F07 AdmissionRollsBackAndCompletedResultsStayBounded 已验证临时占用后回收；本场景的释放/重开是新增容量拒绝的后置条件，不另建第二份常驻 F07 套件。共同 C4 仍承担真实三节点业务恢复。阶段源码归 F34 同一个包，F07/F10 只引用此证据。
- 两个定向变异：只把 checkpoint 准入恢复成普通资源错误，必须触发 `checkpoint Journal Full must be Busy`；Flush 预占名额从 1 改为 0，必须触发 `Journal must count data plus Flush before admission`。编译/链接失败、任意非零退出和外层超时不能当检出。
- 没有为测试改变生产控制流或接口。MetadataAdmission 仍仅在测试链接中注入原有一次 EIO；JournalAdmission 无故障替身，所有 IO 真实执行。每次新设备、固定正文，压力来自完成结果的实际持有；协调仍是条件变量，不靠 sleep。超时是失败上限，不能证明任意慢设备环境都不会超时。
- 不声称已穷举 Journal NoSpace、Stopped/Faulted 或持久化不明确组合；这些分支本轮以代码分析核对，不增加镜像实现的参数测试。

### 12.4 本轮结果

本轮 GCC 16 Release 重建。MetadataAdmission 扩展场景 6.986 s 通过；JournalAdmission 0.065 s 通过；两个定向变异均命中指定断言。ClosePressure（14.006 s）、StorePressure（14.059 s）在当前源码通过。新增 Journal fixture 初次误用构造签名、第二次遗漏 Bootstrap 的显式 Open，均在测试准备阶段失败；修正为已有正式接口后才运行成功，没有扩充 production API，准备失败不算杀伤力证据。

| Test | invariant | Oracle | 新 failure mode / 与其他测试重复 | Production pollution | 建议 |
|---|---|---|---|---|---|
| MetadataAdmission（本轮扩展） | checkpoint 暂满可退让并再次发布，错误后仍可提交 | Clean 前置＋准确 Busy 类型＋Durable＋独立输入内容 | Journal 阶段失败，区别于既有页 IO 阶段；复用同场景 | 无 | 保留 |
| JournalAdmission | 段写和独立 Flush 必须同时可容纳，拒绝不留下日志 | 固定总名额＋RequestTooLarge/Full 区别＋重开仅有成功正文 | flush 独立名额遗漏；重放作拒绝后的完整性判据，不复制独立持久化套件 | 无 | 保留 |
| ClosePressure / StorePressure | B 压力下关闭/维护提交仍可推进 | 原 Durable、重开字节、完成边界 | 原样复验 | 无 | 保留 |
| 原 C4 | 三节点已确认业务恢复、去重、继续写入 | 原独立业务模型及进程恢复 | 正式节点组合路径 | 无 | 保留 |
| C1 / 低缓存 P2 / 性能矩阵 | 正式部署完整资格 | 原共同判据 | 尚未复验解决原失败 | 无 | 需要补充 |

本轮新增/扩展的核心保护是 checkpoint 准入以及 Journal 数据+Flush 合计容量；没有新增只检查语言、标准库或私有格式排列的测试。未单独重跑 §11 三个变异，保留其原源码证据；本轮两项变异不冒充全部历史变异复验。C1/低缓存 P2 尚未复验解决，S12 总验收仍不完整。

原共同 C4 在本轮最终源码通过（54.164 s）：确认边界 82，恢复后 8 次写入，重复请求去重，cluster_exit=0。沿用原工作量/模型/重试和选主规则；这是三节点进程崩溃恢复，不是性能窗口或真实掉电验证。最新源快照及结果归唯一包的 `review3/`；恢复指引已指向 `review3/source/`，此前根 source、review、review2 不混作当前源码。

## 13. 第四次复查：交接语义、测试证据与重复职责

### 13.1 本次改动和范围

本次先复核代码逻辑，再阅读归档测试及其 oracle。没有发现新的、能够确认的生产缺陷；**本次只补充本节审查记录，生产代码和测试源码均未修改，没有重新编译或运行 C++/E2E，也没有 commit/push。** §10–§12 的修复和运行属于此前轮次，不计作本次新增改动。

复核范围包括：对象提交的失败/重试及资源释放、B 页 IO 和 checkpoint 的准入、Journal 数据与 Flush 的联合容量、范围扫描的关闭保护、正式节点状态观测与共同测试适配。此结论限定于这些交接，不能推导为全部项目均无缺陷或 S12 已通过验收。

### 13.2 方案与实现一致性

- **暂满不是所有阶段都原地重试。** `object_transaction.cpp` 内部的 `Transient` 对普通 IO 名额争用重试；共享 RAM 不足可能经 `MetadataCommitBusy` 使尚未提交的请求返回 NotCommitted、归还输入。这符合 [资源协议 §2](s12_resource_gc.md)，避免请求占着输入等待自身释放内存。已 durable 的 Deferred 任务仍由持久任务拥有者接续，不能因压力丢掉。
- **发布阶段仍保留已有 Data 结果。** `CommitLoop` 在 B 尚未接纳时重试发布，Busy 时有界协助现有写回器；不重复已完成的数据写入，也不在此执行整轮 GC/checkpoint。关闭与 Store 同步维护不再必须依赖周期角色的下一轮。
- **容量计算复用 F02，IO 形状分别处理。** B 的 `PrepareMetadataPages` 供加载、写回、扫描和恢复读取共用；Journal 的数据批次还要为独立 Flush 留一个名额。单批永久超限和当前资源暂被持有分别处理，真正的 IO 失败继续上报。
- **范围查询没有失去寿命保护。** 两个 `MetadataSnapshot::Scan` 重载共用 `ScanMetadata`，其 `MetadataPager` 仍取得 `MetadataCache::Call`；上界检查在读取越界键对应的值之前发生。复用 helper 没有把关闭保护或有界查询条件删掉。
- **共同测试的观测不充当业务 oracle。** STORAGE_STATUS 使用原 Driver 的请求关联和并发保护；缺失观测标为不完整，不当作零占用。原业务模型、健康场景重试规则和选主参数没有为通过验收而放宽。状态采样仍有 CPU/名额成本，不能称作无开销监控。

### 13.3 测试设计再审

本次未增加测试，沿用 §10–§12 的逐项设计，重新确认以下八方面：目标和输入对应真实失败模式；路径经过 Node/B/F07/F02/Direct；预期正文来自独立输入；失败使用指定断言定位；生产接口有真实调用方；场景不重复同一等待关系；变异按指定失败而非任意非零退出判定；时序依赖条件变量/真实 IO 持有，超时仅作失败上限。

| Test | 验证 invariant / Oracle | 与其他测试的关系、限制 | Production pollution | 建议 |
|---|---|---|---|---|
| ClosePressure | 停止接纳后已执行 IO 仍可 durable、关闭及重开；正文/控制记录对照独立输入 | 维护停止导致的进展风险；不能代替完整低缓存 P2 | 无 | 保留 |
| StorePressure | 同步 Store 维护不等待自己的下一轮；完成票据与重开字节 | 与 ClosePressure 共用 fixture，阻塞依赖不同 | 无 | 保留 |
| MetadataAdmission | 页 IO/checkpoint 暂满可退让，永久超限拒绝，真实 EIO 上报 | 同一场景扩展 Journal 阶段；先写回 Clean，避免错误命中页 IO 的 Busy | 无 | 保留 |
| JournalAdmission | 段写及独立 Flush 必须联合容纳；重开只有成功正文 | 保护固定容量计算，区别于上层 Busy 分类；与旧 F07 的临时占用场景有部分后置检查重合，无需再拆成额外测试 | 无 | 保留 |
| 原 C4 | 三节点恢复已确认业务/去重并继续写入 | 最后一次同源执行通过；SIGKILL 不等于真实断电 | 无 | 保留 |
| 原 C1、低缓存 P2、完整性能矩阵 | 原共同业务及压力/性能判据 | 原失败或未完成仍然存在，组件通过不能替代 | 无 | 需要补充 |

四个阶段场景仍只放在 F34 归档，不新增常驻小测试。10/40 秒等期限在极慢环境下仍可能失败；不通过放宽期限自动重跑来取得通过。没有为本轮添加参数排列、标准库行为测试或镜像生产公式的 expected。既有变异和运行证据仍按原轮次归属，本次不计新的变异检出数。

### 13.4 接口与重复模块检查

本轮没有新增 public API/default/getter/hook/wrapper/configuration/ABI/测试分支。再次核对本阶段此前引入的接口：有上界的 Scan 用于对象范围查询与回收；Usage/CacheStatus 用于正式运维观测；自动 B 维护选项控制生产调度。删除阶段测试后这些调用仍存在，不属于纯测试接口。

未在所审范围发现第二套权威目录、预算、分配器、回收器或线程池。Store 退出逻辑引用、F14 判断范围可释放、F12 归还分配单位的职责不同，不能因都涉及回收而合并。提交角色协助与周期 B 维护共用同一个写回器及门控，也不是两份待写回清单。两个私有准入 helper 分别处理 B 页 IO 与 Journal 提交策略；它们没有复制 F02 的预算算法。

### 13.5 同源证据与清理核验

实际核验 F34 deployment 清单的 **497 个生产文件、17 个共同测试文件**，均与当前工作区一致；根清单 16 个成员、deployment 清单 12 个成员、全部 28 个模块源码包以及结果包 1,762 个成员哈希通过。源码包保持 `3f846e4e75eda431ae6bb26f788a5a12043966a48f09738ffbf3697fac193c1a`，结果包保持 `9d5a5b5e2b4c0059233e6f73d22aa553c0284d81ba9f74e84b275b1c1af97ef5`。本节是后补审查说明，不在此前运行的源快照内；没有为仅文档变化重打包相同代码。

当前仅一个 F34 源码包和一个 S12-node 结果包；没有其 `.tmp/.bak/.old` 副本。清理记录所列六个临时目录、两个 Python 缓存目录仍不存在，未发现残留测试节点/驱动/代理进程；结果包仍命中 `.gitignore`。`git diff --check` 通过。

**结论：本阶段已实现部分与约定一致，定向测试具有有效保护；S12 总验收仍不符合全部完成条件。** C1 的健康读取换主、低缓存 P2 的推进/关闭以及完整性能证据仍需处理。S13 的协议锁交接没有在本次复查中实施，不能把其待讨论工作标成已解决。

## 14. 提交门槛复查：跨模块去重与原场景复验

### 14.1 本次实际修改

先沿调用关系审查，发现 `DiskScheduler` 在当前库内已无生产调用方：对象页使用 F02，文件页适配器直接使用 DiskManager；只有原 `disk_scheduler_test` 使用该课程调度器。此前它仍列在 `bustub_storage_disk` 的生产编译列表中。

本次只修改两个构建入口：从 `src/storage/disk/CMakeLists.txt` 的生产列表移除该 cpp，由 `test/CMakeLists.txt` 中原 `disk_scheduler_test` 单独编译。保留已有课程源码/测试，不新增调度器、API、运行分支或测试用例；这消除的是构建依赖，不声称此前生产节点实际运行了一个多余线程。已完整构建 `build-storage-e2e`，原课程测试通过；检查生成的 `libbustub.a` 成员确认不含该对象文件。此前生产 cpp/h 与共同测试业务脚本均未改。

### 14.2 超出当前改动的职责与重复工作检查

| 范围 | 当前判断及处理 |
|---|---|
| A BufferPool / B MetadataCache / CLOCK-Pro | 共用替换策略代码，各自持有版本与驻留状态。A 普通页和 B 私有页/WAL 的安全淘汰条件不同，不能合成同一状态机；旧 LRU-K 仅属课程测试。 |
| TranslationDirectory / PathCache / FrameArena | 临时页到帧映射、寻址路径、正文内存分别拥有；PathCache 没有第二份正文，ghost 不阻止空组回收。 |
| B pending / Journal / Deferred | 分别记录待写回最新页、恢复事实和对象正文落位任务；复用既有 IO 与额度，没有理由机械合并成一个队列。 |
| Store / F22 / F14 / F12 | Store 退出逻辑引用，F22 安排有界工作，F14 复核范围使用者，F12 改可分配位图。节点只注册原维护回调，未恢复独立 Store 轮询线程。 |
| F31 / F02 缓冲许可 | 节点总额度、组件在途上限与帧固定压力含义不同；借用已计账帧不再计算一份实际正文。 |
| F28 / F25 / Manifest / lease | 恢复点、传输快照、持久保留与单次访问寿命各有用途；Raft 接收仍 Prepare 一次再 Install 同一候选，没有恢复双重构建。 |
| F30 扫描 / 正常读取 | 共享校验算法；扫描必须读实际设备，正常读取可使用缓存，不能删除其中一条职责。 |
| SnapshotTasks / F02 | 前者执行固定快照正文读取/编解码，后者拥有设备请求和缓冲。后续 F35 通用 CPU worker 若覆盖前者，应接管并删除旧执行入口；不能为“统一队列”让压缩阻塞必要设备完成。 |
| 旧文件部署 / 对象部署 | 仍有冻结基线、兼容和课程消费者；一个节点显式选一种部署，并非同时维护两份权威数据。 |

仍有重复计算：RPC/client/command 解码后的重新编码用于规范性校验；UPDATE/DELETE Prepare 仍手写全表扫描；页访问仍有多层寿命/正文同步。本次登记为 F35/F26 后续优化，未直接删除检查、旧值验证或保护。没有证据将它们全称为“无用防御”；也不能把“未发现第二套权威模块”写成“整个项目不存在冗余成本”。

### 14.3 测试设计与提交条件

本次复用原 `DiskSchedulerTest.ScheduleWriteReadPageTest`、原共同 C1、原低缓存 P2；没有新增 fixture、oracle、mutation 或常驻小测试。课程测试只验证构建拆分后原队列仍可用，不计作新存储正确性。C1 仍以独立业务模型检查 2048 行的更新/删除/重插；P2 仍为 20,000 行、1,000 次预处理写入、4 客户端的资格场景，独立输入/模型、选主和重试规则不变。

P2 沿用此前显式资格参数：准备 300 s、校验 180 s、请求 30 s、拟测量 10 s、外层 641 s；B 正文 64 页、共享内存 64 MiB。这不是默认长性能基线，也不能把未进入测量窗口的运行换算吞吐。源码复核后才复跑；真实节点、TCP 和 Direct 文件，不使用模拟数据库。构建清理不需要新参数排列或重复已有变异，既有 §12 四项阶段/两项变异/C4 保留原源码归属。

本轮 GCC 16 Release 实际结果：

| 检查 | 结果 | 失败含义/范围 |
|---|---|---|
| build-storage-e2e / 原 DiskSchedulerTest | 构建成功；原课程测试 1 项通过；生产库成员不含旧 scheduler | 构建分离正确，不计作新存储压力证明 |
| 原 C1 | 51.764 s，request_failure | 512 次更新成功后 verify_update 收到 NOT_LEADER，term 由 1 变 2；之前一次成功读取约 521.766 ms。这是请求耗时，不是独立测得的锁持有时间。cluster_exit=0，驱动在失败收尾时被终止。 |
| 原低缓存 P2 | 362.419 s，observation_incomplete | 313 次加载批次后，加载校验成功 124 次，第 125 次未确认，最近响应 term=131；未进入预处理写入/测量，business_validated=false、measurement_complete=false。cluster_exit=-9，日志记录节点 1、2 收到 TERM 后未及时退出，被强制 KILL。 |

既有定向测试只证明其明确的局部条件。它们通过不能覆盖本轮健康场景/组合压力失败；本轮没有修改这些测试来放宽判据，也没有反复跑到通过。代码仍显示 `DistributedNode::HandleRead` 在节点锁内调用 SQL，状态机又持生命周期/可见性保护；Receive/Apply 和同步 Store 持久化也有等待路径。需要按 F35 审查整体交接，不能仅依据一条慢请求断言 P2 所有等待的唯一根因。

**用户要求“都没问题就 commit”，本轮条件未满足，因此未暂存、未 commit、未 push。** 尚未通过的条件并非自动审批拒绝；是实际验收失败。未开始 S13 的生产实现。

### 14.4 下一步提议与总体进度（待用户确认）

主责 F35，配合 F26/F34；沿 S13.1 → S13.2a 的顺序组织，先完成能评审的共同交接协议，再编码：

1. **明确业务视图和准备依赖。** ReadIndex 是读取下界，实际执行还要固定合法的已发布状态及其寿命；既有 ExecuteReadSql 只接受当前 published index，不能把任意旧 index 当成可读历史版本。同时按 S13.1 复用已有优化器/只读 executor 为 UPDATE/DELETE 获取候选和完整旧行，保留完整谓词和旧值检查。
2. **在现有单写提案规则下分离耗时工作。** 节点锁内作协议判断、登记身份/完成槽；worker 可靠认领后执行 SQL/准备/持久化等阶段，完成回到协议线程推进。队满保存有界 pending，不持协议锁等待队列。设备 IO 继续 F02；CPU 调度若推广 SnapshotTasks，就接管旧入口，避免双执行器重复维护。
3. **逐个接续协议依赖。** 日志/term/vote 达到持久边界后才发送相应确认；Apply 顺序与已发布位置通过完成事件更新。协议线程不能转而同步等待状态机生命周期锁。读视图、任期变化、快照安装及关闭共同定义，不能简单删 mutex。
4. **原场景作为退出门槛。** C1、低缓存 P2 首先复验；再覆盖原 C2/C3 线性一致性和 C4/C5 恢复/追赶。补充测试只针对已有 E2E 未覆盖的真实交接风险，仍按八项审查与模块归档。此后才扩大多客户端在途提案和单客户端结果窗口。

S0–S11 的各轮已授权核心路径已经落地，S1/S2 等跨阶段验收保留项不能因此全部标完成；当前在 **S12 正式节点整合与验收收尾**。S12 已有预算、回收整合、日志尾部整理、共享零页、校验扫描、B 按需缓存/CLOCK-Pro；仍缺本轮正确性/压力通过与性能证据。

余下工作是：S12 收尾；S13.1、S13.2a、S13.2b、S13.3 及 F26 乐观短读接续；最终原 C/P 同条件比较、长稳与 SS/RS/PL/GT 有效消费者评测。正式编号到 S13，不新增 S14/S15；没有依据把阶段数换成工时百分比或给出固定完工天数。裸设备/真实掉电、原地 RMW/旧尾复用仍按既有范围单列，不默认为已经覆盖。

### 14.5 归档与清理

F34 阶段测试源码未变，源码包仅更新构建指纹/恢复说明并原位替换；当前结果加入同一包的 `final-review/`，含实际命令、配置、源码、失败结果和构建验证，旧通过仍按原轮次归属。当前恢复入口改为 `final-review/source/`。已知错误 F01 初版包不存在；每模块有效证据和原基线保留，不按旧日期误删。

收尾核验记录仍使用结果目录的 `cleanup-verification.json`：包含原临时目录及本轮 `/tmp/s12-final-audit` 的删除、实际节点所在 PID 环境的测试进程检查、两包清单/哈希核验、结果包 ignore 和 diff 检查。构建目录、设备镜像、展开测试不进入结果包或 Git；只保留各模块当前压缩包，没有新增错误旧包/备份包。

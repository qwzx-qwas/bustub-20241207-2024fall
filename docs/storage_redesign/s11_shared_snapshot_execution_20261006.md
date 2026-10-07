# S11 / F25 共享快照：执行与测试设计审查

基准：`8b80b6f` 加本轮工作区。协议：[共享快照](s11_shared_snapshot.md)。本文件只记录本轮，不把以前 F28 的运行次数累加成本次验证；未修改、未重跑共同 C1～C5/P1～P4。

## 1. 具体生产改动与先行逻辑审查

| 位置 | 改动及理由 |
| --- | --- |
| `BusTubRaftStateMachine::WriteSharedSnapshot` | 取得与 Raft 请求相同 index/term 的密封 checkpoint；复用表页寻址和原 allocation，写头部后把固定页共享进 F25 候选。源 checksum 从固定内容计算，头部写完才扩展对象，消除“大于数据库页的分配单位预先映射正文”的冲突。 |
| `BuildLocalCheckpoint` / `checkpoint_build_mutex_` | 序列化 checkpoint 发布/退役和共享引用取得；不让普通 Apply 持有这个锁。已接纳 worker 的等待沿用 Drain；初次 Capture 仍可能等待真实 IO。 |
| `NodeStorage::ShareObjectRange` | 增加显式目标逻辑偏移；只平移逻辑映射，不改原物理地址和分配身份。F28 Clone 传同偏移，F25 传页流位置。 |
| `ObjectSnapshotStore::Capture` | 使用状态机正式共享能力；候选/Manifest/保留/lease/GC 全部沿用。只有不支持共享时才走 canonical；实际共享失败不得静默回退。共享生成不再为了本地校验重建另一份临时数据库；接收方仍完整校验/安装。 |
| `BuildSnapshotState` / `FinishWorkingState` | 明确识别新 `BSPAGE01` 和旧 `BSBUND01`，校验后在新工作空间装页、恢复原页号高水位、重建索引及 Session。复用恢复尾部代码；原 canonical 禁止删除记录的校验仍保留，新原页流允许正常的删除标记。 |
| `ObjectReferenceManager::Reclaim` | 先排除持久共享引用和 RAM 读取保护，再仅隔离本次实际释放范围。原先对整片退役候选设置隔离，会误挡仍活着的共享快照读取。持引用锁期间只计算有界范围，不等 IO；B 原视图提交继续防止并发变化造成错误释放。 |

顺序审查先于测试：明确 fixed page 与当前工作页的区别、对象候选的独立所有权、源 owner 不随目标偏移改变、header 写入与共享顺序、接收端高水位、错误不可发布、锁顺序和长期传输保护，然后编写增量场景。

运行中曾遇到两类失败，均保留诊断：沙箱 socket/LSan 限制（换允许本机网络/线程检查的运行环境，未关闭 sanitizer）；生产 term 校验遗漏和 F14 隔离过宽（修正逻辑后验证）。后续代码复核另识别出分配单位不等于页大小的布局问题，先修生成顺序再加强原兼容场景。失败版本不作为验收，不保存错误测试的另一个源码压缩包。

## 2. 测试全景及独立判据

新场景均在 `F25-shared-snapshot/tests/shared_snapshot_test.cpp`，只压缩保存。

| ID / 测试名称（省略 S11SharedSnapshot） | invariant / 具体 bug | 类型 |
| --- | --- | --- |
| T1 `SharedPagesSurviveRootRetirementAndRestartThenInstallWithSessionsAndSuffix` | F25 的固定页不随 F28 根退出而失效；新快照取代接收端旧 checkpoint，内容/Session/日志后缀一致，已有正文不在发送端重写 | integration |
| T2 `TransferLeaseOutlivesSnapshotAndCheckpointRetirement` | 正在发送的旧快照即使已退役，也不能在两块读取之间被 Collect 删除 | integration |
| T3 `CorruptNativeBodyAndWrongBoundaryCannotReplacePublishedState` | 损坏正文、错误 index/term 不能替换现有状态/快照清单 | integration / regression |
| T4 `CanonicalCompatibilityAndNativeInstallUseExistingFileDeployment` | 旧格式继续可读，新页流能安装到文件后端；分配单位大于页时不能误覆盖待共享范围 | integration |
| T5 `DeferredPageSharingKeepsBytesThroughSourceLocationSwitch` | Data 未完成时快照可用；Journal→Data、关闭重启后字节不变 | integration |
| T6 `ReclaimOnlyFencesDeadUnitsWhileSharedMiddleRemainsReadable` | 正在释放两侧无用单位时，中间仍被共享使用的单位可以正常读取 | integration / regression |

### T1：目标 → 输入 → 执行 → Oracle → 失败含义

- 输入：35 条不同正文的 SQL 记录、两个表和二级索引，页号因此有间隔；实际执行 F28 checkpoint。
- 路径：真实 SQL Apply → F28 → F25 Capture → F13/F14 共享 → Manifest；更新原表并发布下一 checkpoint、回收、重启；接收端先用相同前缀建立 index 2 的本地 checkpoint，经 StageChunk、Validate/Publish/Load 安装 index 5 的快照，再提交日志后缀并重启；不在接收后建立新本地 checkpoint，确保恢复必须处理旧根与新快照并存。
- Oracle：从 SQL 构造的完整预期行、原 Session 回复、重启后先停在快照边界而非旧 checkpoint、接收端新插入/建表后原表仍正确；不以生产编码器生成 expected。只在 Capture 时观察包含独特正文的 Data 写入应为零，约束“不重写已有正文”，不固定一般 IO 次数。
- 失败含义：共享引用/来源身份/目标偏移错误、重复复制退化、安装高水位或恢复边界错误。属于组合保护，异常仍需结合阶段定位；不是每个失败都能唯一定位函数。

### T2

- 目标：整次传输使用期独立于 latest/previous 清单。
- 输入：三代真实 SQL 快照；第一代先取得 SnapshotInput 并读一块，再生成两代新状态、退出保留并执行 Collect。
- 路径：既有 F25 Source/Lease → Manifest 退役 → RaftObjectStorage::Collect → 再读取并在真实 BusTub 安装第一代。
- Oracle：新取得已退役快照应拒绝；已有会话仍读出第一代 SQL 值。退出最后会话后 Collect 确实完成删除。此处不再复制 S8/F28 的“填满设备再写入”物理复用场景。
- 失败含义：生命周期交接错误或旧版本内容被回收/替换。`removed > 0` 单独不能证明物理单位已复用，文档不作该宣称。

### T3

- 输入：源数据库的一份真实快照，接收端已有不同表；改变一个正文 byte、传错 index，并对下一次 Capture 传错 term。
- 路径：真正的 ValidateSnapshot/LoadSnapshot/Capture，非假解码器。
- Oracle：拒绝损坏或不匹配输入，接收端原 SQL 内容和发送端已发布快照 ID 不变。CRC expected 仍是原始流中保存的值，测试不重新计算损坏后的 CRC 来自证。
- 失败含义：校验缺失、跨边界错标或错误发布。未知格式/完整性边界依然属于生产输入检查，不是为测试扩大 API。

### T4

- 输入：真实 SQL；F12 分配单位取已查询基准的两倍，不把任何设备对齐写死为 4 KiB。
- 路径：保留的 canonical WriteSnapshot → 新对象部署 Load；同一数据的新共享 Capture → 既有文件部署 Load。
- Oracle：两边都得到明确 SQL 值。它覆盖兼容和不同分配形状，不能证明跨架构的 native ABI 可移植。
- 失败含义：格式分派/兼容被破坏，或 header 写入过早映射后续页范围。

### T5

- 输入：真实 SQL 页面，沿已有显式选项启用 Deferred；链接器在包含特定输入正文的 Data pwrite 暂停。
- 路径：F28/F25 真实生成、Journal 来源读取；释放暂停后关闭、重启，再安装到另一对象部署。
- Oracle：暂停期间 Capture 能完成；前后完整快照字节相等，恢复后 SQL 值正确。等待上限为进度保护，不是性能阈值；超时会释放 IO 闸门。
- 失败含义：错误等待所有 Data 落位、按错误来源读取、正文引用在转换/重启时丢失。

### T6

- 输入：三个真实分配单位，仅中间一个被另一个对象共享；原对象删除，两侧可以回收。后台周期 GC 使用已有预算暂缓，测试显式调用真实 ReclaimObject。
- 路径：真实 FS 对象写入/共享/解除引用 → F14 回收事务 → fdatasync 处确定性暂停 → 读取中间范围 → 释放暂停并完成两侧回收。
- Oracle：中间字节等于独立固定输入的中段，读取不能报 Busy；最终只释放两侧。没有检查内部 bitmap、Gate 数量或地址实现。
- 失败含义：把仍存活范围一起隔离或释放；把旧的整候选隔离放回去会在读取时确定失败。

## 3. Production pollution / 接口审查

| 接口或变化 | 生产必要性 / 删除测试后是否保留 |
| --- | --- |
| `RaftStateMachine::WriteSharedSnapshot` 及 BusTub override | F25 在不依赖具体 BusTub 类型的前提下使用共享能力；旧 KV/文件部署明确 unsupported，生产调用在 Capture。保留。 |
| `ShareObjectRange(..., destination_offset)` | 新页流位置与源对象位置不同，是生产需求；Clone 显式传原偏移。没有为了测试添加默认值或 getter。保留。 |
| 私有 `BuildSnapshotState` / `FinishWorkingState` | 正式格式分派及恢复代码去重，不暴露内部状态。保留。 |
| 私有 checkpoint 发布/共享锁 | 保护源根被替换时的引用取得；不改变每次业务 IO 的调度。保留。 |
| F14 回收范围计算顺序 | 修复真实并发错误，不是测试分支。保留。 |

没有新增 production test hook、配置开关、内部状态 getter、TEST 宏、只透传的公共 wrapper、测试默认参数或默认路径变更。测试故障与观察仅在测试可执行文件的系统调用包装中；生产库不链接这些包装。

## 4. 重复、兼容与接管

- F28 的 `NewerPortableSnapshotSupersedesLocalCheckpointWithoutLosingSuffix` 从本轮组合执行中排除：新 Capture 本身会推进本地 checkpoint，旧场景不再独立验证原分支。首次 T1 使用没有本地 checkpoint 的接收端，只能验证 portable 安装/重启/后缀，不能等价接管旧 checkpoint 被更新快照取代的风险。本次复审加强同一个 T1，让接收端具有更旧的 checkpoint，再验证重启选择和后缀；没有本地 checkpoint 的接收由复用的三节点场景保留。旧 F28 归档保持原历史版本，不作为新模块的错误旧包删除。
- S8 文件 SQL 场景保持 canonical 部署，验证兼容；已有三节点 TCP 场景使用当前对象页部署，验证真实 InstallSnapshot 和后缀。没有再复制一套相同网络测试。
- F28 保留其余 7 项；F27 保留 12 项。它们由旧有效源码包引入，输入/oracle 不变；唯一接口适配是显式目标偏移。
- T2 的物理复用由 S8/F28 的现有风险验证承担，本轮不再自建各层 bitmap 测试。
- SS/RS/PL/GT 文档是补充评测设计，未宣称已经有结果。未来共同 C/P 接管时保持业务内容和独立预期，再删减阶段测试，不在项目中展开常驻小测试。

## 5. 变异验证

在 `/tmp` 复制生产源，独立编译并链接错误版本，不编辑生产代码/库。预期触发：

| 最小错误 | 对应保护 |
| --- | --- |
| 强制退回 canonical、再次复制正文 | T1 的正文写入观察及真实业务恢复 |
| 忽略目标逻辑偏移 | T1 无法正确构建/恢复共享页流 |
| 删除传输 Lease | T2 在回收后继续读取失败 |
| 跳过页流 CRC | T3 不再拒绝损坏正文 |
| 跳过已应用 term 核对 | T3 错误发布下一代 |
| 回收忽略持久共享引用 | T1 旧根退出后内容/恢复失败 |
| 对所有候选而非实际释放范围设置隔离 | T6 暂停提交时，活着的中间范围读报错 |
| 写 header 前预先扩展整个快照 | T4 在较大分配单位下共享范围与 header 填充相撞 |
| 只要有本地 checkpoint 就优先选它，不比较快照边界 | 加强后的 T1 拒绝回到旧恢复点，核对新快照和后缀 |

编译失败、watchdog 超时、sanitizer 环境错误不算检出。实际数量和命中记录以结果包为准。

## 6. 稳定性与资源

- 输入固定且有真实差异，无未固定随机 seed；每例独立设备/NodeDirectory。
- IO 闸门由具体事件控制，等待有上限，退出释放；没有用固定 sleep 猜测 IO 到了哪个阶段。
- 网络测试借用系统分配的 loopback 端口；仍有端口交接和机器调度导致超时的环境边界，不能把它当性能指标。
- 共享的系统调用包装在每例收尾复位；测试 runner 串行运行用例，不依赖用例顺序。包装仅该测试二进制有效。
- 默认按原有课程量级预算运行；构建 `-j3`，不需要 GPU。ASan/UBSan/LSan 开启；未进行 TSan、真实断电、裸设备或性能前后对比。

## 7. 最终测试矩阵

| Test | invariant | Oracle | 新 failure mode | 重复 | Production pollution | 建议 |
| --- | --- | --- | --- | --- | --- | --- |
| T1 | 独立固定版本与正确恢复 | SQL/Session/后缀、无正文重写 | 原根退出后快照失效、共享偏移错 | 接管 F28 原 T6 | 无 | 保留 |
| T2 | 传输会话跨退役有效 | 旧值与退出后的删除进度 | 两块间丢失引用 | 复用下层物理释放验证 | 无 | 保留 |
| T3 | 拒绝坏正文/错边界 | 抛错且原状态/清单不变 | 新格式损坏或错标 | S8 保留通用 chunk 重试 | 无 | 保留 |
| T4 | 格式/部署/分配形状兼容 | 相同 SQL 值 | 非等页大小分配冲突 | 与 S8 网络边界不同 | 无 | 保留 |
| T5 | Journal/Data 切换内容不变 | 固定字节和重启后 SQL | 长期快照丢失尚未落位正文 | F27 仍测任务协议 | 无 | 保留 |
| T6 | 只隔离实际释放范围 | 提交暂停时活范围可读 | GC 虚假阻断共享读 | 非 bitmap 或下层空间计数复刻 | 无 | 保留 |
| F28 原 T6 | portable 新于 local | 原单节点 Capture 已改变所经分支 | 本次加强 T1 后接管 | 由接收端旧 checkpoint 场景合并 | 无 | 合并 |

核心保护为 T1/T3/T5/T6；T2 保护传输寿命；T4 保护兼容与合法部署形状。没有保留不能说明具体 bug 的新场景。尚未穷举所有发布阶段的断电点，未证明 native 格式跨 ABI 兼容，未实现增量/压缩/全面异步协议推进。

## 8. 首次执行的运行证据与归档（后续复审见 §9）

- 最终正常实现：**33/33 通过**（`complete/`：F27 12 + 组合 21；其中新场景 6、F28 7、S8 8）。开启 Clang14 Debug / ASan / UBSan / LSan；真实 TCP 场景在这次组合中通过。
- 最终隔离变异：**8/8 检出**（`final-mutations/`），每个均编译/链接成功并在指定测试失败；没有把超时或 sanitizer 环境失败计为检出。
- 先前失败和中间复验只保留日志/XML/命令作为诊断，不混计进最终通过数。最后回收改动和布局改动之后重新执行了上述全部 33 项；格式检查无差异，`git diff --check` 通过。
- 最终源码唯一包：[F25-shared-snapshot.tar.gz](../../test/archives/F25-shared-snapshot.tar.gz)；[结果入口](../../test-results/storage-f25-s11-20261006/README.md) 保存本地压缩证据和校验说明。首次建立 F25 专属包，无旧 F25 包或备份。S8、F28 等有效依赖保持原哈希。
- 展开的测试、构建库、设备镜像、隔离变异可执行文件均位于本次 `/tmp/f25-work`，归档校验后删除；源码树没有新增常驻测试。结果 tar 沿已有 `.gitignore` 排除，README/SHA256SUMS 可保存入 Git。本轮未 commit。
- 剩余限制：当前 Capture 同步入口可能等 checkpoint 和 IO；只支持当前 native 页 ABI；完整跨节点正文仍需传输/校验，接收端重建索引；增量、压缩、真实掉电、TSan 和共同前后性能比较尚未完成。

## 9. 再次复审：不能把“没有 checkpoint”当作“已有旧 checkpoint”（2026-10-07）

### 9.1 先行代码结论

核对 `RecoverRaftPersistentState` 的 `use_local = local && local->index_ >= latest_index`、快照边界/term 核对和后缀恢复顺序，现有生产逻辑按边界选择，没有为修测试修改生产代码。复核共享源保护、目标偏移、持久所有权、Deferred 来源切换、仅隔离实际释放范围和候选发布顺序，未发现新的实现偏离。同步 Capture、页目录遍历和接收端索引重建成本仍在，不以功能测试声称性能改善。

### 9.2 本轮实际改动

- **修正覆盖说明**：上一版将接收端没有本地 checkpoint 的 T1 称为接管原 F28“新快照覆盖旧 checkpoint”不准确；无本地恢复点与存在较旧恢复点是不同分支。
- **加强现有 T1，不新增重复测试**：接收端先执行与发送端相同的前两条 SQL、发布本地 checkpoint（index 2），再接收共享快照（index 5），提交两条后缀后重启。断言恢复先选择 index 5、恢复对应 Session，再重放后缀并核对完整 SQL 内容。正文共享、原根退役/GC/重启、索引和高水位原断言保留。
- **新增一个隔离变异**：把恢复选择改成“有 local 就用 local”；正常接收不受影响，但重启时无法以错误旧边界恢复。只有指定 T1 执行并失败才算检出，编译失败/超时不计。
- **修正归档和说明**：保留六个新场景和原有复用场景数量；同步 RESTORE、MANIFEST 和唯一 F25 源码包。S8/F28 等独立依赖包保持不变。

### 9.3 测试要求复核

Oracle 仍来自独立 SQL 输入、明确的 index 边界和 Session 回复，不读取控制记录或内部页排布。`LocalRecoveryPoint()` 是已有生产恢复接口，本次不新增 getter。旧根在接收前通过正式 Checkpoint 创建，禁止手写伪元数据。没有新增 public API、默认参数、测试配置、TEST 分支或生产 wrapper；没有修改 C/P 基线。每例隔离设备和目录，完成/回收沿真实路径；原测试进度上限及 cleanup 保持。

本轮运行与归档结果在下节记录；此前 33/8 的运行保留为历史证据，不冒充本轮复验。

### 9.4 本轮复验与清理

- 正常实现重新执行 **33/33 通过**：F27 12、组合 21（F28 7、S8 8、共享快照 6），含真实三节点 TCP；Clang14 Debug / ASan / UBSan / LSan。
- 隔离变异重新执行 **9/9 检出**：原 8 个及新增恢复选择变异。新增变异命中 T1 的恢复路径，报 `local checkpoint does not match committed Raft history`；编译/链接均成功，未把环境失败或超时计入。
- 证据包 `runs/review-run`、`runs/review-mutations` 是当前结果；原 `complete` / `final-mutations` 仅为历史证据，不累加数量。当前包中只包含一份修正后的 F25 测试源码归档。
- 本轮 production 哈希与首次最终运行一致；原七个依赖源码包哈希未变。源码与结果包原位替换，没有旧 F25 包或备份；展开测试、编译产物、设备镜像和变异程序在归档核验后清理。
- 功能结论仍限定于 Direct 普通文件及本机 TCP；未完成真实掉电/裸设备/TSan/性能比较。SS/RS/PL/GT 只纳入方案，未运行新增性能评测。本轮未 commit。

## 10. 提交前复核（2026-10-07）

按用户本轮指令再次从生产路径核对：固定 checkpoint 边界、源引用交接、目标逻辑偏移、候选原子发布、接收校验与恢复、Deferred 来源及 GC 范围保护；没有新增生产或测试修改。

本次只修正 F25 模块文档的当前职责、兼容边界和测试归属：全量共享格式已落实，增量/压缩未实现，接收重启从零开始；S8 历史证据与 S11/F25 当前证据分开。历史 prompt 保留当时范围，当前共同协议优先。

直接核验当前压缩包而非只读取文字摘要：生产源、七个依赖包、测试源码、包内证据及外部校验共 706 项哈希匹配；正常运行 XML 为 12 + 21 项，无失败/跳过；9 个变异的 27 个编译/链接/执行命令符合预期，未超时。代码与测试字节未变，本次不重复运行此前已通过的相同测试。

F25 源码包、结果包各一份，源码包内嵌到证据包的副本与当前文件相同；没有旧版本/备份。`/tmp/f25-work` 和 `/tmp/f25-review` 均已删除；阶段测试不注册常驻入口。结果 tar 按现有规则忽略，源码 tar 与结果摘要随本次提交保存。压缩结果保留运行当时的文档快照，本节提交前文档修订由 Git 记录。

本次提交范围为 S11/F25 生产实现、方案、测试源码归档与证据摘要，以及已授权的 SS/RS/PL/GT 方案文档。没有开始增量/压缩、S12/S13 或新增性能评测。

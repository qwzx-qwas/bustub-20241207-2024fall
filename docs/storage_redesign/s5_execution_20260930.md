# S5 Journal 基础回收：实现、验证与测试设计审查

日期：2026-09-30（Asia/Shanghai）

[总方案](README.md) · [当轮协议](s5_journal_recycling.md) · [测试归档](../../test/archives/README.md) · [结果与恢复](../../test-results/storage-s5-20260930/README.md)

## 0. 结论与范围

**本轮 S5 基础 Journal 回收与恢复：已完成。** 基准为 `cce998a`，叠加本次工作区修改；精确生产文件哈希、测试源码、命令与结果见归档，不用未提交的 Git 提交号冒充源码身份。

- 用户选择第一种兼容：新建 Journal 默认 v2；旧 v1 保留原读写、checkpoint 与恢复能力，不迁移、不重格式化。
- 新格式分离逻辑 LSN 与物理槽，单元带段身份；checkpoint 发布后持久化退役边界，再允许前缀整段复用。
- 维护保留一个 checkpoint 数据单元；段头另计预算。普通提交仍共用一次组 Flush，未恢复每组三次 Flush 或重复 B WAL。
- B 恢复完成前补完中断的退役发布；过期入口即使 CRC 正确、仍位于保留段中，也不能恢复使用。
- 新增 8 项真实组件集成场景通过；复用 F03 8 项、F08 6 项、F09 4 项及适用 F10 5 项，共 23 项通过。8 个定向变异由指定判据检出。Clang 14、ASan/UBSan。
- 本轮使用 Linux Direct 普通文件后端。没有宣称裸设备、真实掉电、任意介质回滚、SQL/Raft E2E 或性能提升已经验证。共同 C/P 内容与历史基线未改。

S5 的 F33/F34 节点统一状态/启动编排仍未完成；普通对象、Deferred、多消费者日志保留及旧格式迁移均不在本轮范围。

## 1. 先分析生产逻辑，再编写测试

先阅读 F03、F06/F07、B 的 FULL/PATCH、页写回及 checkpoint 依赖，再从正常提交、空间满、发布失败、重启及关闭逐项推演。未先堆回归测试寻找修复方向。

### 1.1 实际修改归属

| 位置 | 实际变化与原因 |
| --- | --- |
| [JournalService](../../src/storage/disk/journal_service.cpp) 的 `Create/Open/LoadSegments/Scan` | 新建 v2，识别 v1；验证段目录、必要后缀与退役旧尾，继续使用同一分片/批次解析器 |
| 同文件 `Layout/Physical/AvailableUnits` | 单调逻辑位置映射至循环物理槽；只复用已退役前缀，不引入普通对象 extent 树 |
| 同文件 `ReserveSequences/RetireBefore` | 预留覆盖下一完整物理圈的段身份；排空后持久退役，失败关闭写入资格 |
| 同文件 `Append/Run` | checkpoint 专用额度与实际段头成本；跟踪活动组，回收等待真实 IO 完成；组持久化规则沿用 |
| [BootstrapStore](../../src/storage/disk/bootstrap_store.cpp) `Encode/Decode` | v3 解释 checkpoint 位置为逻辑位置；v2 仍按物理范围验证，v1 无 checkpoint |
| [MetadataEngine](../../src/storage/disk/metadata_engine.cpp) `Checkpoint/Open` | 页→checkpoint→引导→退役控制；验证后补完中断发布，Ready 仍在恢复末尾设置 |

F06 的物理范围接口已能承接此次操作，因此复用而未加转发类。B 的页 LSN 本来就是 64 位，FULL/PATCH 的比较接收新的逻辑值，不改页格式或另写一套恢复器。A/SQL/Raft 生产路径没有改动。

### 1.2 复核中处理的问题

1. **代码推演发现：** checkpoint 引导已发布，但退役控制尚未持久化时中断，最后一个维护单元可能已经用掉。只恢复 B 而不补完退役会永久缺空间。修法是在完整验证后、开放 B 前补完控制发布。
2. **实际集成发现：** 重开后的下一段可能接近原预留区间末尾，仅增加一个固定区间不一定覆盖下一整圈。修法按真实下一号码计算所需上界，再向整圈边界取整；不临时在普通提交内添加控制 Flush。
3. 测试首稿的头文件遗漏、读取私有接口、单条输入超过测试配置上限均属于测试问题。补依赖、改成独立磁盘观察和合法非空输入；未把私有接口改 public，也未放宽 production 限制。
4. 变异审查发现原压力输入未耗到最后单元，原旧入口场景只靠整段退出就能拒绝。补强同一场景：小记录实际更新填满余量；旧新入口保持在同一段内，验证逻辑退役边界。没有因此增加一组重复测试。

### 1.3 锁、寿命与失败

- 追加门负责位置预留；短队列锁不持有到设备 IO 完成。回收关闭追加门，等待活动组及排队项退出，再提交独立控制单元。
- B checkpoint 阻止新修改，但纯内存旧快照仍可读；这些读者不读取 Journal，不应阻塞日志回收。
- Flush 或控制写失败原样传播，已接受且结果不确定的操作不能声称“没提交”。B 隔离到重开，不偷偷重试或用旧入口掩盖损坏。
- Close 排空已有维护与 IO。对齐缓冲保持初始化；不全段擦零，不用文件 punch-hole 或 HPArray 的 RAM 回收替代日志使用权协议。

## 2. 测试全景

测试源仅保留在 [S5-journal-recycling.tar.gz](../../test/archives/S5-journal-recycling.tar.gz) 内的 `test/storage_redesign/recycling_test.cpp`。以下均属于真实存储组件 integration；兼容和旧模块复用同时承担 regression，均不标为业务 E2E。

| Test（前缀 `S5Recycle.`） | invariant / 防止的具体错误 |
| --- | --- |
| `WrapReopenAndLiveSnapshot` | 多轮物理环回和重启后顺序仍向前、内容完整、旧尾不复活、旧 RAM 视图不变；防止用物理偏移作为 LSN、错拼旧段、错误回收读者页 |
| `FullLogStillCheckpointsAndResumes` | 普通写耗尽后 checkpoint 仍有前进空间，且之后可以写入并恢复；防止前台吃掉最后维护额度 |
| `InterruptedRetirementCompletesBeforeOpening` | 发布控制失败不报成功、不继续服务；重启补完已发布入口的退役并能继续写；防止永久满盘及提前复用 |
| `MaintenanceDrainsAndPreventsEarlyReuse` | 控制 IO 未完成时 checkpoint/Close 不得完成，新修改应被拒绝，已有视图可读；防止提前结束维护与缓冲寿命错误 |
| `OldValidBootstrapCannotResurrectRetiredHistory` | CRC 合法的旧恢复入口小于持久边界就拒绝，即使同段还在；防止旧引导把退役历史重新变为权威 |
| `RequiredCorruptFragmentCannotBeSkipped` | 必要后缀损坏时不能开放 B；防止将当前损坏误作可忽略旧尾 |
| `EmptyReopenReservesIdentityWithoutInventingHistory` | 空 Journal 多次重开、跨段追加再重开，只重放实际完整批次；防止预留号码被当成已有日志、空隙连接错误 |
| `LegacyImageRetainsAppendOnlyReadWriteRecovery` | 改造前真实生产镜像能读、改、checkpoint、再恢复，控制格式不被改写；防止隐式升级、物理位置被解释为逻辑位置 |

没有无法说明具体风险的新增测试。未给每个私有方法建立 unit test。

## 3. 逐项：目标 → 输入 → 执行 → Oracle → 失败含义

共同 production 路径：`MetadataEngine` → 唯一 `JournalService` / `MetadataBackend` → F06/F05 → F04 → F02 → F01 → Direct 文件 IO。空 Journal 场景直接使用 F07 正式接口。不存在模拟 B 的手写引擎。`fixture.h` 只负责外部设备准备、明确预算及输入模型。

### 3.1 WrapReopenAndLiveSnapshot

- **目标：** 逻辑提交顺序与物理环回无关，恢复结果等于所有成功请求，老视图保持原值。
- **输入：** 28 次生命周期，每轮两个不同长度的非空值更新（9013、47 字节），实际小 Journal、checkpoint、关闭重开；保留第一轮快照。
- **执行：** 正式 B 提交/写回/恢复，实际发生跨物理槽批次与较短复用尾部；测试独立观察并断言这些输入前提确实出现。
- **Oracle：** 从提交输入维护 `std::map`，逐项比较 Get/Scan 内容；LSN 大于上次成功 LSN；物理环回次数超过两次。输入模型不调用生产页、日志 codec。
- **失败含义：** 数据/顺序/旧尾/版本寿命有问题；恢复异常只能定位至该链路，还需结合错误信息分析，不能宣称单条断言精确定位所有层。

### 3.2 FullLogStillCheckpointsAndResumes

- **目标：** 空间压力不能阻断用于释放空间的维护操作。
- **输入：** 有实际数据的大小记录，先用大更新逼近容量，再通过小记录更新填满普通可用余量；循环有明确上限。
- **执行：** 正式 Commit 返回资源不足，然后 Checkpoint，再 Commit、重开。
- **Oracle：** 确认出现容量拒绝且此前有成功提交；拒绝不改变模型；checkpoint durable、后续新写 durable、重开内容等于模型。不是只检查进程未挂。
- **失败含义：** 普通准入、维护额度或回收接续有问题；移除维护预留后在 checkpoint 准入处确定失败。

### 3.3 InterruptedRetirementCompletesBeforeOpening

- **目标：** 新入口与退役发布之间的中断可接续，故障实例不再对外服务。
- **输入：** 日志接近容量，分别在控制单元 `pwrite`、以及该写完成后的 `fdatasync` 注入一次 EIO；此前页面、checkpoint 与引导调用真实 IO。两个独立镜像共用同一场景和输入模型。
- **执行：** Checkpoint → 控制失败 → 销毁 → 正式 Open → 新提交。
- **Oracle：** 返回 Indeterminate 和原错误、Read 拒绝；写失败时独立读取的控制边界小于新入口；Flush 失败时新边界虽已可读，仍不得报告 durable。重开后边界等于已发布位置，数据模型不变且能继续写。
- **失败含义：** 未隔离故障、过早确认或漏掉启动补完。分别检查写失败和写后 Flush 失败；读回可见不证明持久化，均不模拟真实断电缓存丢失。

### 3.4 MaintenanceDrainsAndPreventsEarlyReuse

- **目标：** 实际控制 IO 完成是 checkpoint/关闭的前提，维护期修改不能绕过门。
- **输入：** 暂停控制写完成后的真实 Flush，已有非空已提交数据；condition_variable 明确通知已进入暂停点。
- **执行：** 后台 Checkpoint，主线读取旧视图，通过独立任务发起 Commit 和 Close，再释放 IO。
- **Oracle：** 暂停期间 checkpoint 未完成、Commit 资源不足、Close 等待；释放后操作按契约收尾，旧模型仍一致。
- **失败含义：** 提前返回、维护门或寿命错误。有限等待只作调度/watchdog，不作性能指标。释放守卫在 future 析构前运行，失败也不永久卡住。

### 3.5 OldValidBootstrapCannotResurrectRetiredHistory

- **目标：** 入口是否退役由持久边界决定，不只比较物理段是否存在。
- **输入：** 两次真实 checkpoint，保留第一轮原始引导副本；调整明确 Journal 段大小，断言两入口在同一段，再恢复旧副本。
- **执行：** Bootstrap 能读到合法旧副本，B Open 必须因其退役失败。
- **Oracle：** 原始副本来自先前成功 checkpoint，未经生产测试 codec 重编码；Open 抛 JournalError，Read 保持未开放。
- **失败含义：** 旧入口被误授权，或测试前提没有成立。该用例不要求清零旧物理内容。

### 3.6 RequiredCorruptFragmentCannotBeSkipped

- **目标：** 当前恢复所需数据损坏时不能把错误吞掉。
- **输入：** checkpoint 后成功追加实际大值，关闭后改变必要单元中的一个字节，保留原校验码。
- **执行：** 正式 Open 的 Journal 校验/批次恢复路径。
- **Oracle：** 在任何可读视图发布前抛错；没有用被测编码器生成“错误正确值”。
- **失败含义：** 完整性检查或错误传播可能失效；不主张单独删去某一层 CRC 必然漏检，因为批次与单元检查可能共同保护同一损坏。

### 3.7 EmptyReopenReservesIdentityWithoutInventingHistory

- **目标：** 尚未使用的预留编号不形成历史，跳号不影响真实批次恢复。
- **输入：** 空格式多次重开；随后追加两条非空、合计跨段的记录，并再次重开。
- **执行：** F07 Create/Open/TryAppend，真实 F02/F01 路径；不绕过 Journal 构造记录。
- **Oracle：** 回调条数严格等于成功追加条数，每一记录等于原输入，顺序大于前值。
- **失败含义：** 空日志起点、跳过预留余量或片段连接错误。

### 3.8 LegacyImageRetainsAppendOnlyReadWriteRecovery

- **目标：** 新代码不破坏已存在 v1 数据，也不偷偷迁移。
- **输入：** 用基准 `cce998a` 的 Bootstrap/Journal/B 源码和头文件编译旧生产生成器，产生非空值、checkpoint 和后缀；其余未改生产依赖复用当前库。
- **执行：** 新生产 Open/Commit/Checkpoint，再次 Open；不是新编码器手写一份“旧格式”。
- **Oracle：** 已知输入模型、原控制单元字节保持不变、引导仍为 v2（物理位置），新旧值都能恢复。
- **失败含义：** 兼容路径改变格式、错误解释位置或丢失历史。没有验证本轮未提供的迁移功能。

## 4. Production / 接口污染审查

| 修改 | 正式用途 | 删除测试后是否保留 | 结论 |
| --- | --- | --- | --- |
| `MetadataCheckpointRef::logical_` | 区分两种真实持久地址语义；默认 false 保留旧 v2 语义 | 是 | 正式格式需要，不是测试默认参数 |
| Journal 私有 `AppendCheckpoint` | 只允许真实 B 维护使用推导出的预留单元 | 是 | 未开放任意绕过准入的 public 开关 |
| Journal 私有 `CheckpointRef/RetireBefore` | 格式所有者产生引用并发布安全退役；B 为 friend | 是 | 有真实调用方，无测试调用/状态 getter |
| `active_`、维护排空及 Open 接续 | 保证真实 IO 寿命与中断恢复 | 是 | 正式控制流，不依赖测试宏 |

无新增 public 方法、测试 hook、配置默认路径、`#ifdef TEST`、内部状态 getter 或仅供测试调用的 wrapper。测试曾错误引用私有接口，随后改成独立磁盘读法，未改变封装。链接 `--wrap=pwrite/--wrap=fdatasync` 只存在测试二进制；普通场景调用真实系统调用。

## 5. 重复、兼容与后续交接

- 复用 F03/F08/F09 的原源码，共 18 项，不复制其矩阵。
- 复用 F10 五项，测试侧只让持久根解析识别 v3；尚未环回的该组输入无需改变业务预期。
- F10 历史 `DamagedRootAndDamagedTargetDiffer` 的旧根可回退预期依赖“旧日志永久保留”，不适用于新退役规则，本轮不执行该组合用例。新的旧入口拒绝交给 S5 对应场景；必要内容损坏拒绝由 S5 损坏场景及其余 F10 恢复校验接管。旧档案是有效历史证据，不篡改为新格式测试。
- F07 八项含 v1 原始位置/字段的测试不原样套到 v2；本轮实际旧生产镜像测试承担兼容接续，不复制一套 v1/v2 格式矩阵。
- 空 Journal 场景保护尚无 B checkpoint 的启动分支；有数据的 B 环回场景保护有 checkpoint 的恢复链，不能互相替代。
- 两个控制失败/暂停场景分别保护错误隔离与在途等待，不是换参数重复测成功返回。
- 后续 F27/引用管理必须接管“持久保留＋临时 IO”风险；真实 A/业务接入后应沿相同输入/独立模型演进共同 E2E，而不是把模块套件永久挂到默认测试入口。

## 6. 杀伤力：定向变异

在临时副本替换一个生产编译单元，正式工作树不改；预期是指定测试的指定断言/恢复错误，不把编译失败、超时或 sanitizer 崩溃当作检出。

| 变异 | 指定场景 / 结果 |
| --- | --- |
| 编号只预留至下一号码所在区间，未覆盖完整圈 | Wrap；明确出现 `reservation exhausted`，检出 |
| 普通准入不再保留一个维护单元 | FullLog；checkpoint admission 失败，检出 |
| Open 不补完中断的退役 | Interrupted；磁盘边界不等于已发布入口，检出 |
| 不识别可校验的退役旧尾 | Wrap；当前单元段身份不符，检出 |
| 同时绕过恢复入口及退役发布的持久下界限制 | OldValidBootstrap；Open/Read 意外成功，检出 |
| 控制单元写入后省略 Flush | Interrupted 的 Flush 故障分支；checkpoint 错报 Durable，检出 |
| Close 提前返回，不执行排空 | Maintenance；Close 在控制 Flush 暂停期间返回，检出 |
| 去掉 B checkpoint 对新修改的准入限制 | Maintenance；Commit 未及时拒绝而陷入维护 IO 等待，检出 |

8/8 检出。未宣称覆盖每种错误；部分完整性条件存在多层共同约束，单独删除一个校验仍可能被另一个检测，这不代表对应测试无价值，也不靠增加测试把所有内部条件逐条绑定。

## 7. 稳定性、资源与范围

- 每例独立 mkstemp 文件，RAII 删除；单例基础镜像 8 MiB，F02 3 worker、16 MiB 工作预算，B 活页数/批量显式有界。基础测试日志 16 个槽，每段 4 个查询对齐后的单元；旧入口场景改为较大段，专测同段内退役。
- 设备对齐实时查询，测试不能支持的几何明确报错，不写死生产 4 KiB Direct IO 要求。F03/F08/F09/F10 回归沿用其原 fixture 资源。
- 输入公式与种子固定，非空，大小不同；无未设种子的随机性。没有性能断言或 sleep 排列竞态。
- 故障门是测试进程局部共享状态，测试串行执行，不支持同一进程并行跑这些用例。暂停先握手，所有 future 在放行后收尾；外层 watchdog 防挂起。
- Filesystem/Direct 支持及编译环境仍是前提。ASan/UBSan 不等于 TSan 或真实断电验证。
- 新控制区单份覆盖；其校验损坏会拒绝恢复，可能损失可用性。本轮未实现自动重建。单份 Journal 无外部确认清单，不能检测介质把完整已确认尾部回滚成另一份历史；此故障模型限制沿用 F07。

## 8. 最终矩阵与清理

| Test | invariant / Oracle | 新 failure mode | 与其他测试重复 | Production pollution | 建议 |
| --- | --- | --- | --- | --- | --- |
| Wrap | 模型一致、LSN 单调、老视图稳定 | 环回/跳号/短尾拼接 | 无，新增复用真实路径 | 无 | 保留 |
| FullLog | 压力后维护及恢复仍成功 | 普通写吞掉维护额度 | 不重复一般容量拒绝 | 无 | 保留 |
| Interrupted | 故障隔离＋磁盘边界补完＋模型 | 新根发布与退役之间失败 | 与等待场景不同 | 无 | 保留 |
| Maintenance | IO 握手下未完成/拒绝/排空 | 控制 Flush 在途生命周期 | 接续 F10，新暂停点 | 无 | 保留 |
| OldValidBootstrap | 合法旧入口仍拒绝 | 同段内过期入口复活 | 接管旧格式回退规则 | 无 | 保留 |
| RequiredCorruptFragment | 未发布视图前明确报错 | 活跃后缀被误作旧尾 | 不复制全格式 fuzz | 无 | 保留 |
| EmptyReopen | 回调数、顺序、原输入 | 空历史跳过预留号码 | B 有 checkpoint 不能替代 | 无 | 保留 |
| LegacyImage | 旧生产输入模型＋原控制字节 | 隐式迁移/位置误解 | 不再复制 F07 v1 矩阵 | 无 | 保留 |

“保留”表示保留最终按模块压缩档案供后续接续，不注册长期常驻小测试。源码、runner、恢复说明与哈希在一个 S5 包内；原始结果、XML、变异证据及生产源码快照在单独结果包中。临时展开测试、二进制、设备镜像和构建目录归档后删除。

没有被此次改动淘汰的旧 v1 生产读写路径：它们仍受明确兼容承诺保护，不能当冗余删除。共用原分片/事务解析器；无新增未用 public 函数。S5 仅留本次复审后的最终包；此前缺少独立控制 Flush 故障场景的同名包被替换，不留旧版备份。其他有效模块依赖档案保持原哈希。


## 9. 2026-09-30 再次方案与测试复审

### 9.1 结论与本次补强

先重新推演 `SaveControl → ReserveSequences/RetireBefore → Checkpoint/Open`，以及锁、活动 IO 排空、旧格式兼容和旧入口拒绝；本次未发现需要调整生产代码的偏差。已有 `SaveControl` 明确要求 Flush，只有完成后才允许复用或报告 checkpoint durable。

测试审查发现原中断场景只在控制 **写入** 处失败，不能保护“写完成但 Flush 失败”的边界。修正现有 `InterruptedRetirementCompletesBeforeOpening`：用两个独立镜像分别在写、写后 Flush 注入 EIO，并验证后者即使读回新边界也不能确认持久化。`MaintenanceDrainsAndPreventsEarlyReuse` 改为在写后 Flush 暂停，检查 checkpoint/Close 排空，避免只验证写调用返回。测试数仍为 8，没有按层增加重复套件。

新增一个定向变异：只将 `SaveControl` 的 `PrepareUnit(..., true)` 改为 false。期待故障场景明确发现错误的 Durable 结果，不把未进入暂停点的 watchdog 当作检出。生产代码未新增测试开关、getter、默认参数或包装层。

本次重新构建并执行：S5 **8/8**、相关回归 **23/23**、定向变异 **8/8**；ASan/UBSan 开启。八项审查复核未发现新增 production/API 污染或重复矩阵。生产文件与上轮归档哈希一致，原 GCC/格式/cpplint 结果继续有效；此次重新检查文档链接、脚本语法、压缩包完整性及 `git diff --check`。未重跑共同 C/P 或添加性能结论。

### 9.2 这次验证与后续接入的关系

当前 Direct 文件测试实际经过 B、Journal、区域后端、IO 执行器和正式设备代码；不是手写假数据库。Direct 表示绕过宿主文件系统页缓存的 IO 方式，普通文件仍有宿主文件系统路径，因此不能据此声称测过裸设备或设备真实掉电。

| 尚未覆盖 | 何时接续 | 应验证的差异 |
| --- | --- | --- |
| 节点统一启动/关闭与故障状态 | F33/F34 实际接入时 | 正式节点能否按依赖恢复、排空，并正确开放或拒绝服务 |
| 业务正确性与性能对比 | S8 接 Raft Store、S9 接 A，按实际接入边界演进 | 复用共同 C/P 内容和独立 oracle，适配正式节点接口，确认业务请求确实经过新存储；不复制模块小测试 |
| 裸设备 | 有经授权可破坏的专用块设备及已审查用例时 | 块设备能力查询、寻址、对齐和错误传播；不能只拿普通文件结果改名 |
| 真实掉电 | 故障模型、设备及切电/重启控制可用，并讨论确认具体实验时 | 已确认数据在设备缓存丢失后的恢复；进程退出、函数返回 EIO 均不能替代 |

“后续验证”表示保留明确缺口并按接入条件安排，不表示整个 S5、真实掉电或业务 E2E 已完成，也不表示届时重跑同一组件脚本就自动覆盖这些边界。尚未具备条件的验证不得预先登记通过。


## 10. 提交前复审（2026-09-30）

再次按方案核对逻辑地址/物理槽分离、编号预留、段/单元身份、维护额度、旧入口退役、`Checkpoint/Open` 的发布顺序与关闭排空。未发现新的生产或测试问题。本次没有修改生产和测试源码；重新核验源码包内生产/依赖/测试哈希、结果包全部成员、31 项通过 XML 及 8 个指定变异判据，与 §9 实测一致，未无依据重复执行测试。

修正主方案当前阶段概述、S3 衔接及 §13.2/§13.3 中“回收待 S5”“计数协议仅候选”等过时文字；按日期记录的历史范围仍保留。F33/F34 仍待讨论实施，不将本轮基础回收提交等同于整个 S5 完成。

确认只保留一个最终 S5 测试源码包和一个最终结果包，没有旧 S5 备份或展开源码；两个历次临时构建目录均已不存在。F03/F08/F09/F10 等有效历史依赖保留原哈希。结果包继续按现有规则忽略，Git 保存其说明、哈希和清理记录；不会把大型设备镜像或构建目录纳入提交。

当前归档里的审查文档快照止于 §9；本节是之后的提交前静态核验，不改变归档中的实际运行证据，也不为追加文档重复打包或测试。

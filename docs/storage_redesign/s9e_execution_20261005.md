# S9.1e / F26：真实查询窗口与异步预取

2026-10-04 开始，2026-10-05 完成；基线 `4e9fc47`。范围为 S9.1e，F36、S10–S13 未实施。各轮复审按发生时状态记录，提交前核对见 §11。

[共同协议 §17](s9_buffer_pool_protocol.md#s9e-execution) · [F26](modules/page-storage-adapter.md) · [数组子方案](modules/array-buffer-pool.md)

## 1. 实际改动及方案落实

| 层 / 文件 | 实际改变 | 保持的边界 |
| --- | --- | --- |
| `IndexScanExecutor` | 复用既有点查 RID 集合和全索引扫描项，有限窗口去重页号；消费前发起预取 | 不预先计算未来谓词、不改 MVCC/去重/输出顺序；删除旧的整段注释版 Next |
| `TranslationDirectory` | 对已经存在的中间目录、叶地址发出 CPU 预取提示 | 不为提示构造冷目录，不返回失效条目指针；不把 CPU 提示算设备 IO |
| `BufferPoolManager` | 复用加载任务与帧选择；增加预取额度、失败帧和回调完成；需求读同页共享 Loading | 不新建线程；预取不等待设备，也不驱逐脏页；正式 ReadPage 仍同步返回 Guard |
| `PageStorage` / 对象页后端 | 可选非等待 Prefetch；对象部署接到真实对象读，文件后端明确不接纳 | 文件后端没有额外 IO 池，正常同步 Read/Write 保留 |
| `NodeStorage` / `ObjectIO` | 在受保护映射及帧许可下发起直接读取；真实完成后通知帧，未接纳撤销准备 | 复用 ReadInto 的映射/对齐/许可规划；需边缘拼接、混合空洞的预取跳过，正式需求读照常处理 |
| `RegionManager` / `IOExecutor` | 增加只读预取准入，复用原 external batch、worker、错误及寿命协议 | 从共享成员/外部容量中为另一个同等大小请求留余量；不保证任意更大请求立即成功 |
| 关闭 | 既等待调用退出，也等待回调持有的预取任务退出 | 不能以 Prefetch 调用返回代替设备操作完成 |

加载竞争时如果别的请求已登记同页，归还私有帧并返回 Fetch 重查/等待；不能留在帧选择循环里继续淘汰不相关页。这是本轮逻辑复查修正，不为它复制一个内部细节测试。

调用链：

```text
真实索引查询的有限 RID 窗口
  → BufferPool：CPU 提示、合并同页、登记 Loading、预留帧和额度
  → ObjectPageStorage → NodeStorage → ObjectIO/F13/F14
  → RegionManager → F02：接纳只读批次 → 原 worker → F01
  → 完成回调：Resident 或 Failed → 唤醒真正需求读
```

一个窗口逐页做短准备/交接，下层已经接纳的独立操作并行执行；不是把窗口串行读完，也不是每页创建一个线程。两次交接仍区分：Prepare 接管许可，Submit 接管执行责任。回调只更新短状态，不等待其他 IO。BufferPool Close 保活回调上下文直到任务额度归还，丢弃查询不会使帧或物理范围提前复用。

`prefetch_pages_` 是显式配置，0 关闭，小于帧数和总页任务数；已有文件配置显式置 0。对象部署由调用者选择，不暗改 CLI。测试部署值只是复现配置，不宣称生产最优值。预取失败不提前替代正在消费的其他页结果，但实际消费失败页时必须报错；设备故障依旧上报 NodeStorage，不能把这个局部行为解释成节点故障被忽略。

参考及内化：沿 [Calico §5.1](https://arxiv.org/html/2604.00423v1) 对已知候选成组提示翻译与页内存，内化为索引窗口；沿 [RocksDB 异步 IO](https://github.com/facebook/rocksdb/wiki/Asynchronous-IO) 先发起独立读取再按需等待，内化为原 F02 的异步页加载。未引入依赖、io_uring 或作者代码，也不把论文的性能数据当作本项目结果。

## 2. 先逻辑审查，再测试

先逐路径检查：帧选择/同页竞争 → 登记 Loading → 下层拒绝/接纳 → 真正完成 → 报错/发布 → 淘汰/关闭。确认 IO 等待不持 owner/目录锁；回调不能在持 executor 锁时进入帧；未接纳回滚必须唤醒需求读重查；已接纳后不得按调用者寿命释放帧。共享 F02 额度在 executor 内原子预留，不能由各 BufferPool 独立估算剩余容量。

随后新增三个场景，沿用旧六个真实页场景与四个 RAM 场景，没有新增设备边界镜像套件。生产代码未加入 hook、测试分支或内部计数 getter。

## 3. 测试全景

以下 F26 测试均在唯一源码压缩包中，非默认常驻测试目标。

| 测试名（省略 F26Pages 前缀） | invariant / 具体 bug | 类别 |
| --- | --- | --- |
| SparseRealFilePagesAndOpenedInstancesStayIndependent | 跨 prefix 同 suffix、不同打开实例不读错正文；防数组别名/旧 PathCache | integration / regression |
| DirectFramesBatchDurabilityAndUnchangedMiddlePage | 实际 IO 使用帧，批量写回达到对象发布，未改中间页保持；防错偏移/隐式中转/错误拼批 | integration |
| SharedLoadIndependentProgressAndCloseDrain | 同页需求读合并、独立页推进、关闭排空同步调用；防重复加载/全局锁等待/提前关闭 | integration |
| PublicationBoundaryKeepsSourceStableAndOtherPagesUsable | 数据 IO 完成至 B 发布期间源页稳定，其他页可用；防子 IO 结束提前放权 | integration |
| FailedWritePreservesDirtyBodyAndAcknowledgedOldVersion | 写失败仍保留脏正文和原已确认版本；防失败清脏/假成功 | integration |
| TranslationMemoryReclaimsReusesAndReportsOSFailure | 真实 RAM 可归还并重新使用，失败不破坏正文和帧可用性；防漏计账/活跃归还/批量准备漏释放 | integration |
| PrefetchSharesLoadsReservesDemandAndDrains（新增） | 并行预取不占尽正常请求容量、同页共享、Close 排空回调；预取不刷脏页 | integration |
| PrefetchFailureIsObservedByDemand（新增） | 已接纳预取失败不发布正文，消费时错误可见；防失败缓存成成功 | integration |
| SqlIndexWindowsPreserveVisibilityAndResults（新增） | 真实窗口消费不改变旧事务/当前事务和索引顺序，且下层确实接纳预取 | SQL integration |

基础四项 `FrameBodiesGeometryAndAdvice`、`ConcurrentFirstUsePublishesOneDirectory`、`FailureBudgetAndLazyInitialization`、`EntryAccessSerializesChanges` 继续保护帧布局/能力报告、首次发布、资源计账、条目及回收访问门；详细输入、oracle 和局限沿 [d 审查](s9d_execution_20261004.md)。它们不是模拟数据库或业务 E2E。课程 11 项、S8 8 项由 runner 复用；不复制源正文、不扩大场景计数。

## 4. 新增场景：目标 → 输入 → 执行 → Oracle → 失败含义

### 4.1 PrefetchSharesLoadsReservesDemandAndDrains

- **目标**：接纳与完成分离；预取复用同页任务；给需求读取留容量；Close 必须等待调用返回后仍在途的预取；可选读取不得触发同步脏淘汰。
- **输入**：页 1/5/9/13 保存各不相同的非空 4 KiB 正文，先真实持久化再冷缓存。8 帧、4 页任务、预取窗口 3、F02 外部容量 3 页、3 个 worker；不是空参和单一全零数据。
- **执行**：测试链接处只暂停前两次真实 Data pread。提交窗口 5/9/13，在 IO 尚未完成时提交返回；正常读 5 加入已有加载，读 1 独立完成。释放 IO 后比较正文。第二轮没有需求调用，只有预取回调仍在途，启动 Close。最后用仅两帧且都脏的缓存预取另一页。
- **Oracle**：外部 syscall 记录与独立正文生成函数；两项在途预取加一次正常读取，共三次 Data 读取；Close 不得先于被暂停 IO 返回。脏帧场景的 pread/pwrite 记录为空、两个原正文仍正确。计数针对确定性暂停区间，不测调度速度。
- **失败含义**：对应断言分别定位容量耗尽/重复读、提前关闭、可选预取刷脏或损坏页；字节不等说明读错页或寿命错误。它不能量化预取收益，也不证明任意大小的复合请求都得到即时额度。

### 4.2 PrefetchFailureIsObservedByDemand

- **目标**：设备已接受读取后失败，不能把未定义正文当成有效页。
- **输入**：页 1、5 具有不同已持久化正文；页 1 留在缓存，在页 5 的真实 Data pread 注入一次 EIO。
- **执行**：真实 Prefetch 返回；读无关驻留页 1；随后需求读 5；关闭重开再读 5。
- **Oracle**：页 1 完整正文、读 5 必须抛错、故障确实消耗，以及重开后页 5 与写入时正文一致。不依赖内部 Failed 标记或返回状态 getter。
- **失败含义**：错误被吞、错误页提前发布、预取改变了持久正文，或错误被提前转交给其他记录。此测试不承诺发生设备故障后节点仍允许所有新请求。

### 4.3 SqlIndexWindowsPreserveVisibilityAndResults

- **目标**：优化不得改变数据库可见性、索引过滤/去重及有序扫描，并必须有真实生产消费者。
- **输入**：36 条 id 不同、bucket 为 7/8/9、650 字符正文不同的记录；创建真实二级索引。保持一个旧事务，再更新 id=3 的键、删除 id=6、插入 id=99。
- **执行**：真实 ExecuteSql/ExecuteSqlTxn → IndexScanExecutor → MVCC tuple/undo → BufferPool → 对象 FS/Direct 文件。先刷页并通过当前缓存淘汰接口形成冷数据。分别查询旧事务、当前事务的 bucket=7（含重复 OR）和按 bucket 全索引扫描。EXPLAIN 只用于确认 fixture 实际选择两类 IndexScan，不把优化器计划文本作为稳定接口。
- **Oracle**：根据输入 id 算出旧集合、按手写更新/删除/插入算出新集合；全扫描比较从输入构造的完整 (id, bucket) 集合，同时检查唯一 id 和 bucket 顺序。不存在“调用生产过滤器生成 expected”。测试侧 PageStorage 包装只委托真实后端并记录接纳次数，两条查询在各自冷缓存下必须分别大于零，防止仅有一条路径接通仍通过正文测试。
- **失败含义**：集合/可见性/顺序不符定位查询语义退化；接纳为零定位消费者未接通。无生产内部计数 getter。
- **边界**：使用既有 BusTubInstance 公有组装点在建表前接入真实对象缓存，是 SQL integration；不是全自动节点启动 E2E。节点装配继续由 S8 原场景覆盖。F36 改变 DeletePage 的物理释放含义后，此处冷缓存 setup 必须换成合法的冷实例/淘汰方式，不能继续把删除持久页当作淘汰；测试业务内容及 oracle 不变。

## 5. Production / 接口污染审查

| 新接口/配置 | 正式调用方与必要性 | 删除测试后是否保留 |
| --- | --- | --- |
| BufferPool PrefetchPages / PrefetchWindow / prefetch_pages_ | IndexScanExecutor 有限窗口及可选读取额度；Window 读取配置，不暴露内部帧/计数 | 是 |
| PageStorage Prefetch 及对象后端实现 | 同一个缓存支持真实异步对象后端与同步文件后端；false 明确表示未接纳 | 是；默认 false 是能力边界，不是方便测试 |
| NodeStorage PrefetchObjectInto / 私有 ObjectIO PrefetchInto | 生命周期接纳、映射/范围/缓冲保留和完成交接 | 是 |
| RegionManager / IOExecutor TryPrepareReadAhead | 区域解析与全 executor 共享准入额度；复用原外部缓冲路径 | 是；跨层适配有权限/寻址职责，非测试 pass-through |
| TranslationDirectory Prefetch | BufferPool 的真实批量 CPU 提示 | 是；不导出内部指针 |

未新增测试 hook、内部状态 getter、测试宏、为测试改变的默认路径/参数。PrepareExternal 和 PrepareFrame 为私有实现复用；公开 Read/Write/Guard 的既有语义未为测试放宽。测试 `PreparationFault` 是归档内的包装，SQL setup 使用已有公有组装点；未反向修改 production 来服务 fixture。删除旧注释版 Next，不删除仍服务文件兼容和非页对象的实际路径。

## 6. 重复、检错能力与稳定性

- 需求读并发旧场景检查“调用仍在执行”；新增预取场景检查“调用已返回、只有回调仍在执行”。不是重复的关闭边界。保留各自失败条件，不再新建 F02 队列测试。
- 旧写失败与新预取读失败分别保护 dirty 源和未完成读目标，不能用其中一项替代另一项。
- SQL 新场景保护优化消费者的 MVCC/顺序；S8 的 SQL 快照恢复和 TCP 节点组装没有这些触发条件。保留 S8 原输入及判据，测试侧仅接入对象页配置与预取额度。
- RAM 四项中能力/资源错误不容易由共同业务 E2E 确定性触发，继续仅压缩保存。共同 E2E 真正接管相同触发/判据后再删减；不拿未来覆盖冒充当前覆盖。
- 定向变异：移除 F02 需求余量 → 读取数断言失败；吞预取 EIO → 必须抛错断言失败；分别移除点查/全扫描投递 → 各自接纳断言失败；Close 不等待 tasks → 提前返回/顺序断言失败。基础 9 个与其余页变异继续复用；当前完整计数见 §9。编译失败、sanitizer 崩溃、超时均不算有效检出。
- 同步使用条件变量和真实 IO 边界，没有用 sleep 猜 IO 顺序。10 秒只作有限失败上限，不是性能阈值。数据生成固定，Image 独立；全局链接观测仅在本可执行文件串行案例中使用，不能把案例并发运行。
- 正常及变异失败清理先释放暂停门、等待任务并排空节点 IO，后销毁缓存。初次 Close 变异已触发断言但清理挂起，修正 fixture 后重测；该次挂起不算检出证据。
- 初次编译出现 Value 名称冲突及旧事务 Begin 签名不符，只修测试调用。初次容量 fixture 为 4 页却按 3 页预期拒绝第三预取；按已定规则修为 3 页，并保留原严格断言。LSan 在沙箱 ptrace 限制下不能退出检查，正式运行使用允许线程检查的环境；不禁用 sanitizer 冒充通过。

## 7. 最终测试矩阵

| Test | invariant / Oracle | 新 failure mode / 重复情况 | production pollution | 建议 |
| --- | --- | --- | --- | --- |
| SparseRealFile… | 非空正文及实例隔离 | 跨 prefix/owner 别名；不等于 SQL 回归 | 无 | 保留 |
| DirectFrames… | 正文、实际帧地址及重开 | 错偏移/重复拷贝/拼批覆盖中间页 | 无 | 保留 |
| SharedLoad… | 实际读取数、完成先后、正文 | 同步调用期间的合并/关闭 | 无 | 保留 |
| PublicationBoundary… | 暂停 B 提交、源/其他页正文 | 子 IO 与事务发布不是同一边界 | 无 | 保留 |
| FailedWrite… | dirty 正文与重开旧版本 | 写错误，未被读错误覆盖 | 无 | 保留 |
| TranslationMemory… | mincore/正文/后继占满帧 | 物化/归还/失败清理；非净 RSS 测量 | 无 | 保留 |
| PrefetchShares… | 真实 IO 数、Close 顺序、脏页正文 | 异步返回后的寿命、需求余量、不得刷脏 | 无 | 保留 |
| PrefetchFailure… | 消费抛错、独立正文和重开 | 异步读取失败缓存 | 无 | 保留 |
| SqlIndexWindows… | 手算集合/排序、真实接纳 | 实际消费者语义；不替代节点恢复 | 无 | 保留 |
| 基础 RAM 四项 | 几何/线程/资源独立断言 | 仍有业务路径难直接触发的组件风险 | 无 | 保留 |
| 原课程 / S8 | 原有接口 / 原业务恢复结果 | 接入兼容及真实节点，未复制正文 | 无 | 保留 |

以上“保留”均指本阶段压缩证据，不要求永久常驻小测试。核心新增保护是异步寿命/需求余量和实际 SQL 语义；没有为每层新增一个相同读写测试。尚无性能提升证据、真实掉电或裸设备证据。CPU hint 是否带来收益、不同预取窗口/内存预算下的效果由后续同口径性能比较决定。

## 8. 执行结果与清理

**S9.1e：已完成。**

- 32 个不同场景：基础 RAM 4、真实页/SQL 9、课程 11、S8 8；ASan/UBSan/LSan 正常通过，无跳过。
- ThreadSanitizer：基础 4 + 页/SQL 9，全部通过；不把重复运行增加到场景总数。
- 定向变异 25：基础 9 + 页/消费者 16，全部由指定测试的 assertion failure 检出。当前预取变异为需求余量、预取读错误、点查消费者、全扫描消费者、回调排空；旧同步 Close 与新异步 Close 的破坏范围不同。
- 最终页测试及变异在 `runs/review-asan`；原课程/S8 在 `runs/asan-final`（其业务测试内容不变）；基础在 `runs/foundation-asan`；并发证据在 `runs/review-tsan`、`runs/foundation-thread`。首次失败和清理问题只留诊断日志于 history，不留错误测试源码包。
- 唯一源码为 `test/archives/F26-array-buffer-pool.tar.gz`，当前结果为 `test-results/storage-f26-s9e-20261005/F26-results.tar.gz`。旧 b/c/d 结果包删除，旧 F26 源码原位替换；其他 19 个有效模块源码包不动。展开测试、设备镜像、可执行文件和临时构建目录清理；结果包沿用 .gitignore。

[F26 结果入口与校验](../../test-results/storage-f26-s9e-20261005/README.md)。不自动推进 S9.2/F36；其后仍有 S9.3、S10–S13。F26 的 S9.1 核心组合完成不等于全节点预算调优、业务页安全释放或总重构完成。共同 C/P 未修改、未在本轮重跑，裸设备、物理掉电、跨机和性能收益未验收。

## 9. 2026-10-05 再次复审：SQL 判据分开验证消费者

### 9.1 生产代码与方案

先从代码重查，随后修改测试。本轮没有修改生产代码；核对的 39 份生产源码与上一轮归档哈希一致。

| 方案要求 | 当前实现及核对结果 |
| --- | --- |
| 用真实已知页集合驱动预取，保持业务语义 | `IndexScanExecutor::PrefetchWindow` 接续既有 RID/索引项；两个 Next 分支各有调用；原 MVCC、去重和排序检查保留 |
| 调用返回后仍保有任务及缓冲 | `PrefetchPages` 回调持有 Slot/任务；ObjectIO/F02 保留目标帧与物理读取许可，实际 IO 完成后发布 |
| 同页合并、独立页推进、不为预取刷脏 | Loading/Failed 走既有 PageTask；PrepareFrame 的预取分支拒绝 dirty/io 候选，不在该分支写出 |
| 共享执行器额度保留正常请求余量 | F02 `IOBudget::Reserve` 在共享锁下检查/预留；各 BufferPool 的局部预算不能绕过此处 |
| 失败不能变成有效正文；关闭排空 | `CompletePrefetch` 成功/失败分开；`AbandonPrefetch` 清除未接纳的加载并通知重试；Close 同时等 calls/tasks |
| 不污染生产接口、不重复实现 | 本轮没有 API/默认参数/getter/hook/测试宏变化；既有 Prefetch 接口均有正式消费者，无新 worker 或缓存实现 |

保持原阶段边界：同步 PageGuard 仍会等待它真正需要的页；异步的是提前发起的预取。SQL fixture 手动组装真实对象后端，属于集成验证；S8 继续覆盖正式节点装配，不能混称完整共同 C/P 验收。

### 9.2 发现的问题及实际修改

修改唯一压缩源码中的 `SqlIndexWindowsPreserveVisibilityAndResults`，没有增加测试数量：

1. **旧判据只要求整个 SQL 场景接纳过一次预取。** 如果只有点查正常、全扫描不再投递，旧判据仍可能通过。现在在点查、全扫描开始前分别清理已持久化的缓存页、清零测试侧接纳计数；两条真实查询结束后分别要求发生接纳。旧的全场累计检查删除。冷缓存只改变测试 setup，不代替生产功能；F36 的 DeletePage 交接限制继续保留。
2. **旧全扫描判据没有完整约束记录身份集合。** 它检查数量、唯一性、排序和按实际 id 计算的 bucket；在不属于输入的数据恰好满足这些条件时可能漏错。现在从原始输入及明确的更新/删除/插入独立构造完整 `(id, bucket)` 预期集合，比较实际集合；仍单独检查重复 id 和 bucket 非递减，不强加相同 bucket 下未承诺的 id 次序。
3. **原单个 query-skips-prefetch 变异过于整体。** 改成分别移除点查、全扫描分支的 PrefetchWindow 调用；两个变异必须各自由对应的接纳断言发现。替换原整体变异，净增一个变异，不重复保留三种相同改点。

这两类判据保护项目自己的查询结果与已承诺消费者，不验证标准库 set 或第三方行为。计数仍位于归档测试的 PageStorage 包装中，所有 IO 委托真实生产后端；不为测试增加生产观测接口。

### 9.3 复验与归档

本轮重新运行修改后的 9 项页/SQL 场景及相同场景的 TSan，全部通过；16 个页/消费者变异全部被指定断言检出。生产代码、基础 RAM/课程/S8 源码未变，复用并校验原 4 项 RAM、19 项课程/S8、4 项 RAM TSan 及 9 个基础变异证据；不能说本轮重新跑了全部 32 项。


| 改坏版本 | 实际失败判据 | 结论 |
| --- | --- | --- |
| point-query-skips-prefetch | point lookup did not prefetch | 点查调用缺失会失败，不能被全扫描掩盖 |
| full-query-skips-prefetch | full index scan did not prefetch | 全扫描调用缺失会失败，不能被点查掩盖 |

当前保留 32 个不同正常场景、13 项 TSan 和 25 个有效变异证据；本轮执行量是 9/9/16，其余沿用已校验的未改内容。SQL 判据增强没有增加正常场景数，也没有改变共同 C/P。测试、runner、恢复说明及本审查均已同步；唯一 F26 源码/结果包原位替换，旧弱判据版本和旧整体 query 变异不再作为当前证据。临时构建、展开测试及镜像清理，其他 19 个有效模块源码包未改；未 commit。

<a id="s9e-final-review"></a>

## 10. 接续复审：交接、收尾与证据核对

2026-10-05，接续 §9。**本次没有改生产代码、测试正文或 runner，也没有重新执行测试。** 实际修改仅为本节、主方案索引及结果归档的审查说明和校验；§9 的 SQL 判据增强与 9/9/16 复验属于上一轮。

先复查代码依赖，再查证据：

| 核对点 | 代码依据及结论 |
| --- | --- |
| 接纳后才能交出执行责任 | `PrefetchPages → ObjectIO::ReadIntoImpl → TryPrepareReadAhead/TrySubmit`；先保留 Loading、帧和范围，接纳后由原 worker 完成，无新增等待线程 |
| 资源拒绝与设备错误分开 | 未接纳调用 `AbandonPrefetch` 清除临时映射、归还帧并通知需求读重查；已接纳的读失败由 `CompletePrefetch` 保留错误，需求读不能取得有效正文 |
| 回调与关闭的寿命 | F02 `Finish` 在设备成员结束后锁外归还许可、执行完成回调；BufferPool `Close` 同时等待调用和任务退出，NodeStorage 继续排空实际对象 IO |
| 预取不阻塞脏页写回 | `PrepareFrame` 的预取分支拒绝 dirty/io 候选；短期元数据锁仍存在，不把“不等待设备”说成完全无锁 |
| 前台余量与查询语义 | F02 在共享预算锁内为同等大小请求保留余量；点查/全扫描都接入窗口，原事务可见性、过滤、去重和顺序保留 |

没有发现新的方案冲突或需要修复的代码问题。公开的预取接口均有实际生产调用方；未新增测试 hook、内部计数 getter、默认路径或测试分支。关闭阶段与读写失败阶段保护不同风险，继续保留既有场景，不追加一套重复测试。

### 10.1 证据核对与可证明的边界

- 核对唯一源码包内 39 份生产文件、5 份测试/运行说明文件、6 项依赖的哈希；结果包的生产快照和嵌入源码包一致。
- 从原始 XML 核实 32 个不同正常场景和 13 项 TSan 均运行且无失败、无跳过；25 个变异在指定测试产生断言失败。页层 16 个变异另核实进程退出码为 1，正常运行退出码为 0，未把编译失败、崩溃或超时计为检出。
- 点查和全扫描变异分别落在对应的接纳断言；提前 Close 变异落在实际 IO/关闭先后断言；吞读取错误变异落在需求读必须报错的断言。这些是既有运行证据，不是本次新跑的结果。
- `PrefetchSharesLoadsReservesDemandAndDrains` 能确定设备读被暂停、正常读仍能推进及 Close 排空。其同页需求线程可能被调度得较晚；该子检查不能单独证明需求读一定在放行 IO 前进入 Loading 等待，也不能证明所有交错。现阶段同时依靠共享任务的代码核对、原需求加载场景及 TSan；未为证明线程进度增加 production hook。
- SQL 测试仍为真实生产链的 integration；共同 C/P、裸设备、真实掉电和性能收益没有新增证据。F36 改变 DeletePage 语义后的 setup 迁移约束保持。

旧 b/c/d 结果包仍不存在，F26 只保留当前源码/结果包；其他 19 个有效模块源包未变。本次直接读取压缩内容核对，没有恢复构建目录、镜像或展开测试。归档只更新审查说明及校验，不改原运行日志；未 commit，未开始 F36。

<a id="s9e-commit-review"></a>

## 11. 提交前复审与后续交接

2026-10-05，用户授权检查通过后提交本阶段。先重新审查预取资源交接、任务完成、拒绝/失败收尾及关闭，未发现新的生产修复项。生产和测试源码与 §9 复验版本相同；本次没有重新执行测试，继续核验 §10 所列原始 XML、变异断言、命令退出码及源码哈希。

本次发现并修正文档问题：共同协议 §7 的当前状态、§10 的 e 行仍写未实施，§11 仍写 e 尚未运行测试，与后面的执行协议及本报告冲突。现在统一为当前 e 的核心实现/组合验证已完成，性能比较未执行；F26 文档标清 d 的“未执行 e”仅指当轮，主方案收录日期、进度和复审索引同步。未因文档修正修改测试内容、扩大验证结论或实施后续阶段。

归档只保留当前 F26 源码包和结果包；旧 b/c/d 结果包已不存在，源包为修正测试后的版本，其他 19 个模块源包未变。结果包沿原 .gitignore 本地保存，Git 提交源码包、结果摘要/校验、生产和方案文件，不强制加入运行结果压缩包。无新增展开测试、镜像、编译产物或常驻小测试目标。

后续入口为 [F36 §5–§7](modules/table-space-management.md#5-待冻结事项)：先讨论并冻结 S9.2 的旧记录回收依据、存活 slot/RID 的保护、页格式/空槽语义和可重建空闲摘要，再进行页内整理及已有页复用。S9.3 的空页脱链/正式对象范围释放另行接续；不能把当前 BufferPool::DeletePage 的缓存退出当成已实现物理回收。总进度仍为 S9.1a–e 完成，S9.2/.3、S10–S13 以及跨阶段部署/共同验收未完成。本次不实施 F36。

# F26 共享零页接续：实现与测试设计审查

2026-10-08。依据[共同协议](s12_translation_zero_page.md)。先审查对象寿命、登记/回收及帧交接，再编写测试；本轮已完成；本文件与压缩结果共同记录实际验证范围。

## 1. 实际改动与职责去重

- `translation_directory.cpp`：移除 TranslationEntry 中逐项构造的 `atomic<uint64_t>{0}`、Construct/OpenGroup 和 shared_mutex。叶区只调用一次 `start_lifetime_as_array<uint64_t>`；后续通过 atomic_ref 访问，归还后不构造写零。
- `Lookup` 只读，`Access` 保留独占修改。两者共用同一个 Open/TranslationAccess，不建第二套映射。组外保留代次、计账和存活数；只有写访问预留物理组额度。
- 现有 Call 在产生副作用前建立 TranslationContext，预留并重复使用登记资源；不跨 IO 持有组指针。空页退役提交后的清理无需临时申请新登记。
- 访问登记节点属于目录，正常线程复用自己的节点；嵌套访问取得另一个空闲节点。节点按并发高水位复用，目录关闭才销毁。回收仍走原分层 Candidates，纯查询不进入回收候选。
- 回收关闭入口后扫描登记，并**再次核对 live_entries**：第一次看到空组之后，可能已有写者发布新映射并退出。只检查最初计数会打掉刚装入的页；本轮逻辑审查已补齐第二次核对。
- `BufferPoolState::Fetch/Flush`、GetPinCount、FlushPage、PrefetchPages 使用 Lookup。在帧短锁内复核页身份、映射和阶段；普通命中不再独占条目。加载、淘汰和清除保留 Access，PageGuard 的正文保护保持现有职责。
- 翻译区独立 C++23 object target；公共头和其他目标保留 C++17。CMake 检查标准库能力。项目源码保留严格警告，第三方头声明 SYSTEM；两个旧头补直接 `<cstdint>`，修复新版标准库不再间接提供该类型的问题。

没有新增持久页格式、FS 缓存、设备位图、线程池、独立回收器或第二套预算。FrameArena/ResourceAccount/PathCache/候选层级仍由原组件负责。目录、帧正文和设备存储的分离不变。

仍有的成本明确保留：BufferPool Call 保护、帧锁与 pin、LRU-K、shared_ptr 所有权。此次降低翻译查询的共享写入，不宣称整个缓冲池无锁；正文不可变版本/乐观短读仍归 S13。

## 2. 全景：本轮新增或接续的阶段测试

下面六项基础测试实际调用生产 FrameArena/TranslationDirectory，十项页测试实际调用 BufferPool、文件/对象页后端及其中的 SQL 消费者。仅源码保存在 F26 压缩包；没有添加常驻 CTest 目标。

| 测试 | 行为 / invariant 与具体 bug | 类别 |
| --- | --- | --- |
| FrameBodiesGeometryAndAdvice | 每帧独立且满足实际对齐；建议失败不能谎报支持。防槽步长错误、帧重叠和错误吞掉 | integration，复用 |
| ConcurrentFirstUsePublishesOneDirectory | 并发首次访问同叶各自保存正确映射，只有一个发布结果。防候选泄漏/覆盖 | integration，复用 |
| FailureBudgetAndLazyInitialization | 失败归还额度、按组预留、旧映射在压力下仍可用。防未计账物化/失败泄漏 | integration，适配只读查询 |
| EntryAccessSerializesChanges | 多写者变更不丢失，活跃空组不被归还。防删除独占 latch 和翻译版本不更新 | integration，适配 |
| ReadOnlyProbesStayZeroUntilMutationAndReturnAfterReclaim | 稀疏只读不私有化；写后增长、安全归还后重读仍零；持有者/失败阻止虚假归还 | integration，新 |
| ReadersWritersAndReclaimerKeepPublishedMappings | 并发登记、映射变更、嵌套访问及维护不丢失当前映射 | integration，新 |
| SparseRealFilePagesAndOpenedInstancesStayIndependent | 跨 suffix/prefix/root 的正文及实例隔离。防目录地址别名、旧 PathCache 混用 | integration，复用 |
| DirectFramesBatchDurabilityAndUnchangedMiddlePage | 正式帧 IO 批量写、未修改页及重启正文正确。防地址/缓冲/范围处理错误 | integration，复用 |
| SharedLoadIndependentProgressAndCloseDrain | 同页合并、其他页推进，Close 晚于已接纳 IO 完成。防锁跨 IO 和提前销毁 | integration，复用 |
| PublicationBoundaryKeepsSourceStableAndOtherPagesUsable | 数据/元数据发布期间帧来源稳定，独立页能推进。防旧写覆盖新内容 | integration，复用 |
| FailedWritePreservesDirtyBodyAndAcknowledgedOldVersion | 错误保留脏正文，恢复承诺边界不被改写。防错误时清脏/丢已持久内容 | regression，复用 |
| TranslationMemoryReclaimsReusesAndReportsOSFailure | 真实页访问下翻译内存可循环归还，OS 错误明确报告。防压力死路/吞错 | integration，复用 |
| PrefetchSharesLoadsReservesDemandAndDrains | 实际对象 IO 合并、需求余量及关闭排空。防预取耗尽资源/提前释放帧 | integration，复用 |
| PrefetchFailureIsObservedByDemand | 预取读取失败由实际消费者观察。防假成功或返回残留正文 | regression，复用 |
| SqlIndexWindowsPreserveVisibilityAndResults | 真实 SQL 点查/扫描的内容及 MVCC 可见性不因预取改变 | integration，复用 |
| ConcurrentHitsAndEvictionKeepPageBodies | 热命中与冷页淘汰并行时逐页正文仍匹配输入 | integration，新 |

此外复用课程 BufferPool 七项、PageGuard 两项、LRU-K 一项、DiskScheduler 一项；S8 节点/对象 Store 八项通过测试接口侧适配接入。课程及 S8 没有在 F26 复制第二份测试正文；实际是否通过以下方最终运行表为准。

## 3. 目标 → 输入 → 执行 → Oracle → 失败含义

### 基础六项

1. **FrameBodiesGeometryAndAdvice**：9 帧，正文尺寸 4096/4103/8197、不同非整除对齐；写入由帧序号生成的不同字节，再逐帧比较。经过真实 Arena/mmap，系统建议失败由测试链接包装注入。预期来自输入模式/对齐约束，不从 Arena 步长生成。失败定位重叠、对齐或错误报告；THP 实际采用只是观察，未作为必过条件。
2. **ConcurrentFirstUsePublishesOneDirectory**：两个线程、同一非零 prefix 的两个不同 suffix，mmap 边界 rendezvous 让两个候选确实并存。真实 Resolve/发布/丢弃 → Lookup，独立预期 71/72；映射数辅助确认只留下一个候选。这项有阶段实现耦合，不充当业务 E2E，后续接管时可以简化。
3. **FailureBudgetAndLazyInitialization**：反复 mmap/advice 失败后再成功；256 KiB 上限下逐组写不同帧号，最终拒绝新组，既有映射继续正确。生产预算/分配/异常路径；以请求生成的帧号、实际拒绝和映射退出为依据，不能用“used 等于自身实现算出的数”作判据。缺少预算检查会越过上限；资源泄漏会妨碍后续合法请求。
4. **EntryAccessSerializesChanges**：六个写者各 1500 次读旧帧号再加一，中间 yield 放大冲突；最终 9000 和相应翻译版本由操作次数推导。另一空组保持访问时并行维护，之后仍可发布映射。失败指出独占交接或回收错误；有限调度不构成并发证明。
5. **ReadOnlyProbesStayZeroUntilMutationAndReturnAfterReclaim**：一个真实叶的所有 OS 页选择非零 suffix；预算 2 MiB，确保错误的整叶私有化也能执行到内存 oracle，避免仅以预算异常间接检出。Lookup → atomic load；外部 smaps 读取该叶 VMA 的 Private_Clean/Private_Dirty/Swap，总量独立于生产预算。只读应 0，单组写后应一个实际 OS 页，归还后应 0；失败注入不应归还额度/重置旧访问。嵌套访问检验同线程记录不覆盖；持有只读访问期间维护必须不丢该组。此测试验证本项目没有偷偷写入数组，不是在重复测试 mmap 的基础行为。环境必须提供可辨识 smaps，否则报告失败，不伪报成功。
6. **ReadersWritersAndReclaimerKeepPublishedMappings**：四个线程、不同非零 prefix、每个 2000 次发布/读取/清除；独立维护线程持续推进。Lookup 存续时嵌套修改，旧登记必须继续有效；当前帧由轮次和线程编号确定。失败定位登记复用、回收交接或发布丢失。配合 TSan，而非依赖随机碰巧无错。

### 十项页/SQL 路径

前九项沿既有 F26 业务输入与 oracle：文件页使用不同页号对应的非空字节；对象页走 F26→F13/F14→F02/F01 和 B 发布；暂停/错误位于测试程序的真实系统调用边界。持久化前后比较明确的旧/新正文，关闭检查事件先后，SQL 比较独立输入导出的完整记录集合。没有将目录内部返回值当作预期数据库内容。

新增 **ConcurrentHitsAndEvictionKeepPageBodies**：先通过生产 BufferPool 写出 64 张不同非零页并 Flush；重开 8 帧缓存，4 个线程各读取 1000 次，交替共享热页与固定序列冷页。数据小于持久设备容量、大于缓存，必须产生替换；所有正文按页序号生成，与翻译实现独立。防候选帧已经换页却被无条件固定、读到别页或未完成加载正文。无需暴露帧身份或改 production 默认容量；调度覆盖有限，不能声称任一竞态必然在这一次发生。

## 4. Production / 接口污染与冗余

新增生产接口是 `TranslationDirectory::Lookup` 和一次调用内预留/复用登记节点的 `TranslationContext`；后者用于现有 BufferPool Call，防止持久提交后的清理临时申请登记内存。它提供零页只读语义，删除测试后仍必需。Open 为两个入口复用内部协议；没有新增默认参数、测试 getter、test-only hook、条件编译测试分支、配置捷径或旁路 IO。原有 Frame()/Version() 与独占 Access 保留协议含义，SetFrame 拒绝只读访问的错误使用。

已删除旧组 shared_mutex、constructed_、逐项构造/析构，以及旧原子条目包装类型。登记记录用于瞬时访问，live_entries 用于驻留/加载映射，两者不能互相替代；节点预算控制资源总额，回收 bitmap 只是候选摘要，两者也不重复。旧测试源码/结果包在新验证完成后换代，不并存两个 F26 当前实现。

测试侧旧页 mutation 脚本依赖已移除的 gate/constructed_，本轮删除这些失效锚点，采用八个对当前协议有效的变异；不把历史 25 个变异继续计作当前证据。

## 5. 重复判断与最终矩阵

| Test | invariant / Oracle | 新 failure mode / 重复 | Production pollution | 建议 |
| --- | --- | --- | --- | --- |
| FrameBodiesGeometryAndAdvice | 帧几何/输入字节 | 旧基础风险；与页正文测试部分重叠，但对齐/建议返回不同 | 无 | 保留：仅阶段包 |
| ConcurrentFirstUsePublishesOneDirectory | 发布赢家/71、72和映射退出 | 页 E2E 不确定制造候选竞争；含内部数量断言 | 无 | 简化：E2E 能控制该竞争后删数量断言 |
| FailureBudgetAndLazyInitialization | 限额、失败可恢复/输入映射 | 与页压力场景部分重叠；预算拒绝边界独立 | 无 | 保留 |
| EntryAccessSerializesChanges | 串行修改/总次数 | 正文并发测试不能替代条目写者串行；是阶段契约 | 无 | 保留 |
| ReadOnlyProbesStayZeroUntilMutationAndReturnAfterReclaim | 零页、保护和归还/外部 OS 观察 | 旧内存测试只测已物化组，本项覆盖纯查询及只读登记 | 无 | 保留 |
| ReadersWritersAndReclaimerKeepPublishedMappings | 发布不丢失/固定输入 | 与上一项部分重叠，增加并发登记再用和重开 | 无 | 保留 |
| SparseRealFilePagesAndOpenedInstancesStayIndependent | 正文隔离/输入 | 跨目录及跨实例，新增热冷测试没有替代 | 无 | 保留 |
| DirectFramesBatchDurabilityAndUnchangedMiddlePage | 对象页完整性/旧新正文 | 与 F02 基础测试部分重叠，新增帧适配链 | 无 | 保留 |
| SharedLoadIndependentProgressAndCloseDrain | 合并/完成次序 | 课程 tests 缺实际设备暂停 | 无 | 保留 |
| PublicationBoundaryKeepsSourceStableAndOtherPagesUsable | 发布期间稳定/独立正文 | 与失败写测试不同：无错误的并发窗口 | 无 | 保留 |
| FailedWritePreservesDirtyBodyAndAcknowledgedOldVersion | 错误/重启正文 | 故障与持久承诺边界 | 无 | 保留 |
| TranslationMemoryReclaimsReusesAndReportsOSFailure | 真页读取/输入正文与系统返回 | 与新零页测试部分重叠，组合预算/页 IO/失败责任不同 | 无 | 保留 |
| PrefetchSharesLoadsReservesDemandAndDrains | 预取准入/真实事件 | 与需求合并测试部分重叠，多了预取占用与需求余量 | 无 | 保留 |
| PrefetchFailureIsObservedByDemand | 故障必须交付/实际 EIO | 不重复成功预取 | 无 | 保留 |
| SqlIndexWindowsPreserveVisibilityAndResults | SQL 可见性/输入记录集合 | 其他页测试不能覆盖执行器消费者 | 无 | 保留 |
| ConcurrentHitsAndEvictionKeepPageBodies | 热冷并发/输入逐字节 | 课程 contention 有部分重叠，新增工作集大于缓存时身份交接 | 无 | 保留 |

“保留”指模块压缩归档，不把这些小测试变成永久默认测试集合。长期仍由共同生产 E2E/性能矩阵承接；接管同一故障与 oracle 后再合并或删除阶段场景。

## 6. 杀伤力与稳定性

八个 mutation：Lookup 改用写访问、忽略活访问、跳过 MADV_DONTNEED、不预留组额度、跳过条目独占、停止翻译版本递增、相邻帧地址别名、可选路径缓存申请额度失败。每项必须正常编译运行，指定测试以断言失败结束；编译失败、超时、sanitizer 崩溃不算检出。具体 XML/命令放结果包。

测试使用固定输入，任务同步用 promise/CV，yield 只扩大调度机会；没有随机 seed 漏记录。旧页测试少量 wait_for 作为挂死上限，关闭的主 oracle 是事件次序，不是“睡够时间”。基础包装全局状态仅属于独立测试进程，测试串行运行，作用域退出清理；并行工作线程结束后才销毁 hooks。

smaps、Linux Direct IO、文件及本机 TCP 是环境前提；不可得明确失败。初轮 sandbox 的 LSan 退出检查受限，转到允许该检查的环境重跑。初轮交叉工具链旧 Linux 头缺 STATX_DIOALIGN，属于构建环境失败；改用本机 sysroot 重建复验，没有硬编码对齐或退回 buffered IO。

## 7. 运行结果与限制

以下是上一轮完整复验结果；最新一轮只修改判据运行器，实际复验范围与沿用证据见 §9。

完整复验结果：

| 范围 | 结果 | 证据 |
| --- | --- | --- |
| F26 基础组件 | 6/6，ASan/UBSan/LSan | foundation/stage.xml |
| F26 真实页/对象/SQL | 10/10，ASan/UBSan/LSan | pages/pages.xml |
| 课程 BufferPool/PageGuard/LRU-K/DiskScheduler | 7+2+1+1，通过 | pages/对应 XML |
| S8 对象 Store/SQL 恢复/三节点 TCP | 8/8，ASan/UBSan/LSan | pages/s8.xml |
| F26 并发组件 | 同六项 TSan 通过，不增加场景数 | tsan/stage.xml |
| 最小变异 | 8/8，指定测试断言失败 | foundation/各变异 XML |

共 35 个不同正常场景。对最终代码没有沿用旧运行凑数。初轮弱预算的零页 fixture 已改为足够容纳整叶私有化的 2 MiB；最终源码重跑后，错误的查询 CAS 由实际私有内存断言检出。初轮 sandbox/旧 sysroot 失败不计通过；结果只保存最终测试与有效运行，历史原因在本审查文字说明。

源码包：[唯一 F26 包](../../test/archives/F26-array-buffer-pool.tar.gz)。结果：[本轮摘要/校验](../../test-results/storage-f26-zero-20261008/README.md)。旧 `storage-f26-s9e-20261005/F26-results.tar.gz` 删除，旧结果目录只保留历史说明和迁移指引，没有旧压缩备份。其他模块包不删除、不改写成新验证。

组件测试已经观察只读私有内存 0、单组写入 4096、归还后 0；4096 是本机查询值，生产与测试均按 OS 页计算。组件内存证据不等于 SS/RS/PL/GT 或共同 P1–P4 的性能结论；本轮未进行前后吞吐对比、裸设备或真实断电试验。


## 8. 接续复审：可选缓存不能阻断已有映射收尾（2026-10-08）

本次先逐条检查 Open/登记、Reclaim、Fetch/Flush/RetirePage 的实际控制流及异常路径，再改代码、补已有场景。不是先增加测试数量再推断正确。

### 8.1 本次具体修正

1. **PathCache 的异常范围遗漏。** 原 `ThreadPaths::For` 只处理 `make_unique<PathCache>` 的 bad_alloc；前面的 `Budget::TryTake → ResourceAccount::Reserve → ReclaimIdle` 也可能在复制账号清单时申请内存并抛出 bad_alloc。这样即使已有映射、已计账翻译组和调用预留登记都在，可选缓存仍会使收尾失败。现在把申请额度与缓存分配放在同一异常范围，成功取得额度后失败才归还；只对该可选优化的 bad_alloc 走原数组查询。必需目录/组申请不足、OS 错误继续报告。没有给每次访问另加准入器或第二套路径缓存。
2. **构建要求不一致。** 根 CMake 仍声明最低 3.10，但新目标要求 CXX_STANDARD=23；改为 3.20，并同步 README/协议。[CMake 官方说明](https://cmake.org/cmake/help/latest/prop_tgt/CXX_STANDARD.html)标明 3.20 开始支持这一标准值。配置时继续检查实际标准库能力，版本数字不能代替能力检测。
3. **性能方案遗漏同步。** `testing_translation_access.md` 仍把驻留命中写成独占 Access/组门，现同步为 Lookup、组登记与帧复核；只更新当前路径，不把功能验证记为 SS/RS/PL/GT 性能成绩。

### 8.2 测试设计复审：合并已有风险场景

在 **FailureBudgetAndLazyInitialization** 末尾补充一个共享额度故障输入，没有新建重复的正常测试项：

- **目标：** 已有映射的必需资源齐备时，可选 PathCache 的内存申请失败不能使映射操作失败。
- **输入：** 非零页号、初值 41；持有真实 TranslationContext，通过另一实例访问使线程路径缓存换主，再通过正式 ResourceAccount 接口占满节点额度。
- **执行：** Access → PathCache 额度请求 → ResourceAccount 归还闲置额度 → 原数组定位及受保护的映射修改。只在测试链接阶段包装 operator new，触发下一次堆分配失败；没有生产 hook、getter 或替代预算实现。
- **Oracle：** 注入确实发生、操作没有抛出异常、原映射最终为输入指定的 42；预期不来自预算内部 used 计数或翻译位布局。
- **失败含义：** 若异常外泄，已有映射仍是 41，说明可选缓存阻断了应完成的操作。新增 `optional-cache-credit-failure` 变异恢复该异常外泄，指定测试必须出现上述断言失败，编译失败/超时不计检出。
- **重复与稳定性：** 并入现有失败/预算测试；不复制 F31 算法或新增整套预算测试。故障开关是测试程序的 thread_local，操作结束立即清除，断言与清理不继承注入；没有 sleep、随机数和机器内存耗尽。该场景验证资源交接契约，不冒称完整的物理掉电/页退役 E2E。

| 补强场景 | Oracle | 新 failure mode | 重复 | Production pollution | 建议 |
| --- | --- | --- | --- | --- | --- |
| FailureBudgetAndLazyInitialization 中的缓存额度失败 | 故障被注入且映射由 41 变为 42 | optional cache 的额度整理先于缓存 new 抛错 | 与原 mmap/组预算失败位置不同，共用一个场景 | 无；new 包装仅在测试二进制 | 合并：已落实 |

### 8.3 已复核、未额外增加结构的部分

- 读者先发布登记、复核组代次后才碰条目；维护先关门再扫描登记，没有在持慢锁时等待读者。扫描后再次核对存活映射，避免短写者退出后被错误打洞。
- 条目读到候选帧后，正式取页在帧锁内重新核对身份、映射和阶段；修改映射仍需 Access。没有将映射版本当作正文版本，未提前实现 S13 正文乐观读取。
- Context 只预留登记资源，空闲时 group=null，不阻止跨 IO 的空组回收。组描述符/登记负责内存寿命，FrameHeader/pin 负责帧身份，候选 bitmap 只提供提示；没有重复的权威页表、回收器或预算器。
- 纯读 0 私有字节的结论只针对翻译叶正文，根/组描述符、访问记录和 OS 页表仍有开销。有限并发测试和 TSan 不能穷尽所有调度，完整性能比较仍待后续。

本次复验结果按 §7 记录；本模块源码/结果包原位换代，不保留上轮 F26 压缩备份。其他模块历史包作为依赖保留，不能当作当前实现的新证据。


## 9. 再次复审：变异失败必须对应预期风险（2026-10-08）

### 9.1 逻辑与方案核对

本次没有改生产代码或 C++ 测试正文。重新检查了以下关系：

| 承诺 | 实际控制流 | 审查结论 |
| --- | --- | --- |
| 只读探测不物化翻译叶 | start_lifetime_as_array → Lookup → atomic_ref.load；无逐项初始化/独占 CAS | 已落实；目录描述符和 OS 页表仍有成本 |
| 首次写入先取得预算 | 进入组访问保护 → ReserveWrite → 独占 CAS | 已落实；PathCache 的可选申请失败退路仅处理 bad_alloc |
| 在途访问不被打洞 | 发布访问登记并复核 epoch；维护关门、扫描、再次核对 live_entries | 代码已落实；共同协议原来省略了第二次存活检查，本次补全，防后续按简化文字误实现 |
| 查到帧号后安全固定 | Fetch/Flush 等在帧锁内重新核对页身份、当前映射和阶段 | 已落实；仍保留正文保护和 pin，不宣称完整无锁 |
| 关闭、异常及跨 IO 寿命 | Call 预留登记，空闲登记不占组；任务/帧保护持续至 IO 完成 | 现有职责保持，无新增重复队列、预算或回收模块 |

最后一处文档补充的原因：首次看到组为空后，可能有短写者发布新映射并退出。扫描登记时看不到这个写者，不等于组仍然为空。因此代码的第二次 live_entries 检查是必要步骤；本次只将已经实现的条件补回协议。

### 9.2 测试运行器的缺口及修正

原 runner 要求“正常编译、退出码 1、指定测试出现 failure”，但未核对 failure 内容。若同一测试因为无关 fixture 错误失败，也可能误报该变异已经检出。本次将八个变异逐项绑定到预期断言，且要求每组关键字出现在**同一个 failure 节点**，不能从不同错误拼接出匹配：

| 故意改坏的部分 | 必须出现的失败依据 |
| --- | --- |
| Lookup 改成写访问 | 真实叶私有内存与零预期不符，且为只读探测私有化断言 |
| 忽略活访问登记 | 持有访问的 Version 与回收前值不符 |
| 跳过物理归还 | 真实叶私有内存与归还后零预期不符 |
| 跳过组计账 | 有限预算没有拒绝新组，即 full=false |
| 去掉条目独占 | 并发修改后的帧值不等于输入操作总数 9000 |
| 不递增翻译版本 | 版本不等于输入操作推导的 9001 |
| PathCache 额度异常外泄 | 收尾抛错，并且目标映射未到 42；两条断言都要求匹配 |
| 相邻帧地址重叠 | 帧正文与独立输入模式的 memcmp 断言失败 |

没有按断言行号匹配，不依赖线程调度下某个特定错误值，也不新增生产 hook。原八个 XML 已核验确实因对应断言失败，之前的实际检出结论没有撤销；修正的是运行器今后接受证据的条件。

### 9.3 此次修改的八项测试设计审查

- **目标/输入/执行：** 对八个原有变异及其真实失败结果执行更严格的验收，不改变业务输入或被测生产路径。
- **Oracle：** 来源于每项已声明的 invariant 与独立预期值；编译失败、超时、未执行、错误测试名或无关失败都拒绝。
- **失败含义/杀伤力：** 对八份真实 XML 分别将 failure 正文替换为“无关 fixture 失败”，新判据全部拒绝；这些是运行器反例检查，不增加业务测试数量。
- **Production/接口污染：** 只有压缩阶段包中的 Python 运行器变化，C++ 接口和控制流均未改。
- **重复：** 沿用原八个变异和正常场景，未建立第二套测试框架、预算实现或长期测试目标。
- **稳定性：** 匹配同一项目归档 GTest 的断言文本及表达式，不匹配路径、行号、耗时或竞态的具体实际值；断言或框架格式改变时明确报判据不匹配，不默默放宽。

| Test/检查 | invariant 与 Oracle | 新 failure mode | 与其他测试重复 | Production pollution | 建议 |
| --- | --- | --- | --- | --- | --- |
| run_stage 的八项变异验收 | 每个故障必须由对应断言检出 | 同一个测试因无关原因失败而被算成有效检出 | 原变异不增项，仅加强判据 | 无 | 保留：仅模块归档 |

### 9.4 实际验证与归档口径

此次重新运行基础六项 ASan/UBSan/LSan 与八个定向变异；同时执行八个无关错误反例检查。其余 29 项正常场景和六项 TSan 使用上一轮结果：已核对 463 个生产文件、两个依赖包及不变的 C++ 测试正文，不称作本次重新运行。完整已验证场景仍为 35 个，不因 runner 检查数量增加而抬高覆盖数。

源码包/结果包原位更新；当前结果包替换 foundation 证据，保留仍匹配当前生产代码的 pages/tsan 原始证据，并用 provenance 标明各自来源。没有保留旧错误 runner 的压缩备份。协议、主方案和结果摘要同步说明上述范围。

## 10. 提交前复核：跨模块职责与归档清理（2026-10-08）

本次先重新检查实际控制流，再核验压缩证据。没有新增生产代码或 C++ 测试；下列结论是限定范围的代码审查，不把全项目没有重复设计作为已证明的结论。

### 10.1 本轮实现与测试结论

`TranslationDirectory::Open` 先登记并核对组代次，再允许访问条目；写入口在 CAS 前取得组额度。`Leaf::Reclaim` 关闭入口、扫描登记、再次核对存活数，成功 DONTNEED 后才归还额度。读者退出、失败重开及嵌套登记均保持原寿命约束。`Fetch/Flush/GetPinCount/FlushPage/PrefetchPages` 在帧锁内重新核对页身份、映射和阶段；查询结果不会直接授权读取已换页的正文。

生产接口仍有真实调用方，未发现本轮新增 test-only hook、默认参数捷径或第二套翻译表。六项基础场景和十项页场景的取舍维持 §5；并发及预算等小场景只压缩保留。常驻共同 E2E 和性能验收没有被这些组件结果替代。

此次核验 463 个生产文件、两个依赖包、五个测试/运行说明文件的哈希；检查原始 XML 为 35 个不同正常场景、同六项组件的 TSan、八个指定断言变异。再次将八个失败结果替换为无关错误，运行器全部拒绝。**此次没有重跑 C++ 测试**；上一轮实际重跑六项基础与八个变异，其余证据沿用关系见 §9。未改变被测代码，无新失败或未决代码风险要求再次整套构建。

### 10.2 不只检查当前模块：职责与重复工作的核对

| 检查范围 | 当前事实与判断 | 本次处理 / 后续归属 |
| --- | --- | --- |
| TranslationDirectory、PathCache、FrameHeader、FrameArena | 分别提供驻留映射、路径提示、帧身份/占用和正文内存。持久寻址仍在 PageStorage/FS，不是四份权威页表 | 旧 eager 初始化、组 shared_mutex 已由本轮替换；保留仍必要的帧 pin、正文保护与 LRU-K |
| 候选 bitmap、F14、F12 | 分别提示翻译 RAM 可回收组、验证物理范围引用、管理设备分配；回收的是不同资源 | 不合并权威状态，不引入第二个回收器 |
| Store 维护、F22 GC、Deferred | `DistributedNode::Start` 注册 Store 维护至原对象流水线；`GarbageLoop` 调度 Store 与范围回收。`DeferredLoop` 完成已提交正文落位，职责不同 | 未发现旧独立 Store 维护线程仍重复工作；提交循环不再执行周期 GC |
| F23 / F29 | `PlanCleaning` 复用有效段目录与索引，先解除无效范围，搬迁限定连续尾部；候选只在发布后替换目录 | 没有第二份日志权威索引；保留用户选定的旧清单格式 |
| F31、组件局部额度、F02 | 总量、组件授信、任务/IO 名额限制不同；帧正文由原所有者计一次，借用不另造正文 | 沿用 ResourceBudget/Account/Charge；不把计账当成进程全部 RSS |
| F28 / F25、Manifest / lease | 本地业务恢复点与节点间快照职责不同；Manifest 保留重启后的权威引用，lease 保护正在使用的内容 | `RaftNode` 接收安装已使用 PrepareSnapshot → 发布 → prepared.Install，同一候选不再构建两次 |
| A BufferPool / B MetadataPager | 独立实例有启动依赖和 B 私有版本/WAL 约束，共用算法并不意味着共用缓存身份 | 不为消除表面重复而合并实例；后续缓存策略仍归 F32 |
| PrepareUpdate / PrepareDelete | `sql_command_preparer.cpp` 仍逐表扫描；并未因数组翻译改造自动复用主键访问路径 | 确认为剩余重复读取成本，按 F35/S13 接续，不标为已消除 |
| 编解码校验 | `rpc_codec.cpp` 及 command/client codec 等存在 Decode 后 Encode 比较，用于现有规范编码契约，会再遍历/分配正文 | 有减少重复工作的空间，但不能直接删校验；须先将格式约束完整移入解码器并验证等价，列入后续协议热路径复核 |

F26 当前仍有 Call、shared_ptr、帧锁/pin、替换器等同步成本；本轮减少的是条目独占及组读计数争用。用户认可的不可变正文乐观短读仍在 S13，不能把零页查找称作完整无锁 BufferPool。

### 10.3 实际修正：压缩包中的中间产物

本次打开压缩包逐项检查，发现源码包和结果包均误带 `__pycache__/run_stage.cpython-312.pyc`，前轮清理检查遗漏了 Python 字节码。现从两包中删除该文件及空目录；测试正文、运行器、原始日志/XML 不变。重新计算源码包及结果包校验清单；没有保留旧包备份。

当前只保留一个 F26 测试源码包和一个当前结果包；旧 `storage-f26-s9e-20261005/F26-results.tar.gz` 不存在。历史目录只保留说明/当时清理记录，不保留旧测试正文。其他模块的有效归档作为依赖保留。结果包仍按既有 .gitignore 仅存本机；源码包、摘要与校验清单随 Git，不能把 commit 等同于结果包已上传。

### 10.4 下一步和总体剩余工作

下一步仍建议 [S12/F30 校验扫描 §11](modules/integrity-scrubber.md#11-下一步候选2026-10-08待用户确认)：先确定普通 Data 的校验依据、粒度和旧格式兼容，再固定小批版本并经正式设备路径扫描，复用 F22/F31/F02。不要把“成功读出”当成没有损坏；检查失败定位并上报，修复能力另行约定。本轮没有启用其实现 prompt 或执行 F30。

总体已到 S12 中后段，S0–S11 的已授权主路径和 S12.1/.2/.3、零页接续已交付。仍有 S12 的 F30/缓存和后台剩余策略、S13 的 Raft/SQL/会话并发及不可变正文短读、最终正式部署接入与共同 C/P 前后对比、SS/RS/PL/GT 评测。裸设备、真实掉电和尚未启用的原地覆盖等边界仍需按后续验收范围处理。阶段工作量不等，不据阶段个数给出完成百分比或虚构工期。

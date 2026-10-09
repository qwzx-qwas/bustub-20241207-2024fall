# S12 / F32 执行与测试设计审查（2026-10-08）

协议：[按需 B 缓存与 CLOCK-Pro](s12_metadata_cache.md)。本轮已获执行授权；后续 S13 不在本轮实现范围。各节保留当时执行状态；最新提交前核对见 [跨模块总审](design_review_f32_20261009.md)，用户已授权检查通过后 commit。

## 1. 实际改动及先行逻辑审查

1. `ClockProReplacer` 由 A/B 共用代码，状态分别持有。固定节点池、直接帧标记、冷热环、测试历史和自适应冷目标取代 A 的 LRU-K。候选还须由缓存所有者核对。旧课程替换器从生产库退出，仅对应课程测试编译；删除失效 K 参数/命令行开关和生产专用 LRU-K 扩展。
2. `metadata_engine.cpp` 的 `PageState` 保存版本身份；`PageImage` 只代表可持有的正文。`MetadataPager::ReadGuard/Image` 取得正文所有权，树、记录链、校验代码不再拿可能失效的裸正文引用。内部 `MetadataCache` 属于原 MetadataEngine 的驻留状态，不是新增服务或持久索引。
3. `MetadataCache::Get/LoadLocked` 沿 F05/F02/F01 换入；同一位置串行并在锁内重新检查，合并相同版本加载。等待 IO 时不持全局缓存锁。CRC、页身份、generation、LSN 使用原解码器核对。
4. `Preserve/Overwrite/Durable` 处理旧视图：先保留旧正文，实际接纳覆盖后禁止旧版再从该位置重读；成功写回使新正文可淘汰。失败保留旧视图正文及 pending。资源未接纳时不无谓使旧版本不可淘汰。
5. 每个物理位置仅一个侵入式待写回节点；`Publish` 在 WAL durable 后更新最新 pending，`Pending` 有界取批，`Durable` 仅移除相同版本。修改页集合使用提交时的 dirty 集合；正常写回不再遍历全部持久页。
6. checkpoint 仍通过原恢复入口发布；Open 流式加载，恢复批次以现有写回器落位，使恢复工作集也受正文预算约束。普通旧视图在 Close 后禁止新查询，Close 等待已接纳查询，已复制的值不失效。

编码后、编写新测试前，推演了：固定磁盘槽位覆盖、两个并发读、较旧批次完成、队列更新、IO 拒绝/失败、关闭以及恢复。发现并先修正：写回队列可能比捕获的视图更新，状态表须按选中页扩容；旧正文的可重读资格须在 IO 接纳后才撤销。没有靠忽略异常、返回旧 Data 或扩大配置来掩盖这些问题。

测试进一步发现并修正了两处选择/交接问题：

- 冷页都被持有时，可淘汰热页仍须可达；B 在一次选择中临时排除被持有的候选，结束后恢复资格，避免两个阻塞候选轮流被选中。
- A 同页加载竞争失败是 Retry。获胜映射可能在再次查找前已退出，不能把这种竞争误报为缓存容量不足。复用 F26 原并发场景验证，删除本轮重复的 A 并发字节测试。

## 2. 测试全景及「目标 → 输入 → 路径 → Oracle → 失败含义」

下列 11 项只保存在 F32 模块压缩包，不新增常驻目标。设备文件、二进制及解压源码均为临时产物。

| Test | Invariant / 类别 / 防止的 bug | 输入与 production 路径 | 独立 Oracle 与失败含义 |
| --- | --- | --- | --- |
| `ObjectCommonDeferredAndReopenWithSmallMetadataCache` | integration：正文缓存小于对象元数据工作集时，两条写路径及恢复仍正确 | 32 页 B 正文预算；64 个不连续范围、不同非零正文，Common/Deferred 交替；NodeStorage→正式对象事务→B/F05/F02/Direct，checkpoint 后重开 | 逐范围与提交前输入比较；失败指向对象/缓存/恢复组合。可定位范围，但不能只凭此测试定位某个缓存函数 |
| `WorkingSetExceedsBodiesAndRestartsFromCheckpointAndRedo` | integration：冷页可换出并精确换入；checkpoint＋后缀恢复不用全量常驻 | 24 正文额度、48 个变长值；超过缓存容量；写回、checkpoint、重开、修改并保留最后脏后缀、再次重开 | 独立 `std::map` 比较完整有序键/值，真实 pread 增加确认换入。旧视图关闭后查询拒绝、此前复制值有效。能发现只限量却不淘汰、恢复全驻留及恢复丢后缀 |
| `HeldOldViewsBackpressureThenResumeAfterRelease` | integration：被历史版本占满时明确背压，释放读者后可前进 | 32 正文额度，持续保留旧视图并覆盖同一键；周期 checkpoint 排除 Journal 容量干扰，随后清空视图 | 必须出现 MetadataCommitBusy；释放后再次写入/读取与模型一致。防止偷偷丢旧版本或额度永久不归还；不依赖精确第几次触顶 |
| `ColdOldVersionSurvivesOverwriteAndPressure` | integration：旧视图不能重读被新版覆盖的固定槽位 | 先保存旧值、扫描使其冷却，再更新同键、写回并制造压力；正式 B 查询/写回 | 旧视图等于旧输入，新视图等于新输入。expected 不来自磁盘解码器。错误重读新版会产生版本错误或正文比较失败 |
| `SameVersionLoadsCoalesceAndUnrelatedHitsProgress` | integration：同版本共享加载，独立热页不等待冷 IO | 在真实 metadata pread 门控一处冷读取；六个并发同键查询及一处热查询；按实际观测偏移统计 IO | 所有正文等于输入、该位置只读一次、门控释放前热查询完成。读取位置来自 syscall，未窥探页目录；失败区分重复 IO、全局锁阻塞、正文错误 |
| `OlderCompletionKeepsNewerPendingAndQueueDeduplicates` | integration：旧写回完成不能清掉新提交；同页只留一项 pending | 门控真实页写，期间对同键再提交两版；释放后排空、checkpoint、重开 | 第二轮每物理页仅一次写、必须有新写、恢复等于最后输入。没有检查链表排列；防止重复入队和旧回调清除最新版本 |
| `FailedOverwriteRetainsOldViewAndPendingWork` | regression：接受后写失败不撤销已 durable 提交，旧视图仍可读 | 对真实页写注入 EIO；旧视图与新提交并存，显式重试写回并重开 | Failed 结果、旧输入、当前/恢复输入三者分别检查；不将失败解释成磁盘回滚 |
| `ColdReadChecksActualPersistentBytes` | regression：冷换入仍验证持久正文 | 先写非零值，扫冷；在真实冷 pread 前修改实际字节、保留旧 CRC | Get 必须报告 MetadataError。oracle 是主动损坏与错误结果，不是重新用同一编码器计算 expected；不接受返回旧缓存掩盖损坏 |
| `CloseDrainsAdmittedColdRead` | integration：关闭先排空已接纳的缺页读取，再拒绝旧视图新查询 | 门控实际读取，并发 Close，释放门控 | Close 在 IO 被阻挡时不能完成；读值正确，结束后新 Scan 被拒绝。30ms 否定性窗口只能检出提前结束，不能证明任意调度下的时序，故同时有正文/关闭后状态断言 |
| `ClockProFindsSoleEligibleFrameDuringChurn` | unit：全部 pinned 时无候选；唯一可淘汰帧必须可达 | 容量 1/3/16，800 轮固定/释放、不同页身份交替接纳；真实策略入口 | eligible 集合由测试输入独立决定；候选必须属于此单元素集合。覆盖冷热手可达性与 pin；不声称单独证明全部 ghost 自适应细节 |
| `ClockProProtectsReusedPagesAcrossScansAndHotsetChanges` | unit／有限策略实验：访问标记给予第二次机会，扫描后热点可适应 | 四个初始候选只有最早者被引用；另用 12 帧、两组热点各 900 次访问与一次性扫描交替 | 首个候选不能立即淘汰唯一被引用页；两个阶段分别记录热点缺页数。原单一缺页阈值曾漏掉删除访问标记，已补第二次机会断言；这不是跨机器性能结论 |

## 3. 已有测试的兼容与重复审查

- 复用 F26 原 10 项真实页/SQL 场景，覆盖脏淘汰失败、外部帧、并行加载、预取、translation RAM 归还和同页竞争。本轮另写的 `AEvictionKeepsBytesAcrossDirtyReadsAndConcurrentHits` 与其重复，已删除。
- F30＋F27 原 23 项、F29＋S8 原 18 项经现有归档 runner 组合；使用当前 production，不伪造历史包中 production hash。适配只在解压工作目录执行，正文、空间复用、恢复和网络 oracle 沿用。
- 常驻 `BufferPoolManagerTest` 的 `VeryBasicTest`、`PagePinEasyTest`、`PagePinMediumTest`、`PageAccessTest`、`ContentionTest`、`DeadlockTest`、`EvictableTest`，及 `PageGuardTest.DropTest/MoveTest` 仅删除无效 K 入参；保持 pin、内容访问、移动/释放行为断言。另跑原 B+Tree 插入/删除场景。没有把它们称作 CLOCK-Pro 策略证据。
- 旧 LRU-K 课程测试保留原算法契约、只在 test target 编译，另做兼容性运行；不计作新生产策略覆盖。旧 LRU/CLOCK 空壳同样不进入生产库。
- T1/T2 有正文/恢复重叠，但 T1 覆盖 FS 事务、引用与 Deferred，T2 独立固定 B 预算并检查元数据后缀；两者失效边界不同。T4/T7 分别覆盖成功覆盖与 IO 失败，不能相互替代。

## 4. Production / 接口污染审查

- 没有新增 test-only hook、getter、配置开关、`#ifdef TEST`，没有为测试扩充默认参数。
- `ClockProReplacer` 是新生产策略接口，A/B 均调用；删除测试仍需要。`Rekey` 在 B 的 WAL 确认后更新版本身份，不能用测试 helper 替代。
- `MetadataCache`、PageState、Slot、Call、FramePreparation 都是生产内部实现；Get/Scan/Writeback 的公共入口保持。FramePreparation 的 retry 标志修复真实并发交接，不暴露给测试。
- 删除旧 K 参数、全局 K 常量及 benchmark 选项是算法换代；LogManager 的既有参数未为测试增加。拆出 AccessType 只是解除旧算法头依赖，没有额外转发函数。
- MetadataSnapshot 的关闭语义因真实冷 IO 而改变，文档已更新；不能继续承诺在设备关闭后还能发起新查询。关闭后返回值仍可用。
- syscall wrapper 全在临时测试程序中，通过链接器替换；production 不知道测试门控。未创建第二份对象映射、bitmap、缓存正文或回收模块。

## 5. 变异检查

在隔离源码副本编译，要求对应 GTest exit=1 和指定断言/异常原因；编译失败、超时、sanitizer 崩溃不算检出。覆盖：禁止淘汰、重复选择被持有候选、允许旧版重读已覆盖位置、旧完成误清新 pending、取消加载位置互斥、跳过页 CRC、关闭不排空、忽略 pin、热页不可达、忽略访问标记。

首次只修改 LoadLocked 的二次检查，会在准备阶段破坏驻留计账，不能准确证明并发合并 oracle；最终变异改为取消 Get 的位置互斥，要求真实重复 pread 断言失败。首次热点缺页阈值没有检出忽略访问标记，已修正测试，未把这两次不充分结果记为成功证据。

## 6. 稳定性与范围

固定输入，无随机 seed 和执行顺序依赖。每例独立 mkstemp 设备；门控在 future 析构前释放，文件/线程/节点正常收尾。测试以条件变量制造交错；deadline 用来诊断不推进，不将 sleep 当作持久化证据。文件系统必须支持真实 Direct IO；对齐由设备查询，固定 2–3 MiB 只是该夹具声明的 metadata 区域。先前沙箱内 LSan 失败不算通过，最终在可运行 LSan 的环境重验。

本轮未运行 TSan、裸设备/掉电或共同 C/P、SS/RS/PL/GT 性能比较。CLOCK-Pro 保留有界 history，仍有 miss 侧短锁/哈希、读侧引用位和缓存所有者同步；B 的旧版本/目录仍占 RAM，不承诺无代价或任意旧视图都可持续前进。

## 7. 最终测试矩阵与建议

| Test | 验证 invariant | Oracle | 新 failure mode / 与其他测试重复 | Production pollution | 建议 |
| --- | --- | --- | --- | --- | --- |
| T1 正式对象 | Common/Deferred 在小 B 缓存恢复一致 | 非零输入向量 | FS 与缓存组合；与 T2 部分重叠但跨层责任不同 | 无 | 保留 |
| T2 B 工作集 | 换入换出与后缀恢复 | map＋真实 IO | 全量常驻、冷页恢复 | 无 | 保留 |
| T3 旧视图压力 | 保留/释放后可推进 | 明确 Busy 与最终模型 | 旧版本无限保留或提前丢弃 | 无 | 保留 |
| T4 旧版本覆盖 | 旧、新视图分离 | 两份独立输入 | 固定槽覆盖旧版 | 无 | 保留 |
| T5 合并加载 | 一次 IO、独立页可推进 | syscall 次数及正文 | 嵌套全局锁、重复加载 | 无 | 保留 |
| T6 去重队列 | 新 dirty 不被旧完成清除 | 第二轮写及恢复结果 | 队列/版本交错 | 无 | 保留 |
| T7 写失败 | 旧视图、pending 均保留 | EIO＋正文＋重启 | T4 未覆盖的失败窗口 | 无 | 保留 |
| T8 冷读取损坏 | 检查持久字节 | 注入位翻转后拒绝 | F30 scanner 不能替代 Get 路径 | 无 | 保留 |
| T9 Close | 排空再关闭 | 门控、正文、新查询拒绝 | 与 F26 Close 不同所有者 | 无 | 保留 |
| T10 候选 | pin 与热区可达性 | 独立 eligible 集合 | 策略局部失败，方便定位 | 无 | 保留 |
| T11 访问/热点 | 第二次机会与热点变化 | 输入引用位、缺页次数 | 有限策略轨迹；完整性能仍缺 | 无 | 需要补充 |
| 已删 A 并发新例 | 并发淘汰内容 | 输入字节 | 与原 F26 并发场景重复 | 无 | 删除 |

所有“保留”均指模块包，不增加常驻细碎套件。核心保护是正式对象恢复、旧版本覆盖、真实 IO 交接；T10/T11 是局部策略证据，不能替代共同业务/性能验收。后续相同 failure mode 被共同套件接管时合并退出。

## 8. 2026-10-08 执行与归档（历史，本轮结果见 §9）

最终正常实现共 **77 项通过**：F32 新增 11、F30/F27 23、F29/S8 18、F26 10、原课程/树/页守卫 15。其中 1 项为旧 LRU-K 课程兼容性检查，余下 76 项覆盖当前生产路径或新策略。10 个指定变异全部检出；编译/超时/工具异常没有计为检出。ASan/LSan 开启，三个 benchmark 目标编译成功但未进行性能比较。

热点/扫描轨迹中，两次热点阶段各 900 次访问、各 3 次热点缺页；该结果仅解释 T11 的输入轨迹，不推出 CLOCK-Pro 全面优于 LRU-K。本轮没有运行共同 C/P、SS/RS/PL/GT、TSan 或真实掉电。

[源码归档](../../test/archives/F32-metadata-cache.tar.gz)包含最终 11 项、三个 runner、恢复说明与生产/测试/依赖哈希；[结果入口](../../test-results/storage-f32-20261008/README.md)保存唯一最终结果包。首次创建 F32 包，无旧 F32 包；调试轮次、失效 oracle、二进制和展开源码退出工作目录。其他模块有效压缩包是回归依赖，保持原哈希；补齐总 SHA256SUMS 原先漏登的 F30 项，没有改动 F30 包。实测后仅六个文件换行/include 排序格式化，前后源码哈希分开记录。未提交 commit。


## 9. 2026-10-09 再次审查：所有权交接与历史判据

### 9.1 先行代码结论与实际修改

重新对照主方案 §1.5/§1.7、F32 协议，检查 B 页描述/正文分离、位置锁与加载、覆盖旧视图、pending 写回和关闭；检查 A/B 策略共享、ghost 身份、有限节点池及历史冷热调整。未新增模块、缓存权威目录、线程池、持久格式或 public API。

本次确认并修复 `MetadataCache::NewImage` 的异常计账回归：创建 `shared_ptr` 控制块失败会调用传入的删除器；原先返回表达式处在外层 catch 内，删除器减一次 live_、catch 又减一次。计数偏小后，后续加载可能超过缓存槽位上限并报告 `metadata residency exceeds image budget`。现在只在所有权交接前由 catch 归还额度；交接后统一由删除器归还，包括控制块分配失败。未增加兜底返回或吞掉异常。

### 9.2 对现有测试的补强与八项检查

没有新增场景数量；修改以下两项：

| Test | 本次目标 → 输入 → 执行 → Oracle → 失败含义 | 建议 |
| --- | --- | --- |
| `WorkingSetExceedsBodiesAndRestartsFromCheckpointAndRedo` | 原小缓存场景加载第一处冷内容时，线程局部链接包装器在页正文分配后的下一次分配注入 bad_alloc；必须确认注入发生，随后继续全工作集读取、checkpoint/后缀恢复，与原独立 map 比较。失败意味着额度交接错误影响后续容量/内容，不只是在验证 shared_ptr 自身 | 保留 |
| `ClockProProtectsReusedPagesAcrossScansAndHotsetChanges` | 四帧全部可淘汰且无额外引用，先淘汰一页再接纳；相同完整身份应获得测试期历史保护，分别改变页号/generation/LSN 时应作为新身份参与轮转。仅观察公开的候选结果，不读冷热计数和扫描手；沿用原两阶段热点/扫描轨迹 | 保留；完整性能仍需补充 |

- **Oracle 独立性：** 正文仍来自提交前输入；历史规则来自 CLOCK-Pro 测试期再次访问的提升语义（[原论文 §4.3/4.4](https://static.usenix.org/publications/library/proceedings/usenix05/tech/general/full_papers/jiang/jiang_html/html.html)），不从实现生成 expected。该有界轨迹不证明任意负载的命中率或精确比例调节性能。
- **实现耦合与局限：** 分配故障定位通过测试进程的 operator new 链接包装，识别项目页大小附近的正文分配，再使下一分配失败；这是阶段故障注入的实现耦合，未写死 shared_ptr 控制块大小。正文表示改变时须调整定位，注入未发生必须失败。最终 oracle 是容量下的真实读取与恢复，不能只检查分配次数。
- **Production/API 污染：** 包装器和线程局部触发器只在阶段测试；生产无 getter/hook/默认参数/测试分支。NewImage 的修改只调整既有归还责任。
- **重复：** 在原 T2 加异常交接，在原 T11 加非驻留历史及完整身份；未复制正式对象、F26 并发或旧课程测试。
- **杀伤力：** 增加两个隔离变异，分别恢复重复归还、取消历史命中的热页提升；需在指定场景和判据失败，编译失败/超时/工具异常不算检出。
- **稳定性：** 分配故障只作用于调用线程，有作用域收尾，线程退出或测试异常不会留下持续故障；历史序列固定、无时间阈值。原并发门控及 Close 时间窗口的局限继续保留。
- **归档：** 仅替换 F32 当前源码/结果包；七个有效依赖包不变。旧 F32 包不得作为新断言的有效证据保留。未提交 commit。

### 9.3 实测结果与剩余限制

本次按当前生产代码完整复验 **77 项正常测试，全部通过**：F32 11、F30/F27 23、F29/S8 18、F26 10、原常驻测试 15。最后 15 项中含 1 项旧 LRU-K 课程兼容检查。ASan/LSan 开启；三个 benchmark 目标编译成功，未执行性能比较。**12 个指定变异全部在预期判据失败**，包括本次新增的两个：

- 恢复 NewImage 重复归还后，T2 在故障后的工作集读取中报告 `metadata residency exceeds image budget`。
- 去掉非驻留历史再次命中的热页提升后，T11 的 `test-period history` 断言失败；不是靠编译错误或崩溃判定。

故障注入也经过反查：首次分配日志为 `4300 → 4152`，实际错认了返回值缓冲，不能作为控制块失败证据。改用返回值长度超出正文识别区间的键，并断言这一前提后，当前日志为 `4152 → 40`，对应本工具链的正文和随后控制块分配；正常实现通过、重复归还变异失败。最终包只包含修正后的完整正常测试和变异日志。

未修改 CLOCK-Pro 生产算法。本轮补强证明了特定轨迹下的历史保护和完整身份区分，尚不能证明各种负载下冷目标调节的收益；共同 C/P、SS/RS/PL/GT、多核争用、TSan、裸设备和真实掉电仍未验证。原 Close 测试的时间窗口与故障定位的实现耦合已在上文注明，没有为了让阶段测试稳定而增设生产接口。

F32 源码包与结果包原位替换，旧版不备份；其他七个有效模块依赖保持原哈希。主方案、相关模块和结果入口同步为本次 77/12，不将历史结果重复计数。临时解压源码、变异副本、构建和设备产物在归档核验后清理，结果目录沿用 `storage-f32-20261008`。未提交 commit。

## 10. 2026-10-09 再次复审：预期正文必须来自独立输入

### 10.1 方案、代码与重复职责

本次先核对 `MetadataCache::Get/LoadLocked/Preserve/Overwrite/Durable/Close`、去重队列和 checkpoint 重放，再核对 `ClockProReplacer` 的冷热/测试期、三个扫描位置、固定节点池、完整身份和 A 的候选复核。未发现新的需要修正的生产逻辑。另发现三个 benchmark 仍通过旧 LRU-K 头取得 AccessType，已改为直接包含 access_type.h；算法和运行逻辑不变。这三处工具文件需重新编译，其余 503 个生产清单文件与 §9 实测哈希相同。

A/B 共用替换算法代码、分别持有缓存状态；B 的页描述、驻留正文和待写回节点各承担身份、读取及落位责任。CLOCK-Pro 历史只记录身份，不保存正文、不保活翻译数组，未新增重复缓存或权威目录。原课程 LRU-K 仅参与兼容性测试，未回到生产库。对照原论文 §4.3/4.4 的提升、测试期退出及自适应方向，本项目保留数据库 pin 所需的有界搜索；不把它称为论文性能的复现。

### 10.2 本次测试修改及设计审查

发现旧 T4、T7 的 expected 取自首次 `Get()`，T5 的热页 expected 也取自 `Get()`。这种比较只能证明前后读值相同，初次读错且之后一直相同可能漏检；§2 中对这些正文 oracle 独立性的描述当时不够准确。本次改成写入前输入模型，未改变测试数量：

| 测试 | 目标 → 关键输入 → 正常路径 → Oracle → 失败含义 | 建议 |
| --- | --- | --- |
| T4 `ColdOldVersionSurvivesOverwriteAndPressure` | 旧版在新槽位覆盖及缓存压力下仍等于旧输入；保存模型旧值→正式 Get、覆盖、写回、换出→旧视图等于旧模型，新视图等于新模型；初次读错也必须失败 | 保留 |
| T5 `SameVersionLoadsCoalesceAndUnrelatedHitsProgress` | 同版本仅一次设备读取，独立热页能推进且正文正确；一次扫描使早期键变冷、用模型检查并预热另一键→门控真实 pread、并发 Get→模型字节＋指定物理位置读取次数＋完成顺序；分别定位字节错误、重复 IO、全局阻塞 | 简化 |
| T7 `FailedOverwriteRetainsOldViewAndPendingWork` | 写回失败不能损坏已发布旧视图或丢待写回新值；先保存旧输入→真实写回 EIO、重试、重开→旧/新独立模型与原失败结果；不能仅靠两次错误结果一致通过 | 保留 |

- **去重与低价值步骤：** T5 删除单独起线程读取冷键后立即等待的预热，以及随后重复的全表扫描；仍通过真实缺页门控确认被测路径。T4/T7 的成功覆盖与写失败不同，T5 的合并加载不同，不新增镜像测试。
- **Production / 接口污染：** 生产库逻辑、public API、默认参数、配置和控制流均未修改；未添加 getter 或 hook，复用原链接侧门控。
- **Oracle：** 模型由提交前字节建立，读取实现不能反向更新 expected。IO 次数和错误结果仍是辅助判据，不能代替正文比较。
- **杀伤力：** 旧版重读变异的判据从宽泛的 `metadata` 收紧到实际版本/校验失败，避免将无关额度或状态错误算作该风险的检出。取消位置互斥仍须触发 `duplicate load`；其余变异继续使用指定测试及失败原因，不接受超时/编译失败/崩溃。
- **稳定性：** 新 expected 不依赖时间、缓存排列或首次实现输出。原门控的线程收尾、Close 的有限时间观察、分配故障定位的实现耦合仍按 §6/§9 限定；没有声称消除所有调度不确定性。
- **不足：** 完整冷热目标反馈、相同总内存下的 LRU-K 对比、多核争用及 SS/RS/PL/GT 仍需共同性能验收；本轮功能复核不能替代。

### 10.3 验证与归档

本次 **11 项 F32 全部通过，12 个定向变异全部检出**，ASan/LSan 开启。精简后的 T5 仍在取消位置互斥时报告 `duplicate load`；旧版重读变异触发收紧后的 `metadata page checksum/version mismatch`。三个 benchmark 重新编译成功，未运行性能负载。生产库归档成员核验仅含 `clock_pro_replacer.cpp.o`，无旧替换器对象。

其余 **66 项沿用 §9 的既有结果**：F30/F27 23、F29/S8 18、F26 10、原常驻测试 15（含 1 项旧课程兼容）。核对 503 个未改生产文件及七个依赖包哈希相同；三个仅改头依赖的工具文件不参与这些测试，并已另行编译。因此有效证据仍覆盖 77 项，不能写成本轮重新运行了 77 项。结果包分别保存当前正常/变异日志、沿用的 regression 日志及哈希差异说明。

F32 源码/结果包原位替换，旧版不备份；总方案、子模块和归档入口同步到本节。源码包仍只含 11 个场景、三个 runner、恢复说明和 MANIFEST；临时解压源码、构建和变异产物在校验后清理。未提交 commit。

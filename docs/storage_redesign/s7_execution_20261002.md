# S7 Common 对象事务：实施与测试设计审查

日期：2026-10-02（Asia/Shanghai）。方案见 [S7 执行协议](s7_common_pipeline.md)。本轮先分析生产路径与职责交接，再编写测试；审查中修改生产或测试后重新验证最终版本。

## 1. 实际落点与方案覆盖

| 模块 | 实际实现 | 交接与限制 |
| --- | --- | --- |
| F15 | `object_transaction.h`、`NodeStorage::SubmitObjects`、`ObjectMappingSnapshot::Control` | 创建、Write、Append、Resize、Remove 和控制项可组成一份事务；每对象每批至多一项修改；Accepted 接收输入所有权，最终票据另报持久化结果 |
| F16/F31 | `ObjectTransactionPipeline::Impl::Submit`、`RequestBudget` | 请求、待执行正文/组装空间、操作数量有显式上限；保留结果仍占请求额度；正文按移动后实际 capacity 记账，F02 工作缓冲另计。全业务节点预算尚未实现 |
| F17 | `Conflicts`、`Drive` | 本轮同对象（含同 owner 控制项）按接收顺序；独立对象 IO 重叠。不声称同对象所有不相交写已经并行 |
| F18 | `Plan`、`Assemble`、`ObjectIO::WriteCommon` | 大写 COW，只读需要保留的边缘；小写读一单元后组装 COW；小尾部也采用 COW；F01 未提供掉电隔离单元，原分配复用未启用。没有无日志保护的原地覆盖 |
| F19 | `Drive` 与 `CommitLoop` | 有界请求上下文组织阶段，一个元数据角色等待既有 B；不占 IO worker，不另建 WAL。正常推进用完成通知；只有预提交资源暂满才按显式间隔重试 |
| F20 | `ObjectMappingAccess::Apply` → F12/B | 一次 B 提交发布所有映射、描述、分配、退出和控制记录；正文先 durable。B 既有不可变视图是唯一发布点 |
| F21 | S6 `ObjectIO::Read` | 固定映射、F14 保护、Data/Hole 拼接；S7 RMW 复用。Deferred 来源待 S10 |
| F22 | `ObjectMappingAccess::Garbage`、`CommitLoop` → F14/F12 | 分页发现持久退出记录，有界后台回收；读者只挡住对应单位，重开重新发现；不复制存活性/释放算法 |
| F02 | `IOBatch::RetainUntilComplete` 的终态后通知重载 | 原资源回调仍在终态前归还保护；新增通知在终态后唤醒协调器。不在回调里提交/等待 B |
| F33/F34 | NodeStorage 对象事务能力、错误汇总与反序关闭 | B→分配/映射/引用→对象IO→事务服务；关闭先收束事务，再关闭下层。仍支持元数据专用及 S6 分步部署 |

S7 本轮本地 Common/COW 范围已完成；原分配尾部复用未启用，需先明确真实设备保障。S8 Raft Store、S9 页/FrameArena/数组目录与 A 空间管理、S10 Deferred、S11 共享快照、S12 完整后台治理、S13 业务流水线均未执行。本轮不改变 SQL/Raft 当前业务路径。

### 代码逻辑先行审查

- 接收责任：`Submit` 先有任务/结果所有者和队列节点，再移动输入；拒绝/分配失败不消费输入。F02 先队列持有 batch 再返回 Accepted。B 提交槽同样先保存任务再推进 Committing。
- 完成竞态：IO 可先于提交函数返回完成，通知只唤醒而不修改 Tx 状态；协调角色检查正式终态后推进。因此迟到的“接收”不会覆盖“完成”。结果槽不会因完成队列满而丢失。
- 锁与进展：设备/Journal 等待不持协调锁。数据批次释放 IO 名额后，预留空间责任仍由 `CommonDataWrite` 保留，元数据阶段不会等待被自身数据结果占满的名额。没有每请求等待线程。空闲时不轮询。
- 原子范围：正文不写入第二份 WAL；F12 一次真实预留按对象分段，F13 核对完整分区并同批更新 B。批次数据共用 Flush 不代表自动原子；B 提交才建立原子发布。
- 写安全：已存在对象取规划版本，提交时读取最新 B 并校验该对象版本；不把“刷新 base”当作忽略业务依赖的理由。COW 不覆盖旧有效字节；当前不能从 IO 对齐推定掉电隔离，尾部采用 COW；原分配复用及初始化边界扩展留待真实设备保障明确后接续。
- 失败/关闭：未提交准备错误明确 NotCommitted；已提交元数据的实际 NotCommitted/Indeterminate 原样保留，不能凭 IO 失败释放可能已提交的分配。观察票据退出不取消 IO。关闭可结束尚未发出的工作，但必须收束已经发出的数据/元数据操作。
- 回收：复用 F14 持久退出、剩余所有权和范围 pin；后台不擦零、不越权释放；GC 与前台提交角色轮转。Journal 的 checkpoint/回收仍用既有维护接口，本轮没有擅自选择自动 checkpoint 阈值。

### 开源机制

[etcd/raft](https://github.com/etcd-io/raft/blob/main/doc.go) 用阶段消息分离协议和耗时存储，同目标可靠有序交付；本地采用有主人的 pending、接收确认和完成结果，未实现 S13 协议线程。[BlueStore](https://github.com/ceph/ceph/blob/main/src/os/bluestore/BlueStore.cc) 按 TransContext 阶段推进、保留必要的 sequencer 顺序；本地采用薄协调角色与异步数据依赖。[其小写策略](https://docs.ceph.com/en/latest/dev/bluestore/)区分新位置、未使用范围与日志保护覆盖；本地采用 COW、尾部 COW 和后续安全复用/Deferred 的区别，未复制源代码或引入新第三方运行依赖。

## 2. 测试全景与逐项设计

新增十项均为 **integration / regression**。生产路径：`NodeStorage → S7 请求/规划 → S6 ObjectIO → F13/F14/F12 → F04/F02/F01 → Linux Direct 文件`，元数据走真实 B/Journal。没有手写数据库替身；表中的 model 是独立预期，不是生产实现。

| Test | 目标 → 输入 → 执行 | Oracle / expected 来源 | 失败含义 |
| --- | --- | --- | --- |
| AtomicRangesControlsAndRecovery | 多对象/控制项完整提交；两个对象多个非零内部覆盖、空洞、缩小再扩展，保留旧视图并重开 | 预先生成的字节数组按逻辑下标自行覆盖；旧视图读旧数组；控制值等于独立输入；截断后扩展部分为零 | 对象拼接/旧版本/控制提交/恢复任一失效；整体定位到对象事务路径，不能单凭它定位具体设备层 |
| HandoffOrdersDependenciesAndLetsIndependentWorkFinish | 接收不等于发布；暂停 X 第一笔实际写，排 X 追加与独立 Y；Y 应先完成，X 后续必须等前序 | Y 字节等于输入；X 未发布、追加未完成；放行后 X 等于两份输入拼接 | 全局串行堵塞、依赖被越过、提前发布或丢完成通知 |
| RetainedResultsBoundAdmissionAndDroppedTicketStillDrains | 两个请求额度：一个 IO 暂停并丢票据，另一个完成但保留结果；第三个申请，随后关闭 | 第三项 Full 且输入仍在；Draining 不可提交；关闭等真实 IO；重开内容等于输入 | 只统计队长、执行责任随票据丢失、错误移交输入或提前关闭 |
| SmallAppendsPreservePreviousVersions | 从实际 IO 对齐推导初始尾部；一次对齐尾部、一次不对齐尾部追加，读旧版，再回收和重开 | 两次追加均换分配身份，不把 IO 对齐当原地写保障；正文由输入拼接，旧分配退出后可完整回收 | 尾部错误复写旧分配、前缀/旧版本损坏，或旧 COW 分配无法回收 |
| MultiObjectDataBatchFlushesOnceBeforeAtomicPublication | 两对象同事务；暂停一成员，观察另一成员进入真实 IO，然后放行 | 全成员结束前无数据 Flush/Journal 写，旧视图两对象和控制项均未变；进入 Journal 前恰有一次数据 Flush；重开全部新内容 | 拆成逐对象 Flush、过早 Flush/元数据提交、批次结果或原子内容不全 |
| LargeOverwriteReadsOnlySurvivingEdges | 已有 12u 数据；在非零位置覆盖 6u+131 字节，两边均需保留 | 写期间实际 Data pread 总量至多 2u；最终正文等于输入模型。上限来自“只读边缘”的项目策略，不来自生产映射生成 expected | 不必要地读取已被完全覆盖的中间数据，或边缘组装损坏；不等同于吞吐基准 |
| FailureStagesNeverReportPartialTransactionSuccess | 同批两对象+控制值，分别在真实读取、数据写、数据 Flush、Journal 写和 Journal Flush 注入 EIO | 不返回 durable，保留错误；NotCommitted 必须恢复完整旧模型；仅 Indeterminate 允许完整旧或完整新模型，两个对象须与控制值一致 | 吞错/伪成功，正文与元数据或两对象恢复结果不一致。只验证这些注入点，不证明所有崩溃点 |
| CapacityRetryKeepsAcceptedWorkAndOldVersionsConflict | 用真实 S6 写结果占满 F02 名额及 F12 预留票据；S7 读取/写入请求先等待，归还后完成；另一请求数据在途时正式接口截断目标 | 容量不足时保持未完成，归还后正确；外部修改后 S7 返回版本冲突，不能覆盖新长度 | 接纳后丢工作、错误重试/忙等协议、刷新 base 却未验证对象依赖 |
| UnsatisfiableRequestsFinishAndLeaveOldStateIntact | 合法独立配置中，分别超过 Common 写字节、RMW 读字节、F02 成员数/内存（含对齐开销）和 F12 碎片预算；再发可满足请求 | 5s 防挂起期限内返回 NotCommitted 与明确资源上限错误；旧长度/正文/控制项不变，节点仍可接受合法写并重开 | 将永久超限当暂满无限重试、错误提交、准备资源未归还或误将输入限制隔离成设备故障 |
| BackgroundReclaimRespectsReadersAndRestartsFromDurableFacts | 暂停一单位内读取，删除 4u 对象；申请除该单位外的 Data 空间；另用无后台 GC 的正式 S6 部署生成确定待回收记录，再以 S7 重开 | 先可申请总容量−u，但不能再申请 u；读者返回原 47 字节；旧映射不能读已释放范围；重开最终可申请整区 | 在途读取被回收、后台不推进、旧映射复活或重启丢 GC 工作。新分配只是探测正式空间可用性，正文判定不依赖 allocator 内部 bitmap |

输入使用已有 S5 可复现字节生成器，多个种子、不同非零偏移和长度；正文 expected 只用输入数组计算。不调用生产编码/映射算法构造 expected。分配身份用于尾部 COW 不复写旧分配的承诺，系统调用计数只用于已承诺的批次/边缘 IO；均不检查私有字段布局。

## 3. Production pollution 与接口审查

| 变更 | production 是否需要；删掉测试后是否保留 | 是否能只用原接口 |
| --- | --- | --- |
| ObjectTransaction/票据、SubmitObjects | 需要。调用方提交完整对象操作并取得真正提交结果；不再自行组织数据 durable→映射发布 | 原分步 WriteObjectData/PublishObjectData 要求调用方同步组织，无法代替本轮组合事务与接收责任 |
| ObjectTransactionOptions、NodeStorageOptions.transactions_、object_transaction_ | 需要。显式资源预算及可选服务，区分元数据专用/S6/S7 部署；节点只在服务可用时开放 | 原对象 IO 能力不代表完整事务能力。未修改默认设备路径、Direct 对齐或原 API 的必填参数 |
| IOBatch 终态后通知重载 | 需要。S7 有真实消费者；不把 B 提交放进旧终态前资源回调，不增加等待线程 | 原回调不能安全消费终态 Result；新增重载保留原调用契约，没有测试专用 callback |
| ObjectMappingSnapshot::Control | 需要。正式控制记录的读取，与对象事务使用同一视图 | 原 Get 是 B 的一般元数据接口；不向上层暴露内部类别编码或原始 B 指针 |
| ObjectMappingAccess / object_change_internal.h | 需要。私有的同批映射及 GC 协作，复用 F13 编码和 F12/B 提交 | 无法通过多次原单对象提交拼出原子批次。桥接保持私有，不是 public getter |
| ObjectIO::WriteCommon/FinishCommon | 需要。多个对象分配共用数据批次，IO 额度与待发布分配责任分离 | 原单次正文接口不携带多个对象，保留真实 S6 部署使用，未为测试增加旁路 |

无 `#ifdef TEST`、test-only hook、测试默认值、暴露内部状态 getter 或测试反向依赖。系统调用门/EIO/观察都由测试链接包装，production 无测试分支。未新建第二个设备线程池、WAL 或一层只有转发意义的 Admission/Sequencer 服务类；这些角色落在实际有数据和责任的函数中。

## 4. 重复测试与跨阶段接管

- 原 F02/S6 15 项、F34 5 项、F12/F13/F14 18 项和 S5 8 项从各自唯一档案解压复用，测试正文不复制进 F19 包，也不重新编写相同矩阵。
- S6 只承诺分步正文 IO/显式映射发布；S7 新增自动阶段交接、多对象同提交、同对象顺序和自动回收。旧底层测试不能替代这十项组合风险；十项也不能替代外部帧许可、Journal 槽位代次等原独立契约。
- 尾部场景保护 COW 前缀、旧版本和旧分配回收；原分配尾部复用的“初始化边界同批推进”仍是后续交接项，不声称本轮实现。大块边缘读取保护具体的新读放大风险，不建立每个 helper 的单测。
- T0 共同 C/P 内容、seed、成功/超时口径不改；本轮不重跑业务性能、不引用局部结果为吞吐提升。S8/S9 正式业务接入后，复用这些风险与独立模型改适配；满足等价覆盖才退出阶段档案，不宣称已经被 E2E 接管。

## 5. 杀伤力与发现的问题

实际定向变异：读整个覆盖中间段、允许同对象越过依赖、丢输入字节、截断不移除旧映射、漏控制记录、忽略对象版本、放宽已承诺请求额度、停止后台回收、省略 Common 数据 Flush。各项必须成功编译，测试退出 1 且 XML 命中对应断言；编译失败、超时或 sanitizer 异常退出均不算命中。最终证据为结果包 `mutations.json`。本次复审另加：永久限制仍重试、预留暂满提前失败、不确定误报未提交，共十二个指定变异。

审查/验证过程中修正：

1. 生产规划先实现了正确但多读的 RMW；复审改为大写只读两端，小写同单元只读一次，保留相同正文 oracle，并增加一个对应工作量场景。
2. 额度变异使接收结果变为 Accepted 后，测试原来的非致命 EXPECT 仍访问已移动输入，触发 UBSan。这是测试收尾/判据问题，改为 ASSERT 检查拒绝及容器长度后才比较正文。最终必须正常测试失败，不把该次异常退出算变异命中。
3. GC 重开用例原先在解除读取保护后关闭，GC 可能已抢先完成；改用正式、无后台的 S6 部署生产确定的持久待回收事实，避免用调度运气证明重启恢复。
4. 最终逻辑审查发现初版将 F01 的 Direct IO 对齐用于尾部复写安全判定。该能力不足：修改为本轮所有小尾部追加采用 COW，移除无法安全使用的尾部候选、原地 IO 和初始化边界扩展接口，测试改为保护这一正式策略。原分配复用仍待真实设备保障/恢复保护接续。依据见 [S7 能力核对](s7_common_pipeline.md#尾部复用的能力核对)。
5. 沙箱内首轮 8 项断言通过，但 LSan 无法扫描进程而退出异常；该轮不算完整通过。最终保留泄漏检测，在允许检测器扫描的执行环境重新运行。
6. 归档前逐日志核对发现旧版 cpplint 不识别两个 clang-tidy 类别注释，先前组合命令的末项成功掩盖了 lint 失败。仅修正两行工具注释，分别检查每个命令的退出码后通过。完整运行对应的旧注释源码/哈希另存，和最终源码仅有注释差异；不将初次 lint 失败记为通过。

限制：没有穷举每个故障字节/设备掉电位置，没有宣称能精确强制“IO 在 TrySubmit 返回前完成”的每种交错；后者由责任先登记、终态检查及通知只唤醒的代码关系保证。省略 Flush 的变异会先被生产 durable 检查拒绝，测试通过要求正常事务成功，从而捕获它；不声称仅凭此证明每一层检查的独立性。

## 6. 稳定性与资源

每场景独立 8 MiB Direct 文件；对齐实际查询，分配 u 由设备对齐和 fixture 预算计算，超出能力明确失败，不硬编码设备 4KiB。三个 IO worker、两个固定流水线角色及既有 Journal worker；没有 GPU/网络依赖。固定数据/seed，单进程顺序用例，门状态独立 Reset。

故障门用条件变量等待实际 IO 到达；1/20ms 只在已暂停/已占满容量后判定不能提前完成，不测性能。5s 是防挂起期限，进程 180s 总上限；GC 空间观察只使用正式接口，有限时重试。所有提前返回/异常都先释放门再等待 Close future，避免测试自己的死锁。

源码与构建在独立 `/tmp/bustub-s7-20261002`；最终源码只在 F19 压缩包，不注册新常驻 CMake/CTest 目标。原有共同 C/P 和课程测试保持原位。

## 7. 最终矩阵

| Test | invariant / Oracle | 新 failure mode | 与其他测试重复 | Production pollution | 建议 |
| --- | --- | --- | --- | --- | --- |
| AtomicRangesControlsAndRecovery | 完整对象/控制版本；输入字节模型 | S7 原子内容与旧视图/恢复失配 | F13 未接入完整写事务 | 无 | 保留 |
| HandoffOrdersDependenciesAndLetsIndependentWorkFinish | 独立 Y 完成、依赖 X 等待、拼接正确 | 移交/排序/全局阻塞 | F02 不认识对象依赖 | 无 | 保留 |
| RetainedResultsBoundAdmissionAndDroppedTicketStillDrains | Full 保输入、Close 等在途、重开内容 | 新请求责任/预算与观察者混淆 | 与 F02 缓冲寿命不同 | 无 | 保留 |
| SmallAppendsPreservePreviousVersions | 新分配身份、拼接正文、完整回收 | 未经设备保障授权的尾部复写 | 复用旧 F14 算法，新增组合边界 | 无 | 保留 |
| MultiObjectDataBatchFlushesOnceBeforeAtomicPublication | 两成员完成后一次数据 Flush、完整发布 | 把数据批次或接收误当事务提交 | 原 S6 仅单正文多 extent | 无 | 保留 |
| LargeOverwriteReadsOnlySurvivingEdges | 至多 2u 的实际读量及独立正文 | 大 COW 被不必要全段 RMW 拖慢 | 新策略工作量，非重复业务性能基准 | 无 | 保留 |
| FailureStagesNeverReportPartialTransactionSuccess | 五处错误、提交结果与旧/新完整模型一致 | 多阶段失败或恢复部分提交 | 合并多故障点，不逐错误码建用例 | 无 | 保留 |
| CapacityRetryKeepsAcceptedWorkAndOldVersionsConflict | 容量归还后完成、版本冲突拒绝 | 等资源丢任务或错误刷新 base | 已有 B 冲突不涵盖对象规划版本 | 无 | 保留 |
| UnsatisfiableRequestsFinishAndLeaveOldStateIntact | 超限应明确失败、旧状态与合法后续写 | 永久超限进入无限 pending | 原 Full 场景只覆盖临时占用 | 无测试接口 | 保留 |
| BackgroundReclaimRespectsReadersAndRestartsFromDurableFacts | 总空间−u / 总空间、原读字节 | S7 自动发现/重启及在途保护交接 | 原 F14 只显式调用回收动作 | 无 | 保留 |
| 原 46 项回归 | 既有对应执行文档的公开契约 | 相邻模块回归 | 原包引用，不复制场景 | 无新增 | 保留 |

“保留”均指模块压缩归档，不是常驻小测试。核心保护是交接责任、完整提交、正文/旧版正确性、空间与关闭；没有新增只测第三方或标准库的小测试。没有需要为了测试保留的 production 接口。未覆盖的是业务 E2E、原始块设备/掉电、Deferred、共享业务快照及长期性能/GC 稳态，仍按原阶段执行。

## 8. 验证结果与清理

最终验证以本地结果包中的 `commands.json`、XML、`summary.json`、`mutations.json`、`static-checks.json` 和静态检查日志为准。源码归档 `test/archives/F19-common-pipeline.tar.gz`；本地结果 `test-results/storage-s7-common-20261002/F19-results.tar.gz`。最终 **56 项（本轮10＋原46）通过、无跳过，12 个变异均命中指定判据**；Clang 14 ASan/UBSan/LeakSanitizer、仓库配置 cpplint、clang-format、clang-tidy 和 GCC C++17 Werror 检查通过。Tidy 对依赖告警有工具过滤，两个有说明的私有命名空间/区间参数提示使用局部 NOLINT，不宣称整个仓库零告警。首次执行后曾仅修正工具注释；本次复审又修正实际进度问题并完整复验，当前归档的生产/测试哈希对应本次最终运行，不再沿用首次结果。

复用/冗余审查：抽出 F13 共同 replacement 变更组装，原单对象入口和 S7 同批入口复用；保留实际 S6 两段式部署接口，不因新事务接口存在就误删。私有对象变更桥接与流水线线程头分离，避免 F13 依赖上层线程实现。没有未消费的新回调、getter 或空壳接口；初版无法安全启用的尾部原地写相关接口已删除。S8 的业务默认路径迁移尚未发生，不删除仍在生产使用的旧 Raft Store。

F19 首轮归档已由本次复审版原位替换，只保留最终源码和结果包，不保留有漏检问题的旧包或备份。旧的 F02/F12/F13/F14/S5 有效归档仍是独立回归依赖，未复制或误删；结果包逐成员校验后清理专用临时工作目录，不保留镜像、二进制或错误测试展开副本。

## 9. 再次复审：进度保证与恢复判断（2026-10-02）

### 先从生产逻辑确认的问题

`Transient` 原先笼统重试 Metadata/Allocation 的 ResourceUnavailable；这些错误既表示名额被占用，也表示请求本身超过限制。后者在每次重试中没有任何条件改变，可能长期占用结果/输入预算、阻挡同对象后续请求。此问题由代码路径确认后才扩展测试，没有先写回归猜测原因。

修正：

- ObjectIO 先按实际成员数和缓冲长度＋真实内存对齐余量验证能否装入空 F02。使用 NodeStorage 原有配置的私有传递，不新增公开 getter/测试配置；F02 的公开 API 不变。只有容量被其他任务占用时，私有 ObjectIOBusy 才被自动重试。
- F12 预留票据占满单独报告 Busy；请求/搜索/碎片上限保持 ResourceUnavailable 并明确结束。新枚举值追加，旧值和持久格式不变；生产 S7 消费该区别，删除测试后仍然需要。
- 保留范围 Busy、明确 IO 暂满的 pending 和有界重试。错误不吞掉，不用超时伪装提交结果；不可满足请求返回 NotCommitted 后保留旧状态，准备资源归还。
- 复审触及的 F12 原有三处静态风格提示顺带清理：显式 bool→uint64 转换及拆开局部变量声明，不改变分配算法。

### 测试判断的缺口与修正

原容量用例只填满可归还的 IO 名额，无法发现永久限制的无限重试。新增一项正式入口集成场景，合并五类独立限制；它保护“接受后应明确失败而不永久等待”的项目承诺，不逐 helper/错误码建立单测。原容量场景补上真实预留票据占满后再归还，保证修复没有变成“一遇压力就失败”。

原故障场景只验证恢复整体一致，NotCommitted 却恢复全新状态也可能通过。现在将提交结果与恢复状态绑定，并加入 Journal 完整写入后的 Flush 故障；“不确定误报未提交”的变异由独立旧控制值 oracle 发现。expected 正文仍来自输入数组，不用生产编码或映射反推。

10 项阶段场景＋46 项原样回归、12 个指定变异；本轮所有失败判据都要求正常 GTest 失败，编译异常/超时杀进程/sanitizer 崩溃不算命中。原 Full 暂满检查含短 WaitFor 窗口，结果只证明本轮交错，不宣称穷举所有调度；最终恢复/独立正文及实际故障门仍为主要 oracle。新增限额场景的 5s 只作防挂起，不是性能指标。

没有新增 production 测试 hook、默认路径、状态 getter、每请求线程或常驻测试目标。仍由 F19 单一档案维护风险，F12/F02 原档案不修改；未来业务 E2E 按原接管规则覆盖，当前不冒充 E2E。最终归档逐成员、生产/测试/依赖哈希核验后删除复审构建和旧包，结果入口见 [本地记录](../../test-results/storage-s7-common-20261002/README.md)。

# S9.1c / F26：真实页生命周期与对象 IO

> 当前 F26 源码/结果由 [S9.1e](s9e_execution_20261005.md) 接续；下文是当轮历史，旧结果包已删除。恢复使用 [e 入口](../../test-results/storage-f26-s9e-20261005/README.md)。

日期：2026-10-04。依据 [冻结协议](s9_buffer_pool_protocol.md)、[F26](modules/page-storage-adapter.md)；用户已授权实现及测试。本轮先审生产逻辑，再写/运行测试；编译失败、启动失败、超时和未发现变异均不计为通过。

## 1. 实际接线与旧路径清理

```text
SQL / 表页 / B+Tree / 恢复
  → BufferPoolManager
    → TranslationDirectory → FrameHeader（局部同步、pin、正文权、脏版本）
                           → FrameArena（稳定正文地址）
    → PageStorage
      ├─ 文件部署：FilePageStorage → 现有 DiskManager
      └─ 对象部署：ObjectPageStorage → NodeStorage
          ├─ ReadObjectInto → ObjectIO → F14/F04/F02/F01 → 帧
          └─ 带稳定源许可的 Common → 数据 Durable → B 发布 → 页写回完成
```

- 删除 A 的 `page_table_`、淘汰反向遍历、每帧 `vector<char>`、BufferPool 内的 DiskScheduler 和无人使用的 Pin/Unpin/DoDiskIO 公共包装。DiskScheduler 自身仍有独立课程/其他消费者，不为清理 BufferPool 删除它。
- B 继续独立使用私有元数据版本、页后端和 WAL；不会把 B 的页导入普通对象目录。
- owner 锁只保护生命周期、空闲帧和在途额度。条目→帧局部锁核验并 pin，释放条目后才等待内容；Guard Drop 也使用帧局部锁。LRUK 仍有自身短锁。
- LRUK 增加有实际生产消费者的 `Candidate()`：先获取建议候选，不删除历史；在帧锁下确认仍无 pin 后才 `Remove()`。并发命中导致候选失效时保留原历史，排名算法沿用现有 LRU-K。
- 同页 Loading 共享任务结果；加载失败的等待者获得同一失败，不被后继成功覆盖。任务成功后等待者重新定位/固定，不能直接信任任务遗留的 frame_id。
- 写回保持稳定正文到整个 Common 完成。失败保留脏正文；成功才更新 clean version。仅设备数据 IO 结束不能清脏或报告成功。
- FlushAll 采集页身份/帧代次，分有界批次；同一对象每批最多一项，不写零覆盖两页之间的未修改页。批次结果消费后即释放，不能把所有 ticket 累积到最后。

## 2. 工作区与部署规则

对象部署必须显式给出 K（pages_per_object）、目录/帧/在途页预算，以及 NodeStorage 外部缓冲容量。没有许可/容量不偷偷退回自有缓冲。文件部署继续保留现有构造入口及文件语义，并共用数组 BufferPool。

工作区 v1 在 B 的对象控制记录中保存：格式号、数据库页大小、K、已创建对象数、归属登记目录。对象号 `1 + page_id / K`、对象内偏移 `(page_id % K) × page_size`；对象 0 的控制项 0 保存描述。对象长度按 K 页预留，未写范围由现有空洞规则读零。工作区 Create/Open 分开；Open 校验已存在描述，不自动迁移/格式化。

C++ 对象节点使用已预留的 Raft 控制 owner 5 登记 A 工作区，正文在独立新数据空间；0–4 沿用 S8。创建描述和登记一同提交；逐对象创建与描述计数一同提交。启动恢复 B 后，先清理上个进程登记的废弃工作区，再创建新的 A 工作区。快照导入也创建新空间；成功发布后退出旧空间，失败候选同样退出。清理中断保留登记，重启继续。清理按现有 PlanTailTrim 有界缩短后删除，不引入第二套 GC。

PageStorage 目前是同步外观，下方复用有界异步执行；独立调用可以重叠。现有 Raft/SQL 调用线程仍可能等待页任务，全面解耦仍归 F35，不能把锁外 IO 说成 Raft 已经不阻塞。

业务恢复仍从规范化 Snapshot + Raft 日志重建。工作页和目录持久化不提供 Catalog/会话的一致恢复边界，不能擅自用它替代 S11。快照构建仍保留合法的规范化文件消费者。

Close 停止新请求并排空已接纳调用；已有 Guard 继续保有旧 Arena 内存，但不能通过旧 BufferPool 发新 IO。生产节点的上层生命周期先退出业务使用，再关闭/清理工作区。异步 IO 使用逻辑内容许可，不跨线程释放 `std::shared_mutex`。

## 3. 实现前后代码逻辑审查

先审阅协议及调用关系，再写本轮测试。重点检查：条目与帧锁顺序、加载唯一发布、淘汰候选重新核验、父提交与子 IO 的完成边界、持有结果的预算释放、工作区原子登记与清理、A/B 隔离。

实现中发现并修正：

1. 首版接线把驻留和 Guard 退出放在 owner 短锁下，未符合冻结的局部同步要求；改为帧自身短锁/条件通知。
2. 直接调用 LRUK Evict 会在候选核验前删除历史；增加 Candidate 并延后 Remove，保留现有排名策略。
3. 普通文件原有页偏移/文件长度使用 32 位整数，不能支持合法高位页号；改为宽整数计算。真实文件读写失败以前只打印并返回，现在异常传到页调用者；独立 DiskScheduler 捕获异常并交付 promise，不让工作线程 terminate 或遗失完成通知。文件 flush 仍不冒充设备持久化。
4. Common 原只区分当前剩余额度，没有拒绝单次输入本身超过 pending 总预算的情况；补永久容量判断，不无限轮询 Full。
5. 新页 API 明确借用内存，F02 子许可与 Common 父保护各自持续至对应完成边界。结果不保留已归还的借用源。

6. 真三节点复验暴露新工作区 CreateSpace 与后台 GC 的 B 视图冲突；临时诊断副本定位到 Snapshot Validate → OpenWorkingState → CreateSpace → B.Commit，未把诊断代码留在 production。B 增加 `MetadataViewConflict`，沿用原 Conflict 分类；Create 刷新视图后重新计算身份，Common 重建分配/映射变更并再次核验对象内容版本。仅此前置拒绝重试；业务版本冲突和不确定提交仍失败。正式原 TCP 场景连续复验，不用放宽超时替代修复。
7. 删除未被消费的 PageIOCapabilities 分配单位字段，不预留无生产用途的 getter。保留原 GetPinCount/构造参数是既有契约，不是新测试扩大接口。FrameArena 的可选 MADV_* 分支做编译期可用性处理，普通文件消费者不强依赖 Linux 对象 TU；本轮未在非 Linux 运行。

## 4. 测试全景与逐项判断依据

阶段验证沿真实 production 路径；设备为显式临时 Direct 文件，稀疏高页号另用正式文件后端。IO 包装只存在测试链接边界，不进生产代码。输入均非空、各页正文不同，预期由输入模型生成。

| 测试 | 目标 / 输入 / 路径 | Oracle / 失败含义 |
| --- | --- | --- |
| SparseRealFilePagesAndOpenedInstancesStayIndependent | 真实文件，19、65536+19、(1<<23)+19、(1<<23)+65536+19（suffix 全相同，prefix 不同），3 帧导致淘汰；重建实例后同号页改写 | 独立正文模型与旧 Guard 原文。发现 prefix/suffix 混淆、驱逐丢页、跨实例串帧；替代 b 中手工连接目录与 Arena 的 T2 |
| DirectFramesBatchDurabilityAndUnchangedMiddlePage | BufferPool→Common/ReadInto→Direct；12 个不同页，额度仅 2 页/1 个 Common 请求；同对象改两侧页 | 重开后逐页正文与原模型一致，中间页保留；边界观察地址等于真实 Guard 地址，保护默认直传承诺；再写新页验证 ticket 未占满后续额度 |
| SharedLoadIndependentProgressAndCloseDrain | 暂停实际页读，另一页先完成，同页共享加载；独立一轮暂停已接纳 IO 后 Close，用普通缓存页请求确认已停止接纳 | 测试侧记录真实 pread 退出及 Close 返回，要求前者先发生；另检查单映射物理读次数、准确正文和关闭后拒绝。防全局 IO 阻塞、重复加载、提前关闭。此事件顺序补强见 §9 |
| PublicationBoundaryKeepsSourceStableAndOtherPagesUsable | 暂停数据持久化后的 Journal 写；同页写者与另一页访问并行 | 暂停时旧对象正文；提交完成后新正文；随后修改仍脏，重开得到后续已刷版本。防数据 IO 完成冒充发布、帧过早开放修改 |
| FailedWritePreservesDirtyBodyAndAcknowledgedOldVersion | 非空旧页 durable 后修改，实际 Data pwrite 注入 EIO；另一次真实 Journal fdatasync 成功后丢失确认；另设永久不足的外部容量 | Data 写失败：Flush 抛错、保留新脏正文、恢复精确旧正文；Journal 已同步但确认丢失：仍抛错/保留脏态，恢复精确新正文。两个 oracle 分别根据注入点的真实持久结果，不能“旧/新都算对”；永久容量不足立即拒绝 |

还复用 b 的四项独立基础风险：帧几何/大页建议、并发首次创建唯一发布、按需初始化与失败预算归还、条目并发修改。删除旧人工 T2，不把同一风险加一套影子数据库流程。复用原课程 BufferPool/PageGuard/LRUK/DiskScheduler 和 S8 的八项内容；S8 仅替换配置及 FSM 后端，SQL/恢复/TCP oracle 不变。

## 5. Production pollution 与接口审查

- PageStorage：现有文件部署及对象节点都是生产消费者，不是测试 wrapper。
- ObjectReadTarget / ObjectWriteSource：分别表达写入帧和稳定读取帧的许可，接续既有 ObjectIO/Common，避免另起一条提交路径。
- NodeStorage PageIO：生产 FrameArena 获取实际对齐和页预算；不暴露缓存内部状态。
- ObjectPageStorage Create/Open/Space/Retire：持久工作区的身份、恢复、退出；Space 是显式 Create→Open 的身份，不是观测实现状态的测试 getter。
- LRUK Candidate：生产淘汰核验所需；并未为了测试改变排名算法或暴露容器。
- MetadataViewConflict：生产 Create/Common 区分可刷新基准与语义冲突；原 Conflict 捕获保持兼容，不更改 WAL 格式，不作为任意失败重试的兜底。
- 显式 PageCacheDeployment/外部容量：对象节点的真实配置；原文件部署入口保留。没有给缺参数测试补默认值。
- Close：正式生命周期排空；没有测试专用 hook、默认路径变更、ifdef TEST 或控制流分支。
- 原课程测试只补显式标准头文件，避免依赖被删除的生产间接 include；不改变它的输入、oracle 或计数。

## 6. 重复、杀伤力和稳定性

核心正文 oracle 来自输入，不读取生产元数据再推导“正确正文”。地址/次数辅助观察只保护明确的直传/合并承诺，不把每一层调用顺序逐个固化。局部测试不替代共同 C1–C5/P1–P4。

定向变异：忽略 root/middle 位、对象页偏移置零、强制回退自有中转、失败后清脏，以及 §9 新增的删除 Close 排空等待；要求成功编译后由场景断言失败，超时/崩溃/编译失败不算通过变异验证。基础风险复用原变异而不重新造一套。

首次 middle-alias 变异未被检出：输入跨 middle 却用了不同 suffix，无法区分目录中间层串用。修正为四个同 suffix、不同 root/middle 的非零真实页，root/middle 两种变异随后都由正文断言检出。未把此前未发现的结果计作覆盖，也未另留一套从零起点的重复样例。

场景用事件控制 IO，超时用于失败退出或辅助负向观察；测试清理先放开 IO 再等待线程，避免断言失败后析构死锁。没有随机输入、测试顺序依赖、生产 sleep hook。关闭未返回的负向等待只作辅助观察；§9 增加明确的 pread-exit → Close-return 事件顺序，补齐原先只检查放行后两个结果的不足。设备对齐不满足本 fixture 的显式直传条件时报错，不伪装跳过成功。

### 测试层级与覆盖边界

五项新页测试都是 integration（包含真实文件或 FS/B/Direct）；课程属于已有 unit/integration regression；S8 的实际 SQL 和三节点 TCP 场景承担本轮业务接线的 E2E regression。保留的四项 RAM 测试属于实际组件 integration，不包含手写数据库。并发暂停和系统调用故障由测试链接实现，不绕开对象提交/页路径。

普通数据 IO 失败和“数据已 durable / B 尚未发布”验证不同风险，不能合并掉判断依据。未为每个 getter、每种状态编码或第三方 mmap 自身规则新增测试。基础 RAM 预算/互斥风险无法被当前业务 E2E 完整触发，因此仍只在阶段压缩包保留；d/e 后继续检查替代关系。

## 7. 测试矩阵与结论

| Test | invariant | Oracle | 新 failure mode | 重复 | Production pollution | 建议 |
| --- | --- | --- | --- | --- | --- | --- |
| SparseRealFile | 页/实例隔离 | 原始正文 | 目录高位/复用身份错误 | 替代 b T2 | 无 | 保留 |
| DirectFrames | 持久正文及默认帧 IO | 重开正文、地址边界 | 串页、额外中转、ticket 占用 | 不复制 F02 API 测试 | 无 | 保留 |
| SharedLoad | 合并、独立推进、排空 | IO 事件与次数 | 重复加载/提前关闭 | 复用一个组合场景 | 无 | 保留 |
| Publication | 父提交前不可清脏 | 旧/新独立正文 | 提前发布/修改稳定源 | 不重复 FS WAL 编码 | 无 | 保留 |
| FailedWrite | 失败不撤销旧承诺 | 脏状态与重开旧文 | 报假成功/无限 Full | 区别于正常写回 | 无 | 保留 |
| 基础四项 | 几何、并发、预算 | 原独立模型/资源边界 | RAM 组件风险 | 已删除人工 T2 | 无 | 保留 |
| 课程/S8 复用 | 已有页/SQL/网络承诺 | 原 oracle | 生产接线兼容风险 | 不复制测试内容 | 仅显式 include / 配置适配 | 保留 |

“保留”表示放在 F26 模块压缩包及引用已有包；新阶段测试不常驻。尚需后续 d/e 验证 PathCache/归还/批量预取；共同性能测试在约定最终对比阶段运行；裸设备和真实掉电不在 Direct 文件结果的证明范围。

## 8. 首轮验证结果与归档

结果入口：[F26/c 结果与恢复说明](../../test-results/storage-f26-s9c-20261004/README.md)。最终计数以该目录的 XML/命令和 MANIFEST 为准。

- ASan/UBSan/LSan：4 项 RAM 基础、5 项真实页、11 项课程回归、8 项 S8，共 28 个不同场景。
- 8 个基础＋5 个真实页定向变异均由指定断言发现；不把重复运行计为新场景。
- 三节点场景原超时/内容保留。初期有 TIMEOUT/UNAVAILABLE；隔离编译负载仍复现，最终追踪并修复真实 B 视图冲突。不能将早先失败都归因于机器慢。修复后完整 S8 通过，原 TCP 连续三轮通过；诊断副本不计正式通过证据。
- ThreadSanitizer：四项基础与五项真实页全部通过，不重复计为新场景。最后仅补回四个重写文件的原项目版权注释，不改变可执行逻辑；ASan 最终完整复验在最终树运行。WSL2 上曾有 sanitizer 启动退出 -11、零输出； scoped setarch 仅作用于测试进程，不放宽断言、不改变系统配置。
- 共同 C/P 未修改、未重跑；Direct 文件不等于裸设备或掉电试验。当前对象页接入增加工作区管理和实际 IO 成本，不能只凭正确性通过声称性能提升。

源码只保留 `test/archives/F26-array-buffer-pool.tar.gz` 当前一份。旧 b 的 F26 结果包删除，不保留备份；b 的日期化审查文字保留并指向 c。其余模块有效归档为依赖，不删除。结果包仅保留日志/XML、最终源码/方案快照与校验，不保留构建、镜像、可执行文件或临时诊断代码。

## 9. 再次审查：关闭判据、清理和方案状态（2026-10-04）

先复核 `Fetch/Flush/Close`、PageGuard、对象页工作区和 Common 借帧的生产逻辑，再修改测试。与首轮归档的 33 个生产/构建文件逐一核验，本次没有新增生产修改。局部锁顺序、同页任务、父提交前正文保留、写失败不清脏、A/B 隔离及真实对象路径仍与 c 方案相符；此结论不代表 d/e 已实现。

### 9.1 本次实际修改

| 文件/场景 | 原问题 | 修改及判断依据 |
| --- | --- | --- |
| 源码包 `tests/page_io_test.cpp` / SharedLoadIndependentProgressAndCloseDrain | 原关闭轮只有 30 ms 未返回和放行后两个任务完成；没有落实协议 §11.2 的设备退出/关闭返回顺序，也不能证明关闭线程已经停止接纳 | 先预热另一页；实际读暂停后启动 Close，以普通缓存页请求被拒绝确认停止接纳。测试链接边界记录真实 pread 返回，调用侧记录 Close 返回，用同一测试侧事件序号检查先后。30 ms 仅保留为辅助检查 |
| 同一文件 / SparseRealFilePagesAndOpenedInstancesStayIndependent 的 Cleanup | 用 `data_path + ".log"` 删除了错误名字；DiskManager 使用替换扩展名，原做法依赖最后删除整个临时目录兜底 | 改为删除 `sparse-pages.log`；不改生产默认路径，不新增专门清理测试 |
| 源码包 `run_pages.py` | 课程回归只要求测试数大于零，可能把少跑场景算作完整回归 | 分别要求 7/2/1/1 项，复用现有 XML 检查，拒绝 skipped/not-run/failed；已保留的课程 XML 满足这些数量。本次未重新运行未变更的课程内容 |
| 主/子方案 | 部分正文仍将哈希页表、逐帧 vector、未配置外部额度描述为当前代码；与页尾 c 已完成冲突 | 同步共同协议及 F26 主/子文档；明确区分改造前事实、c 当前实现与 d/e 后续工作，不删除日期明确的历史证据 |

### 9.2 测试设计再审查

1. **全景及目标：** 仍是原 5 项真实页 integration 场景，没有另建同义用例；关闭 invariant 是已接纳的实际 IO 退出早于 Close 返回。
2. **输入、路径及 Oracle：** 不同非空页 5/9，经正式 BufferPool→ObjectIO→Direct；页 5 预热后用于观测接纳门已关闭，页 9 在设备边界暂停。预期先后来自关闭合同，事件由测试两端采集，不读取生产 `calls_`/状态计数来证明自己正确。
3. **Production pollution：** 没有新增 public API、默认参数、getter、test hook 或生产分支。修改限于归档测试/runner、文档与归档清单。
4. **接口污染：** 观测关门复用正常 ReadPage；不会为测试增加 IsClosing/WaitUntilClosed 等接口。
5. **重复：** 合并加载、独立页进展和关闭仍在既有风险场景中；未增加一套模拟缓存。关闭事件是补强原 Oracle，测试数量不增加；完整 C/P 接管仍按既有矩阵。
6. **杀伤力：** 删除 `BufferPoolManager::Close` 的 `calls_ == 0` 等待；指定场景实际失败，事件显示 Close 返回序号 1、设备退出序号 2。此失败不是超时或 sanitizer 崩溃，其余 5 个页路径变异继续被断言发现。
7. **稳定性：** 事件使用测试侧互斥锁；暂停先确认进入，关闭先确认停止接纳。轮询只有 10 s 失败上限，先后判据不依赖速度阈值；失败清理先解除 IO 暂停再 join。页 5 预热发生在事件清零之前，不把其物理读混入关闭轮。
8. **矩阵结论：** §7 原 5 项均保留；SharedLoad 的建议为“保留（已补强）”，稀疏文件场景只修正清理。未发现需要新增的测试抽象或重复常驻用例。资源预算四项继续在唯一阶段包中保存，不能从本次结果推断 d/e、物理掉电或性能通过。

本次重新构建并运行修改后的 5 项页场景（ASan/UBSan/LSan），6 个页路径变异均由目标断言发现；另建 ThreadSanitizer 版本，5 项全部通过。未改动的基础、课程、S8 采用 §8 已保存证据，不能说本次又运行了 28 项；当前组合证据的场景数仍为 28，变异为基础 8＋页路径 6，共 14。最终归档校验见结果目录。

## 10. 提交前再审查（2026-10-04）

用户本轮授权：再次核对方案/测试/旧归档，确认后提交；下一阶段须另获认可。本轮没有更改生产代码或测试内容，也未重复运行已通过的场景。

- 复核 BufferPool 的固定/加载/写回/关闭、PageGuard 的正文使用权、ObjectPageStorage 的显式创建/打开/退役，以及节点快照换库和 Common 发布边界；没有发现新的已确认缺陷。同步修正 F26 prompt 的 c 待执行标题及主方案 §13.1 的过时接线状态。
- 当前 33 项生产/构建文件及测试/依赖哈希与归档一致；重新检查页测试 ASan/UBSan/LSan、TSan 的 XML 均为 5 项实际运行且零失败/跳过，6 个页变异均由目标断言失败。完整组合证据仍为 28 个不同场景、14 个变异，含未改动场景的历史记录，不能称为本轮全量重跑。
- 没有新增 production 测试 hook、getter、默认参数或分支；旧人工身份场景已由真实页路径替代，组件风险只在阶段压缩包保存。共同 C/P 的输入和统计口径不变，未把本地 Direct/模拟错误当作裸设备、真实掉电或性能验收。
- 当前只有一份 F26 源码包和一份 c 结果包；旧 b 结果包不存在，19 份其他模块的有效源码包与 HEAD 一致。阶段测试未展开常驻，前次临时构建已清理；结果包继续由现有 `.gitignore` 排除，Git 保存源码包及结果摘要。
- 下一步仅介绍 S9.1d：PathCache、候选摘要与翻译数组 RAM 的并发安全归还。d/e、F36 和 S10–S13 未启动，不因本次提交将整个 F26 或总方案标为完成。

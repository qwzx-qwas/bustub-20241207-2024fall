# Raft 方案执行交接

更新时间：2026-08-30（Asia/Shanghai）

## 当前状态与停止边界

- `raft_implementation_plan.md` 的 M0–M7 已全部执行完成，已提交/推送基线为
  `ec11bb0f9f15d1e5abaedb64ea44dee5c6606e66`；当前没有半执行的功能阶段。
- 2026-08-30 的方案结构复审只修正里程碑归属、文档/实现一致性和 CI 门禁，不是 M8。
  M8 在方案中仍未分配；先冻结唯一功能、协议版本、排除范围和退出门禁，才能实施。
- 若后续会话恢复，先阅读本文件、`raft_implementation_plan.md` 与
  `docs/testing/raft_test_matrix.md`，并核对 `git status`；基线验收数字不得自动为后续源码改动背书。

## 基线后方案复审维护（2026-08-30）

- 方案已改为 A–D 架构工作流 + 横切规则，唯一执行轴是 M0–M7；权威表给出每个里程碑的唯一交付、主测试层和排除范围。
- M0–M2 `CommandLog/SnapshotManager/StateManifest` 是 term-0 恢复验证模式；M3+ `LogStore/SnapshotStore`
  是 distributed authority；StableStore 的磁盘格式有意跨 M2/M3 复用。V1 没有 mode marker/迁移器，
  因此“分布式模式使用全新目录”是受支持部署前提，现有检查不能冒充完整的原地升级检测。
- 删除了 SnapshotManager 中只写不读、与 Manifest 三个 checksum 重复的 `CHECKSUMS`；
  命名掉电 topology 已同步更新。GitHub Actions push/PR filter 已从 `master` 改为默认分支 `main`，
  checkout 已统一 v4。
- `StateVisibilityLatch` 已从 distributed 下沉到 common/M1，M5 只复用它组装批量发布；M2 runtime 的
  `CommitSql` 实现移到 M5 translation unit，recovery target 不再编译依赖 `SqlCommandPreparer`。
- 新增 production/test 共用的 `RecoverRaftPersistentState` 和只读 `LogStore::ProbeRecovery`，修复
  latest snapshot 已覆盖/衔接 durable commit、但 previous bridge 不匹配时启动错误 fail-stop 的缺口。
  恢复先验证完整 FSM、`max(H,S)`、boundary term 和 committed suffix，再决定 rebuild/promote/fail-closed；
  恢复器自己的 HardState/journal/prune 10 个命名事件均完成 PowerLoss 与两次冷启 oracle。
- startup helper 落地后的中间检查点曾以 Clang 14 Debug + ASan（`detect_leaks=0`）通过 8 个二进制/60 个测试：SnapshotManager 4、
  term-0 runtime 1、SQL adapter 3、BusTub FSM 4、Raft BusTub FSM 4、LogStore 10、RaftNode 25、
  DistributedNode 9。受限沙箱内的 loopback 分配拒绝未进入测试逻辑；相同 DistributedNode 命令获准在
  沙箱外运行后 9/9 通过。该 8/60 发生在后续 live InstallSnapshot、application-neutral proposal、测试分层和
  term-0 nested Session 修正之前，只是历史中间证据。
- 当前工作树的最终定向 Clang 14 Debug + ASan 回归通过 17 个二进制/108 个测试；其中 StateManifest 9/9、
  RaftNode 31/31、DistributedNode 9/9。term-0 publisher/recovery 现在同时拒绝外层和内嵌 Session response
  的非零 term；M4 KV inner snapshot 与 M5 BusTub bundle 测试归属已拆开。未重跑全量 M7，不得冒充新的
  122/122、40/40、正式 E2E 或 TSan 证据。
- 本轮全部修改/新增 C/C++ 通过 Clang 14 format dry-run 与仓库参数 cpplint；3 个 shell 通过 `bash -n`，
  CI YAML、`git diff --check` 和 production 反向依赖扫描通过。清理审计修复并复跑了会漏删 DiskManager
  `.log` 的 `table_heap_reopen_test`（1/1）。早期 431,479,634-byte 审计构建树已删除；本轮再精确删除
  1,039,771,113-byte 外部 ASan 构建树和旧 0-byte reopen 日志。复扫后 `/tmp/bustub-*`、后台进程、
  源码树生成/ignored 文件均为空；只剩 44 个已跟踪修改、2 个正式删除和 4 个正式新增源码文件。

## 最终实现范围

- 完成 canonical snapshot、Catalog/表堆重开、Manifest/两代恢复点、bridge log、掉电安全 durable storage、
  segmented CommandLog，以及恢复时的 fail-stop 校验。
- 完成独立 KV Raft 的持久 HardState/LogStore/SnapshotStore、选举、复制、冲突覆盖、当前 term NOOP、
  ReadIndex、新旧快照规则、TCP transport 和重启恢复。选举 deadline 使用生产随机区间且每次 reset 重新
  抽样；确定性测试注入固定 seed/timeout source，不依赖各节点手工设置不同常量。
- 完成 BusTub replicated CommandBatch、固定 wire codec、显式 DDL OID、主键准入、secondary UNIQUE 拒绝、
  canonical mutation 排序、SessionTable 去重、原子发布水位，以及正式 `bustub-node`/`bustub-client`。
- 完成 M6/M7 三进程外部 harness、E2E-01 至 E2E-15 映射、运维/协议/测试文档和分离的 CI 门禁；四条
  聚焦时间线与一条历史文件名为 `raft_m0_m7_chain.sh` 的 M3–M7 distributed 累计链路复用一个测试专用
  process harness，不再复制启动、Leader 定位、客户端交付重试和停止逻辑。term-0 物理恢复由独立门禁验证，
  该累计链路不执行目录格式迁移。
- `magic/version/length/CRC` 与既有 `magic/body/CRC` 验证已抽成公共 framing helper，各协议保留独立类型与
  原有字节布局。正式快照已改为文件切片和有界分块，不再把数据库 bundle 整体装入 128 MiB vector。
- 在验收中补齐单机兼容缺口：二级索引更新和历史键可见性、B+Tree 非唯一全扫描去重、窗口函数、
  `lower`/`upper`、只读 `\d`、多表连接优化 fixed point、Trie/ORSet/Watermark/类型对齐等回归修复。

## 历史完整验收证据（旧修订）

以下结果对应各自记录时的修订，不能追溯性覆盖后来新增的 oracle；M0–M7 基线权威结果见文末
“最终修复收口交接（2026-08-30，M0–M7 基线权威）”。

- Clang 14 Debug + ASan/UBSan：发现 61 个 GTest 二进制，60 个有效测试全部通过；仅
  `trie_debug_test` 因课程仓库故意缺少 Gradescope 隐藏答案而跳过。
- lint 修正后的最终 M0–M7 目标集：23/23 通过。任何重试都由日志证明发生在 `Running main` 前；没有
  测试体失败、sanitizer 报告或超时。
- Release SQLLogicTest：40/40 `.slt` 在一个排序连续批次中通过，包含全部排行榜 timing pass。
- M6 正式三进程：通过写入、ReadIndex、Leader 故障、同请求 byte-stable 重试、新 Leader 写入，以及旧节点
  原目录重启追赶到 `commit_index/published_applied_index = 7`。
- M7 正式三进程：通过 snapshot base 2、canonical capture 窗口 SIGKILL、suffix 提交到 index 4 与离线恢复。
- TSan：直接启动先遇到宿主 `unexpected memory mapping`；仅对子进程使用 `setarch x86_64 -R` 后，
  `raft_node_test` 7/7、`bustub_state_machine_test` 4/4 通过，`halt_on_error=1` 下无 data race。
- 静态检查：134 个方案相关 C/C++ 文件通过 clang-format 14；82 个新增 C/C++ 文件通过仓库 cpplint；
  shell、CI YAML、CRLF-aware `git diff --check` 通过；167 条 production 编译命令中的测试反向依赖为 0。
- CMake 的 format/lint 入口已改为通过 `Python3_EXECUTABLE` 调用普通文件模式的脚本，不再 Permission denied。
  全部旧源码树扫描仍会指出 17 个本方案之外的课程基线文件未格式化，本阶段没有制造无关机械 diff。
- 后续选举超时补强的针对性 ASan/UBSan 验证：`raft_node_test` 8/8、`node_config_test` 1/1、
  `raft_bustub_cluster_test` 1/1，以及使用生产随机 source 的 TCP 隔离 Leader/重选用例 1/1。该次没有重跑
  全量 M0–M7 验收，因此上面的历史全量结果没有被追溯性改写。

## 持久化边界与故障模型补强（2026-08-29）

- 已明确选择 V1 同步持久化 API：`CommandLog::{Append,TruncateSuffix}`、`StableStore::Update`、
  `LogStore::{Append,ReplaceSuffix,InstallSnapshotBase}` 成功返回即表示 durability barrier 完成，失败直接抛出。
  已删除三个 ready `DurableFuture` 适配层及所有对应的立即 `.get()`；没有引入异步 completion 或存储调度器。
- `test/recovery/power_loss_storage.h` 现在是共享的命名故障注入框架，事件固定为 `before_write`、
  `after_fsync`、`after_rename`、`after_dir_fsync`，并按事件类型记录 occurrence 与路径历史。
- Snapshot 发布、StableStore、CommandLog 和 LogStore 原子替换共享 old-or-new oracle；
  InstallSnapshot 只共享命名事件框架，它使用 `max(H,S)`、pre-install term/suffix 和 committed-range
  连续性的跨文件 oracle。LogStore 新建 journal 时补充父目录同步，以保证新文件名本身越过 durability barrier。
- Clang 14 ASan/UBSan 针对性验证通过：`stable_store_test` 3/3、`log_store_test` 6/6、
  `command_log_test` 6/6、`snapshot_manager_test` 3/3、`single_node_runtime_test` 1/1、`raft_node_test` 8/8。
  `log_store_test` 和 `single_node_runtime_test` 首次均在 `Running main` 前空输出退出 139，按既有门禁各重试
  一次后通过；没有测试体失败或 sanitizer 报告被重试。
- 本轮只回补已完成阶段的 durability safety，不进入新阶段。恢复执行时先查看本节末尾的测试与清理结果；
  若均完成，无自动下一步，继续等待用户命令。

## Harness、framing 与流式快照补强（2026-08-29）

- `test/e2e/raft_process_harness.sh` 统一持有三节点启动、同一随机选举区间、ready Leader 搜索与重定位、
  已识别请求的有限交付重试、状态等待、信号停止和 PID trap；每次 node launch 只有一次进程尝试。
- M7 会先等上一快照代正式发布，再监视由 1600 行有效业务数据触发的新 `capture-*`，避免错误命中旧
  capture。当前运行确实在 capture 窗口 SIGKILL 当前 Leader，并逐字面恢复完整快照、bridge suffix 与触发
  capture 的后续 Apply 效果。
- `common/byte_codec` 新增公共 versioned/checksummed frame 骨架并迁移 CommandBatch、client、Raft RPC、
  Catalog、Manifest、HardState、Session 和 bundle；codec 回归证明截断、长度、版本、magic、payload 与 CRC
  边界仍被拒绝，持久/线格式不变。
- `DurableStorage` 新增 append、range read 和 file size；`SnapshotStore` 的发布、校验、恢复、读取和 follower
  staging 使用文件块，Raft 以 64 KiB RPC 块发送，BusTub FSM 从文件 slice 构建候选数据库。Catalog/Session
  仍按各自小型上限解码；完整 database/bundle 不进入正式路径的内存 vector。
- 针对性 Clang 14 ASan/UBSan 验证通过 16 个二进制共 55 个测试，包括公共 frame/全部迁移 codec、三类
  durable store、SnapshotStore、流式 BusTub FSM、RaftNode、6 个 loopback distributed 场景、cluster 与单机
  恢复。4 次空输出 exit 139 均发生在 `Running main` 前并按门禁重试；没有测试体或 sanitizer 失败被重试。
  M6 与修正后的 M7 正式三进程场景均通过；相关 C++ 通过 clang-format 14 dry-run/cpplint，shell 通过 `bash -n`。
- 不替换自研 Raft：当前目标仍是学习和验证 BusTub 状态机。NuRaft 等成熟库评估仅在目标转为生产时另立
  迁移工程；base backup + 连续日志同样是大规模数据库的未来协议演进，不属于本轮局部修改。

## 实验项目定位冻结（2026-08-29）

- 项目定义固定为“教学版 BusTub + 简单静态三节点 Raft”，正式路径只与 test double/harness 相对，不再用
  production 字样暗示大型数据库容量或运维成熟度。
- 保留暂停写入的完整 canonical snapshot、`lastIncludedIndex/Term` 和 64 KiB InstallSnapshot 分块，因为它们
  能直接验证 offset、重复/半传输、校验后发布、进程崩溃和 suffix 追赶；这就是本项目的复杂度上限。
- 正式文件 payload 上限由 1 TiB 收敛为 1 GiB，内存兼容 codec 保持 128 MiB。限制用于尽早拒绝异常输入，
  不代表支持相应规模；新增无分配越界测试并由编译期断言保持 SnapshotStore/BusTub bundle 上限一致。
- 明确不做 base backup + WAL、增量/fuzzy/COW、跨进程续传、压缩、限速、多流、pipeline 或低停顿 SLO。
  未来只有项目定义变化后才能另立阶段，不能以“继续完善流式快照”为由逐步扩张当前实现。
- 历史 Clang 14 ASan/UBSan 针对性验证通过 `snapshot_store_test` 5/5、`raft_state_machine_test` 2/2、
  `raft_node_test` 8/8、`distributed_node_test` 6/6，共 4 个二进制/21 个测试；M7 正式三进程再次通过，
  commit index 4 的 800 行更新在两名 peer 离线时由被杀 Leader 原目录完整恢复。TCP 测试首次在
  `Running main` 前空输出退出 139；M7 有两个节点子进程各一次在服务启动前空输出退出 139，均只按既有
  有限门禁重试，没有测试体失败或 sanitizer 报告被重试。本轮 4 个 C/C++ 文件通过 clang-format 14
  dry-run 和仓库配置的 cpplint。

## 阶段结束清理记录

以下条目是按执行时间追加的历史盘点；其中“当前”只表示该条记录当时。文末最终盘点覆盖这些旧数量。

- 最初审计到 3348 项变化，其中 3230 项为构建中间文件并已清除；方案文档已加入 M0–M7 强制清理门禁，
  `.gitignore` 已覆盖约定构建树、临时日志与 core dump。
- 最终删除了本轮可再生成的 ASan/UBSan、Release、TSan 外部构建树（约 3.8 GiB）、所有 GTest/SQL 日志、
  M6/M7 成功与 pre-main 失败 artifact、临时数据库，以及仓库根部 32 KiB CMake 缓存。它们未保留副本，
  可按 runbook 重建。
- 已从 HEAD 精确恢复测试误删的 `test.bustub`（8,392,704 字节）和 `test.log`（0 字节）。
- 选举超时补强使用的 `/tmp/bustub-raft-random-timeout-build`（约 1.1 GiB）在针对性验证完成后已清理；
  恢复执行时再次确认该目录不存在，源码树仍为 155 项正式改动。
- 持久化边界补强使用的 `/tmp/bustub-raft-durable-boundary-build`（约 811 MiB）在针对性验证和静态检查
  完成后已精确清理；再次扫描 `/tmp/bustub-*` 为空，仓库内没有新增 CMake/对象/XML/core 中间产物。
  工作树仍为 155 项正式改动（61 个已跟踪修改、94 个正式新增文件），说明本轮未引入交付物之外的文件。
- 清理后仓库为 555 MiB；没有 `build/`、`/tmp/bustub-*`、core、后台 node/client/test 进程或删除状态。
- 当前 `git status` 共 155 项：61 个已跟踪正式修改，94 个正式新增文件；新增项按顶层分为 docs 4、
  方案/交接 2、src 57、test 27、tools 4。它们都是实现、注册测试、稳定 harness 或文档，不是中间产物。
- 本次 harness/framing/流式快照补强使用的 `/tmp/bustub-raft-architecture-build`（约 1.5 GiB）及 8 个
  M6/M7 成功、失败和重跑 artifact 根目录均在核对真实路径与大小后精确删除；`/tmp/bustub-*` 再次为空。
  源码树无新增 CMake/Ninja/对象库、测试 XML、profraw、core 或后台 node/client/test 进程；production 源码对
  test harness、GTest、PowerLossStorage、InMemoryRaftTransport/ManualClock 的反向依赖扫描为 0。
- 当前工作树更新为 157 项正式交付：61 个已跟踪修改、96 个新增文件；新增项为 docs 4、方案/交接 2、
  src 57、test 29、tools 4。相对上次 155 项只增加 `test/common/versioned_frame_test.cpp` 与共享
  `test/e2e/raft_process_harness.sh`，二者都是已注册测试/复用 harness，不是中间产物。仓库占用约 556 MiB。
- 实验快照范围收敛使用的 `/tmp/bustub-raft-experimental-snapshot-build`（956 MiB）与 M7 artifact（1.4 MiB）
  已在验证后精确删除；`/tmp/bustub-*`、源码树 CMake/对象/XML/profraw/core 和后台 node/client/test 进程均为空。
  工作树仍为 157 项正式交付（61 个已跟踪修改、96 个新增文件、0 个删除），仓库占用仍约 556 MiB。
- 已跟踪的嵌套目录 `bustub-20241207-2024fall/` 是原始基线树，不是临时复制，已保留且未改动。
- 测试缺口回补最终精确删除 2.4 GiB 的 ASan/UBSan、TSan 构建树、成功/失败进程现场和组件日志；
  `/tmp/bustub-raft-m7-*`、`/tmp/bustub-raft-gap-*` 与后台 node/client/proxy 进程均为空。源码树没有 ignored
  build/cache/core 或未跟踪运行产物，仍为 556 MiB。最终工作树是 164 项正式交付：61 个已跟踪修改、
  103 个新增文件、0 个删除；新增项为 src 57、test 36、tools 4、docs 4、方案/交接 2，其中新增的严格
  component gate、三个外部故障工具和四条时间线均为可复用正式测试资产。

## 下一步

无自动下一步。M7 已完成，保持当前工作树并等待用户命令；如果未来需要重新验收，所有构建必须继续放在
`/tmp/bustub-raft-build-*` 或其他源码树外目录，并在结束后执行同一清理门禁。

## 测试缺口回补（2026-08-29，历史修订）

- 正式进程证据当时由两条扩为四条共享 harness 的时间线，随后补入第五条连续链路；E2E-01～15 全部经过
  三个 `bustub-node` 进程；
  test-only 代理负责响应丢失、单向消息丢失、Snapshot 帧记录/延迟重放，离线脚本负责最新快照损坏。
- 新增 durable `node.conf`，绑定 node ID、group ID 与 voter set；重启允许地址变化但拒绝身份漂移或损坏。
- RaftNode 新增 durable-before-send、投票与响应乱序、单向丢包、InstallSnapshot 跨子系统崩溃矩阵；同时
  补齐 SessionTable、CommandBatch permutation、Manifest cross-copy 与 SQL/主键准入边界。
- 该修订源码 ASan/UBSan：25 个二进制/81 个测试通过；四条进程时间线通过；TSan：15/15 + 4/4 通过。
  该修订当时的门禁已固化为 `test/e2e/raft_gtest_gate.py`；它只重试 stdout/stderr 均为空的 SIGSEGV/139，
  当时的最终脚本
  运行发生 17 次该宿主启动抖动，且曾正确拒绝沙箱中的 TCP bind 失败，证明不会吞掉测试体错误。
  历史 Release SQLLogicTest 40/40 未重跑，因为本轮不改变单机 SQL 执行语义。
- `.github/workflows/cmake.yml` 已加入严格组件 runner、全部进程矩阵、`session_table_test`/
  `versioned_frame_test` 和独立 nightly 固定 seed 压力任务。本回补止于 M7，清理外部构建和 artifact 后无
  自动下一步。
- 最终清理门禁已完成；本轮 2.4 GiB 外部构建/现场均已删除，源码树保留的 164 项变化全部属于正式交付。

## 最终盲点复查历史记录（2026-08-29；由文末交接取代）

本轮按 BusTub 原测试的字面结果风格，清除了“空夹具、同源 expected、自我 round-trip、只看成功状态或
COUNT”的剩余盲点，并由测试实际暴露和修复了三个实现错误：

- `SessionTable::RecordCommitted` 在验证首请求 gap 前曾用 `operator[]` 插入空 session；现先只读分类，合法时
  才 `insert_or_assign`，测试证明 gap、过旧和损坏恢复不会污染状态。
- `IndexIterator::operator*` 曾返回指向 `KeyAt/ValueAt` 临时返回值的引用，真实 `ORDER BY`/索引扫描可产生损坏
  RID；现返回 owning pair，并由原生 `b_plus_tree_insert_test` 的编译期契约和 3/3 回归固定。
- `DistributedClient` 曾只验证外层 request ID；现对 COMMITTED write 解码内层 `WriteResponseV1` 并要求 ID
  一致，同时拒绝 write/read/status 在相同 ID 下的成功状态串线。loopback fake server 覆盖正常、outer mismatch、
  outer 相同但 inner mismatch 及 request-kind mismatch。

固定格式测试现在使用独立手写 V1 golden，覆盖 Log、CommandBatch、client、Raft RPC、HardState、Manifest、
Catalog、Session 和 BusTub snapshot bundle。后者同时要求 aggregate Encode/Decode、流式 `EncodeFiles` 与带
非零 slice offset 的 `DecodeFile` 精确一致，不再由 production encoder 生成 expected。命名掉电框架统一要求
`before_write / after_fsync / after_rename / after_dir_fsync` 的事件类型、次数、路径、related path 和顺序完全
匹配；一个目录的 fsync 不能发布 sibling 目录项。

五条正式进程证据均为该修订源码的单次场景运行：

- M6：真实响应整帧丢失、Leader KILL、相同请求 byte-identical retry、非幂等 `+7` 只发生一次、精确有序行、
  stale/read 水位和旧节点追赶。当时把一次后续客户端写后的水位称作 NOOP 证据并不精确；基线后复审已把
  该断言移到 replacement Leader ready 后、任何新 proposal 前。
- M7 snapshot crash：1600 行；命中实际 `capture-*` 后 KILL，单节点先恢复；精确核对 id 1/2/1600、secondary
  lookup、Catalog 名称、Session 原响应 bytes/index、无新 log 和无二次 `+7`。
- M7 transfer/replay：记录 3 块、135,485 字节，要求 `last_included_index == S < suffix`；完整旧帧重放得到
  matching stale-complete response，之后继续 Apply。
- M7 recovery matrix：在线旧 Leader 隔离/高 term 降级、全停全启、最新 snapshot 持久截断到 16 字节、独立
  解析并精确选择前一代 index、bridge replay；进程调度只验证所有实际读为完整旧/新集合，确定性 Apply 临界区
  由 `BusTubStateMachineTest.ReaderBlocksUntilDataIndexSessionAndWatermarkPublishTogether` 的 test-owned gate 覆盖，未向 production
  增加暂停 Apply 的测试 RPC。
- distributed 累计链路：从全新集群目录开始，在同一份 M3–M7 durable state 上记录 5 块、266,737 字节、
  Snapshot@12 term 2，覆盖准入无副作用、响应丢失、切主 exact-once、snapshot+suffix、多块追赶、
  stale replay、真实 node.conf 身份拒绝、3/1/2 全停全启和恢复后 BIGINT PK/secondary index/OID 继续分配；
  它累计复验早期逻辑性质，但不迁移 term-0 文件。

该轮 Release 严格门禁：26/26 组件二进制、102 个具体测试、0 failed、0 disabled、每个二进制一个 process
attempt、`process_retries=0`；五条进程场景也没有整场或节点启动重试。`eventually` 仅可在命名的网络/Leader
结果后，以不变的 client/request identity 重发同一逻辑请求；安全断言使用 `strict`。harness 现在核对 node、
proxy 和登记过的后台 helper 退出状态，代理协议/块内容错误会使成功场景在 cleanup 时失败。

这 26/102 是该修订当时的 Release 证据；没有把后续 oracle 修正冒充为新一轮全量 ASan/UBSan、TSan 或
SQLLogic。
旧 sanitizer/TSan/SQL 结果保留在上文作为历史修订证据，CI 已改为构建 `build-raft-component-gates`，并在
ASan/UBSan 与 Release job 中都运行全部五条时间线。

该轮清理门禁完成时：逐项核对并删除 49 个 `/tmp` 构建/日志/artifact 目录和 6 个零字节测试日志，
合计 2,154,049,193 bytes（2.006 GiB），均可由 runbook 重建且未保留副本。复查后工作树为 169 项正式交付
（64 个已跟踪修改、105 个新增、0 删除）；当时源码树无 build/Ninja/对象/XML/profraw/core/pycache 等生成物，
production 源码无测试反向依赖，`/tmp` 的 `bustub*` 候选与后台 node/client/proxy/场景脚本均为空。

停止边界不变：M7 完成后无自动下一步，不进入 M8/V2；等待用户新命令。

## 最终修复收口交接（2026-08-30，M0–M7 基线权威）

### 当前执行位置

M0–M7 已按方案完成，当前没有半执行阶段。恢复执行时已先核对本文件、方案、测试矩阵、工作树和上次现场，
没有跳到 M8/V2。除非源码再次变化或用户明确要求，不需要重做 M7；下一步仍是等待用户命令。

### 本次最终修复

- `DiskManager` WAL 双缓冲从进程共享状态改为实例状态；B+Tree root 发布修复跨页锁顺序；TCP transport
  `Stop()` 关闭队列并丢弃 backlog 后再 join，避免关闭时处理陈旧帧。
- InstallSnapshot 使用 per-peer transfer。heartbeat 保持同一块的 snapshot/request ID、offset 和 bytes；
  Follower 重复块响应携带实际 durable high-water，Leader 只接受匹配活动请求的单调进度。组件测试使用真实
  多块文件安装，覆盖慢 fsync ACK、预置 durable 前缀、高水位跳进、最终 COMPLETE ACK 丢失、从零重启、
  stale-complete、旧 ACK 晚到、独立 KV oracle 和后缀 Apply。
- `raft_message_proxy.py` 主动恢复 `SIGTERM` 终止语义，解决继承 ignored signal 后 harness 无法正常回收的
  问题。最终十条进程场景没有 cleanup 强杀、退出异常、进程或端口残留。

### 基线源码验收

- ASan/UBSan component gate：26/26 binaries、122/122 tests，0 failed/errors/disabled/not-run，26 个 JSON/log
  均非空可解析，`process_retries=0`，sanitizer markers 为 0。
- TSan：4/4 TCP + 17/17 RaftNode + 4/4 BusTub FSM + 9/9 DistributedNode，合计 34/34；无 data race 或
  lock-order-inversion。
- Release SQLLogicTest：40/40，0 failed；`leaderboard-q1-index` 669.72 秒自然结束，无重跑。
- ASan/UBSan 与 Release 各自运行五条正式三进程 E2E，各 24 个 timeline、全部 fresh 且单次通过。两种构建
  都记录 transfer 的 3 块/135,485 bytes/Snapshot@4 term 1，以及同一份 M3–M7 distributed durable state
  累计链路的 5 块/266,737 bytes/Snapshot@12 term 2。
- 全源码树 Clang 14 format dry-run 与仓库 cpplint、全部 6 个 shell 语法、4 个 Python AST、
  `git diff --check`、production→test 反向依赖检查均通过。

WSL 上正常 ASan/TSan 启动曾在 `Running main` 前因 sanitizer address layout 失败，host 日志为
`overflowed sigaltstack`；这不是测试体失败。证据保留到记录完成后，最终验证将整个 component gate/E2E 父
进程树在 fresh 目录中单次置于 `setarch x86_64 -R`，而不是重试个别二进制或节点，因此仍为
`process_retries=0`。此处理只适用于 runbook 中严格识别的 WSL pre-main 情形，不用于 Release、原生 CI 或
sanitizer/assertion/timeout/protocol 失败。

### 最终清理与工作树

已按绝对路径逐项删除 21 个外部构建树、组件日志、成功/失败进程现场和零字节诊断日志，共
2,676,096,997 bytes（2.492 GiB），未保留不可再生副本。复扫没有 `/tmp` 任务前缀、后台
`bustub-node`/`bustub-client`/proxy、18,100–31,899 监听端口或源码树 ignored/generated 文件。

基线提交前 `git status --short --untracked-files=all` 的 208 项正式交付（103 个 tracked 修改、
105 个新增文件、0 删除）已全部收入 `ec11bb0`；该数字不再表示后续工作树状态。仓库当时占用 570,615,493
bytes。`bustub-20241207-2024fall/` 的 1,347 个文件均已跟踪，占 36,074,734 bytes，是课程基线而非临时复制，
已保留。

### 下一次恢复

无自动下一功能步骤。先读本节、方案文档的基线权威段和测试矩阵，再核对 `git status`；
M8 只能在方案先冻结唯一功能定义后开始。任何后续源码改动都需按影响范围重新验证，
构建仍放到源码树外并执行同一清理门禁。

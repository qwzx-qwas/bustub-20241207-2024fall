# Raft 方案执行交接

更新时间：2026-08-30（Asia/Shanghai）

## 当前状态与停止边界

- M0–M7 历史全量验收基线为 `ec11bb0`；recovery/owner/CI 维护为 `1178cdf`，M7 后候选 DAG 与 M8 分配
  契约为 `66cb1e9`。获准的过时嵌套副本和四个根目录运行/诊断产物由 `2a1d2ce` 清除。
- 唯一分配的 M8“写重试 payload 绑定”已经实现并提交为 `958fc80`。CommandBatch/Session 采用 V2，其他格式
  保持冻结；完整测试与清理证据见下一节。推送后全量 CI 收敛修正为 `20f1af2`，不增加
  M8 协议或下一阶段功能。当前没有半执行的功能阶段。
- 停止边界已到达：停在 M8，不选择、设计或预执行候选 DAG 中的下一节点，等待用户明确命令。
- 若后续会话恢复，先阅读本文件、`raft_implementation_plan.md` 与
  `docs/testing/raft_test_matrix.md`，并核对 `git status`；基线验收数字不得自动为后续源码改动背书。

## M8 已完成契约与结果（2026-08-30）

- fingerprint 在 state-dependent SQL prepare 前按 exact raw SQL bytes 计算。固定 preimage 为
  `BUSTUB_RAFT_WRITE_INTENT` + big-endian `u32 version=1` + big-endian `u32 WRITE_SQL=1` +
  big-endian `u32 payload_length` + payload；SHA-256 固定输出 32 raw bytes。client/request identity 不进入
  digest，而由 Session key 独立绑定。
- 只升级 CommandBatch 与 Session snapshot 到 V2，并保存 `u32 fingerprint_version + 32 bytes`；client wire、
  WriteResponse、Raft RPC、Catalog、outer BusTub snapshot bundle 与 node marker 保持原版本。部署只支持同版本
  三节点和全新目录，不读 M7 durable directory，不做 dual-read、迁移、mixed binary 或 rolling upgrade。
- 同一最近 ID + 同 fingerprint 返回原 cached response；同一最近 ID + 不同 fingerprint 用现有 V1 response 的
  `REJECTED` 和精确文本 `request payload does not match request identity` fail-closed。更老 ID 仍是 `TOO_OLD`；
  本阶段不引入 request window、并发 proposal、client 注册/认证或全局 precommit reservation。
- 测试必须把业务无副作用与 durable no-append 分开证明：literal SQL 行/计数/OID/Session 是业务 oracle，
  pre/post LogStore index/bytes 或命名 storage event 是日志 oracle。格式使用手写非空 fixture 和标准 hash vector，
  禁止 production encode/fingerprint 生成自己的 expected。还须覆盖响应丢失切主、snapshot、三节点 cold restart，
  注册 CMake/CI，并跑受影响回归、定向 ASan/UBSan 与并发路径 TSan。
- M8 全部门禁与清理完成后立即停止，不选择下一候选。

实现由 `958fc80` 固定：自包含 FIPS SHA-256 先于 SQL prepare 绑定 exact raw payload；CommandBatchV2 和
SessionV2 持久化 `version + 32-byte digest`；committed/active identity 的 changed payload 使用精确文本
`request payload does not match request identity` 拒绝。Apply 再次验证 fingerprint，CommandBatch 完整 frame
上限与 64 MiB LogCodec payload 相等，超限在 proposal 前拒绝。没有引入认证、client 注册、并发 request
window、多个 in-flight proposal、迁移或 rolling upgrade。

## M8 最终验收与清理（2026-08-30）

- Clang 14 ASan+UBSan component gate：27 个二进制、146 项具体测试全部通过；0 failed/error/disabled，
  `process_retries=0`，sanitizer marker 为 0。默认沙箱预检只在 TCP bind 前被 loopback 权限拒绝，不计作测试
  尝试；有效 gate 以整个父进程在允许回环的环境中 fresh、单次执行。
- M8 专属 ASan+UBSan 与 Release 正式三进程链路各单次通过。6 个 mismatch 阶段均验证精确拒绝文本、literal
  rows、稳定 Raft 字段及 `LOG-MUTATIONS` size/SHA 不变；4 个 exact retry 阶段返回 byte-identical、
  committed-index-4 response 且无 append。链路覆盖丢响应切主、Snapshot@8、2 块共 70,163 bytes 的
  InstallSnapshot、安装节点当选 Leader 和连续两次三节点 cold reopen。
- 原有五条 M6/M7 正式进程链路按受影响回归分别在 ASan+UBSan 与 Release 当前 M8 executable 上 fresh、
  单次执行并全部通过；精确结果记录在测试矩阵，不以旧 `ec11bb0` 的历史结果代替。
- TSan `raft-tsan-core`：TCP 4/4、RaftNode 31/31、SessionTable 7/7、BusTub FSM 5/5、
  DistributedNode 9/9，共 56/56，无 data race/lock-order 报告。
- 静态门禁：Clang-format 29/29、全仓 cpplint 457/457、shell 3/3、Python AST 1/1、YAML 1/1、
  `git diff --check` 均通过；三个组件清单均为 27，production→test、旧 API/格式暗读和外部 crypto 依赖为 0。
- `2a1d2ce` 删除 1,347 个嵌套旧副本文件及 `test.bustub`、`test.log`、`expected.log`、`result.log`，合计
  1,351 个文件、44,467,543 bytes；另加入四条根路径 anchored ignore。删除经用户明确授权、可从 Git 历史
  恢复；嵌套目录不忽略，以便复发时暴露。
- 最终精确删除 28 个 M8 `/tmp` 构建/组件日志/E2E 现场，共 5,057,640,577 bytes。复扫无 M8 临时项、
  node/client/proxy 进程、源码树 ignored/generated 文件或未解释的 untracked 文件。
- 本地结论限定为“M8 受影响门禁完成”，不冒充 `958fc80` 已重跑完整 M7 acceptance。普通 BusTub
  binder/planner/executor/storage 路径未变，因此本地不重复耗时的 Release SQLLogic 40/40；public regression、
  SQLLogic、六条 ASan/UBSan 与六条 Release 进程链及 TSan 均已配置进 GitHub Actions：public regression 是
  build job 内的 step，其余四组是分立 jobs；这里不声称远端 workflow 已运行完成。

## M8 推送后 CI 收敛（2026-08-30）

- `66591c1` 首次真正在 `main` 上触发完整 workflow。该 run 的 M8 ASan/UBSan 六条进程链、Release 六条
  进程链、TSan 和 Release SQLLogic 已通过；Ubuntu Clang/GCC 在默认 `all` 的 Build 步骤提前失败，
  因而不能用部分绿色 jobs 宣称整个 workflow 成功。
- `20f1af2` 关闭这些早于 M8 的基线门禁问题：四个 tool/bench 改用 production `Schema/Column` API，
  不再反向依赖 `test_util.h`；删除 B+Tree 两处未读取指针；时间戳时区输出去掉固定小缓冲区；
  改写会被 GCC 解释为续行的注释；九个 clang-tidy target 显式通过 Python 运行 `100644` 脚本；
  并修正一个既有 HNSW 常量命名门禁。这些修正不改变 Raft 协议、CommandBatch/Session 格式或业务语义。
- 新增时间戳回归直接使用四个预计算 packed `uint64_t` 和四个字面 expected string，不经 VARCHAR parser
  或 production formatter 生成 expected；覆盖 `-12/-01/+00/+14`。本地 Clang 14 ASan 定向结果为时间戳
  1/1、B+Tree 删除/合并 2/2、HNSW 7/7；printer 真实插入 `41,42`、删除 `41` 后仅余 `42`，
  且运行目录为空。受影响 native translation unit 逐个 tidy 通过，WASM 源文件通过
  Clang 14 `-fsyntax-only`。
- 本机 VSCode/WSL 资源限制是强制运行约束：一次只运行一个重型构建/测试，本地 CMake 使用 `-j1`，
  不并发启动编译器、sanitizer、E2E 或测试子代理。这只是主机调度约束，不改变“每场景一次、
  无重试”的验收语义；独立 GitHub runner 仍运行完整 public regression/SQLLogic/sanitizer/TSan 门禁。
- `22d51cc` 的 run `33311990646` 在 Release E2E-11 发现了独立于 M8 协议的测试竞态：第二个
  `SNAPSHOT-*` 完成 rename 时，同一 Tick 可能仍在完成 retained bridge-log 边界，脚本却立即用 10 秒
  `TERM` 门禁测量关停。修复保留 10 秒门禁和单次场景语义，在两代文件可见后通过正式 status API 建立
  快照发布完成屏障，并要求字面 `status=OK` 与 `last_applied >= suffix_index`；损坏最新文件、独立解析前一代
  index 和 bridge replay oracle 均未改变。fresh Release recovery matrix 已单次通过 E2E-02/06/07/09/11/12。
- 同一 run 首次真正执行完整 `check-clang-tidy`，暴露 21 个唯一诊断：20 个是 include、命名、值类别和
  标准算法等机械清理，另一个是 `PlanSelect` 在同一函数调用中推断 schema 与 move 表达式容器造成的真实
  求值顺序/use-after-move 风险。修复先建立 schema；DISTINCT 分支保留一份很小的 `shared_ptr` 容器副本，
  避免后续条件路径观察 moved-from 状态。18 个报错 translation unit 已按资源约束逐个 tidy 通过。
- 修复后使用单线程 Release 增量构建链接全部受影响目标；9 个 GoogleTest 二进制以真实状态、持久目录、
  畸形帧、随机选举、快照恢复和 TCP loopback 通过 71/71，vector-index SQLLogic production 链亦通过。
  22 个改动 C/C++ 文件通过 format/cpplint，E2E shell 通过 `bash -n`，`git diff --check` 通过。
- 后续 run `33314397739` 的 artifact 又证明 E2E-11 残留了固定节点假设：node 1 仅保留一代，而同样已
  apply 到 suffix 的 node 2/3 各有两代，脚本在任何损坏前因硬编码 node 1 退出。快照发布时机受角色与
  调度影响，节点编号不是 correctness oracle。脚本现从三者中选择首个实际保留两代的节点，并把 status
  屏障、目录、损坏、单节点重启、上一代 index 与真实查询全部绑定到该节点；三者皆不满足仍明确失败，
  开始损坏后绝不换目标。fresh 本地 Release matrix 已单次通过 E2E-02/06/07/09/11/12；随后 run
  `33314927956` 的完整 Release production-process job 也在独立 runner 上一次通过。
- 三个连续 run 的 `macos-13` job 始终无 runner；GitHub 官方已于 2025-12-04 退役该标签，并建议迁移到
  `macos-14`/`macos-15`。为继续固定 LLVM 14 且使用 Homebrew 仍提供 bottle 的平台，矩阵迁移到受支持的
  `macos-14` ARM64，并把 format/tidy 路径改为 `/opt/homebrew/opt/llvm@14/bin`；静态检查和 public tests
  范围不缩小，同时新增 ARM64 编译覆盖。依据：<https://github.blog/changelog/2025-09-19-github-actions-macos-13-runner-image-is-closing-down/>、
  <https://formulae.brew.sh/formula/llvm%4014>。
- run `33314927956` 随即证明 runner 与 LLVM 14 bottle 均正常，但 macOS image 的 CMake 4.3 已删除对
  policy floor `<3.5` 的兼容，配置在第一个 vendored root `murmur3` 停止。扫描确认顶层实际纳入的
  `murmur3/libfort/utf8proc/backward-cpp/libpg_query/linenoise` 六个根均仍声明 3.0；现分别提升为 3.5，
  不使用 `CMAKE_POLICY_VERSION_MINIMUM` 全局绕过，也不改未构建的 vendor 示例。fresh 本地 CMake 3.28
  已完整遍历全部子目录并成功生成。
- run `33315282437` 已证明上述 CMake 4.3 配置修复在 macOS 14 ARM64 生效，随后 build 暴露
  `PosixDurableStorage::SyncFile` 直接调用 Linux-only `fdatasync`。修复在 Linux/其他 POSIX 保留
  `fdatasync`，在 macOS 改用 Apple 的强持久化屏障 `fcntl(F_FULLFSYNC)`；两者以及目录 `fsync` 均重试
  `EINTR`，目录同步错误仍 fail-closed。Apple 文档明确指出普通 `fsync` 可能不刷新设备 cache，故不以它
  代替 `F_FULLFSYNC`：<https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fsync.2.html>、
  <https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fcntl.2.html>。
  本地 `-j1` Release recovery 编译、直接 clang-tidy、项目配置 cpplint 和真实 canonical snapshot 2/2
  已通过；新的 macOS branch 尚须由下一次远端 ARM64 build 验证，不能由本地 Linux 结果代替。
- 只有包含 `20f1af2` 的最终文档 HEAD 所触发 workflow 全部必需 jobs 终态成功，才能把这次 CI 收敛记为
  完成；远端 run ID/结果由最终交接报告记录，不为了回填动态编号再制造 docs-only CI 循环。

## M7 后路线图复审（2026-08-30，M8 分配前历史记录）

- 原候选表把所有项目称为“互相独立”，实际混合了 safety hardening、DB 语义、性能依赖链、离线迁移和
  会改变项目定位的系统工程。方案现改为非线性候选 DAG：一次里程碑只选择一个 ready node，不能捆入它
  尚未完成的依赖；共享 overlay、scheduler 或 migration 前置只能有一个 owner。
- 单 statement secondary UNIQUE、多 statement 原子 batch 和跨 statement secondary UNIQUE deferred checking
  已拆开；后者依赖前两者的 overlay/final-state 规则且不暗含其他 constraint family。一条 statement 展开多行
  mutation 不再被误称为多语句事务。
- 复合 `NOT NULL` key 与新增确定性非空 scalar key type 已拆开；nullable primary key 与主键身份语义冲突，
  永久排除。payload 绑定也与 client 生命周期/认证拆开，认证不属于 crash-stop、非 Byzantine 当前范围。
- completion/storage scheduler、多个 in-flight proposal、group commit 和 AppendEntries pipeline 分成依赖明确的
  性能实验；durable mode marker 与 term-0 离线迁移拆开，迁移只有在真实需要复用旧目录时才立项。
- mode marker 必须先于任何 mode-owned 文件创建；非空但无 marker 的旧目录默认 fail-closed，不能猜测内容后
  就地补 marker，只能走显式 offline adoption/migration；迁移还须先冻结 term-0 Snapshot/Session 到 distributed
  SnapshotStore/LogStore/HardState 的 index、term 与 commit mapping。
- base backup + WAL、fuzzy/COW/低停顿快照、成熟 Raft 库迁移、动态成员、安全认证、通用路由、rolling
  upgrade 和分片已移出普通 M8；这些能力只有项目定位改变后才建立新顶层方案。
- 任一未来阶段都须逐项冻结 client wire、Raft RPC、CommandBatch/log、Catalog、Session、snapshot bundle 和
  node marker 的格式影响。默认不支持 mixed executable/wire version 或 rolling，且这只是部署前置；没有
  capability-epoch 握手和真实 old/new binary 测试时，不得声称旧 binary 会协议级 fail-closed。受影响格式只能
  选择不保留旧状态的 fresh-directory homogeneous deployment，或有三节点目录+identity/config 备份、逐格式
  转换/crash oracle 的 preserving-state offline upgrade。只有 append log 使用 V1 prefix/V2 suffix；
  Catalog/Session/snapshot 使用完整新 generation/离线转换，wire/RPC 同代协商，marker 有单独 adoption 规则。
  cutover marker 必须先于首份 V2 authority byte durable；既有 V1 不认识未来 marker，因此 downgrade 默认是运维禁令。
- 共同测试门禁要求真实业务结果的独立 oracle、失败无 append/storage/state 副作用，以及至少一条三个正式
  进程的 E2E；格式变化再要求手写非空 golden、exact bytes、未知/未允许组合拒绝及允许升级序列的正向测试，
  磁盘/持久化变化再要求两次冷启和命名 crash event。不能靠同一 codec encode/decode 自循环证明正确；
  修改并发路径强制 TSan，只有性能目标强制性能基线；所有候选还须跑定向 ASan/UBSan、受影响既有 gate，
  并把新测试注册进 CMake/CI。
- 推荐但尚未分配的首个候选是“写重试 payload 绑定”：在 state-dependent prepare 前对稳定 write-intent
  bytes 计算版本化 fingerprint，并随首次 batch、Session 和 snapshot 持久化。它不包含认证、并发 proposal
  或迁移；只对 `request_id == last_request_id` 重放 cached response，更老 ID 仍为 `TOO_OLD`。V1 Session 无原
  payload，不能回填 digest，实验项目优先考虑 fresh-directory homogeneous deployment。测试必须用手写 intent、
  标准 fingerprint vector、literal Session fixture 和真实 raw request，不能调用 production fingerprint 生成期望值。

以上只记录 M8 分配前的路线图状态；后续用户已经明确选择该候选，并由 `958fc80` 完成。

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
- 提交 `1178cdf` 的最终定向 Clang 14 Debug + ASan 回归通过 17 个二进制/108 个测试；其中 StateManifest 9/9、
  RaftNode 31/31、DistributedNode 9/9。term-0 publisher/recovery 现在同时拒绝外层和内嵌 Session response
  的非零 term；M4 KV inner snapshot 与 M5 BusTub bundle 测试归属已拆开。未重跑全量 M7，不得冒充新的
  122/122、40/40、正式 E2E 或 TSan 证据。
- 本轮全部修改/新增 C/C++ 通过 Clang 14 format dry-run 与仓库参数 cpplint；3 个 shell 通过 `bash -n`，
  CI YAML、`git diff --check` 和 production 反向依赖扫描通过。清理审计修复并复跑了会漏删 DiskManager
  `.log` 的 `table_heap_reopen_test`（1/1）。早期 431,479,634-byte 审计构建树已删除；本轮再精确删除
  1,039,771,113-byte 外部 ASan 构建树和旧 0-byte reopen 日志。复扫后 `/tmp/bustub-*`、后台进程、
  源码树生成/ignored 文件均为空；提交前盘点的 44 个已跟踪修改、2 个正式删除和 4 个正式新增源码文件
  均已收入 `1178cdf`，不再是当前工作树状态。

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
- 完成 M8 exact payload binding：原始 SQL 的 versioned SHA-256、CommandBatchV2、SessionV2、active/committed
  mismatch fail-closed，以及第六条正式三进程时间线；不扩展为认证、并发 proposal 或升级工程。

## 历史完整验收证据（旧修订）

以下结果对应各自记录时的修订，不能追溯性覆盖后来新增的 oracle；`ec11bb0` 的历史全量验收结果见文末
“最终修复收口交接（2026-08-30，历史 M0–M7 全量验收基线）”。

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
- 当时曾从 HEAD 恢复 `test.bustub`（8,392,704 字节）和 `test.log`（0 字节）；M8 来源审计后来证明它们
  是本地运行产物而非 fixture，该历史动作已由 `2a1d2ce` 的删除与根路径 ignore 取代。
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
- 当时把已跟踪嵌套目录 `bustub-20241207-2024fall/` 判断为原始基线；M8 来源审计后来证明它是顶层源码
  提升后遗留且无人引用的过时副本，该历史判断已由 `2a1d2ce` 取代。
- 测试缺口回补最终精确删除 2.4 GiB 的 ASan/UBSan、TSan 构建树、成功/失败进程现场和组件日志；
  `/tmp/bustub-raft-m7-*`、`/tmp/bustub-raft-gap-*` 与后台 node/client/proxy 进程均为空。源码树没有 ignored
  build/cache/core 或未跟踪运行产物，仍为 556 MiB。最终工作树是 164 项正式交付：61 个已跟踪修改、
  103 个新增文件、0 个删除；新增项为 src 57、test 36、tools 4、docs 4、方案/交接 2，其中新增的严格
  component gate、三个外部故障工具和四条时间线均为可复用正式测试资产。

## 下一步（M7 完成时的历史停止点；已由后续 M8 授权取代）

该历史时点无自动下一步并停在 M7；用户后来明确分配且只分配 M8，现已由 `958fc80` 完成。构建继续只能
放在源码树外目录，并在每个明确里程碑结束后执行同一清理门禁。

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

该历史停止边界随后被用户对 M8 payload binding 的明确授权取代；它不授权 M8 之后的任何候选。

## 最终修复收口交接（2026-08-30，历史 M0–M7 全量验收基线）

### `ec11bb0` 当时的执行位置（已由后续 M8 授权取代）

`ec11bb0` 当时已按方案完成 M0–M7 且没有半执行阶段，恢复执行时没有跳到 M8/V2。后续源码确已由
`1178cdf` 修改，因此“源码不变则无需重验”的条件已经不成立；阶段实现没有重新打开，但若要声称当前维护
提交通过完整 M7 acceptance，必须重跑 122/122、40/40、正式 E2E 与 TSan。当时下一功能步骤仍等待用户；
后来用户只授权 M8 payload binding，当前完成状态以文首为准。

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
bytes。当时把 `bustub-20241207-2024fall/` 的 1,347 个已跟踪文件（36,074,734 bytes）认作课程基线而保留；
M8 来源审计证明它是顶层源码提升后的过时副本，且无构建、测试、脚本或 CI 引用。经用户明确授权，
`2a1d2ce` 已删除该目录和四个根目录运行/诊断产物；这是普通 Git 历史中的可恢复删除，不是历史改写。

### 下一次恢复（M7 基线时的历史指引；已由 M8 完成记录取代）

该指引描述 `ec11bb0` 的 M7 停止点。用户后来明确选择 M8 payload binding，并由 `958fc80` 完成；当前恢复
应读取文首“M8 已完成契约与结果”，不得把本历史段误读成 M8 未分配。任何未来源码改动仍需先明确选择一个
ready node、冻结格式/升级/测试门禁，构建放到源码树外，并执行同一清理门禁。

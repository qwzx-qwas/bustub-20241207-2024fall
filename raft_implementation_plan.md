# BusTub 单机恢复与 Raft 复制实施方案

## 背景

当前项目具备页式存储、Buffer Pool、Catalog、SQL 执行器、事务与 MVCC 等基础能力，但尚不具备可作为 Raft 状态机底座的持久化闭环：

- `Catalog` 仍是内存结构，进程重启后不能仅凭现有 `db.bustub` 恢复表、索引和 OID 分配状态。
- `LogManager`、`LogRecovery`、`CheckpointManager` 仍是教学骨架，不能保证任意写入阶段崩溃后的恢复。
- 当前单个 `db.bustub` 同时被当作运行文件和持久状态，但没有一个原子元数据入口说明“哪个数据库文件与哪段日志共同组成最近一次可信状态”。
- 现有 shell 面向单实例，尚未定义节点身份、数据目录、Raft RPC、Leader 路由以及客户端请求去重语义。

本方案不采用完整 ARIES 的原地页恢复路线。第一版选择更接近 LevelDB 与 rqlite 的恢复范式：

```text
权威状态 = 最近一次校验通过的不可变快照
         + 快照之后、截至 commit_index 的已提交命令日志

working/db.bustub = 权威状态的可重建物化结果
```

## 项目定位与快照复杂度上限

本项目是“在教学版 BusTub 上实现一个简单、可验证的静态三节点 Raft 层”的实验系统，不以大型数据库
在线备份、TB 级容量或严格低停顿 SLO 为目标。本文中的 production/formal path 只表示正式节点、协议和
持久化代码相对于 test double/harness 的边界，不表示已经达到通用生产数据库的容量与运维成熟度。

V1 保留“暂停写入生成完整 canonical snapshot + `lastIncludedIndex/Term` + 分块 InstallSnapshot”。文件化和
分块的目的，是避免一次性大 RPC，并能够实验 offset、重复块、半传输、临时文件、校验后发布和快照后 suffix
追赶；它不是继续建设在线备份系统的起点。正式文件 payload 设 1 GiB 防御上限，兼容内存 codec 仍为
128 MiB；二者都是拒绝异常输入的实验边界，不是容量承诺。

这样做不是降低可靠性要求，而是以暂停写入的全量快照和 redo-only 重放换取更小的实现面。V1 的 data-page policy 明确为 `NO-STEAL + NO-FORCE`：proposal 前只构造 private mutation buffer，未 committed command 不修改公开 Buffer Pool、TableHeap、Catalog、索引或 MVCC version chain；Raft commit 和客户端成功也不等待对应 working data pages 落盘，持久性由 committed Command/Raft Log 保证。命令/Raft 日志则必须在 durability acknowledgement 前执行 FORCE。周期性生成并同步新的 canonical snapshot 不属于逐事务 data-page FORCE。只有未来允许未提交页进入公开 working file、采用 fuzzy checkpoint 或要求快速原地页恢复时，才引入 pageLSN、CLR、Dirty Page Table 和 ARIES redo/undo。

## 总体目标

最终交付一个具备下列能力的三节点 BusTub 集群：

- 三个静态节点、一个 Raft Group、一个数据库，不做分片。
- 所有写请求进入 Leader；V1 一条 autocommit DDL/DML statement 对应一个原子的、确定性的 Raft `CommandBatch`，一条 statement 可以展开为任意多行 mutation。
- Follower 必须先持久化日志再回复 `AppendEntries` 成功；Leader 在多数节点持久化后推进提交。
- 每个节点严格按日志索引顺序 Apply 已提交命令；客户端只在提交并完成本地 Apply 后收到成功。
- 任一节点都能由“快照 + 已提交日志”恢复 Catalog、表数据、索引定义、OID、MVCC 提交顺序和请求去重状态。
- 支持选举、日志冲突回退、旧 Leader 回归、Follower 追赶、快照安装和进程崩溃恢复。
- 测试既覆盖局部不变量，也必须使用生产二进制、真实文件系统和真实 RPC 打通生产链路。

## 总体边界

### 本方案要做

- 不可变全量快照与最多两代有效恢复点：M0–M2 的 term-0 验证模式使用
  `state/CURRENT -> MANIFEST-N -> SNAPSHOT-N/`；M4 起的分布式模式使用
  `raft/snapshots/CURRENT -> SNAPSHOT-N`。两者共享 canonical 逻辑状态和 bridge-log 可恢复性规则，
  但不共享物理发布封装。
- Catalog 完整持久化与现有 TableHeap 的重新打开。
- 有 framing、版本、长度、校验和、尾部截断处理和明确同步 durability boundary 的命令日志。
- 持久化 Raft `term`、`voted_for`、日志、`commit_index` 和快照元数据。
- V1 一个 autocommit write request 对应一个 `TransactionCommandBatch`；DDL 与 DML batch 进入同一全局日志序列。
- 客户端请求 ID、重复请求去重、Leader 故障后的不确定响应重试。
- Leader 线性一致读、显式 Follower stale read，二者都使用 Raft 派生的 MVCC read timestamp。
- 单元、组件、集成、进程级 E2E、崩溃点和网络故障测试。

### 第一版不做

- ARIES、pageLSN、页级 before/after image、CLR、Dirty Page Table、fuzzy checkpoint。
- 动态成员变更、joint consensus、learner、分片、多 Raft Group、跨组事务。
- 多 Leader 写、Follower 线性一致读、跨节点交互式长事务迁移。
- 多语句 non-interactive transaction；保留 TransactionCommandBatch 格式作为 V2 扩展点，但 V1 每个 batch 只来源于一条 autocommit statement。
- 增量快照、远程对象存储、日志或快照加密。
- PostgreSQL 式独立 base backup + 连续 WAL 归档；V1 仍使用 Raft 的完整状态快照 + committed suffix 模型。
- 在线/模糊/COW snapshot、跨进程断点续传、压缩、限速、多流并发和 snapshot pipeline/group commit。
- 以大型数据集吞吐、最低 RSS 或写停顿 SLO 作为 V1 验收条件；V1 只验协议、持久化和逻辑恢复正确性。
- 任意索引物理字节持久化；第一版把包括 primary-key index 在内的 B+Tree、Hash、HNSW、IVFFlat 等结构统一视为 derived state，只持久化定义并从表数据重建。
- 以原始 SQL 文本作为最终复制协议。
- secondary UNIQUE index/constraint 和任何需要 deferred constraint checking 的 batch；V1 只保留不可更新的 primary-key uniqueness 与普通 non-unique secondary index。
- 多个 in-flight proposal 和通用 group-commit scheduler；Follower 追赶时一次 `AppendEntries` 携带多条 entry 只属于 batch append。

### V1 范围冻结

V1 功能范围保持冻结。本轮只补 safety contract、确定性细节和已经确认的边界收敛，不引入新的 Snapshot
形态、事务模式或性能调度器。V1 持久化接口明确选择同步 API，不保留“同步执行后包装 ready future”的
伪异步层；真正异步 completion 和可控存储调度器属于未来性能阶段。完整文件快照和分块传输到此为止，
不再以 production scalability 为理由继续增加 base backup/WAL、增量/fuzzy/COW、续传、压缩或 pipeline。
多语句事务、并行 proposal、group commit 和 secondary UNIQUE 仅作为 M7 后候选 DAG 的不同节点保留；
它们并非全部独立，也不会在没有明确分配时因“继续完善”自动进入范围。

## 文档权威、执行轴与共享前置契约

本文只有一条可执行门禁轴：`M0 -> M1 -> ... -> M7`。后文 A–D 是架构工作流，
“横切测试与交付规则”从 M0 开始持续生效，它们都不是另一套串行阶段。出现表述冲突时，
权威顺序为：本节与“里程碑唯一归属表” > 全局不变量 > 各工作流的细化说明 >
非规范的历史执行记录。

M0 前只冻结跨阶段最小契约：`VersionedFrame`/checksum 骨架、`ReplicatedLogEntry`
envelope、canonical `db/catalog/session` 逻辑内容、`DurableStorage` 语义以及命名故障事件词汇。
M0 的 spike scaffolding 和未冻结假设可丢弃/演进，但其可执行 vertical slice 首次实现的最小 production
Catalog/Session/WriteResponse codecs、replicated V1 Catalog shape/restore admission、TableHeap reopen 与
`CanonicalSnapshotBuilder` 是后续阶段的正式 contracts/code，不能按“临时实验”清理。M0 不包含 durable
publication，也不把 SQL 生产者提前到 M0。
M0–M2 的恢复退出门禁只依赖
预构造、格式已冻结的非空 entry/payload fixture；源码可复用已存在的 codec/consumer。M2 先拥有
consumer 方向：`TransactionCommandBatch` 固定 wire 类型/解码、预构造 committed batch 的
state-dependent admission、`BusTubStateMachine::Apply`、Session request/稳定 response 状态转换与 term-0
恢复。M5 后拥有 producer 方向：raw SQL -> canonical batch、SQL shape/unsupported 语义准入、canonical
`CommandBuilder`、完整 command-set 确定性强化和 BusTub Raft snapshot hooks；它复用 M2 admission 并累计
复验 consumer，但不建立第二条 Apply、Session 状态机或日志/快照恢复路径。

共享类型按“最早必需能力”而不是目录名归属：M0 为非空 self-contained fixture 引入稳定
`WriteResponseV1` frame 和可持久化的 `SessionRecord/SessionSnapshotCodec` 容器；M1 增加 production
`StateVisibilityLatch`、Snapshot@S 边界校验与原子发布/回退；M2 增加请求分类、`RecordCommitted`、
committed Apply 中的 response 构造/重放和 exact-once 状态转换。M0 只用预构造 record，不调用这些
状态转换。M2 的 `SingleNodeCommandRuntime` 只接收
预构造 batch；`CommitSql` 是 M5 放在 distributed translation unit 中增加的 producer adapter，不能使
recovery target 反向编译依赖 `SqlCommandPreparer`。

两种运行模式的持久化权威必须互斥：

| 运行模式 | 所属里程碑 | 唯一日志/快照权威 | 允许共享的内容 |
| --- | --- | --- | --- |
| term-0 单节点恢复验证 | M0–M2 | `CommandLog` + `SnapshotManager/StateManifestStore` + `StableStore` 的 term-0 commit marker | entry/frame codec、canonical logical snapshot builder、Catalog/Session codec、durable storage/fault vocabulary、StableStore disk format |
| 静态三节点分布式运行 | M3–M7 | `LogStore` + `SnapshotStore` + `StableStore` + `BusTubRaftStateMachine` | 有意复用 StableStore 和上述逻辑原语；不复用 term-0 的 CommandLog/StateManifest 日志与快照 envelope |

V1 没有持久化运行模式 marker，也没有 term-0 -> distributed 的原地迁移器。因此受支持的部署规则是：
一个进程不得同时打开两套日志或两个 `CURRENT` 权威，分布式节点必须使用全新目录，不能把 term-0
目录当作集群目录继续打开。现有 HardState/日志一致性检查可能拒绝部分误用，但不是完整的模式检测器；
若未来需要强制检测或迁移，必须另定义 versioned mode marker、离线迁移工具和验收，不能靠路径猜测。

## 必须始终成立的不变量

1. 完成启动恢复归一化后，运行态必须满足 `last_applied <= commit_index <= last_log_index`；被快照压缩的日志以 `last_included_index` 作为逻辑日志基点。
2. `published_applied_index` 表示“效果已完整发布的最高连续 Raft log index”，不是“最近一条修改数据的 CommandBatch index”。Apply 每一种 entry（包括不修改数据库的 `NOOP`）都必须推进它；由 `Snapshot@S` 恢复或安装快照后，`published_applied_index` 与 `last_applied` 都初始化为 `S`。
3. Apply 线程只能按连续递增的日志索引执行，不能跳过、并行乱序或重复产生副作用。
4. 节点回复日志持久化成功前，对应日志字节必须已经越过 `fdatasync/fsync` 持久化屏障。
5. term-0 模式的 `state/CURRENT` 只能指向已同步且校验通过的不可变
   `MANIFEST-N`；分布式模式的 `raft/snapshots/CURRENT` 只能指向已同步且校验通过的
   framed `SNAPSHOT-N`，其外层不存在 `MANIFEST-N`。
6. 一个快照必须同时描述同一日志索引处的数据库文件、Catalog、OID 分配器、`schema_epoch` 和请求去重表。
7. 未提交日志永远不能 Apply；只复制到少数节点的日志允许被后续 Leader 覆盖。
8. 已提交日志不能丢失。新 Leader 必须包含所有已提交条目。
9. 相同快照和相同已提交日志前缀必须得到相同的逻辑数据库状态。
10. 一个 CommandBatch 是一个原子可见状态转换；表数据、Catalog、全部索引、MVCC commit timestamp 和 SessionTable/去重状态不能向并发读者暴露部分新、部分旧的组合。
11. 若最老的保留快照边界为 `S_old`，本地必须保留从 `S_old + 1` 到当前日志尾的完整连续日志；没有这段 bridge log 的旧快照不能被计为可回退恢复点。
12. 测试不得通过测试专用网络 API、隐藏管理命令或 production 默认分支改变生产行为。
13. InstallSnapshot 只能在安装前的本地逻辑日志满足 `TermAt(S) == T` 时保留 `index > S` 的旧 suffix。
    若不匹配，只有 `E=max(H,S)==S`（Snapshot 自身覆盖全部 durable commit）时才能丢弃 suffix 并建立
    `snapshot_base=(S,T)`；若 `E>S`，必须在发布 CURRENT 前 fail-closed，不能破坏 committed suffix。
14. 任何使 `current_term` 增大的事件都必须先持久化新的 `HardState{term, voted_for}`，再发送或回复任何依赖新 term 的 RPC；Candidate 自增 term 并自投票也遵守同一规则。
15. 每次线性一致读必须使用在该读到达后创建的唯一 ReadIndex context 完成一次当前 term 的新鲜 quorum round；旧 heartbeat ACK 或已经完成的旧 read barrier 不能复用。
16. V1 distributed mode 中每个可写用户表必须具有协议支持的逻辑主键；无主键或主键编码不受支持的 CREATE TABLE 必须在 proposal 前拒绝，恢复时无法验证主键定义则节点不得开放服务。
17. InstallSnapshot 在最终发布前若发现 `S <= published_applied_index`，必须作为 stale/idempotent no-op 忽略；它不能修改 CURRENT、snapshot base、日志、working FSM、`last_applied` 或 `published_applied_index`，已发布状态绝不回滚。
18. V1 不接受 secondary UNIQUE index/constraint 或其他依赖 deferred constraint checking 的写入；相关 DDL 必须在 proposal 前拒绝，因此 committed batch 的逐行普通 secondary-index 维护不会遇到最终合法但中间态冲突。
19. 从 Snapshot@S 恢复时，primary index 与所有 secondary indexes 必须先由 canonical rows 重建到 state@S，之后才能把 applied 水位置为 S 并开始 suffix replay；suffix Apply 必须经这些索引定位并增量维护它们。
20. 正式快照数据路径必须是文件/文件切片和有界分块；`db.bustub + catalog + session` 不得先合并为一个受 128 MiB 上限约束的 `vector<byte>`。小型 KV/codec 单元测试可保留显式的内存便利接口，但 Raft 创建、持久化、发送、接收、重启恢复和 BusTub 安装必须走文件路径。该要求用于协议与故障实验，不得解释为在线大库快照能力。
21. 正式客户端收到响应后必须先验证外层 `request_id` 与本次发送的 write/read/status 请求一致；write 成功响应中的稳定 `WriteResponseV1.request_id` 还必须与外层一致。错误连接、陈旧响应或串线响应不能被当前请求接受。
22. 每个 Follower 的 InstallSnapshot 传输状态必须相互独立且只能单调前进；heartbeat 只能以相同 snapshot ID、
    offset、data 和 request ID 重发当前未确认块，不能用新的请求身份覆盖仍在 fsync 的块。Follower 对重复旧块
    返回实际 durable high-water，Leader 只能据此向前跳进；延迟、重复或旧 ACK 不能回退、饿死或复活已结束的传输。

---

# 架构工作流 A（M0–M2）：节点目录、Catalog 持久化与单机恢复闭环

## 背景

Raft 不能弥补状态机自身不能重启恢复的问题。M0–M2 先让单个 BusTub 节点在任意日志追加、Apply、快照发布时崩溃后，都能找到最近一次可信快照并重放已提交命令。

该工作流不是只搭接口。M0–M2 结束时必须有一个可运行的 term-0 纵向链路：
预构造的确定性 entry 进入、日志持久化、恢复 consumer Apply、快照、杀进程、重启和字面状态验证。
SQL 解析/绑定/CommandBuilder producer 不作为本工作流的前置退出条件；M2 已用预构造 batch 验证
state-dependent admission、Session exact-once consumer 与恢复，M5 再用真实 SQL 累计重跑这条链路。

## 目标

实现以下恢复流程：

```text
获取节点 LOCK
  -> 读取 CURRENT，校验 MANIFEST/快照；必要时回退上一有效代
  -> 以 Snapshot@S 创建新的 working/db.bustub
  -> 打开 canonical TableHeap，恢复每行 latest_committed_version_ts
  -> 恢复 Catalog、schema_epoch、OID allocator 与 SessionTable
  -> 扫描 Snapshot@S 的 rows，先重建 primary index 和全部 secondary indexes 到 state@S
  -> 初始化 published_applied_index = last_applied = S
  -> 扫描日志，按规则截断仅位于 committed boundary 之后的损坏尾
  -> 通过恢复 consumer Apply 入口重放 Log[S+1..effective_commit_index]
     每条 Apply 同步增量维护 primary/secondary indexes
  -> 完成一致性校验后开放请求
```

M2 用预构造 entry 验证该 consumer/恢复顺序；当前实现为减少重复而复用
`BusTubStateMachine::{ValidateProposal,Apply}`；M2 验收已构造 batch 的 state-dependent admission 与
Session/WriteResponse consumer，但不验收 SQL 解析/准备语义。M5 的累计门禁证明同一入口由真实 SQL
producer 驱动，并复验而不重新拥有这些 consumer 语义。

本工作流内的出口边界是：M0 用最小 production codec、TableHeap reopen 和 canonical builder 证明一份
无旧进程内存依赖的状态可表达已提交结果，但不引入 `CURRENT`/Manifest/日志；M1 负责 NodeDirectory、
StateManifest、capture barrier 和完整 `Snapshot@S` 的原子发布/精确恢复，最新代损坏时最多退回到上一
快照边界；M2 才拥有 term-0 commit marker、
durable CommandLog、bridge replay、恢复到当前 `effective_commit_index`、两代联动回收和掉电矩阵。

## term-0 节点目录范式

一个节点是一个进程和一个独立目录。下图只是 M0–M2 term-0 验证模式的物理布局，不是分布式目录的
过渡态。到集群工作流时必须为 `node-1`、`node-2`、`node-3` 使用独立目录，绝不能共享
同一个 `db.bustub`。

```text
node-1/
├── LOCK
├── raft/
│   ├── HARD_STATE
│   └── log/
│       ├── LOG-00000000000000000008
│       └── LOG-00000000000000000009
├── state/
│   ├── CURRENT
│   ├── MANIFEST-00000000000000000006
│   ├── MANIFEST-00000000000000000007
│   ├── SNAPSHOT-00000000000000000006/
│   │   ├── db.bustub
│   │   ├── catalog.bin
│   │   └── session.bin
│   └── SNAPSHOT-00000000000000000007/
│       ├── db.bustub
│       ├── catalog.bin
│       └── session.bin
└── working/
    └── db.bustub
```

term-0 runtime 不创建 `node.conf`；该身份文件只属于 distributed `EnsureIdentity`。`CURRENT` 不是数据库内容，
而是很小的原子入口，例如只包含 `MANIFEST-00000000000000000007\n`。`MANIFEST-N` 是第 N 代快照的
不可变说明书，记录文件名、边界索引和校验值。它不是不断 append 的单一大文件；每代新建一个小文件。
三个内容 checksum 只存于 Manifest；不再写一份无恢复消费者的 `CHECKSUMS` 旁路文件。

M3 以后的分布式目录使用下列互斥布局；`working/` 只是可重建物化状态，
`state/` 即使由通用 `NodeDirectory` 预建也不是分布式恢复权威：

```text
node-1/
├── LOCK
├── node.conf
├── raft/
│   ├── HARD_STATE
│   ├── log/
│   │   └── LOG-MUTATIONS
│   └── snapshots/
│       ├── CURRENT
│       ├── SNAPSHOT-00000000000000000006
│       └── SNAPSHOT-00000000000000000007
├── state/                    # 预留，非 distributed authority
└── working/
    └── bustub-raft-fsm/      # 启动时可从 snapshot + log 重建
```

“保留两代”指最多保留两个**可独立恢复到当前 committed state 的恢复点**，而不只是留下两个快照目录。假设旧代边界为 `A`、当前代边界为 `B`，就必须同时保留 `Log[A+1..last_log_index]`。只有 `Snapshot-A + Log[A+1..commit_index]` 完整可用时，Snapshot A 才有实际 fallback 能力。

```text
Snapshot-7 at 7000
Snapshot-8 at 8000
current commit at 8500

必须保留：
Snapshot-7、Snapshot-8、Log[7001..last_log_index]
```

生成 `Snapshot-9 at 9000` 后，先删除 Snapshot 7，再把日志压缩边界推进到 8000，最终保留 Snapshot 8、Snapshot 9 和 `Log[8001..last_log_index]`。generation 可以持续递增，但稳定状态下快照文件数固定，日志也按最老有效恢复点推进回收。

## Manifest 字段

```cpp
struct StateManifest {
  uint32_t format_version;
  uint64_t generation;
  uint64_t last_included_index;
  uint64_t last_included_term;
  uint64_t schema_epoch;
  std::string database_file;
  std::string catalog_file;
  std::string session_file;
  uint32_t database_checksum;
  uint32_t catalog_checksum;
  uint32_t session_checksum;
  table_oid_t next_table_oid;
  index_oid_t next_index_oid;
};
```

- `format_version`：磁盘格式版本；不支持的版本必须明确拒绝启动，不能猜测解析。
- `generation`：Manifest/快照发布代数，只用于选择新旧快照，不等同于 Raft index。
- `last_included_index`：快照已经包含到哪条 Raft/单机命令日志。
- `last_included_term`：term-0 StateManifest 中固定为 `0`。M4 distributed Snapshot 的实际 Raft term
  位于独立 `SnapshotStore` framed metadata，不复用本 Manifest 字段。
- `schema_epoch`：快照边界处已发布的 Catalog 版本。它也写入 Catalog snapshot，恢复时两份值必须相等，否则该代不可用。
- `database_file`、`catalog_file`、`session_file`：本代快照中各文件的相对路径，禁止逃逸节点目录。
- 三个 checksum：检测半写、误配和静默损坏；校验失败时回退上一代，不允许继续打开可疑文件。
- `next_table_oid`、`next_index_oid`：恢复 OID 分配器，防止重启后复用已有 OID。它们也写入 Catalog 快照，Manifest 中的副本用于交叉校验。

## 本工作流只做

1. 节点数据目录、独占 `LOCK` 和路径合法性校验。
2. 版本化命令日志格式与有效前缀扫描。
3. `CatalogSnapshotCodec`：序列化与恢复表、Schema、首页面 ID、索引定义、`schema_epoch` 与 OID 分配器，并与 Manifest 副本交叉校验。
4. 增加“按 `first_page_id` 打开已有 TableHeap”的明确接口；不能调用创建新表的构造函数代替恢复。
5. 暂停写入的全量快照、原子发布、启动回退和两代保留。
6. 单节点对预构造、格式已冻结的确定性 entry payload 执行 state-dependent admission 与 consumer replay，
   并验证 Session exact-once 状态转换；SQL -> `TransactionCommandBatch` 生产归 M5。
7. 用 Raft index 兼容的单调序号作为本地 `commit_index/published_applied_index/last_applied`；M0–M2 term 固定为 `0`。

## 本工作流不做

- 选举、Raft RPC、多节点日志复制。
- 模糊快照和后台增量复制。
- 直接修补崩溃前的 `working/db.bustub`。
- 依据当前 `LogRecord` 骨架补齐页级 ARIES。

## 命令日志要求

日志从一开始使用可升级、接近 Raft entry 的封装：

```cpp
struct ReplicatedLogEntry {
  uint32_t format_version;
  uint64_t index;
  uint64_t term;
  EntryType type;
  uint32_t payload_size;
  uint32_t checksum;
  std::vector<std::byte> payload;
};
```

空逻辑日志使用唯一 sentinel，所有阶段共用同一定义：

```text
empty snapshot_base = (0, 0)
TermAt(0)            = 0
first real log index = 1
initial commit_index = 0
initial last_applied = 0
initial published_applied_index = 0
```

index 0 不对应物理日志记录，也不能被 Apply；它只让第一条 AppendEntries 使用 `prev_log_index = 0, prev_log_term = 0`，并允许空数据库的初始 Snapshot/Manifest 表示为 `last_included_index = 0, last_included_term = 0`。LogStore、SnapshotStore、恢复代码和测试不得各自发明不同的 empty-state special case。

- 每条记录使用固定 header、长度和 checksum；启动扫描只接受完整且连续的有效前缀。
- 尾部出现半条 header、长度越界或 checksum 错误时，只有当损坏范围全部位于 `effective_commit_index` 之后，才允许截断到最后一条有效记录。
- `<= effective_commit_index` 的任一日志缺失、checksum 错误或不连续都属于 committed-range corruption：禁止降低 commit point 或静默截断；单机模式 fail-stop，Raft 模式先停止服务并从健康副本安装快照/日志。中间段损坏同样必须拒绝启动。
- 日志按 segment 滚动。可回收上界由最老有效恢复点决定，而不是由 `CURRENT` 指向的新快照决定：若 `oldest_retained_snapshot_index = S_old`，只能删除完全落在 `<= S_old` 的 segment。
- V1 的 durable append contract 为同步 `void Append(entries)`：将本次调用中的一条或多条连续 entry 编码并顺序写入，执行一次 `fdatasync` 后才成功返回；写入或同步失败直接抛出异常，整次调用失败，不能确认其中任一 entry。它不跨调用合并请求，也不实现等待队列、completion 或 group-commit scheduler。
- Leader 单写路径通常每次 append 一条 entry；Follower 追赶可以把一次 AppendEntries 中的多条 entry 作为同一次 `Append(entries)` 落盘，这只是 batch append。
- 单机阶段 append 成功即形成已提交项，但必须先持久化日志，再 Apply，再向客户端返回成功。

M2 同时拥有 `StableStore` 的版本化 HardState 磁盘格式、generation/checksum 原子替换与
term-0 `commit_index` marker 用法。M3 复用同一 Store，但才拥有 `current_term/voted_for`、
higher-term transition 和 durable-before-RPC 的 Raft 状态转换语义。

## Catalog 持久化要求

Catalog 快照至少包含：

- 表 OID、表名、完整 Schema、TableHeap `first_page_id`。
- 索引 OID、索引名、所属表、键列、键 Schema、`constraint_kind = PRIMARY_KEY | NON_UNIQUE_SECONDARY` 和 `IndexType`；每个用户表还必须能唯一恢复其 replicated logical primary-key column、类型与 codec version。V1 codec 遇到 `SECONDARY_UNIQUE` 或未知 constraint kind 必须拒绝，不保存任何索引 root/header page id。
- `next_table_oid`、`next_index_oid`。
- 快照边界处的 `schema_epoch`；它与 Manifest 中的副本必须交叉校验。
- 格式版本、记录数量、每段长度和 checksum。

第一版明确选择“所有索引物理结构（包括 primary-key index）均为 derived state”。快照中的 `db.bustub` 是在 barrier 内重新物化的 canonical table file，不携带可复用的索引物理页。每行必须至少保存以下逻辑信息：

```text
CanonicalRow {
  table_oid,
  primary_key,
  complete_tuple,
  latest_committed_version_ts
}
```

`latest_committed_version_ts` 必须保留该行最近一次真实提交的 Raft index，不能统一改写为快照边界 `S`。例如一行最后更新于 800、快照创建于 1000，则快照仍记录 800；否则快照后的 UPDATE/DELETE entry 所携带 `expected_old_commit_ts = 800` 会被错误判定为副本漂移。快照边界 `S` 只表示状态已包含日志前缀 `<= S`，不表示所有行都在 `S` 产生了新版本。快照 Catalog 中每个 TableHeap 的 `first_page_id` 必须指向这份新 canonical file。

恢复只能采用上文唯一顺序：必须先从 Snapshot@S 的 canonical rows 重建 primary-key index 与全部 secondary indexes 到 state@S，再令 `published_applied_index = last_applied = S` 并重放 suffix。primary index 是 `UPDATE_ROW/DELETE_ROW` 按逻辑主键定位旧行的正式路径，suffix replay 禁止依赖临时全表扫描，也禁止等日志重放完再建索引；`Log[S+1..effective_commit_index]` 期间由正常 `BusTubStateMachine::Apply` 增量维护所有 derived indexes，最后才开放服务。

## term-0 SnapshotManager 发布协议

以下物理封装只适用于 `state/CURRENT -> MANIFEST-N -> SNAPSHOT-N/`；M4 的 framed
`SnapshotStore` 发布协议在工作流 B 单独定义，不能把本段的 Manifest 步骤套到 distributed 目录。
`SnapshotManager`、`StateManifestCodec/Store` 与 `CommandLog` 都必须对任意非零 term fail-closed；
CommandLog 恢复遇到 committed 非零 term 必须拒绝启动，只能把 durable commit 之后的外来 term 当作
不可信 tail 截断。`SnapshotManager` 发布和 `StateManifestStore` 候选校验还必须逐条解码 Session response，
要求其 `term == 0`；否则即使外层 checksum 自洽也不得发布或恢复。共享 `LogCodec`/`StableStore`/
`WriteResponseCodec` 可表达非零 term 供 distributed 模式使用，不能因此放宽 term-0 物理 authority。

### M1：冻结 capture 的原子发布器

M1 不依赖 M2 的 CommandLog、StableStore commit marker 或三水位。调用者提供 Catalog/Session 引用、
`S/term=0` 元数据和 common `StateVisibilityLatch`；M1 publisher 自己获取 exclusive latch 形成内部自洽的
逻辑 capture。M1 拥有该 shared/exclusive primitive，并复用 M0 的 Session record/snapshot codec 负责以下发布：

1. M1 `CreateSnapshot` 内部获取 exclusive barrier，冻结并扫描 state@S；做必要 MVCC
   GC/canonicalization，确保内容不依赖旧进程内存。调用者不得预先获取同一非递归 latch。
2. 在同一文件系统创建 `SNAPSHOT-N.tmp/`，扫描 S 可见的 committed rows，保留每行原始
   `latest_committed_version_ts`，并序列化 Catalog、`schema_epoch`、OID 和 Session 内容。
3. 封闭 capture 后可释放 exclusive barrier；调用者的 mutation freeze 在 CURRENT 发布前仍保持，临时文件只再做持久化。
4. 对三个 canonical 文件计算 checksum，逐个 `fsync/fdatasync`，再同步临时目录；不以 Flush 旧 working file 代替。
5. rename 为不可变 `SNAPSHOT-N/` 并同步 `state/`；写入/同步/rename `MANIFEST-N.tmp` 后再同步父目录。
6. 写入并同步 `CURRENT.tmp`，原子 rename 覆盖 `CURRENT`，再次同步父目录，随后允许调用者解除 freeze。

任一步崩溃后，M1 只能选择旧完整 Snapshot 边界或新完整 Snapshot 边界。CURRENT 损坏时可按 generation
从新到旧扫描并校验 Manifest/三个内容文件；M1 不声称已把选中的边界重放到更高 commit。

### M2：commit orchestration、bridge 与联动回收

M2 在调用上述 M1 publisher 前后增加唯一属于 runtime/log 的步骤：

1. 关闭新的 term-0 append/Apply 准入，排空已准入转换；记录 `target=commit_index`。
2. 等待 `last_applied=published_applied_index=target` 且 Apply loop 空闲，并在整个发布期间继续持有
   admission/write freeze；把 Catalog/Session、latch 与 `S=target,term=0` 交给 M1，由 M1 内部获取
   exclusive latch capture。M2 不能在调用前重复锁住同一 latch。
3. CURRENT durable 后恢复准入。启动恢复选择候选 Snapshot@S 时，除 M1 的完整性校验外，还必须证明
   `Log[S+1..effective_commit_index]` 连续可用；否则该代不能作为恢复点。
4. 新代发布后可暂时有三代。先删除最老 Snapshot/Manifest 并同步目录，再把 CommandLog 回收上界推进到
   新的最老有效边界；删除顺序不能反过来。只有具备 bridge 的两代才计为两个恢复点。

## 建议修改或新增文件（按最早 owner 分组）

```text
# M0 executable feasibility / minimum canonical logical codecs and reopen
src/include/recovery/canonical_snapshot.h
src/recovery/canonical_snapshot.cpp
src/include/catalog/catalog_snapshot.h
src/catalog/catalog_snapshot.cpp
src/include/distributed/session_table.h      # WriteResponse frame + SessionRecord/SnapshotCodec foundation
src/distributed/session_table.cpp
src/include/storage/table/table_heap.h
src/storage/table/table_heap.cpp

# M1 immutable publication / shared capture / recovery selection hardening
src/include/recovery/node_directory.h
src/recovery/node_directory.cpp
src/include/recovery/state_manifest.h
src/recovery/state_manifest.cpp
src/include/recovery/snapshot_manager.h
src/recovery/snapshot_manager.cpp
src/include/common/state_visibility.h

# M2 term-0 commit/log/replay orchestration
src/include/recovery/command_log.h
src/recovery/command_log.cpp
src/include/recovery/log_codec.h
src/recovery/log_codec.cpp
src/include/raft/stable_store.h
src/raft/stable_store.cpp
src/include/recovery/single_node_runtime.h
src/recovery/single_node_runtime.cpp
src/include/distributed/command.h          # consumer wire model/codec; fixture 不调用 CommandBuilder
src/distributed/command_codec.cpp
src/include/distributed/bustub_state_machine.h
src/distributed/bustub_state_machine.cpp

# M5 extension, not part of the M2 recovery target
src/distributed/single_node_sql_runtime.cpp
src/include/common/bustub_instance.h
src/common/bustub_instance.cpp
```

同一源码文件可在后续阶段增加能力，但最早 owner 不改变：M1 在 M0 的 Session container 上增加
Snapshot@S boundary validation，M2 再增加 exact-once transition；测试必须按能力标注，不能把整文件重复归属。

不要直接把旧的页级 `LogManager` 改造成同时承担命令日志和 Raft 日志的混合类。可以复用文件 I/O 基础设施，但对外语义必须分开，避免未来误把 page LSN 当作 Raft index。

## 测试要求

### 单元测试

- `LogCodecTest`：正常编解码、最大长度、版本拒绝、checksum 错误、半 header、半 payload、segment 边界。
- `LogBaseSentinelTest`（M2）：空状态固定为 `snapshot_base=(0,0)`、`TermAt(0)=0`、首条真实 entry 为 1；覆盖空数据库 Snapshot@0 和 CommandLog 恢复，确认 index 0 永不被编码或 Apply。首条 AppendEntries 归 M3，SnapshotStore 空基线归 M4。
- `ManifestTest`：M1 覆盖原子发布、CURRENT/最新代损坏后选择上一完整 Snapshot 边界、路径逃逸拒绝；M2 再覆盖用 bridge log 恢复到当前 commit 与两代联动回收。Manifest 与 Catalog snapshot 的 `schema_epoch` 或 OID 副本不一致时拒绝该代。
- M0 的 Session 测试用预构造 `SessionRecord` 验证非空 WriteResponse frame、snapshot round-trip 和坏字节拒绝；
  M1 增加与 Snapshot@S 的 commit boundary；M2 再验证 `NEW/RETRY/TOO_OLD/GAP`、RecordCommitted、byte-identical response
  和恢复后的同请求重放。M5 只把这些 consumer 语义接到真实 SQL producer，不能把它们倒称为首次实现。
- `CatalogSnapshotTest`：M0 覆盖多表、多类型、普通索引 definition、OID 连续性、`schema_epoch` round-trip、
  未知 IndexType/版本拒绝、TableHeap reopen，以及 replicated logical primary key 定义及缺失/不受支持/
  mismatch、secondary UNIQUE definition 的 restore 拒绝，并确认格式不依赖任何索引 root/header page。
  M5 只验证 SQL parser/producer 在 proposal 前执行同一 policy 且无 Catalog/Raft 副作用，不能把静态 restore
  validator 倒称为首次实现。
- `DurableAppendTest`：单条和单次多 entry append 都只能在 `fdatasync` 后成功返回；同步失败时本次调用直接失败，不能确认部分 entry；两个独立 append 调用不要求被调度器合并。

### 单节点集成测试

- M0–M2 使用真实 `BusTubInstance`/临时目录和预构造的非空正式 entry payload，验证恢复 consumer 及表、索引定义、OID、`schema_epoch` 的字面状态；不把 SQL preparer 的正确性当成本层 oracle。
- M5 累计门禁再从真实 `CREATE TABLE/INDEX`、`INSERT/UPDATE/DELETE` SQL 生产 batch，并在快照前、快照后和 suffix 存在时重启查询。
- 构造“行最后更新于 `K < S`、Snapshot 边界为 S、Log S+1 的 UPDATE 携带
  `expected_old_commit_ts = K`”的恢复场景；先断言 Snapshot@S 的 primary/secondary indexes 已完成重建，
  再允许 replay S+1。UPDATE 必须通过 primary index 定位且恢复后 commit timestamp 为 S+1；
  测试配置禁止 fallback 全表扫描，也禁止 replay 后才建索引。
- M1 让 reader 持有 `StateVisibilityLatch shared` 时启动快照，确认 snapshot capture/GC 在 exclusive lock 处等待；释放 reader 后得到的快照只能对应一个完整 capture 边界。三水位对齐与 runtime 准入由 M2 测试，Apply-vs-reader 的整批发布由 M5 测试。
- 比较 SQL 查询得到的逻辑结果；不要比较 `db.bustub` 的原始字节，因为合法的物理布局可以不同。

### 崩溃点测试

- 公共命名故障框架固定 `before_write / after_fsync / after_rename / after_dir_fsync`
  事件和 occurrence/path topology。M1/M2 只注册 SnapshotManager、CommandLog 与 term-0 StableStore 的适用点，
  单个原子发布使用 old-or-new oracle。M3 扩展 StableStore/LogStore，M4 扩展 InstallSnapshot；
  InstallSnapshot 的跨文件中间 durable 组合必须用 `max(H,S)`、pre-install `TermAt(S)`、
  committed-range 连续性和 fail-stop 专用 oracle，不得简化成单文件 old-or-new。
- 掉电模型同时维护 volatile image 和 durable image；普通 `SIGKILL` 只能证明进程崩溃，不能代替断电后缓存未落盘测试。
- M1/M2 的单次 SnapshotManager 代发布以及各 Store 的单文件原子 mutation，在每个崩溃点后只允许恢复
  完整旧逻辑状态或完整新逻辑状态，禁止混合两代文件；本条不覆盖 M4 InstallSnapshot 的跨文件中间态。
- 构造 Snapshot 7000、Snapshot 8000 和 commit 8500，损坏当前快照后必须由 Snapshot 7000 + Log 7001..8500 恢复；再生成 Snapshot 9000，验证先删 Snapshot 7000、再删除 `<= 8000` 日志的 crash ordering。
- M2 在 term-0 的预构造 entry append/commit、Apply、`StateVisibilityLatch` 获取、MVCC canonicalization、
  逻辑 capture 与新 SnapshotManager 文件同步边界请求快照，验证
  `Snapshot(S) = Apply(Log[1..S])` 且不含 speculative state。M5 只累计增加真实 SQL prepare 边界；
  M4 先以 KV FSM 对 distributed proposal/commit/Apply + `SnapshotStore` 验证同一逻辑等式，M6 只在
  正式 BusTub 装配中累计复验；二者都不能调用 term-0 CURRENT/Manifest 路径。
- 保持 working Buffer Pool 含未 Flush 的 committed dirty pages，完成逻辑扫描与 canonical snapshot 落盘后模拟崩溃；恢复必须只依靠新快照成功，证明 snapshot correctness 不错误依赖 working file FORCE。

## 验收标准

- M2 term-0 链路完成“持久化预构造 entry -> consumer Apply -> 杀进程 -> 重启 ->
  字面逻辑状态一致”；M5 再完成“真实 autocommit SQL -> batch -> 同一恢复链路”的累计门禁。
- 截断任意 uncommitted 日志尾字节后，能恢复最后完整记录且不 Apply 半条记录；损坏 committed range 必须 fail-stop。
- 任一快照发布步骤模拟掉电后，均能选择一代完整快照恢复。
- Catalog、TableHeap、全部索引 definitions、OID 和 `schema_epoch` 在重启后正确；canonical rows 保留各自原始 `latest_committed_version_ts`，所有索引均由这些 rows 重建。
- 连续执行多轮 snapshot/recovery，canonical snapshot 不携带上一轮 derived index pages，文件空间不会因不可达索引页代际累积。
- 日志未完成持久化时，客户端绝不收到成功。
- 最近两代以外的 Manifest/快照被安全回收；长期运行不会无限堆积快照。
- 当前代损坏时，上一有效代及其 bridge log 能恢复到 `effective_commit_index`；不存在“保留了快照却删掉恢复日志”的伪 fallback。

## 输出要求

- 单机恢复设计说明和磁盘格式说明。
- 每个持久化边界的注释与对应测试编号。
- 单机恢复测试报告，列出注入的全部崩溃点。

---

# 架构工作流 B（M3–M4）：独立 KV 状态机上的 Raft

## 背景

在把 M2 已验证的 BusTub consumer 接入 Raft 前，先用很小的 KV FSM 隔离验证 Raft 本身的安全性。
这样选举或日志冲突错误不会被 SQL producer、Catalog 和 MVCC 的复杂行为掩盖。

## 目标

实现持久化的 Raft 核心：

- Follower/Candidate/Leader 状态转换与随机选举超时。
- `RequestVote`、`AppendEntries`、心跳和 term 更新。
- 日志匹配、冲突提示与 `next_index/match_index` 回退。
- 多数提交、严格顺序 Apply、当前 term no-op。
- 快照创建、日志压缩和 `InstallSnapshot`。
- 节点重启、网络分区、丢包、重复包、乱序和旧 Leader 回归。

M3 的退出边界到选举/投票、AppendEntries/冲突回退、多数提交、current-term NOOP、
新鲜 ReadIndex 和无快照的持久 KV 重启为止。M4 唯一拥有 SnapshotStore、压缩、
InstallSnapshot 分块/重传、两次 stale guard、suffix 保留与 compacted follower catch-up。
旧 Leader 冲突算法归 M3，结合 durable restart/snapshot 的回归归 M4，真实 TCP 三进程证据归 M6；
这是同一风险在不同边界的不同 oracle，不是重复实现。

## 本工作流只做

- 三个静态 voter、单 Raft Group、内存 KV FSM。
- M3 以进程内可控 Transport 和固定 RPC codec 验证协议；TCP Transport 可做独立组件验证，
  但首次组装进 production 三进程链路唯一归 M6。
- 复用 M2 的 `StableStore` 物理格式，并使 `LogStore`、M4 `SnapshotStore` 与 FSM 明确分层。
- Raft core 把 application payload 当作 opaque bytes，并经 `RaftStateMachine::ValidateProposalPayload`
  委托状态机做类型/格式/admission 校验；`src/raft` 不得 include `distributed/*`。M3 KV 实现只接受
  `KV_COMMAND`，M5 BusTub adapter 再实现 `COMMAND_BATCH`，RaftNode 不直接解析任一业务 codec。
- 每个节点仍使用独立真实磁盘目录。
- 生产节点使用相同配置的选举超时区间，并在每次 deadline reset 时独立重新抽样；测试通过固定 seed 或
  timeout source 注入确定性序列。禁止依赖操作者为三个节点手工设置不同固定常量。

## 本工作流不做

- BusTub SQL/MVCC 集成。
- 动态成员、learner、分片、跨组事务。
- 依赖 sleep 碰运气的选举测试。

## 持久化状态

```cpp
struct HardState {
  uint64_t current_term;
  std::optional<NodeId> voted_for;
  uint64_t commit_index;
};
```

- `current_term/voted_for` 必须作为同一 HardState generation 持久化。任何观察到 higher term、Candidate 自增 term 或修改 `voted_for` 的路径，都必须先越过这一 durable boundary，再发送依赖新状态的 RPC。
- Follower 必须在回复 `AppendEntries(success=true)` 前持久化新增日志或完整的冲突后缀替换。
- 本项目额外持久化 `commit_index`，使重启时可以安全重建 working state；它只能单调增加。
- `last_applied` 属于可重建状态，可保存在工作元数据中加速，但不能超过恢复后计算出的 `effective_commit_index`。

## HARD_STATE 原子落盘协议

禁止用 `write(sizeof(HardState))` 原地覆盖正式文件。第一版使用 generation + checksum 的原子替换：

```text
编码完整 HardState record 到 HARD_STATE.tmp
  -> fsync(HARD_STATE.tmp)
  -> rename(HARD_STATE.tmp, HARD_STATE)
  -> fsync(raft/ directory)
  -> 才允许发送依赖该状态的 RPC response
```

record 至少包含 `format_version/generation/current_term/voted_for/commit_index/checksum`，使用固定字节序和显式长度，不直接 dump C++ struct。同步 `StableStore::Update` 只有在文件和父目录都同步后才能成功返回；调用者在此之前不得发送 granted/success response、发送新 term 请求、推进 Apply 或宣称新的 commit durable。崩溃发生在 rename 前时旧 generation 仍可信；rename 后只有完成父目录同步才允许对外声称投票、term 或 commit 已持久化。启动时忽略或隔离遗留的 `.tmp` 文件，只读取正式 HARD_STATE；正式文件 checksum 或版本错误必须 fail-stop，不能从半条字段猜 term/vote。generation 必须严格递增，StableStore 内部串行化所有更新。

higher-term transition contract 固定为：

```text
收到任意 RPC/request/response，其 term = T_new > current_term
  -> 立即停止旧 Leader/Candidate 的服务与发包能力，进入 TermPersisting
  -> 构造 HardState { current_term = T_new, voted_for = null, commit_index }
  -> 同步调用 StableStore::Update，成功返回表示已原子持久化
  -> 切换为 Follower，继续处理原消息
  -> 才能发送属于 T_new 的 response 或后续 RPC

Candidate election timeout
  -> 构造 HardState { current_term = current_term + 1,
                       voted_for = self,
                       commit_index }
  -> 同步调用 StableStore::Update，成功返回表示已原子持久化
  -> 才能广播 RequestVote
```

该规则覆盖 RequestVote 的同意与拒绝、AppendEntries、InstallSnapshot，以及 Leader/Candidate 收到 higher-term response 后的降级；StableStore 不能只绑定 granted-vote path。持久化等待期间节点不得继续以旧 Leader 身份处理读写。本项目持久化 `commit_index` 是为了让节点在重新加入集群前确定本地 committed replay 边界而主动采用的恢复简化，并非 Raft safety 对 HardState 的原始要求。V1 接受每次 commit 的 tmp + fsync + rename + directory fsync 成本，本方案不再增加其性能优化。

## 跨文件 crash ordering 与恢复规则

`HARD_STATE`、Raft `SnapshotStore` 的 framed `SNAPSHOT-N`/`CURRENT` 与 `LogStore` 是不同文件，
不能假定它们在一次原子写中更新。这里不参与 term-0 `state/MANIFEST-N`。
设当前校验通过的快照边界为 `S = last_included_index`，持久化 HardState 中的提交位置为 `H`：

```text
effective_commit_index       = max(H, S)
recovered_last_applied        = S
recovered_published_applied_index = S
```

正式发布的 Raft Snapshot 本身证明 `<= S` 已提交，因此 `S` 是 commit index 的 durable lower bound。若 `effective_commit_index > S`，恢复必须验证并重放连续的 `Log[S+1..effective_commit_index]`；缺少任一条时不得降低 commit index 或开放服务，只能 fail-stop，或在加入集群后先从 Leader 获取缺失日志/更新快照。日志已压缩时，`last_log_index` 的逻辑下界至少为快照边界 `S`。

收到远端 `Snapshot(S,T)` 时，先按 higher-term contract 处理并持久化 RPC term，再执行不会改变 Snapshot/Log/FSM 状态的 stale guard：

```text
P = published_applied_index

if S <= P:
    ignore_as_stale_or_idempotent()
    # CURRENT、snapshot_base、LogStore、working FSM、last_applied、P 全部不变
```

`S == P` 也不重新安装 working state；V1 直接视为幂等 no-op。初次检查只能避免明显无效的传输，因为 Snapshot 下载期间 Apply 可能继续推进 P。临时文件完整校验后，安装任务必须进入与 FSM Apply 相同的单线程序列，在发布 CURRENT 前重新读取 P 并执行同一 guard；只有第二次满足 `S > P` 才能冻结后续 Apply 并进入真正安装流程。若第二次已变成 `S <= P`，删除/隔离临时下载并按已忽略请求回复，不能修改 CURRENT、snapshot base、LogStore、working FSM 或两个 applied index。若 RPC 自带更高 Raft term，只有前述独立的 HardState term 更新仍必须 durable，不能因为 Snapshot stale 而跳过。

通过最终 stale guard 后，suffix 决策必须使用**安装前**的本地逻辑日志视图：

```text
old_term = PreInstallLog.TermAt(S)  # 可来自旧 snapshot_base 或尚未压缩的 entry
retain_old_suffix = old_term.exists && old_term == T

if retain_old_suffix:
    retained = PreInstallLog.entries(index > S)
else:
    if max(H, S) > S:
        fail_closed_before_durable_publication()
    retained = empty  # only when E == S

new snapshot_base = (S, T)
```

新下载或新发布 Snapshot 自己携带的 T 不能反过来充当 `PreInstallLog.TermAt(S)` 的匹配证据。只比较
`index > S` 也不成立。若 `E>S`，还必须在同一只读 preflight 中证明连续
`Log[S+1..E]`；term/连续性任一不可证明，除前文独立要求的 higher-term
`HardState.current_term/voted_for` transition 外，都必须在改 CURRENT、`HardState.commit_index`、LogStore
或 FSM 前失败。
即使保留 suffix，后续 AppendEntries 仍按正常冲突规则校验和修复。

InstallSnapshot 固定采用以下可崩溃顺序：

1. 收到 metadata 时执行首次 stale guard；仅当 `S > published_applied_index` 时下载到临时目录并校验、同步所有文件。
2. 将安装任务提交给单线程 FSM Apply/Install 序列，在其中执行最终 stale guard。若 `S <= published_applied_index`，以 no-op 结束；否则从这里到第 6 步禁止其他 Apply 穿插，并从旧 LogStore view 计算 `retain_old_suffix`。同时计算 `E=max(H,S)`：若 `E>S`，必须先证明 `retain_old_suffix` 且 `(S,E]` 连续；失败时取消 staged 临时文件并 fail-stop/reject，以下权威步骤一个都不能执行。
3. 通过 `SnapshotStore` 写入/同步 framed `SNAPSHOT-N.tmp`、rename 为 `SNAPSHOT-N`、
   同步目录，再原子发布/同步 `CURRENT`；从这一步起 `S` 已成为 durable commit lower bound。
4. 持久化 `HARD_STATE.commit_index = max(H, S)`。
5. 调用 `LogStore::InstallSnapshotBase(S, T, retain_old_suffix)`，以一个 durable framed mutation 建立
   `snapshot_base=(S,T)`；匹配时保留全部 `index>S` entries，只有 preflight 已证明 `E==S` 的不匹配路径
   才能丢弃旧 suffix。完成后再按最老有效恢复点规则回收前缀。
6. 通过 FSM 自己的原子发布屏障切换 working state，令 `published_applied_index = last_applied = S`，
   随后按顺序 Apply 保留或由 Leader 补齐的后缀 entry。M4 的 KV FSM 在单线程 Apply/Install 序列内
   直接替换；M5 BusTub FSM 复用 M1 common `StateVisibilityLatch exclusive`，M4 不拥有第二套 latch。

在第 3、4、5、6 步任意位置崩溃，重启都按上述 `E=max(H,S)` 规则解释，而不是把四份文件拼成一个
不存在的原子事务。若第 3 步已发布而第 5 步尚未 durable，恢复必须针对仍在磁盘上的 pre-install log
material 重做同一 term/continuity 证明；当前 Snapshot 的新 base 不能自证匹配。若 `E>S` 且证明失败，
必须保持权威 LOG/HARD_STATE/CURRENT 字节并 fail-closed，绝不能先丢 suffix 再发现 committed range 缺失；
只有 `E==S` 时，完整验证 Snapshot 内层状态后才允许丢弃不可信 suffix。关闭服务的启动恢复期间，working
FSM 与两个 applied 水位允许先从 Snapshot@S 重建，再连续 replay 到 E；完成归一化前不得开放读写。
服务已开放后，对外可观察的逻辑状态和两个 published/applied 水位不得回退。

## 新 Leader 为什么不会缺少已提交日志

Raft 不是“指定只能从收到该日志的节点中选举”，而是通过多数交集和投票时的日志新旧比较自然排除不合格候选者：

1. 日志只有持久化到多数节点后才 committed。
2. 任何新一轮获胜者也必须得到多数票；两个多数集合必有至少一个交集节点。
3. `RequestVote` 携带候选者的 `last_log_term/last_log_index`。投票者只有在候选者日志至少与自己一样新时才投票。
4. 多数交集、Election Restriction 和 Log Matching Property 共同保证缺少已提交条目的候选者无法成为后续 Leader；不能只实现“多数票”而省略日志新旧比较。
5. 如果旧 Leader 只发给少数节点就崩溃，该条目尚未提交，可以由新 Leader 覆盖；客户端也不能收到成功。

日志“至少一样新”的比较必须先比较最后一条日志的 term，再比较 index，不能只比较长度。新 Leader 当选后先提交一条当前 term 的 NOOP，并 Apply 完既有已提交前缀，再开始服务客户端写入。NOOP 虽不修改数据库、Catalog 或 SessionTable，仍必须作为普通已提交 entry 经过有序 Apply，并把 `published_applied_index/last_applied` 推进到自身 index；否则当前 term 已提交但线性一致读的发布水位会永久落后。Leader 只能按多数 `match_index` 直接提交当前 term 的条目；旧 term 条目随当前 term 条目一并被确认提交。

## AppendEntries 要求

- 请求携带 `prev_log_index/prev_log_term`、连续 entries 和 `leader_commit`。
- Follower 只有在前缀匹配后才追加；同 index 不同 term 的条目及其后缀必须被截断。
- Follower 返回冲突 term 和该 term 的首 index，Leader 用提示跳跃回退，避免逐条探测。
- 重复 RPC 必须幂等；乱序的旧 term RPC 必须拒绝。
- 推进 `commit_index` 后先持久化 HardState，再通知单线程 Apply loop。

## LogStore 冲突后缀替换协议

Follower 对冲突日志调用一个具有明确 durable 语义的操作：

```cpp
void ReplaceSuffix(from_index, new_entries)
```

第一版不采用“原地 truncate 文件，再逐条 append”的两步协议。M3+ `LogStore`
是单个 checksummed mutation journal `LOG-MUTATIONS`：普通 `Append` 向当前 journal 追加一个 durable
frame；`ReplaceSuffix` 与 `InstallSnapshotBase` 则编码完整的 canonical journal 到
`LOG-MUTATIONS.tmp`，执行 `fsync -> rename -> directory fsync` 后替换。它不是 M0–M2
分段 `LOG-*` 的原地扩展。

因此恢复只能看到完整旧 journal 或完整新 canonical journal，不会重新显露已被替换的
物理后缀。`from_index <= effective_commit_index` 必须拒绝并 fail-stop。只有新 journal
完成文件和目录持久化后，同步调用才可成功返回并回复 `AppendEntries(success=true)`；
该 success 同时证明冲突删除与新后缀已作为一个 durable 逻辑操作发布。

## distributed 本地快照发布要求

- M4 的发布输入是状态机生成的完整文件 payload；KV FSM 用它验证 Raft 协议。M5 唯一实现 canonical
  BusTub `db/catalog/session` bundle 的 `CreateSnapshotFile/InstallSnapshotFile` hooks，M6 只把该 FSM 装配进
  M4 RaftNode 和正式节点。M4 不读取或生成 term-0 StateManifest。
- 仅在 `last_applied = published_applied_index = commit_index = S` 的稳定边界 capture，并在任何日志压缩前
  从 pre-compaction `LogStore::TermAt(S)` 取得 T；无法取得 T 时取消，不能猜测。
- `SnapshotStore::PublishFile` 将 payload 包成单个 checksummed `SNAPSHOT-N.tmp`，同步文件后 rename 为
  `SNAPSHOT-N` 并同步 `raft/snapshots/`；随后以同样的 write/fsync/rename/dir-fsync 顺序发布该目录的
  `CURRENT`。distributed 外层没有 `MANIFEST-N`，也不使用 `state/CURRENT`。
- 发布新代后，`OldestRetained()` 只有在 `Snapshot@old + Log[old+1..commit]` 连续可用时才算 fallback。
  LogStore 的压缩基点最多推进到这个最老有效边界；不能仅因 latest 已发布就删除 bridge。
- 如果安装远端 Snapshot@S 时没有从旧快照到 S 的 bridge，旧快照必须移出有效恢复点集合并只保留新代；
  等以后再成功生成一代本地快照，才重新形成两个有效恢复点。
- 本地 capture 不切换 HardState 或 FSM；若 CURRENT 已发布而 LogStore 尚未推进恢复基点，启动时用原日志
  验证 latest `(S,T)` 并保留可证明匹配的 bridge/suffix，不能因为 `H == S` 就无条件重建成空 suffix。

## 快照安装要求

- 快照的内层逻辑内容复用 M0 定义的 canonical `db/catalog/session` 一致状态，
  但外层由 M4 `SnapshotStore` 封装成带 `last_included_index/term` 的单个 framed 文件；
  它不复用 M1/M2 的 StateManifest 物理发布协议，也不是只复制裸 `db.bustub`。
- 分块传输必须带 snapshot ID、offset、总长度和 checksum；重复块不产生副作用。Leader 从已发布快照文件按 offset 读取固定上限块，Follower 按 offset 追加到临时文件并逐块同步，禁止在任一端把完整 payload 物化为单个内存 vector。
- Leader 为每个 Follower 保留一个活动传输和一个 in-flight 块。heartbeat 重发该块时必须保持 request ID、offset
  和 bytes 不变；只有与活动 request ID 匹配且证明 durable high-water 不小于本块末端的 ACK 才能推进 offset。
  Follower 收到已经 durable 的旧块时报告真实 high-water，而不是简单回显请求末端；最终 COMPLETE ACK 丢失后，
  Leader 必须能通过重复末块失败关闭、从 offset 0 重启和 stale-complete 收敛，且不重新安装或回滚状态。
- Follower 下载到临时目录，完整校验、同步并按“跨文件 crash ordering”发布后，才能更新日志起点和 Apply 状态。
- 完成外层长度/checksum 校验后，还必须先经 `RaftStateMachine::ValidateSnapshotFile` 完整验证内层 KV 或
  BusTub bundle；该步骤只能构造并清理非权威 candidate，不能切换 working FSM。higher-term request 仍须先
  按全局不变量持久化 term；除此独立 transition 外，只有内层验证成功后才能发布 CURRENT、推进 HardState
  commit index、重建/裁剪 LogStore 或安装 FSM。
- 远端安装中 CURRENT 已发布而 HardState/LogStore/FSM 尚未切换的崩溃不是单文件 old-or-new 事务；
  启动必须解释 `max(H,S)`、pre-install term、suffix continuity 与 fail-stop。
- metadata 到达时和正式发布前各执行一次 `S <= published_applied_index` stale guard；第二次判定在单线程 Apply/Install 序列中完成。stale/duplicate Snapshot 只能被忽略，不能借机压缩日志或替换 working state。
- 对通过最终 stale guard 的 Snapshot，`SnapshotStore/LogStore` contract 明确采用
  `PreInstallLog.TermAt(S) == snapshot.last_included_term` 作为唯一 suffix 保留条件：相等时保留全部
  `index>S` entries；不相等/不存在时只有 `E==S` 才能丢弃旧 suffix 并建立 `snapshot_base=(S,T)`，
  `E>S` 必须在任何权威发布前 fail-closed。
- 如果安装时不存在旧快照到 `S` 的 bridge log，旧快照不得继续标记为 fallback；此时只有新快照是有效恢复点。

## 建议修改或新增文件（按最早 owner 分组）

```text
# M3 Raft core / durable election-log / controllable transport
src/include/raft/raft_node.h
src/raft/raft_node.cpp
src/include/raft/raft_types.h
src/include/raft/log_store.h
src/raft/log_store.cpp
src/include/raft/transport.h
src/raft/tcp_transport.cpp
src/include/raft/state_machine.h
src/raft/kv_state_machine.cpp

# M4 snapshot/compaction/install and cross-file startup recovery
src/include/raft/snapshot_store.h
src/raft/snapshot_store.cpp
src/include/raft/persistent_state.h
src/raft/persistent_state.cpp
# raft_node/log_store/state_machine 上的 snapshot extension 也归 M4
```

可以参考 HashiCorp Raft 的组件边界和存储接口，但协议安全规则以 Raft 论文为准；不要把第三方库的 API 形状直接嵌入 BusTub 执行器。

## 测试要求

### 确定性协议测试

- **M3**：由 `ManualClock`、固定随机源和 `InMemoryTransport` 驱动逻辑时间，不通过真实 sleep 猜选举结果。
- **M3**：覆盖 sentinel `(0,0)` 上的首次选举、首条 AppendEntries 与首个 current-term NOOP，再覆盖单候选者当选、分票后重新选举、旧 term RPC、重复投票、日志不够新的候选者被拒绝。
- **M3**：在 StableStore 的命名持久化事件注入失败，分别触发 Candidate 自增 term、higher-term
  RequestVote（包括拒绝票）、AppendEntries 和 RPC response；同步调用成功返回前不得发送新 term
  RPC/response，也不得继续旧 Leader 服务，返回后才能继续状态转换。**M4** 再把同一 higher-term
  contract 扩展到 InstallSnapshot request/response，不把该 RPC 倒置成 M3 前置。
- **M3**：覆盖快速冲突回退、Follower 多余后缀替换、重复 AppendEntries、乱序响应。
- **M3**：对 `ReplaceSuffix` canonical journal 重写的 `before_write / after_fsync / after_rename /
  after_dir_fsync` 逐点崩溃，恢复结果只能是完整旧后缀或完整新后缀，不能出现混合状态；
  success reply 只能发生在新 journal 和父目录 durable 之后。
- **M3**：损坏 uncommitted tail 可以截断；损坏或缺失 committed range 必须 fail-stop，不能把 `effective_commit_index` 从 100 降到 99。
- **M3**：覆盖两种关键故障：只复制到少数节点后 Leader 崩溃，该日志被覆盖；复制到多数节点后 Leader 崩溃，新 Leader 必须保留并最终 Apply。
- **M3**：在 Leader 本地 LogStore 的命名持久化事件注入失败，断言同步调用未成功返回时不会发送该 entry 的 AppendEntries，也不会推进 commit；本地 durable 后才允许开始复制。
- **M3**：ReadIndex fresh-round 测试先让旧 Leader 收到一次完整 heartbeat quorum ACK，再隔离它并选出新 Leader；随后到达旧 Leader 的读必须创建新 context，旧 ACK、错误 context/term ACK 都不能完成该读，最终只能超时或因降级失败。

### 组件故障测试

- **M3**：三节点真实 `LogStore`，轮流重启每个节点，验证 term、vote、日志与 commit 恢复；覆盖双向分区、单向丢包、重复包、消息延迟、旧 Leader 回归和无压缩日志的 Follower 追赶。
- **M3**：对 HARD_STATE 临时文件写入、文件同步、rename 和目录同步逐点崩溃；RequestVote/AppendEntries
  response 或 Candidate RequestVote 只能在完整可信 generation durable 后发出。**M4** 再覆盖
  InstallSnapshot response 的同一约束。
- **M4**：快照覆盖落后节点所需日志后，Follower 必须经 `InstallSnapshot` 追上。
- **M4**：让真实 `StageChunk` 的 fsync ACK 跨越至少一个 heartbeat，断言 Leader 重发同一 request ID/offset/bytes；预置多个
  durable 块后从 offset 0 开始，断言 Follower 返回实际 high-water 且 Leader 直接跳进。再丢弃最终 COMPLETE ACK，
  验证重复末块失败关闭、下一 heartbeat 从 0 重启、已发布 Follower 返回 stale-complete、原 ACK 任意晚到也无副作用。
- **M4**：分别构造本地 `TermAt(S) == T` 与 `TermAt(S) != T/不存在`：前者保留完整 `index > S`
  suffix；后者只有 `E=max(H,S)==S` 才能丢弃不可信 suffix，`E>S` 必须在任何权威字节变化前 fail-closed。
  禁止用新 Snapshot base 自证匹配。本地创建 Snapshot 时验证 T 在 compact S 前取得。
- **M4**：stale Snapshot 覆盖三种情况：到达时 `S < P`、重复安装 `S == P`、开始下载时 `S > P` 但发布前 Apply 已推进到 `P >= S`。三者均断言 CURRENT/snapshot base/LogStore/FSM digest/`last_applied/P` 完全不变且索引单调不减；额外用 higher-term stale Snapshot 验证只有 HardState term 先 durable，FSM 仍不回滚。
- **M4**：用外层 CRC 正确但内层 KV frame 无效的 live InstallSnapshot，证明除 higher-term contract
  独立要求的 durable term transition 外，在 CURRENT/HardState commit index/LogStore/FSM 任一权威变化前拒绝；
  再在发布 Snapshot、更新 HardState、更新日志基点和切换 FSM 的每个间隙
  崩溃，重启后验证 `E=effective_commit_index=max(H,S)`、suffix term 决策以及所需日志连续性。无法从
  pre-install material 证明匹配时，只有 `E==S` 可走经完整内层验证的保守丢弃，`E>S` 必须 fail-closed。
- **M5**：再用外层 CRC 正确但内层 BusTub bundle 无效的 live InstallSnapshot 累计复验同一 generic
  validation/authority-ordering 边界；M4 不依赖 BusTub snapshot hook。

### 性质检查

每次状态变化后检查：

- 同一 term 最多一个 Leader。
- 每个节点 `commit_index` 与 `last_applied` 单调不减。
- 任意两个节点在相同 index、term 上的日志前缀一致。
- 已提交 index 上所有已 Apply 节点的 KV 状态一致。
- 已提交命令在单节点上最多 Apply 一次可见副作用。

## 验收标准

- 固定随机种子下，故障调度测试可重复，不出现偶发性依赖。
- 三节点在任意单节点失效时仍可选举和提交；失效节点恢复后能追赶。
- 已提交日志在 Leader 崩溃和旧 Leader 回归后不丢失；未提交日志可被安全覆盖。
- 快照安装完成后，KV 逻辑状态与 Leader 一致，并可继续接收后续日志。
- HARD_STATE 任一 torn-write 点只能恢复旧或新完整 generation；已回复的 granted vote 不能在重启后丢失。
- ReplaceSuffix 任一崩溃点只能恢复完整旧或新逻辑后缀；已 committed 日志绝不因尾部修复而降低。

## 输出要求

- Raft 状态机转换图、RPC 字段和持久化时序说明。
- 安全不变量到测试用例的映射表。
- 固定种子的故障测试复现命令。

---

# 架构工作流 C（M5）：SQL 到 canonical CommandBatch producer 与 BusTub FSM 完整性强化

## 背景

M2 已冻结并恢复 consumer 侧 `TransactionCommandBatch`、Session response 和 committed Apply。M5 不复制
原始 SQL，而是把一条 autocommit statement 编译成该格式的确定性 producer，并补齐所有 V1 command 的
SQL shape/unsupported 语义准入、canonicalization、跨实例确定性和 BusTub Raft snapshot hooks。已构造 batch
相对当前状态的 proposal admission 属于 M2，M5 producer 复用它而不重新拥有。一个 `UPDATE/DELETE`
statement 可以展开成许多行 mutation，因此 batch 仍然是有意义的原子复制单位。多语句 non-interactive
transaction、跨语句私有 Table/Catalog overlay 和跨语句约束验证推迟到 V2，避免其实现复杂度淹没 Raft 主线。

## 目标

建立以下 producer→既有 consumer 边界；M5 的测试不伪造 majority/Leader 响应作为自身 oracle：

```text
客户端 autocommit DDL/DML statement
  -> 在已追至 committed boundary 的 BusTub 状态上解析、绑定、校验并物化 mutations
  -> 生成版本化 TransactionCommandBatch
  -> 测试/上层注入带 term/index 的 committed ReplicatedLogEntry
  -> 两个独立 BusTub FSM 按 index 串行 Apply
  -> 比较字面 rows/Catalog/index/OID/epoch/Session response
```

Raft 多数持久化、Leader 等待本地 Apply 和正式客户端响应唯一归 M6。

## CommandBatch 格式

```cpp
struct TransactionCommandBatch {
  uint32_t format_version;
  uint64_t client_id;
  uint64_t request_id;
  uint64_t expected_start_schema_epoch;
  std::vector<Command> commands;
};
```

命令至少包括：

```text
CREATE_TABLE {
  table_oid,
  primary_index_oid,
  table_name,
  schema,
  primary_key_definition
}

CREATE_INDEX {
  index_oid,
  table_oid,
  index_name,
  key_columns,
  index_type,
  constraint_kind
}

INSERT_ROW
UPDATE_ROW
DELETE_ROW
```

V1 命令集精确到上述五类。`DROP TABLE`/`DROP INDEX` 不存在“若当前支持”的隐式分支；
它们必须在 proposal 前以稳定 unsupported 结果无副作用拒绝，直到独立的版本化协议扩展被定义。

DDL OID 是 Leader prepare 已决定并写入二进制 Command 的状态转换输入，不是 Follower Apply 时的隐式选择。由于 V1 每张表必须有 primary index，`CREATE_TABLE` 同时显式携带 `table_oid` 与 `primary_index_oid`；显式 CREATE_INDEX 携带自己的 `index_oid/table_oid`。V1 单写 prepare 从 committed Catalog allocator 读取候选 OID，但不提前修改公开 allocator；committed Apply 必须校验 command OID 等于本地对应 `next_table_oid/next_index_oid`，使用精确 OID 创建对象，然后逐项推进 allocator。Follower、恢复 replay 和 Leader Apply 都不得调用本地 allocator 重新决定 OID；不匹配表示状态漂移并 fail-stop。未提交 proposal 不消耗 OID。

DML 使用逻辑身份和完整值，而不是复制页面字节：

```text
INSERT_ROW { table_oid, primary_key, complete_tuple }
UPDATE_ROW { table_oid, primary_key, expected_old_commit_ts, expected_old_tuple, complete_new_tuple }
DELETE_ROW { table_oid, primary_key, expected_old_commit_ts, expected_old_tuple }
```

不能把 RID 当作长期逻辑身份，因为不同节点的物理页分配和索引重建不应成为协议正确性的前提。

## V1 replicated table 准入规则

V1 distributed mode 对所有可写用户表强制执行以下协议，不存在“只在本节点写、不进入 Raft”的例外：

- `CREATE TABLE` 必须声明恰好一个单列、`NOT NULL` 的 `PRIMARY KEY`；V1 `PrimaryKeyCodecV1` 白名单固定为 `INTEGER`、`BIGINT` 和 `VARCHAR`。复合 `NOT NULL` 主键和额外的确定性、非空标量 key type 可以分别作为后续扩展；nullable primary key 与主键身份不变量冲突，永久不接受，不能列为 V2 能力。
- `INTEGER` wire encoding 固定为 4-byte signed two’s-complement big-endian，`BIGINT` 固定为 8-byte signed two’s-complement big-endian；禁止 `memcpy` host-native C++ value 或依赖 host endian、padding、compiler ABI。其 canonical comparator 使用有符号数值升序。
- `VARCHAR` 主键 wire encoding 固定为“4-byte unsigned big-endian 长度 + 原始字节序列”。identity equality 是 raw-byte equality；primary-index 与 canonical-command comparator 对 raw bytes 做 unsigned lexicographic comparison，区分大小写、尾随空格有意义、不做 Unicode normalization、不使用 locale-dependent collation。该 `PrimaryKeyCodecV1::CanonicalCompare` 必须与 BusTub V1 stable Value equality/primary-index comparator 一致，否则该主键定义在 proposal 前拒绝。
- Parser/Binder/CommandBuilder 在 proposal 前检查该定义。缺少主键、多个主键或类型不受支持时返回稳定的 `UNSUPPORTED_REPLICATED_PRIMARY_KEY`，不得分配公开 OID、修改 Catalog 或追加 Raft log。
- V1 只支持 primary-key uniqueness 和 ordinary non-unique secondary index。`CREATE UNIQUE INDEX`、非主键 `UNIQUE` constraint 或其他需要 deferred constraint checking 的 DDL 返回 `UNSUPPORTED_DEFERRED_UNIQUE_CONSTRAINT`，必须在 proposal 前无副作用地拒绝；unique index 也不能事后充当 replicated table identity。
- `INSERT_ROW/UPDATE_ROW/DELETE_ROW` 中的 `primary_key` 使用版本化 `PrimaryKeyCodecV1` 编码。V1 拒绝修改主键列的 UPDATE；需要改变主键时由客户端显式 DELETE 后 INSERT，并分别遵循 autocommit 语义。
- 节点启动恢复和 InstallSnapshot 在开放服务前验证每个用户表的主键列存在、非空、类型/codec version 受支持且与 Catalog primary-key definition 一致，并拒绝包含 secondary UNIQUE definition 的 V1 Catalog。任一表无法确定 replicated logical key 时 fail-stop，并要求离线迁移或重新安装合法快照，不能等到第一条 UPDATE 才报错。

## V1 prepare 与提交边界

- M5 preparer API 的前置条件是“调用者已串行化写准备，且状态已追平 committed boundary”；
  `DistributedNode` 的 single-active-write gate、timeout 和 overwritten proposal 处理唯一归 M6。
- prepare 只读取 committed state，在私有 mutation buffer 中解析、绑定、计算表达式、展开本 statement 的全部受影响行并校验约束；不得修改公开 Catalog、TableHeap、索引、Buffer Pool 页面或 MVCC version chain。
- 一条 DML statement 可以生成许多 INSERT/UPDATE/DELETE commands；一条 DDL statement 生成显式携带 Leader 已决定 `table_oid/index_oid` 的确定性 Catalog command，但 V1 不允许在同一 batch 中继续执行下一条 statement。
- 所有可能导致普通业务失败的条件，例如语法、类型、表不存在、primary-key collision、受影响行前置条件和非确定值求值，都必须在 proposal 前解决；secondary UNIQUE/deferred constraint DDL 属于 V1 不支持语义，直接在 proposal 前拒绝。
- 未提交、失去任期或复制失败只需丢弃内存中的构造结果，因为 working state 尚未改变。
- `BusTubStateMachine::Apply(committed_entry)` 是 Leader、Follower、重启恢复和快照后重放修改公开状态的唯一正式路径，不保留“Leader publish 暂存 MVCC 事务”的特殊路径。
- 对格式合法且已完成前置验证的 committed entry，Apply 必须是确定性的、预期不会发生普通业务失败的状态转换。约束、旧 commit timestamp 或旧 tuple 不匹配表示副本漂移、磁盘损坏或实现 bug，节点必须 fail-stop，不能 abort、跳过该 index 或继续服务。
- MVCC `commit_ts` 由 Raft log index 派生或一一映射；所有节点禁止各自分配不同的提交顺序。

## Apply 原子可见性

M2 consumer 已复用 M1 common `StateVisibilityLatch` 建立基本 committed-batch 原子 Apply；M5 不重新定义
publication primitive 或第二条 Apply 路径。M5 的增量 owner 是：对由真实 SQL producer 生成的多行 DML、
DDL、Catalog/Table/index/MVCC/Session/水位组合做完整 command-set 语义与并发验证。SQL 读在整个执行期间
持有 shared lock，FSM Apply 持有 exclusive lock。Apply 内部使用只服务于 committed entry 的 internal apply
transaction，它不是客户端事务，也不会出现在 Raft proposal 之前。
`published_applied_index` 是所有 entry 共用的发布水位，而不是 data-change counter。

```text
Apply(any committed entry at index I)
  -> 获取 StateVisibilityLatch exclusive
  -> 若为 CommandBatch：
       安装全部 row versions，暂不向读者开放
       更新 Catalog 与所有 derived indexes
       更新 SessionTable / request dedup result
       将本 batch 全部新版本的 commit_ts 设为 I
  -> 若为 NOOP：
       不修改数据库逻辑状态
  -> 对两类 entry 都原子发布 published_applied_index = I
  -> 对两类 entry 都更新 last_applied = I
  -> 释放 exclusive lock
```

任何并发读要么在 Apply 前完成并看到 `I-1`，要么在 Apply 后开始并看到 `I`，不能看到中间状态；NOOP 前后数据库内容可以相同，但可用作 read timestamp 的已发布 Raft 水位必须从 `I-1` 前进到 `I`。`last_applied` 和客户端 success response 都只能发生在最终 publish 之后。Apply 中途进程崩溃时丢弃不可信 working 文件，从快照 + committed log 重放完整 entry；Apply 中途出现本地存储错误或不变量错误则节点 fail-stop，不能把半个 batch 标成 applied。Catalog、表数据、索引和 SessionTable 必须被视作同一个逻辑提交单元。所有 SessionTable 查询、重复请求判断和客户端响应路径也必须遵守该 visibility latch/published index，不能提前观察去重结果。

V1 primary-key 列不可更新，secondary indexes 全部 non-unique，因此 Apply 可以按 canonical command 顺序逐项维护 derived secondary indexes，不会遇到“A/B 交换最终合法、逐项插入却瞬时 unique 冲突”的 committed-entry 失败。若 V2 增加 secondary UNIQUE，必须另行设计 batch-aware deferred constraint/index transition，不能直接解除准入拒绝。

未来可以用 versioned Catalog 和更细粒度的可见性发布替代全局 latch，但必须保持同一原子语义。

## 确定性要求

- `now()`、`random()`、UUID 等非确定值必须由 Leader 在 proposal 前求值并作为常量写入 batch，或在第一版明确拒绝。
- 禁止让无 `ORDER BY` 的物理扫描顺序、RID 或 unordered container iteration 泄露进复制协议。Leader 展开最终 DML mutations 后，CommandBuilder 必须先按 `PrimaryKeyCodecV1` identity equality 断言每个 `(table_oid, primary_key)` 在 batch 中至多出现一次，再按 `table_oid` 与 `PrimaryKeyCodecV1::CanonicalCompare` 做 canonical sort，最后才序列化和计算 checksum/digest。
- 相同逻辑 mutation 集合即使来自不同 TableHeap/RID/哈希遍历顺序，也必须生成 byte-identical CommandBatch；DDL 在 V1 每 batch 只有一条，不参与 DML 排序。
- 序列、默认值、浮点特殊值、collation 和类型编码必须版本化并采用稳定字节序。
- Apply 不得访问外部网络、系统时间、进程随机源或节点本地配置。
- INSERT 必须断言主键原先不存在；UPDATE/DELETE 同时校验 `expected_old_commit_ts` 与按稳定 Value codec 编码的完整 `expected_old_tuple`。普通 64-bit hash 只能作为诊断加速，不能单独承担 correctness。
- 上述检查是确定性漂移检测，不是 committed 后重新决定事务成败；任一不匹配都 fail-stop。
- 每次 Apply 可生成逻辑状态 digest 供测试和诊断比较，但 digest 不是共识输入。

## 请求去重的 M5 producer 接线

下述 V1 response frame/persistence foundation 由 M0 引入，Session exact-once consumer 与已构造 batch 的
state-dependent proposal validation 由 M2 为恢复闭环首次实现；M5 负责让真实 SQL producer 和 canonical
batch 使用同一身份，并做累计边界测试，不创建第二份去重表。

V1 committed write response 固定为可完整缓存和重放的稳定协议：

```cpp
struct WriteResponseV1 {
  uint32_t format_version;
  WriteStatus status;   // V1 committed entry 固定为 COMMITTED
  uint64_t request_id;
  uint64_t term;        // 该 Raft entry 的 term，不是重试响应节点的当前 term
  uint64_t commit_index;
};
```

V1 write response 不包含 `affected_rows`、created table/index OID、节点本地消息或其他无法由上述字段重建的数据；DDL 客户端若需要 OID，可在提交后按名称读取 Catalog。codec 使用固定字节序，重试必须返回与首次成功完全相同的 encoded payload。

- 客户端为每个逻辑请求提供稳定的 `client_id/request_id`；网络重试不能生成新的 request ID。
- 第一版规定每个 `client_id` 最多一个 in-flight write，request ID 从 1 开始且新请求必须恰好等于 `last_request_id + 1`。
- `request_id == last_request_id` 表示最近请求重试，返回 SessionTable 中完整缓存的 `WriteResponseV1`；`request_id < last_request_id` 表示已越过的旧请求，禁止重新 Apply；`request_id > last_request_id + 1` 表示存在空洞，直接拒绝。
- FSM 持久化每个 client 的 `last_request_id` 与完整 encoded `WriteResponseV1`；response 内已包含原 entry 的 term 和 commit index，不保存含义不明确的“响应摘要”。若以后允许同 client 并发请求，必须升级为可表示空洞的 request window/map。
- proposal 前失败不推进 SessionTable，也不产生 committed response；去重表属于状态机状态，必须进入快照。
- 新 Leader 完成当前 term no-op、追平已提交前缀并恢复去重表后，才接受客户端写入。

V1 把 `(client_id, request_id)` 定义为逻辑操作身份，Session 不另存 request/batch digest。
因此重试时修改 SQL/payload 属于客户端违约，节点必须返回原稳定响应且不产生第二次副作用。
若后续要对同 ID/不同 payload 显式 fail-closed，必须在状态相关 SQL prepare 之前，从版本化、
domain-separated 的稳定 write-intent bytes 计算 fingerprint，并让首次接受的 batch、SessionRecord 与快照
共同持久化它；重试时不能在状态已经推进后重新 prepare SQL 再计算所谓 canonical batch digest。
payload 绑定与 client ID 生命周期、认证/授权是不同项目。已落盘 V1 Session 不含原 payload 或 fingerprint，
不能追溯补算，也不能静默改变其语义。

## Catalog 与索引

- DDL 和 DML 共用一个 Raft 序列，不能通过节点本地管理接口绕过日志修改 Catalog。
- 所有索引物理结构在正常运行时由 FSM Apply 同步维护，但都不属于第一版权威快照状态；V1 除 primary-key index 外只允许 ordinary non-unique secondary index。
- B+Tree、Hash、HNSW、IVFFlat 统一由 Catalog definition + canonical table rows 重建；测试比较逻辑查询结果，向量索引比较约定的召回约束，不比较 root page、图结构或文件字节。
- `expected_start_schema_epoch` 表示整个 batch 开始前期望的 Catalog epoch；Apply 开始时只校验一次 `current_epoch == expected_start_schema_epoch`。
- 协议格式规定 batch 内每个 schema-changing command 按顺序将 epoch 递增 1，最终 epoch 等于起始值加 schema-changing command 数。V1 每个 batch 最多包含一条 DDL statement；多 DDL 及 DDL 后 DML 的 batch 仅在 V2 启用。
- epoch 不匹配意味着状态机漂移，节点必须停止服务并报警，不能静默跳过。

## 建议修改或新增文件

```text
# extend the M2-owned consumer files; do not create a second codec/FSM
src/include/distributed/command.h
src/distributed/command_codec.cpp                  # CommandBuilder/canonicalization additions
src/include/distributed/bustub_state_machine.h
src/distributed/bustub_state_machine.cpp           # complete V1 command-set hardening
src/include/common/state_visibility.h             # 复用 M1 owner，不在 M5 重建
src/distributed/single_node_sql_runtime.cpp        # M5 SQL -> M2 prebuilt-batch adapter
src/include/distributed/sql_command_preparer.h
src/distributed/sql_command_preparer.cpp
src/include/distributed/raft_state_machine.h       # BusTub snapshot hooks
src/distributed/raft_state_machine.cpp
src/include/concurrency/transaction_manager.h
src/concurrency/transaction_manager.cpp
src/include/common/bustub_instance.h
src/common/bustub_instance.cpp
```

## 测试要求

### 单元测试

- **M2 consumer owner**：至少一个手写、非空且已 canonical 的 `TransactionCommandBatch` 直接匹配固定
  golden frame，并覆盖 bad version、未知 command、checksum/corruption；fixture 不调用 `CommandBuilder`。
- **M5 producer extension**：Schema/Value/主键稳定编码与完整 DDL/DML golden；`CommandBuilder` 对同一 mutation
  集合的不同输入顺序必须产生与 M2 consumer golden 相同的 bytes。
- 将同一 mutation 集合以随机 TableHeap、RID 和 unordered-map 顺序多次交给 CommandBuilder，结果必须都按 `(table_oid, PrimaryKeyCodecV1::CanonicalCompare(primary_key))` canonical sort 并产生 byte-identical payload/checksum；重复 logical key 在 proposal 前拒绝。
- `PrimaryKeyCodecV1` 使用 golden bytes 覆盖 `-1`、`42`、INT32/INT64 MIN/MAX，验证 INTEGER=4-byte、BIGINT=8-byte signed two’s-complement big-endian，模拟不同 host endian 后 payload 仍相同；VARCHAR 覆盖 `ABC`/`abc`、尾随空格、空串和多字节序列，验证长度前缀、raw bytes、binary/no-normalization/no-locale 语义与 index comparator 一致。
- prepare 成功和失败都不能改变公开 Catalog、页面、索引或 MVCC version；只有 committed-entry Apply 能产生可见写。
- distributed mode 的 CREATE TABLE 缺少主键、声明复合/nullable 主键或使用非 `PrimaryKeyCodecV1` 类型时，必须在 proposal 前返回 `UNSUPPORTED_REPLICATED_PRIMARY_KEY`；断言无公开 OID/Catalog 变化、无 Raft entry。主键列 UPDATE 同样在 proposal 前拒绝。
- **M0 restore admission**：恢复出的 V1 secondary UNIQUE definition 必须在 Catalog 恢复/开放服务前 fail-closed；该静态正反例已属于 M0，不是 M5 的首次交付。
- **M5 SQL proposal admission**：`CREATE UNIQUE INDEX` 和 secondary UNIQUE constraint 必须在 proposal 前以 `UNSUPPORTED_DEFERRED_UNIQUE_CONSTRAINT` 无副作用拒绝；无 Raft entry、无公开 Catalog/OID 变化，而 primary-key index 和 ordinary non-unique secondary index 仍可正常创建。

### BusTub FSM/恢复集成测试

- 用测试侧 blocking storage/index adapter 暂停 Apply 中间步骤，同时启动并发 SELECT；读必须阻塞，释放 Apply 后只能看到整个旧 batch 或整个新 batch，不能看到部分 row/Catalog/index/session 状态。
- Apply 在最终 publish 前发生错误时节点 fail-stop；重启重放整条 committed entry 后只能出现完整结果。
- 一条多行 DML statement 生成的整个 batch 只能整体可见，不能让读者观察到部分 row mutations；合法 committed entry 的业务校验不得在 Apply 阶段重新失败。
- 非确定函数被预求值或拒绝；相同 batch 在两个独立实例上产生相同逻辑 digest。
- 相同 `client_id/request_id` 重放多次只产生一次副作用；M2 首次证明 synthetic committed Apply、Session
  快照恢复与同一 entry 重放返回 byte-identical encoded `WriteResponseV1`，其中 term 固定为原 entry term，
  并拒绝并发、空洞和过旧 request ID。M5 只用真实 SQL producer/完整 command set/跨实例链累计复验；
  正式进程中的首次新 Leader 重试证据归 M6，M7 final gate 可再次复验但不重新拥有。
- `expected_start_schema_epoch = 10` 的 CREATE TABLE batch 显式携带 `table_oid = next_table_oid` 与 `primary_index_oid = next_index_oid` 并结束于 epoch 11；CREATE INDEX 同样携带 `index_oid/table_oid`。两个实例必须使用 command OID 并推进 allocator，伪造任一 OID mismatch 时 fail-stop；后续 INSERT batch 必须期望 epoch 11。
- Apply 一条不含 CommandBatch 的 committed NOOP，断言数据库 digest 与 SessionTable 不变，但 `published_applied_index` 和 `last_applied` 都推进到 NOOP index；由 `Snapshot@S` 新建 FSM 时两者均从 `S` 开始。

- 从同一快照启动两个独立实例，Apply 同一批显式携带 table/index OID 的 DDL、INSERT、UPDATE、DELETE，比较 Catalog、allocator 与 SQL 结果；instrumented allocator 断言 Follower Apply 未自行选择 OID。
- 覆盖单条 UPDATE/DELETE 影响多行、DDL batch 后的下一条 DML batch；primary-key collision 等普通失败必须发生在 proposal 前，不能生成 committed entry。
- 构造普通 non-unique secondary index 上 A/B 交换的多行 UPDATE，按 canonical command 顺序 Apply 后结果正确；相同 schema 若声明 secondary UNIQUE 则 DDL 在 proposal 前被拒绝，绝不产生 committed 后的瞬时约束失败。
- 人为构造旧 commit timestamp、旧 tuple 或 schema epoch 不匹配的 committed entry，节点必须 fail-stop，不能跳过后继续 Apply。
- 以 Raft index 驱动提交时间，检查两个实例的可见版本顺序一致。
- 在多行 batch Apply 期间并发循环读取，所有结果只能等于完整旧集合或完整新集合；SessionTable 与数据可见性必须在同一 publish 边界切换。
- 重启后从快照恢复去重表，再次发送最后一个请求，不能重复插入。

## 验收标准

- Raft payload 不依赖原始 SQL 文本、执行计划对象、RID 或页面布局。
- 单条 statement 展开的全部 row mutations 在所有节点上具有相同的 batch 原子边界和逻辑结果。
- proposal 前的私有 mutation buffer 不修改 working state，可直接丢弃；majority-before-success 归 M6 验收。
- 任何调用者都只能经同一个 `BusTubStateMachine::Apply` 修改公开数据库，不存在领导者专用的第二提交路径。
- 任意节点重放同一快照和日志后，Catalog、表数据、索引定义、显式 table/primary-index/secondary-index OID、allocator、commit_ts 顺序和 SessionTable 完整响应一致；每个用户表都能由 Catalog 确定一个受 `PrimaryKeyCodecV1` 支持的逻辑身份。
- 并发读者永远只能观察到 batch publish 前或 publish 后的完整逻辑状态；`published_applied_index/last_applied` 不得提前。

## 输出要求

- CommandBatch 二进制协议与兼容策略，包括整数/VARCHAR wire bytes 和所有 DDL 显式 OID 字段。
- 复用并补充 M0/M2 的 `WriteResponseV1` 与 SessionTable 完整响应重放协议，说明真实 SQL producer 的身份映射。
- autocommit SQL statement 到逻辑 mutation 的映射表，以及 V2 多语句事务扩展边界。
- 所有拒绝的非确定语义和错误信息清单。

---

# 架构工作流 D（M6–M7）：三节点 BusTub 正式链路集成

## 背景

工作流 A–C 已分别验证单机状态恢复、Raft 协议和确定性 BusTub FSM。
M6 把它们接成真实的三个进程；M7 只补齐快照/恢复故障矩阵与最终回归，不重新拥有 M6 的装配。

## 目标

```text
正式客户端入口
  -> 服务端 NOT_LEADER hint + 调用方保持请求 ID 的有界重定位
  -> TransactionCommandBatch
  -> 正式 TCP/RPC Transport
  -> 三节点 LogStore 持久化
  -> majority commit
  -> 三节点 BustubStateMachine Apply
  -> Leader 等待本地 Apply
  -> 正式客户端响应
```

## 本工作流只做

- 一个正式 `bustub-node` 进程对应一个节点目录。
- 静态配置 `node_id/group_id/peer addresses/client address/data_dir`。
- Leader-only write；Follower 返回结构化 `NOT_LEADER` 和已知 Leader 地址。
- Leader 线性一致读、显式 Follower stale read，二者都使用 Raft 派生的 MVCC read timestamp。
- 节点启动恢复、选举、日志追赶、快照创建与安装。
- 优雅关闭和 `SIGKILL` 后恢复。

M6 拥有 `DistributedNode`、正式 TCP/client envelope、single-active-write gate、`NOT_LEADER`、
BusTub ReadIndex/stale-read 接线，以及正常复制、切主、响应丢失和 AppendEntries 追赶的 M6 smoke。
M7 不重新实现这些功能；它拥有需要在线分区/恢复矩阵的正式进程证据、InstallSnapshot 追赶、全停全启、
快照损坏/崩溃/stale replay、全量回归、分布式累计链路和清理总门禁。

## 本工作流不做

- 自动服务发现、动态扩缩容、滚动协议升级编排。
- 跨节点交互式事务和多语句 non-interactive transaction；V1 一个请求只包含一条 autocommit statement。
- 将现有 `nc-shell` 的交互协议直接当作稳定分布式协议。

## 正式节点入口

新增正式节点可执行文件，而不是给 `BusTubInstance::ExecuteSql` 增加测试分支：

```text
bustub-node \
  --node-id 1 \
  --group-id demo \
  --data-dir /var/lib/bustub/node-1 \
  --raft-listen 127.0.0.1:7101 \
  --client-listen 127.0.0.1:7201 \
  --peer 2=127.0.0.1:7102,127.0.0.1:7202 \
  --peer 3=127.0.0.1:7103,127.0.0.1:7203
```

这些参数本身是合法的 production 配置，不是测试开关。正式入口必须校验三个节点的 group ID、成员集合和独立数据目录，拒绝同一目录的并发打开。

V1 `DistributedClient::Send` 只向一个 endpoint 发送一次请求并校验响应 correlation，
不声称自己已实现通用自动路由器。节点返回 Leader hint；CLI/调用方和测试 harness 可做有界重定位，
但 write retry 必须保持同一 `(client_id, request_id)`。`WriteResponseV1` 只带 `request_id`，
因此它的强相关保证限于 V1 的一连接一请求/外层身份已校验模型。未来的 payload 绑定可以由服务端对既有
稳定 write-intent bytes 计算 fingerprint；它本身不等于 client 认证，也不应在没有字段变化时机械升级 client
wire。只有实际改变 request/response encoding 或 correlation 模型时才升级该格式。

## 写入与响应语义

- Leader 收到 autocommit write request 后，先将 entry 追加到本地 LogStore 并等待 `fdatasync` 成功；只有本地 durable 后才向 Follower 发送 AppendEntries。
- Follower 落盘后才回复成功；Leader 在“自身已 durable”且收到包括自身在内多数 durable 确认后推进 `commit_index`。
- 第一版不并行执行 Leader 本地落盘与网络复制。等全部 failure tests 稳定后才能把 pipeline 作为独立性能优化，并保留“本地 durable 是提交必要条件”的不变量。
- Leader 将新的 commit 通过心跳/AppendEntries 通知 Follower；各节点自行按顺序 Apply。
- Leader 只有在本地 `last_applied >= proposal_index` 后才返回完整 `WriteResponseV1{COMMITTED, request_id, entry_term, commit_index=proposal_index}`；首次响应与 SessionTable 缓存使用同一 codec。
- Leader 若在提交后、响应前崩溃，客户端使用相同 request ID 重试，由新 Leader 的去重表返回已提交结果。
- 只到达少数节点的条目不算成功，可在新 Leader 上被覆盖。

## 读取语义

- 每个线性一致读请求到达后，Leader 才创建唯一的 `ReadContext{term, barrier_id}`，向 voter 发送携带该 context 的当前 term heartbeat/ReadIndex probe。只有 term 与 context 都匹配本轮的 ACK 才计入多数；请求到达前收到的 heartbeat ACK、其他 context 的 ACK 和已经完成的旧 barrier 一律不能复用。只有在本轮 quorum confirmation 后才记录 `R = commit_index`。
- V1 不合并不同读请求的 ReadIndex round：一个读请求对应一个新 context 和一次新 quorum confirmation。term 改变、Leader 降级或本轮超时会使该读失败/重试，不能复用其他读的 barrier，也不能回退成本地 stale read。
- 得到本轮 `R` 后等待 `last_applied >= R`，再获取 `StateVisibilityLatch` shared lock并捕获 `P = published_applied_index`。必须满足 `R <= P <= last_applied`，随后通过显式 `BeginReadAt(P)` 创建 MVCC 读事务；V1 在整个查询期间持有 shared lock。
- 新 Leader 在当前 term no-op 提交和 Apply 前不提供线性一致读写。分布式读禁止再从节点本地 timestamp allocator 获取独立时间戳。
- Follower read 必须由客户端显式选择 `stale`；它获取 shared lock，使用捕获的 `P = published_applied_index` 作为 `read_ts`，响应同时携带该值。默认读不能悄悄走 Follower。
- 因此写入 `commit_ts = Raft log index`，Leader 读满足 `read_ts >= ReadIndex`，Follower 陈旧读满足 `read_ts = follower.published_applied_index`，三者共用一条 Raft index 时间轴。
- M6 不使用“Leader lease + 本地时钟”优化，避免时钟假设进入正确性路径。

## 运行与恢复

- 节点启动先完成本地 snapshot + committed log 恢复，再加入 Raft；恢复期间不监听客户端写入口。
- 节点重新加入后先比较快照边界和日志，按 AppendEntries 追赶；日志已压缩则走 InstallSnapshot。
- 工作数据库可以每次启动都从权威状态重建。未来若要复用 working 文件，必须另行设计已持久化的 applied marker 和校验协议，不能默认信任。
- 节点检测到 Catalog epoch、Apply digest 或日志连续性违例时进入 fail-stop 状态，禁止继续对外响应成功。
- 为事务管理器增加显式 `BeginReadAt(timestamp_t)`；生产分布式读路径必须传入 Raft 派生时间戳。

## 建议修改或新增文件（按最早 owner 分组）

```text
# M6 production assembly / transport-facing client
src/include/distributed/node.h
src/distributed/node.cpp
src/include/recovery/node_directory.h       # M1-owned directories; M6 extends durable node/group/voter identity
src/recovery/node_directory.cpp
src/include/distributed/client_protocol.h
src/distributed/client_protocol.cpp
tools/bustub-node/CMakeLists.txt
tools/bustub-node/bustub-node.cpp
tools/bustub-client/CMakeLists.txt
tools/bustub-client/bustub-client.cpp

# M6 shared process harness + smoke; M7 recovery/fault timelines and final gate
test/e2e/raft_process_harness.sh
test/e2e/raft_m6_smoke.sh
test/e2e/raft_m7_snapshot_crash.sh
test/e2e/raft_m7_snapshot_transfer.sh
test/e2e/raft_m7_recovery_matrix.sh
test/e2e/raft_m0_m7_chain.sh              # historical name; M3-M7 distributed state only
```

## 测试要求

### 三节点进程级 E2E

E2E 必须启动三个正式 `bustub-node` 二进制，使用正式客户端协议和各自的真实临时目录：

下表是唯一规范场景清单。`Owner` 指首次必须交付该正式进程场景的里程碑，不等同于底层算法首次实现阶段；
例如 M3 已实现少数派后缀覆盖，但带在线代理分区的 E2E-02 由 M7 recovery matrix 交付。
脚本可将多个编号组合成一条有业务含义的时间线，
但不得在本节再维护一份内容稍有不同的编号列表。多行 UPDATE/DELETE 必须生成“一个含多个 row command
的 batch”，不得误写为多个 batch。

### 必测故障场景

| 编号 | Owner | 场景 | 必须证明的性质 |
| --- | --- | --- | --- |
| E2E-01 | M6 | 正常三副本 batches 与非法 DDL 准入 | 带 V1 主键及普通 secondary index 的 DDL/多行 DML 三节点一致；无主键和 secondary UNIQUE DDL 在 proposal 前拒绝且无状态副作用 |
| E2E-02 | M7 | 在线旧 Leader 与多数隔离并只在少数派形成未提交后缀；多数侧选出新 Leader | 请求未成功，旧未提交后缀在网络恢复后被新 Leader 覆盖，测试期间旧 Leader 进程始终存活 |
| E2E-03 | M6 | 服务端完成提交并发出完整响应，外部代理丢弃整帧，随后 Leader 崩溃 | 客户端未收到首次响应；新 Leader 保留提交，重复 request ID 不重复写，并返回与被丢弃响应 byte-identical 的 WriteResponseV1 |
| E2E-04 | M6 | Follower 落后后重启 | 通过 AppendEntries 追上并继续 Apply |
| E2E-05 | M7 | Follower 落后超过日志保留范围 | 安装快照后继续接收日志 |
| E2E-06 | M7 | 在线旧 Leader 在网络恢复后重新收到多数侧流量 | 识别更高 term、降级并截断冲突后缀 |
| E2E-07 | M7 | 三节点同时停止再重启 | snapshot + committed log 恢复 Catalog、epoch 与 OID allocators，恢复后 DDL Command 使用连续的显式 OID |
| E2E-08 | M6 | Follower stale read | `read_ts = published_applied_index`，响应明确标注该值且不冒充线性一致 |
| E2E-09 | M7 | 在线旧 Leader 先完成一次成功 ReadIndex round，随后与多数失联 | 不再提交写；分区后新到达的线性一致读不能复用分区前 context/ACK，只能超时/非成功 |
| E2E-10 | M7 | 快照生成期间杀节点 | 重启选择完整旧代或新代，不能混合状态 |
| E2E-11 | M7 | 损坏 CURRENT 指向的最新快照 | 使用上一快照与 bridge log 恢复到当前 committed state |
| E2E-12 | M7 | 多行 batch Apply 期间并发读 | 只出现完整旧/新结果，Leader read_ts 不小于 ReadIndex |
| E2E-13 | M6 | 新 Leader 当前 term NOOP 后立即线性一致读 | NOOP 不改数据但推进 `published_applied_index/last_applied`，读水位不落后 |
| E2E-14 | M7 | 旧版本行的 canonical snapshot + 后缀 UPDATE 重放 | 快照保留行的真实 `latest_committed_version_ts = K`，Log S+1 不因被改写为 S 而 fail-stop |
| E2E-15 | M7 | 延迟重复 `Snapshot@S` 到达已发布到 `P > S` 的 Follower | stale guard 把安装视为 no-op，CURRENT/log base/FSM/两个 applied index 不回退且可继续 Apply `P+1` |

E2E-12 使用 production 调度下的大型真实 batch 和并发读压力，对每个实际观测结果做完整 old/new oracle；
它不宣称必然命中 Apply 内部某一条指令。精确临界区阻塞证明归 M5 组件测试，
使用测试自有 adapter，不为此向 production 协议增加 pause RPC。

## 验收标准

- 三个正式进程可以仅依靠配置和各自节点目录启动、选举、写入、查询、停止和恢复。
- 任意单节点故障不丢已提交 CommandBatch；少数派不能提交。
- 新 Leader 必须包含已提交日志；旧 Leader 回归不能覆盖已提交前缀。
- 日志追赶与 InstallSnapshot 两条路径都由生产二进制 E2E 覆盖。
- Leader/Follower 的 MVCC read timestamp 均来自 Raft published index，不使用独立本地时间轴。
- 没有任何测试专用 API、SQL 语句或默认测试路径进入 production。

## 输出要求

- 三节点启动示例、客户端请求/响应协议和一致性语义。
- 可复现的 E2E 命令与故障时间线。
- 每次运行在场景期间收集节点日志与 term/index 轨迹；持久保留摘要和失败诊断产物，
  成功原始现场在归档/上传结果后精确删除，不进入源码树。

---

# 横切规则（M0–M7 及任何后续明确里程碑）：测试体系、生产隔离与最终验收

## 背景

模块测试通过只能证明局部行为，不能证明“客户端请求 -> Leader -> Raft 持久化 -> majority commit -> FSM Apply -> 崩溃恢复”的正式链路成立；但把同一算法在每一层重复穷举，也会制造高成本、低信息量的测试。本章从 M0 开始就生效，用不变量和风险决定测试位置，并将正式代码与测试控制彻底隔离；它不是 M6 之后才开始的第五个串行阶段。

## 目标

- 延续项目现有 GoogleTest、CTest、SQLLogicTest 风格和构建入口。
- 每个关键不变量在最便宜、最稳定的一层得到充分验证。
- 至少一组 production-like E2E 使用正式二进制和正式协议打通全链路。
- 故障注入不污染 production API、协议或默认行为。
- 测试失败能稳定复现并给出 term/index/节点/请求 ID 证据。

## 测试分层

### 1. 单元测试：定位纯逻辑和边界

适合测试：

- Codec、checksum、版本兼容、Manifest 选择。
- RequestVote 日志新旧判断、AppendEntries 冲突规则、commit 推进规则。
- CommandBatch 确定性、去重状态转换、Catalog 序列化。
- 可控持久化接口上的每一个崩溃点。

不适合测试：

- 真实选举是否经过 TCP 完成。
- `SIGKILL` 后三个进程是否真正恢复。
- SQL 是否通过正式客户端入口抵达状态机。

### 2. 组件测试：验证相邻模块契约

适合测试：

- `RaftNode + LogStore + InMemoryTransport + KV FSM`。
- `SnapshotManager + CatalogSnapshot + BusTubInstance`。
- `SqlCommandPreparer + Catalog/Planner + BusTubStateMachine`；`TransactionManager::BeginReadAt`
  只在 M6 的分布式读接线中组装。

组件测试要验证边界顺序，例如“同步持久化调用成功返回后才允许 RPC success”，而不是再次穷举 Codec 的所有坏字节组合。

### 3. 集成测试：验证真实 BusTub 状态

- 复用项目现有 `test/*/*test.cpp` 自动发现方式，新增 `test/recovery`、`test/raft`、`test/distributed`。
- 复用 `StringVectorWriter`、真实 `BusTubInstance` 和事务测试辅助方法，以 SQL 查询结果验证逻辑状态。
- 对本地 SQL 语义扩展 `.slt`，沿用 `bustub-sqllogictest`；分布式故障不要硬塞进 SQLLogicTest。

### 4. 进程级 E2E：验证 production 链路

- 由外部测试 harness 启动正式 `bustub-node`，通过正式 client endpoint 发请求。
- 使用真实 TCP、真实日志文件、真实快照文件和每节点独立目录。
- 使用操作系统进程控制执行 `SIGKILL`、重启和退出码检查。
- 网络故障由测试进程拥有的代理或系统级隔离层制造；production 节点只看到正常的连接、断开、延迟和丢包。
- 快照损坏场景由外部 harness 在节点停止后修改该测试自己的临时文件；不得为此给 production 节点增加“损坏快照”API。
- M6/M7 只允许复用一个 `test/e2e/raft_process_harness.sh` 管理节点启动、随机选举区间、Leader 定位/重定位、客户端重试、状态等待、停止和 PID trap；场景脚本只表达 E2E 时间线与断言，公共生命周期逻辑不得复制回各脚本。
- M6/M7 按工作流 D 的唯一 owner 表合计覆盖 E2E-01 至 E2E-15，
  不允许因各模块测试已通过而删掉完整链路验证。

## 如何决定“测什么”

先建立需求—风险—测试映射，不以代码文件数量决定测试数量：

| 不变量/风险 | 主验证层 | E2E 是否再验证 |
| --- | --- | --- |
| checksum、坏尾识别 | 单元 | 只选一个代表性重启场景 |
| 空日志/snapshot sentinel `(0,0)` | LogStore/SnapshotStore 单元 | 正常集群首次启动自然覆盖 |
| term-0 Manifest 原子发布每个崩溃点 | M1 SnapshotManager 掉电模型 + M2 runtime restart | distributed E2E 不打开 StateManifest，不能替代该物理门禁 |
| distributed 单次 SnapshotStore/CURRENT 原子发布 | M4 SnapshotStore/本地 capture 组件 | E2E-10 在正式节点快照期间杀进程一次 |
| 两代快照与 bridge log 回收 | 单元/恢复集成 | E2E 损坏最新快照并从旧代恢复 |
| 远端 InstallSnapshot 的 Snapshot/HardState/Log/FSM 跨文件顺序 | M4 专用 `max(H,S)` 崩溃矩阵 | E2E 覆盖一次 InstallSnapshot 后重启 |
| InstallSnapshot suffix 保留 | LogStore/SnapshotStore 公式与崩溃组件测试 | Follower 快照追赶后继续追加 E2E |
| stale/duplicate InstallSnapshot 单调性 | Apply/Install 序列组件测试，覆盖两次 guard | 延迟 Snapshot 到达已追上 Follower 的 E2E |
| HARD_STATE generation 原子替换 | StableStore 掉电单元测试 | 由节点重启 E2E 间接覆盖 |
| higher-term durable-before-RPC | Raft 确定性组件测试，覆盖所有 term 更新入口 | 旧 Leader 收到更高 term 后降级 E2E |
| ReplaceSuffix 原子逻辑替换 | LogStore 掉电单元/组件 | 旧 Leader 回归 E2E 代表性覆盖 |
| committed-range corruption | LogRecovery 单元/恢复集成 | 不重复做破坏性穷举 |
| committed entry 唯一 Apply 路径 | FSM 单元/双实例集成 | 正常 autocommit batch E2E 自然覆盖 |
| batch 原子可见性 | FSM 并发集成 | E2E 运行多行 UPDATE 与并发读 |
| Raft/MVCC timestamp 映射 | 事务/FSM 集成 | E2E 检查 read_ts 与 applied index |
| ReadIndex fresh quorum context | Raft 确定性网络分区测试 | E2E 让持有旧 ACK 的隔离 Leader 拒绝新读 |
| 投票日志新旧规则 | 单元/确定性组件 | E2E 验证已提交日志随新 Leader 保留 |
| TCP 重连和节点进程生命周期 | 进程 E2E | 是，主验证层就是 E2E |
| 单 statement 多行 mutation 原子性 | FSM 集成 | E2E 跑一个代表性多行 UPDATE/DELETE |
| SQL 执行器的所有算子语义 | 现有 GTest/SLT | 不在 Raft E2E 重复穷举 |
| B+Tree/HNSW 内部结构 | 现有索引测试 | 仅验证恢复后逻辑查询结果 |
| 请求重复提交与完整响应重放 | FSM/SessionTable codec 单元与重启集成 | E2E 验证提交后响应丢失并比较 WriteResponseV1 bytes |
| request ID 并发、空洞、过旧 | SessionTable 单元 | E2E 只保留一次相同 ID 重试 |
| schema epoch 与 DDL batch | Catalog snapshot/Command/FSM 集成 | E2E 恢复后立即执行 DDL/DML |
| replicated primary-key admission | Binder/CommandBuilder 与 Catalog restore 集成 | E2E 拒绝无主键建表并确认无副作用 |
| secondary UNIQUE/deferred constraint 禁用 | Binder/CommandBuilder/Catalog restore 集成 | E2E 拒绝 secondary UNIQUE DDL |
| PrimaryKeyCodecV1 integer/VARCHAR wire 与比较语义 | Golden-byte codec 与 primary-index comparator 单元测试 | 不在 E2E 重复编码边界组合 |
| DDL OID 由 Command 显式决定 | Command codec/FSM 双实例与 allocator mismatch 测试 | E2E 重启后继续 DDL 验证 OID 连续 |
| DML mutation canonical sort | CommandBuilder 属性测试，随机输入顺序比较 payload bytes | 正常多行 DML E2E 间接覆盖 |
| Snapshot index rebuild-before-replay | 恢复集成禁用全表 fallback 并用 suffix UPDATE 定位 | E2E-14 Snapshot + suffix UPDATE |
| canonical row 原始 commit timestamp | Snapshot/recovery 集成 | E2E 用 Snapshot S + 后缀 UPDATE S+1 代表性覆盖 |
| NOOP 发布 Raft 水位 | FSM 单元/读路径集成 | 新 Leader NOOP 后立即线性一致读 |

新增测试前必须回答：

1. 它要证明哪条不变量或阻止哪种回归？
2. 现有测试是否已经以更稳定、更低成本的方式证明同一件事？
3. 若只做模块测试，会漏掉哪个真实边界？
4. 若放到 E2E，是否只是重复算法组合而没有增加链路证据？

删除或合并重复测试不等于取消 E2E。原则是：局部组合在低层充分覆盖，关键故障在 E2E 选代表场景，完整生产链路始终至少跑通一次。

## 不应测试的实现细节

- 不断言选举必须在某个精确毫秒完成，只断言在配置上界内收敛。
- 不断言线程调度、unordered_map 遍历顺序或 RPC 到达的偶然顺序。
- 不比较三个节点数据库文件、页面 ID 或向量索引图的原始字节。
- 不为达到覆盖率重复项目已有 parser、binder、executor、Buffer Pool、B+Tree 算法用例。
- 不用固定端口、固定 `db.bustub` 或共享目录运行并行测试。

## Production 与测试隔离

### 允许进入 production 的抽象

以下接口本身是正常架构边界，production 与测试都可实现：

```text
DurableStorage / FileSystem
RaftTransport
Clock / Timer
RandomSource
Process-independent NodeConfig
```

生产装配只注入 POSIX 文件系统、正式 TCP Transport、单调时钟和安全随机种子；测试目标可注入内存磁盘、手动时钟、确定性随机源和可控 Transport。

### 禁止进入 production 的测试污染

- 禁止 `if (test_mode)`、全局 `is_testing` 或让 production 默认走 fake storage/transport。
- 禁止 `/test/*` RPC、`TEST_CRASH_NOW` SQL、隐藏 shell 命令或测试专用管理 API。
- 禁止正式节点接受“跳过 fsync”“强制当选 Leader”“直接设置 commit_index”等测试参数。
- 禁止 production 库链接 `test/include`、GTest、故障调度器或 E2E harness。
- 禁止为了测试把内部状态改成 public；通过只读正式诊断状态或测试二进制中的 friend/helper 验证。

### 构建隔离

```text
bustub                      # production library
bustub-node                 # production executable
bustub-client               # production client
*_test                      # test/CMakeLists.txt 自动发现，可使用 test/include
build-raft-component-gates  # 已实现的 M0–M7 组件目标聚合
test/e2e/*.sh               # 外部 harness/场景，启动正式二进制
```

`bustub_test_support`/`bustub_cluster_e2e_test` 若在架构图中出现，只能表示上述测试依赖桶，
不得冒充已实现 CMake target。`ManualClock`、`InMemoryTransport`、故障 storage 和临时集群管理器
位于测试头文件/目标或外部 harness；依赖方向始终由测试指向 production。

### 文件和端口隔离

- 每个测试使用唯一临时根目录，并在其下为三个节点创建独立子目录。
- 端口块由 harness 调用方为本次场景独占；所有 listener 和派生 proxy 端口必须避开 Linux 临时端口区
  （通常从 32768 开始），且不得使用跨并发任务共享的单个固定端口。CI 中彼此隔离的 runner 可以复用同一低端口块。
- 进程、PID、listener 和 proxy 在每个场景结束时必须清理。节点目录/日志先在源码树外的 artifact root
  暂存；成功场景在生成摘要和完成 CI 上传后精确删除，失败场景只保留到诊断、交接或归档完成。
- 测试绝不读取或覆盖仓库根目录的 `db.bustub`，也不接触真实 production 数据目录。

### 里程碑结束清理门禁（M0–M7 及任何后续明确里程碑强制）

每个里程碑在声明完成前都必须执行一次工作树污染审计；后续阶段也必须复查此前阶段留下的文件，不能把可再生成的中间产物累积到最终交付。审计至少包含 `git status --short --untracked-files=all`、未跟踪文件按顶层目录计数、源码树占用统计，以及 production target 对测试目录/测试框架的反向依赖扫描。

- 允许保留：production 源码与配置校验、已注册的正式单元/组件/E2E 测试、测试专用辅助层、稳定可复用的运行脚本、协议/运维/测试文档和明确要求的验收记录。
- 必须清除：源码树内的 CMake/build/Ninja 生成树、目标文件和临时二进制，测试 XML、临时数据库/节点目录、运行日志、core dump、scratch 配置、一次性调试程序，以及没有被正式测试或文档引用的阶段性夹具。
- 所有构建默认使用源码树外目录（例如 `/tmp/bustub-raft-build-*` 或操作者指定的外部路径）；仓库 `.gitignore` 必须覆盖约定的根目录构建树和已知运行时垃圾，但不得用过宽规则隐藏可能应提交的源码、测试或配置。
- production target 只能依赖 production 抽象与实现。`InMemoryTransport`、`ManualClock`、故障注入器、临时集群管理器、GTest 和 E2E harness 必须只存在于测试辅助层或测试目标依赖图中。
- 清理前必须先解析并打印精确目标，禁止对仓库根目录、通配符展开结果或未校验变量执行递归删除。失败现场只保留到问题关闭或交接完成；随后归档到源码树外或删除。
- 阶段交接必须记录：保留了哪些失败现场及原因、已删除哪些可再生文件、当前 `git status` 的剩余项分类，以及下一阶段应使用的外部构建目录。若污染审计未通过，该里程碑不得标记为完成。

## CMake 与现有测试风格

- 继续使用 `test/CMakeLists.txt` 对 `test/*/*test.cpp` 的 GoogleTest 自动发现和 CTest 注册。
- 固定 wire/disk codec 的兼容测试必须从手工固定 golden bytes 解码并逐字段检查，同时要求 encoder 精确匹配；
  只做 `Decode(Encode(A)) == A` 不能作为唯一 oracle，因为同一错误可能在编码和解码两侧互相抵消。
- 空字符串、空 command list、`nullptr` 或零值只有在它们是正式协议的合法边界或明确拒绝输入时才可作为夹具；
  正常链路必须使用能产生可观察副作用的非空 SQL、多个不同主键/客户端和字面量结果。`COUNT(*)` 只适合验证
  原子可见性等集合不变量，Catalog/index/Session/MVCC 恢复还必须分别核对具体行、命名对象、稳定响应和时间戳。
- 为耗时 E2E 单独增加 label，例如 `distributed-e2e`；默认 CI 至少运行稳定 smoke E2E，较长随机压力测试可作为 nightly，但不能让全部 E2E 都变成非门禁测试。
- SQL 语义继续使用现有 `.slt` 风格；分布式测试通过独立 GTest/harness 管理进程。
- ASan/UBSan 运行功能测试；TSan 单独运行选举、Apply，以及正式 `DistributedNode` 的 tick/listener/client worker、
  关闭/join、重启和快照并发用例，不能只运行由测试线程手工 Tick 的单线程 Raft harness。
- 随机故障测试必须输出 seed；CI 失败后同一 seed 可本地复现。

## 最终门禁测试集

1. 当前里程碑的 component manifest 全部通过，并单独运行仓库支持的 public-CI regression set；
   不得用当前 27 个 Raft 组件二进制的结果冒充仓库全部 GTest。CI 还必须以独立 Release job
   构建 `sqllogictest` 并运行所有已注册 `.slt`，不能由排除 SQLLogic 的 public GTest job 代替。
2. M0–M2 全部恢复与掉电点测试通过。
3. M3–M4 全部确定性 Raft safety/snapshot 测试通过。
4. M5 CommandBatch/FSM 双实例一致性与真实 SQL 累计恢复链路通过。
5. M6/M7 按唯一 owner 表覆盖 E2E-01 至 E2E-15，M7 退出时全部通过。
6. 至少一次 ASan/UBSan 全量运行和一次 TSan 并发核心集运行。
7. required gate 每个场景执行一次，任一失败立即使本次 gate 失败，不 retry-until-green。
   多 seed/稳定性重复只放在独立 nightly，每个 attempt/seed/轨迹都必须记录，失败 attempt 不得被后续成功覆盖。
8. 物理模式门禁分成两条且不能互相冒充：M0–M2 的独立 term-0 gate 必须验证
   `CommandLog/StateManifest` 恢复；另有一条正式三进程链路从全新 distributed 目录开始，在同一份
   M3–M7 durable state 上连续覆盖准入拒绝、写入与响应丢失、Leader 切换和 exact-once retry、
   snapshot+suffix、真实多块追赶、stale Snapshot、身份拒绝、全停全启及恢复后继续 DDL/DML。
   后者累计复验 M0–M2 定义的逻辑恢复性质，但不是 term-0 物理目录迁移，也不能替代前者。

## 最终验收标准

- 任一已返回成功的 autocommit write request，在任一单节点故障、Leader 切换和全体进程重启后仍可查询。
- 任一未形成多数提交的 CommandBatch，不会因为旧 Leader 恢复而被错误 Apply。
- Catalog、`schema_epoch`、OID、带原始 `latest_committed_version_ts` 的 canonical table rows、索引定义、数据、提交顺序和去重状态能够由权威状态完整重建；primary 与所有 secondary indexes 必须先重建到 Snapshot@S，suffix 才能经正式 FSM Apply 重放并增量维护它们。
- 一个 CommandBatch 对并发读者原子可见，Leader/Follower 的 MVCC read timestamp 与 Raft published index 位于同一时间轴；NOOP 也推进 published index，canonical snapshot 则保留每行真实的最近提交 index。
- HARD_STATE 和 ReplaceSuffix 在任一规定崩溃点恢复后都只能呈现完整旧代或完整新代，不能出现 torn election/log state；任何 higher-term RPC 路径都先持久化新 term。
- InstallSnapshot 仅在 `S > published_applied_index` 时进入发布流程，并仅在 pre-install `TermAt(S) == T`
  时保留旧 suffix；stale/duplicate 安装不改变任何状态。无法证明匹配时，只有 `E==S` 才能在完整验证
  Snapshot 后丢弃 suffix；`E>S` 必须在权威发布前 fail-closed，绝不降低或先破坏 committed boundary。
- 每次线性一致读都由请求到达后的新鲜 ReadIndex context 确认当前 term quorum；隔离的旧 Leader 不能复用历史 ACK。
- distributed mode 的所有用户表均具备 `PrimaryKeyCodecV1` 支持的逻辑身份；INTEGER/BIGINT 使用固定 signed big-endian wire bytes，VARCHAR 使用固定 binary semantics。非法 CREATE TABLE 与 secondary UNIQUE DDL 在 proposal 前无副作用地拒绝，恢复时非法 Catalog 不开放服务。
- CREATE_TABLE/CREATE_INDEX 的 OID 由 Leader 写入 Command，所有节点使用相同显式 OID；相同 DML mutation 集合不受物理扫描/RID/container 顺序影响，canonical sort 后得到 byte-identical CommandBatch。
- SessionTable 保存完整稳定 `WriteResponseV1`，响应丢失、Leader 切换或重启后的最近请求重试返回与首次成功 byte-identical 的 payload，且不重复副作用。
- 每条关键不变量都有主测试位置，且至少一个 E2E 证明完整生产路径。
- production 二进制、API、默认配置和链接依赖中不存在测试专用行为。
- 正常稳态最多保留两代有效恢复点及从最老边界开始的 bridge log；InstallSnapshot 缺少旧 bridge 时允许暂时只有一代，且快照和日志都按可恢复边界回收。

## 输出要求

- `docs/testing/raft_test_matrix.md`：需求、不变量、测试 ID、测试层、故障点和 CI job 的映射。
- `docs/testing/raft_e2e_runbook.md`：启动、注入故障、复现 seed、保留现场和排障方法。
- CI 配置：单元/集成、稳定 E2E、sanitizer、nightly stress 分开报告。
- 最终演示：真实三进程完成非法 DDL 拒绝、显式 OID 的合法建表/建索引、canonical 多行写入、新鲜 ReadIndex 原子读、Leader 故障、byte-identical 重试响应、新 Leader 写入、旧节点追赶、延迟旧 Snapshot 不回滚、全体重启与 rebuild-index-before-replay 查询校验。

---

# 里程碑唯一执行顺序与门禁

```text
M0 -> M1 -> M2 -> M3 -> M4 -> M5 -> M6 -> M7 -> M8（写重试 payload 绑定）
```

任何里程碑没有通过自己的退出门禁，不进入下一里程碑。M8 是从非线性候选 DAG 中已完成准入的唯一分配，
不代表其余候选已获得后续编号。各章只能细化本表，不得另行创建“第几阶段”的并行执行轴。

## 里程碑唯一归属表

| 里程碑 | 工作流 | 唯一功能交付 | 主验证层 | 明确不属于本里程碑 |
| --- | --- | --- | --- | --- |
| M0 | A | 最小 production Catalog/Session/WriteResponse codec、`ValidateReplicatedCatalogV1` 静态 shape/restore admission、TableHeap reopen 与 CanonicalSnapshotBuilder；证明 canonical `db.bustub + catalog.bin + session.bin` 可无旧索引页、working Flush 或内存 undo 地表达 committed state，包括行原始 commit ts/epoch | 独立 golden、literal replicated Catalog 正反例、预构造非空 SessionRecord、代表性 schema/rows 的 self-contained round-trip | `CURRENT`/Manifest/日志、SQL producer、exact-once transition |
| M1 | A | NodeDirectory、StateManifest/SnapshotManager、common `StateVisibilityLatch`、Snapshot@S boundary validation（包括内嵌 Session response term=0）与 term-0 原子发布/精确恢复；损坏最新代只回到上一 Snapshot 边界 | capture barrier、路径/边界 hardening、发布/恢复集成、Snapshot old-or-new 掉电 | 三水位 orchestration、suffix replay、Session exact-once transition |
| M2 | A | term-0 StableStore commit marker、CommandLog/sentinel/torn tail、`TransactionCommandBatch` consumer codec、预构造 batch 的 state-dependent admission/committed BusTub Apply、Session/WriteResponse exact-once consumer、bridge replay 到 `effective_commit_index`、两代联动回收与 fallback 掉电矩阵 | 不经 CommandBuilder 的手写非空 golden/entry、单节点 crash/restart、literal rows/Catalog/Session response 与 storage oracle | Raft RPC、raw SQL/canonical CommandBuilder producer、CommitSql adapter |
| M3 | B | 随机选举、term/vote durable-before-RPC、AppendEntries/冲突回退、多数提交、NOOP、新鲜 ReadIndex、无快照 KV 重启 | ManualClock/固定 seed、InMemoryTransport、StableStore/LogStore 组件 | SnapshotStore/InstallSnapshot、BusTub SQL、三进程 TCP |
| M4 | B | SnapshotStore、压缩、分块 InstallSnapshot/ACK liveness、两次 stale guard、suffix 保留、compacted follower catch-up 与 durable restart | KV snapshot/命名崩溃组件矩阵 | SQL producer、正式三进程验收 |
| M5 | C | raw SQL -> V1 单 statement canonical batch/`CommitSql` adapter、CommandBuilder/golden、显式 OID、SQL PK/secondary UNIQUE/unsupported 语义的无副作用准入；用完整 command-set/跨实例确定性/多行并发 oracle 累计复验 M2 consumer，实现 BusTub Raft snapshot hooks 与 `BeginReadAt` exact-timestamp primitive，累计重跑 M0–M2 真实 SQL 恢复链 | 独立 producer golden、真实非空 SQL、双实例 cumulative Apply、exact timestamp 单元、组件并发 oracle | M2 batch admission/consumer/Session 状态机、common latch/term-0 publisher、ReadIndex/majority/Leader response、TCP、新 Leader retry |
| M6 | D | DistributedNode/正式 TCP/client、M1 NodeDirectory 上的 durable node/group/voters identity extension、single-write gate、NOT_LEADER hint、ReadIndex 到 M5 `BeginReadAt` 的 BusTub/stale-read 组装、正常链路、切主/响应丢失/AppendEntries 追赶 | 三进程 E2E-01/03/04/08/13；02/06/09/12 的低层 deterministic/TCP 前置 | 在线分区/恢复矩阵、快照故障补集与 final gate |
| M7 | D + 横切 | 在线分区、InstallSnapshot 追赶、全停全启、快照崩溃/损坏/stale replay、E2E 补集、全量回归/sanitizer、M3–M7 分布式累计链与清理总门禁 | E2E-02/05/06/07/09/10/11/12/14/15 + component/public-CI/SLT/sanitizer/清理 | 任何当时未定义的 M8/V2 功能 |
| M8 | application-protocol hardening | 写重试 payload 绑定：在 SQL prepare 前生成稳定 fingerprint，随 CommandBatch 复制并随 Session/snapshot 持久化；同一身份的 changed payload fail-closed | 手写 SHA/intent/CommandBatchV2/SessionV2 golden，真实 SQL 组件 oracle，三正式进程的丢响应、切主、快照与 cold restart E2E | 认证/client 注册、并发 request window、多个 in-flight proposal、迁移/rolling upgrade 及其他候选节点 |

## M7 后非线性路线图与准入：M8 已唯一分配

M0–M7 已经是“实验性 BusTub + 简单静态三节点 Raft”的完整可交付终点，后续阶段不是完成该项目的
必经步骤。2026-08-30 用户已明确授权沿方案继续，因而把唯一 ready node“写重试 payload 绑定”分配为 M8；
这次分配不把其余候选转成默认待办。M7 后内容仍是带依赖关系的候选 DAG，不是 `M8 -> M9 -> ...` 的
预排串行计划；一次里程碑只能选择一个前置已满足的候选节点
（ready node），不能把它的未完成依赖闭包一起塞入该阶段。共享前置只能有一个 owner，不能在多个阶段
重复实现 overlay、scheduler 或 migration。

### 候选 DAG 与定位边界

| 分类 | 可单独分配的候选节点 | 依赖与明确边界 |
| --- | --- | --- |
| 定位内 application-protocol hardening（M8 已完成） | 写重试 payload 绑定（不再位于 ready frontier） | 复用 V1 exact-once 身份；新增 request-entry fingerprint 及 CommandBatch/Session/snapshot 持久化，不包含认证、client 注册或并发 request window |
| 定位内 misuse hardening | durable mode/store-role marker | marker 必须先于任何 mode-owned 文件创建；非空且无 marker 的旧目录默认 fail-closed，只能经显式 offline adoption/migration 处理，禁止根据内容猜测后就地补写 marker |
| 教学型 DB 语义 | 复合 `NOT NULL` primary key | 第一阶段只允许现有 V1 `INTEGER/BIGINT/VARCHAR` 分量，独立定义 tuple identity codec/comparator 与 Catalog 格式；nullable primary key 永久排除 |
| 教学型 DB 语义 | 额外的确定性、非空标量 primary-key type | 每种 type 单独证明跨节点 stable encoding/equality/order；不与复合 key 捆绑 |
| 教学型 DB 语义 | 单 statement secondary UNIQUE 最终态校验 | 一条 SQL statement 可产生多行 command，但仍不是多语句事务；需要 statement 内 net mutation/final-state oracle |
| 教学型 DB 语义 | 多 statement、非交互式原子 batch | 需要 V2 envelope、private Catalog/Table overlay 和 net mutation 归并；不自动启用 deferred constraint |
| 教学型 DB 语义 | 跨 statement secondary UNIQUE deferred checking | 依赖多 statement overlay，并复用已经独立验证的 UNIQUE final-state 规则；不包含 FK/CHECK/exclusion 等其他 constraint family |
| 教学型 DB 语义 | `DROP INDEX` | 独立的版本化 Catalog 命令、依赖检查、快照恢复和无副作用拒绝规则 |
| 教学型 DB 语义 | `DROP TABLE` | 在 `DROP INDEX` 所有权规则上另行定义关联索引/依赖对象的原子删除；不与 `DROP INDEX` 强捆绑，候选表也不声称穷举所有 SQL 能力 |
| 性能实验 | 真正可阻塞 completion + 可控存储调度器 | 替换 V1 同步持久化 API 时必须保留 durable-before-message；它是 group commit 的前置，不因 API 异步就自动并发 proposal |
| 性能实验 | 多个 in-flight proposal | 需要 proposal/result correlation 与 pending map；若允许同一 client 并发，还另依赖可表示空洞的 Session request window |
| 性能实验 | group commit | 同时依赖可控存储调度器和可并发排队的 proposal；有独立 crash、TSan 与吞吐/延迟基线 |
| 性能实验 | AppendEntries replication pipeline/flow control | 与 snapshot chunk pipeline 不同；单独证明响应乱序、重传、背压和 durable ordering，不能塞进 group commit 实现 |
| 有条件的离线工具 | term-0 -> distributed migration | 只有真实需要复用旧目录时才立项，依赖 durable mode marker；开始前先冻结 term-0 `Snapshot@S`/term-0 Session 到 distributed SnapshotStore/LogStore/HardState 的 index、term、commit mapping，再定义备份、crash oracle 与 one-way cutover |

教学型 DB 语义或性能节点只有在用户明确提出相应学习/实验问题后才进入 ready frontier，不能把本表当成
默认待办清单。若两个节点需要同一 net-mutation primitive，由先获分配的节点拥有通用最小实现，后续节点复用，
不得复制一套 statement/batch 校验器。

以下工作会改变当前项目定位，不进入普通后续候选：base backup + 连续 WAL、fuzzy/COW/低停顿快照、
成熟 Raft 库迁移、动态成员/learner、认证和安全 client identity、通用路由/服务发现、rolling upgrade、
分片、多 Raft Group 与跨组事务。只有用户明确改变项目定义后，才重新评估并建立新的顶层方案。

### 分配任一后续里程碑前的格式与升级门禁

开始写代码前，必须记录干净的 baseline commit，并冻结唯一目标、输入/前置、明确排除项、功能 owner、
主测试层、production-like E2E、退出门禁、清理和完成后停止边界。还必须逐项给出格式影响矩阵：client wire、
Raft RPC、CommandBatch/log payload、Catalog、Session、snapshot bundle、node-directory marker；不受影响的格式
不得为了“统一 V2”而升版。

本实验项目默认不支持 mixed-version executable/wire cluster 或 rolling upgrade；在没有额外协议前，这只是
部署前置，不得声称旧 binary 会协议级 fail-closed。每个受影响格式必须明确选择且只能选择一种状态策略：

1. **fresh-directory homogeneous deployment**：只使用空的新目录，所有受影响组件采用同一协议代；不保留旧
   durable state。新版本只保证拒绝自己能识别出的未知/不允许输入，不承诺未升级的旧 binary 能识别未来格式；或
2. **preserving-state offline upgrade**：停止全部受影响进程并先做备份，逐格式定义旧读、转换和新写边界。
   append-only log 才使用 V1 prefix/V2 suffix，并在首条 V2 后拒绝新的 V1 entry；Catalog、Session 和 snapshot
   使用完整新 generation/显式离线转换；client wire/Raft RPC 使用同代 endpoint 的 negotiation/rejection；
   marker/singleton 有独立 adoption 规则。cutover/capability marker 必须在第一份 V2 authority byte 前 durable，
   并逐个命名 crash point 与恢复 oracle。由于既有 V1 binary 不认识未来 marker，no-downgrade 默认是运维禁令，
   不能宣称任意旧 binary 会安全拒绝。所谓“回滚”若不能逆变换，只能指恢复 full-stop 时同时备份的三个 node
   directory 与 identity/config，并用旧 binary 实际验证该备份可恢复。

若未来必须让 mixed binary 在协议层 fail-closed，需另加 cluster protocol/capability epoch，在投票、
AppendEntries/InstallSnapshot 和 client service 开放前完成握手，并用真实 old/new binary 验证；这已接近 rolling
upgrade 项目，不属于本实验项目默认门禁。只给新目录写一个旧 binary 会忽略的旁路 marker，不能充当该保证。

任一候选的共同测试门禁必须包含真实业务输入与独立可观测结果、失败路径无 append/storage/state 副作用，
并最终至少有一条三个正式进程的 E2E。涉及格式时增加手写非空 golden decoder、exact encoder bytes（不能只用
同一 codec encode 后 decode 自证）、未知版本及兼容矩阵未允许的组合/顺序拒绝；若允许 V1 -> V2 序列，
还要有正向测试并拒绝首次 V2 后再出现 V1。涉及磁盘状态时加入 snapshot/recovery 和连续两次 cold reopen；
涉及持久化替换时使用命名 crash event 与适合该协议的 oracle。凡修改并发路径都强制 TSan；只有性能目标
强制记录吞吐/延迟基线。所有候选都必须运行受影响的既有 gate 和定向 ASan/UBSan，并把新增测试注册进
CMake/CI；不得只运行新写的 happy-path 测试。
阶段结束后执行中间产物/进程/端口清理，完成该阶段即停止，不预执行下一候选。

### 已分配 M8：写重试 payload 绑定

M8 只加固已经存在的 exact-once 主线。其输入是已解码的非空 `ClientWriteRequestV1`、V1 的顺序
`(client_id, request_id)` Session 身份、单 active proposal gate 和 fresh distributed directories；功能 owner 是
`RequestFingerprintV1`、CommandBatchV2、SessionV2 及入口/Apply 的原子分类。它不改变 SQL producer/consumer
语义，也不创建第二套 exact-once 或 snapshot owner。

- 当 `request_id == last_request_id` 时，相同 `(client_id, request_id)` 与相同 fingerprint 在丢响应、切 Leader、
  重启和快照恢复后返回 byte-identical cached response；同一最近 ID 与不同 fingerprint 在已知 committed
  identity（或本节点已知的 active proposal）后 fail-closed，且不产生第二次 append/propose/Apply，也不推进
  OID、watermark 或 Session。`request_id < last_request_id` 继续返回 V1 `TOO_OLD`，既不重放旧响应，也不暗中
  引入 request window。
- fingerprint 必须在请求入口、状态相关 SQL prepare 之前，从版本化/domain-separated 的稳定 write-intent
  bytes 计算；首次接受的 replicated batch 携带它，SessionRecord 与 snapshot 保存它。重复请求不能在数据库
  已变化后重新 prepare 来重建 digest。分配阶段必须冻结 collision-resistant fingerprint 的算法、宽度、domain
  tag 及 write-intent 中精确纳入/排除的字段，不能依赖 C++ 对象布局、节点本地配置、`std::hash` 或仅用 CRC
  承担 identity 判定；它在 non-Byzantine 模型下用于误用检测，不是认证。对本项目建议绑定长度分帧后的
  operation kind + 原始 write payload bytes，排除 routing metadata 与独立的 client/request identity；语义等价但
  字节不同的 SQL 也视为 changed payload。M8 冻结的 preimage 是 ASCII
  `BUSTUB_RAFT_WRITE_INTENT`（不含 NUL）+ big-endian `u32 fingerprint_format=1` + big-endian
  `u32 operation_kind=1 (WRITE_SQL)` + big-endian `u32 payload_length` + 已解码请求中的 exact SQL bytes；算法是
  FIPS 180-4 SHA-256，结果为 32 raw bytes。`RequestFingerprintV1` 的持久化表示是 big-endian `u32 version=1`
  加该 32 bytes。它不是认证、签名或抗 Byzantine 证明。mismatch 使用现有 client response V1 的
  `REJECTED` 状态与稳定 UTF-8 payload `request payload does not match request identity`，不为错误文本升 client wire。
- 本候选不做认证、client 注册/token、自动 ID 分配、同 client 并发、多个 in-flight proposal、group commit、
  rolling upgrade 或 term-0 迁移。分区两侧可能暂时各有同 ID 的不同未提交 entry；V1 模型只要求其中一个
  一旦提交，之后的冲突版本 fail-closed 且绝不产生第二次副作用，不声称有全局 precommit reservation。
- 旧 V1 Session 没有原 payload，不能回填可信 fingerprint。M8 只选择 fresh-directory homogeneous cutover：
  三节点使用同一 M8 executable 与三个空的新 distributed directories，CommandBatch/Session 只读写 V2，明确
  不支持复用 M7 durable directory、V1/V2 dual-read、mixed binary 或 rolling upgrade。若以后确有保留旧数据要求，
  再单独设计 `digest-unknown` 的 offline upgrade，不能静默把未知值当作匹配。
- 专属 E2E 至少对最近一次 committed request 覆盖：同 payload byte-stable retry、普通路径 changed-payload 拒绝、
  响应丢失后切主拒绝、snapshot 后拒绝、三节点全停全启后拒绝。业务查询/计数独立证明无第二次业务副作用；
  pre/post durable last-log-index、日志字节或命名 storage-event oracle 另行证明没有第二次 append。组件格式测试
  使用手写 write-intent golden、标准 fingerprint vector 和 literal SessionV2 fixture 恢复后发送真实 raw request；
  禁止先调用 production `Fingerprint(A)` 填 fixture，再调用同一函数比较来形成自循环 oracle。

#### M8 格式影响矩阵

| 格式 | M8 决定 | 理由与拒绝边界 |
| --- | --- | --- |
| client request/response wire | 保持 V1 | 请求已携带 exact SQL bytes，响应已能表达稳定 `REJECTED` payload |
| Raft RPC | 保持 V1 | CommandBatch 是 opaque entry payload；不声称 mixed binary 可用 |
| CommandBatch/log payload | 只接受 V2 | 保持 family magic，在 `client_id/request_id` 后加入 fingerprint version + 32 raw bytes；V1/未知版拒绝 |
| Catalog | 保持 V1 | fingerprint 不改变数据库 schema/state 表达 |
| Session snapshot | 只接受 V2 | 每条 `(client_id,last_request_id)` 后加入 fingerprint version + digest；V1/未知版拒绝 |
| BusTub snapshot bundle | outer 保持 V1 | `session.bin` 是自版本化 opaque member；outer V1 + inner SessionV2 合法，inner V1 fail-closed |
| node-directory marker | 保持现状 | fresh-directory homogeneous deployment 是运维前置，本阶段不实现 capability/rolling marker |

#### M8 测试与退出门禁

1. SHA-256 使用标准 empty/`abc`/long-message known-answer；write intent 使用手写 preimage 与外部固定 digest。
   CommandBatchV2 和 SessionV2 都用非空 literal fixture 验证 exact bytes、decoder、截断、CRC、未知/V1 拒绝；
   关键 expected 不得由 production encoder/fingerprint helper 生成。
2. 组件测试发送真实非空、非幂等 SQL。先提交最近请求，再让其他 client 推进数据库状态，随后用同一身份发送
   在新状态下本可成功的 changed SQL；必须在 prepare/propose 前稳定拒绝。literal 行/计数/OID/Session 水位证明
   无业务副作用，独立 pre/post durable last-log-index 或命名 storage event 证明没有 append，不能以其一代替另一项。
3. 三个正式 `bustub-node` 的专属 E2E 覆盖正常 changed-payload、响应丢失后切 Leader、snapshot 发布/安装、
   三节点全停全启；同 payload 的 cached response 必须 byte-identical，changed payload 必须精确 `REJECTED`，
   并由 literal SQL 查询证明无第二次业务效果。组件层独立承担无法从 client status 推断的 durable no-append oracle。
4. 新测试必须注册到 CMake/CI，重跑所有受影响的既有 component/process gate、定向 ASan/UBSan；ActiveWrite 与
   Session 并发路径变更还须跑定向 TSan。源码静态检查不得新增系统 crypto 依赖、测试反向依赖或旧格式暗读。
5. 退出前停止所有 node/client/proxy，清除 `/tmp` build/artifacts、源码树 build/cache/object/report/core；记录精确
   测试证据和 commit。M8 全部门禁完成即停止，不选择或预执行下一候选。

# 非规范执行与审计记录

本章只保存已发生修订的证据、测试计数和清理记录，不定义新功能、阶段 owner 或下一执行步骤；
若与前文规范部分冲突，以“文档权威、执行轴与共享前置契约”规定的顺序为准。

## 执行状态（基线与当前指针）

M0–M7 的历史全量验收基线是 `ec11bb0f9f15d1e5abaedb64ea44dee5c6606e66`；基线后的
recovery/owner/CI 维护为 `1178cdf125bad28d3030ab78b37e641fa10c6158`，M7 后候选 DAG 与 M8 分配契约为
`66cb1e94e5f7e71fcf3eedf668a268a6d522f495`。获准的过时嵌套源码副本和四个根目录运行/诊断产物由
`2a1d2ce` 清除。唯一已分配的 M8“写重试 payload 绑定”已经实现并提交为 `958fc80`；本节后续的
M0–M7 数字仍只证明其各自历史修订，M8 的当前证据以紧随其后的验收记录为准。

当前没有半执行阶段。M8 完成后停止边界已经生效：不选择或预执行候选 DAG 中的下一节点，等待用户明确命令。

以下所有带 2026-08-29 日期的数字均是对应历史修订的证据，不代表后来源码；
证据以段落明确写出的 commit 为准，不再使用会随后续编辑失效的“当前工作树”指代。

### M8 实现与最终验收（2026-08-30）

`958fc80` 在 state-dependent SQL prepare 前对非空 exact raw SQL 计算 domain-separated FIPS SHA-256，
把 versioned 32-byte fingerprint 随 CommandBatchV2 复制并随 SessionV2/snapshot 持久化。最近一次请求身份的
相同 payload 返回原始 cached response；不同 payload 在 committed Session 或同节点 active proposal 边界
fail-closed，使用稳定拒绝文本，且不进入 prepare、第二次 append 或 Apply。Apply 仍做最终一致性检查；
Leader 变更后若同一身份的另一 payload 获胜，旧 active proposal 只在原日志槽已被覆盖后解除，不误报 fatal。
CommandBatch 内层上限预留 20-byte versioned-frame overhead，使完整 encoded batch 始终能装入一个 64 MiB
LogCodec payload；超限请求在 proposal 前按普通拒绝处理，而不是使节点 fail-stop。

格式只升级 CommandBatch 和 Session snapshot；client/response wire、Raft RPC、Catalog、outer snapshot bundle
和 node marker 保持原版本。V1/未知 CommandBatch 或 Session 使用真实 checksum-valid literal fixture 验证拒绝；
部署边界仍是三个同代 executable + 三个全新目录，不声称 dual-read、mixed binary、rolling upgrade 或旧目录迁移。

最终动态与静态证据如下；所有安全请求使用固定 serving endpoint 的 one-shot 调用，不依赖场景重跑：

- Clang 14 Debug ASan+UBSan component gate：27/27 个二进制、146/146 项具体测试，0 failed/errors/disabled，
  `process_retries=0`，sanitizer marker 为 0。
- M8 专属 ASan+UBSan 与 Release 三进程链路各自 fresh、单次通过：6 个 changed-payload 阶段均保持
  `LOG-MUTATIONS` size/SHA 与 literal rows 不变；4 个 exact retry 阶段返回同一 committed-index-4 response 且
  无 append；覆盖丢响应切主、Snapshot@8、2 块 `65,536 + 4,627 = 70,163` bytes InstallSnapshot、由安装节点
  当选 Leader，以及连续两次三节点 cold reopen。
- TSan 正式 `raft-tsan-core`：TCP 4/4、RaftNode 31/31、SessionTable 7/7、BusTub FSM 5/5、
  DistributedNode 9/9，合计 56/56，无 race/lock-order 报告。
- Clang-format 29/29；全仓 cpplint 457/457；shell 3/3、Python AST 1/1、YAML 1/1、`git diff --check` 均通过。
  CMake source discovery、Python runner 和 aggregate target 的组件清单均为 27 且完全一致；production→test、
  旧 API/旧格式暗读、OpenSSL/libcrypto 外部依赖扫描均为 0。

原有五条 M6/M7 正式进程链路也属于 CommandBatch/Session 格式的受影响回归；它们的最终 ASan+UBSan 与
Release 单次结果记录在 `docs/testing/raft_test_matrix.md`。阶段结束时精确删除 28 个 M8 外部构建/日志/
artifact 目录，共 5,057,640,577 bytes；复扫无 M8 `/tmp` 项、后台 node/client/proxy、源码树 ignored/generated
产物或未解释的未跟踪文件。仓库来源审计另以 `2a1d2ce` 删除 1,347 个过时嵌套副本文件和四个根目录产物，
共 44,467,543 bytes，并只对四个根路径加入 anchored ignore。M8 至此完成并停止。

#### M8 推送后全量 CI 收敛（`20f1af2` 与 E2E-11 发布屏障）

`66591c1` 首次在修正后的 `main` trigger 上跑到完整 workflow。M8 ASan/UBSan 与 Release 各六条
进程链、TSan 和 Release SQLLogic jobs 已通过，但 Ubuntu Clang/GCC 在默认 `all` Build 提前失败，
因而该 run 只是定向 M8 证据，不是全 workflow 成功。失败暴露的都是 M8 前已存在但过去未被
`main` 矩阵触发的基线问题。

`20f1af2` 不改变 Raft 协议或 M8 格式；它删除四个 tool/bench 对 `test_util.h` 的反向依赖，
去掉 B+Tree 未读取局部指针，使时间戳时区格式化不再依赖固定小缓冲区，改写 GCC 会解释为续行的
注释，使九个 clang-tidy targets 显式通过 Python 运行 `100644` 脚本，并修正既有 HNSW 常量命名门禁。
时间戳回归直接使用四个预计算 packed integer 及字面 expected string，不调用 parser/formatter 生成自身
oracle；覆盖 `-12/-01/+00/+14`。

本地在资源受限的 VSCode/WSL 上强制一次只运行一个重型任务并使用 `-j1`；禁止同时启动多个
编译、sanitizer、E2E 或测试代理。这一主机调度约束不改变单次、无重试的验收语义，也不缩小独立
GitHub runners 上的 public regression/SQLLogic/sanitizer/TSan 范围。本地定向证据为时间戳 1/1、B+Tree 2/2、
HNSW 7/7、printer 真实插入/删除及空运行目录，受影响 native translation units 逐个 tidy 与 WASM
syntax-only 通过。包含该修正的最终文档 HEAD 必须由远端完整 workflow 全绿后才能标记本次收敛完成；
不为回填动态 run ID 再制造 docs-only CI 循环。

`22d51cc` 触发的 run `33311990646` 随后在 Release E2E-11 暴露测试时序缺口：脚本只观察到第二个
`SNAPSHOT-*` 文件完成 rename 就立即发送 `TERM`，但同一同步 Tick 仍可能在完成 retained bridge-log
边界；CI 因此在 10 秒关停门禁内强杀节点 1。修复不放宽关停超时，也不重试该失败 run，而是在文件数量
达到两代后发出一次 30 秒上限的正式 status 请求；该请求与快照 Tick 经过同一节点互斥边界，成功响应且
`last_applied >= suffix_index` 才允许停止节点。最新文件截断、独立 header 解析、上一代 index 与 bridge
replay oracle 均保持不变。修复后的 Release recovery matrix 在 fresh 目录单次通过，完整覆盖
E2E-02/06/07/09/11/12。

同一 run 首次进入完整 `check-clang-tidy` 后又暴露 21 个唯一诊断。20 个属于 include 顺序、局部常量命名、
不必要 move、标准算法替代及测试 helper 参数等机械清理；另一个是 `PlanSelect` 在同一调用中一边根据
`deferred_exprs` 推断 schema、一边 move 该容器的真实求值顺序/use-after-move 风险。修复将 schema 推断
明确排在 move 之前，并在 DISTINCT 分支复制很小的 `shared_ptr` vector，确保后续条件路径不会观察
moved-from 状态。18 个报错 translation unit 按 WSL 串行约束逐个通过 clang-tidy。随后单线程 Release
增量构建链接全部受影响生产/测试目标；9 个 GoogleTest 二进制通过 71/71，覆盖真实 DB 状态、持久目录、
畸形 frame、随机选举、快照恢复和 TCP loopback，vector-index SQLLogic production 链亦通过。22 个改动
C/C++ 文件通过 format/cpplint，E2E shell 通过 `bash -n`，`git diff --check` 通过。

包含上述修复的 run `33314397739` 又暴露 E2E-11 的第二个测试假设：脚本固定等待 node 1 的两代文件，
但失败 artifact 中 node 1 只有一代，同样已 apply 到 suffix 的 node 2/3 各有两代，故场景在损坏任何文件
之前退出。快照发布 Tick 受角色与调度影响，节点编号不是 correctness oracle。场景现选择首个实际保留
两代的节点，并将 status 发布屏障、快照目录、损坏、单节点重启、独立上一代 index 和真实 stale read
全部绑定到该节点；若三个节点都不满足则明确失败，损坏开始后不再切换目标。修复后的 fresh Release
matrix 单次通过 E2E-02/06/07/09/11/12；run `33314927956` 随后在独立 GitHub runner 上一次通过完整
Release production-process job，证明该调度修复没有依赖本机节点 1 恰好成为目标。

连续 workflow 的 `macos-13` job 始终停在无 runner 的 queued 状态。GitHub 官方公告确认该 image 已于
2025-12-04 退役，并要求迁移到 `macos-14` 或 `macos-15`；这不是测试结果，也不能通过继续等待变绿。
为保持项目固定的 LLVM 14 format/tidy 语义并使用 Homebrew 仍提供对应 bottle 的平台，CI 迁移到受支持的
`macos-14` ARM64，工具路径从 Intel Homebrew 的 `/usr/local` 改为 Apple Silicon 的 `/opt/homebrew`。
所有原有 build、format、lint、tidy 与 public-test 步骤不变，并新增 ARM64 编译覆盖。官方退役及 LLVM 14
bottle 依据：<https://github.blog/changelog/2025-09-19-github-actions-macos-13-runner-image-is-closing-down/>、
<https://formulae.brew.sh/formula/llvm%4014>。

迁移后的 run `33314927956` 成功分配 ARM64 runner 并安装 LLVM 14，随后由 image 自带 CMake 4.3 在
配置阶段拒绝第一个 policy floor 为 3.0 的 vendored root。仓库扫描确认顶层实际 `add_subdirectory` 的
`murmur3`、`libfort`、`utf8proc`、`backward-cpp`、`libpg_query` 与 `linenoise` 均有相同问题。六个根的
`cmake_minimum_required` 分别提升到 CMake 4 仍支持的 3.5；不通过全局
`CMAKE_POLICY_VERSION_MINIMUM` 掩盖旧声明，也不修改未纳入构建的 vendor 示例/测试。fresh 本地
CMake 3.28 配置已遍历全部子目录并成功生成，CMake 4.3 由下一远端 run 作最终验证。

### 方案结构复审与基线维护（2026-08-30）

本次复审将五个大章改为 A–D 架构工作流 + 横切规则，并以一张 M0–M7 表作为唯一执行轴。
已拆除 M1 -> M2 bridge-log 循环、M0–M2 -> M5 SQL producer 倒置、M3/M4 Raft/snapshot 混合、
M5 -> M6 majority/TCP 越界，以及 M6/M7/横切章对 E2E 的重复 owner。term-0 与 distributed
日志/快照 Store 现在被定义为物理互斥的两种运行模式，只共享逻辑状态与基础原语。

实现一致性修正分为四类，均回补既有 owner，不分配 M8：

- 删除 SnapshotManager 中与 Manifest checksum 重复且无恢复消费者的 `CHECKSUMS`，并相应收缩 term-0
  命名掉电 topology；GitHub Actions push/PR 分支从已不存在的 `master` 改为当前默认 `main`，checkout 统一 v4。
- 将 `StateVisibilityLatch` 从 distributed 下沉到 common，由 M1 拥有 capture primitive、M5 复用；将
  `SingleNodeCommandRuntime::CommitSql` 的实现移到 M5 distributed translation unit，使 M2 recovery target
  不再编译依赖未来 SQL producer。两项都只改依赖方向，不改 wire/disk 格式。
- 修正 M6/M7 进程证据中的阶段标签与时间线：E2E-13 在 replacement Leader ready 且任何新 proposal 前
  取 NOOP 水位；E2E-09 显式先完成一次分区前 ReadIndex；E2E-01 在每个进程执行字面 secondary-key 查询；
  distributed 累计链不再冒充 term-0 -> cluster 物理迁移。
- 审计发现已有 startup 仅用 `OldestRetained` 打开 LogStore，在 latest 已完整覆盖 commit、previous bridge
  不匹配时可能错误 fail-stop。新增统一 `RecoverRaftPersistentState` 与只读 `LogStore::ProbeRecovery`：任何
  durable repair 前先完整验证 latest FSM、`max(H,S)`、latest/pre-install term 和 committed suffix；只在
  `E=max(H,S)==S` 时允许丢弃不可信 suffix，在 latest boundary 与 `(S,H]` 可证明时允许提升 latest 并保留 suffix，
  否则 fail-closed 且不降低 H。恢复 helper 的 HardState/rebuild/prune 10 个实际命名事件均经过
  PowerLoss、冷启和第二次冷启的专用 cross-file oracle。

在 startup recovery helper 落地后的**中间检查点**，Clang 14 Debug + ASan
（`detect_leaks=0:halt_on_error=1`）曾验证通过 8 个二进制/60 个测试：
`snapshot_manager_test` 4/4、`single_node_runtime_test` 1/1、`sql_command_preparer_test` 3/3、
`bustub_state_machine_test` 4/4、`raft_state_machine_test` 4/4、`log_store_test` 10/10、
`raft_node_test` 25/25、`distributed_node_test` 9/9。后者在受限沙箱内只因无法分配 loopback socket
于测试入口拒绝，获准在沙箱外原样运行后 9/9 通过。该数字早于后续 live InstallSnapshot、application-neutral
proposal hook、分层测试 fixture 与 term-0 nested Session 修正，只能证明当时的子集，不能替后续源码背书。

终审随后补齐：Raft core 通过 `RaftStateMachine` hook 验证 opaque proposal；live InstallSnapshot 在任何
CURRENT/commit-index/Log/FSM authority 变化前完成 inner-state 与 pre-install suffix preflight（higher-term
的独立 durable term transition 除外）；term-0 publisher/recovery 还拒绝内嵌非零-term Session response。
提交 `1178cdf` 的最终定向 Clang 14 Debug + ASan 回归覆盖 17 个二进制/108 个测试并全部通过，其中
`raft_node_test` 为 31/31、`state_manifest_test` 为 9/9、`distributed_node_test` 为 9/9；loopback 用例只在
受限沙箱入口被拒绝，原命令在允许本机 loopback 的环境中通过。本次仍没有重跑 122 个基线组件、40 个 SLT、
全部正式 E2E 或 TSan，不能把 108 个定向测试冒充新一轮全量 M7 验收。最终静态门禁通过全部本轮
C/C++ 的 Clang 14 format dry-run 与仓库参数 cpplint、3 个修改 shell 的 `bash -n`、CI YAML 解析、
`git diff --check`、Raft -> distributed、recovery -> SQL producer 及 production -> test 反向依赖扫描。
清理审计还发现 `table_heap_reopen_test` 只删 `.bustub`、漏删同 stem `.log`；夹具已修复并复跑 1/1。
早期 431,479,634-byte 审计构建树已删除；本轮又精确删除 1,039,771,113-byte 外部 ASan 构建树和遗留的
0-byte reopen 日志。复扫后 `/tmp/bustub-*`、后台 node/client/proxy、源码树生成/ignored 文件均为空；
提交前盘点的 44 个已跟踪修改、2 个正式删除和 4 个已注册源码新增文件均已收入 `1178cdf`，不再是工作树状态。

2026-08-29 选举超时补强：固定单值配置已替换为生产随机区间；Raft 核心接受可注入 timeout source，
确定性测试显式使用固定值或固定 seed。M6/M7 进程 harness 的三个节点使用同一区间，不再按节点编号设置
不同常量。该补强只修正 M3/M6/M7 已有范围，不开启新阶段；针对性验证记录见测试矩阵。

2026-08-29 持久化边界收敛：CommandLog、StableStore、LogStore 的三个 `DurableFuture` 接口改为同步
`void` API，成功返回即表示相应文件持久化屏障已经完成，失败直接抛出；删除同步执行后再包装 ready future、
调用方立刻 `.get()` 的冗余层。本选择借鉴 etcd/raft `Ready` 的关键顺序约束——依赖 HardState/Entries 的
消息不得先于持久化发送——但 V1 不复制其异步 surface，也不引入没有调度器支撑的 completion。测试层统一
使用 `before_write / after_fsync / after_rename / after_dir_fsync` 命名事件。Snapshot/单 Store 原子替换使用
old-or-new oracle；InstallSnapshot 只复用事件框架，使用跨文件 `max(H,S)`/suffix/continuity 专用 oracle。
该修正仍位于已完成阶段的 durability
验收范围，不进入新阶段。

2026-08-29 架构收敛补强：M6/M7 的进程生命周期与客户端 Leader 重定位已抽到单一测试 harness；当前四条
聚焦脚本和一条文件名沿用 `raft_m0_m7_chain.sh` 的 distributed 累计链路只保留 E2E 时间线。
该脚本从全新 distributed 目录开始，不执行 term-0 格式迁移。`magic/version/length/CRC` 校验抽成公共
`VersionedFrame`/`ChecksummedFrame` 骨架，
CommandBatch、客户端协议、Raft RPC、Manifest、HardState、Session、Catalog 与既有 snapshot bundle 保留各自
协议类型和原有线格式，只共享边界检查。正式 BusTub 快照改为 `DurableFileSlice` + 有界块：canonical
文件打包、SnapshotStore 发布/恢复、64 KiB InstallSnapshot 传输、Follower 临时下载及 FSM 安装均不再持有
完整 payload vector；128 MiB 内存上限只留给测试便利 codec/KV 示例，正式文件 payload 使用 1 GiB
实验安全上限。分块用于验证 Raft InstallSnapshot 和崩溃边界，不构成大型数据库容量声明。

V1 继续保留自研 Raft 核心以服务 BusTub 状态机学习和不变量验证，不在本轮替换库。若目标改成真实生产，
应另立迁移项目比较 NuRaft 等成熟 C++ 实现的选举、压缩、快照、可插拔 LogStore/FSM、group commit 与 pipeline，
并重新完成 wire/disk 兼容、故障语义和整套 E2E；这不是当前实现上的局部重构。Raft 当前的“完整状态快照 +
lastIncludedIndex/Term + 分块 InstallSnapshot”仍遵循论文模型；base backup + 连续日志只在项目目标改为更大数据库
时重新立项，而不是在本轮偷偷改变恢复协议。针对性 ASan/UBSan 验证通过 16 个二进制/55 个测试及最终 M6/M7
三进程场景；格式、cpplint、shell 语法和 production 依赖审计通过，1.5 GiB 外部构建树与 8 个过程 artifact
已按清理门禁删除。本补强完成后仍停在 M7，等待新的用户命令。

2026-08-29 实验范围收敛：项目定位明确为“教学版 BusTub + 简单静态三节点 Raft”，不把正式路径误称为
大型数据库生产能力。保留暂停写入的完整 canonical snapshot、`lastIncludedIndex/Term`、文件持久化和 64 KiB
InstallSnapshot 分块，用于验证 offset、重复/半传输、崩溃发布与 suffix 恢复；不继续实现 base backup + WAL、
增量/fuzzy/COW、跨进程续传、压缩、限速、多流、pipeline 或低停顿 SLO。正式文件 payload 防御上限由 1 TiB
收紧为 1 GiB，内存兼容 codec 保持 128 MiB，并以编译期断言保持两个文件上限一致。新增测试只构造越界元数据，
验证在分配或写入前拒绝异常输入。针对性 Clang 14 ASan/UBSan 验证为 4 个二进制/21 个测试全部通过，M7
正式三进程快照崩溃时间线通过；一次 TCP 测试和 M7 的两个节点启动尝试在进入测试/服务前空输出退出 139，
各自仅由既有有限门禁重试，没有测试体失败或 sanitizer 报告被重试。该收敛不新增阶段，完成后仍停在 M7。

2026-08-29 测试缺口回补（历史修订记录）：审计发现原“E2E-01～15 已覆盖”的表述混用了同进程 TCP 集成与
正式进程证据，当时改为四条共享 harness 的正式三进程时间线，随后又增加一条 M3–M7 distributed
durable state 累计链路（文件名因历史原因仍为 `raft_m0_m7_chain.sh`）。M6 增加
无主键/secondary UNIQUE 无副作用拒绝、真实响应丢失、
`WriteResponseV1` 精确字节比较、三节点逻辑结果比较和 stale/read 水位断言；当时标为 NOOP 的进程断言
实际发生在后续客户端写之后，已由本次基线后复审改为 replacement Leader ready 后、任何新 proposal 前取证。M7 增加少数派后缀覆盖、旧
Leader 新读超时、全停全启后继续 DDL/OID 分配、损坏最新快照回退、800 行 batch 并发读、Snapshot S + 后缀
S+1、以及延迟完整旧 Snapshot 重放后继续 Apply。网络丢失、响应丢失、文件损坏均由 test-only 外部代理/脚本
制造，production 协议没有测试开关。节点根目录新增 checksummed `node.conf`，首次原子持久化 node/group/voters，
以后配置不符或损坏均拒绝打开。Raft 组件补齐所有 higher-term 入口和本地 append 的 durable-before-send、
投票/旧 term/日志新旧/乱序响应/单向丢包，以及 Snapshot/HardState/Log/FSM 的 InstallSnapshot 命名崩溃矩阵。
Session 空洞/过旧、64 次固定 seed canonical permutation、Manifest 跨副本字段和主键准入矩阵也已补齐。

该修订的源码验收为 Clang 14 ASan/UBSan 25 个二进制/81 个测试全部通过，四条正式进程时间线覆盖 E2E-01～15，
TSan 核心为 15/15 + 4/4；CI 新增正式矩阵、独立 nightly schedule、固定 GTest seeds 与失败 artifact。统一
CTest 在本宿主 PRE_TEST discovery 阶段遇到空输出 pre-main 139，未进入测试体，因此以
`test/e2e/raft_gtest_gate.py` 固化同一严格规则逐二进制运行；仅 SIGSEGV/139 且 stdout/stderr 均为空可在五次
总尝试内重试，其他失败立即终止。最终脚本运行发生 17 次该宿主启动抖动，并曾正确拒绝有输出的 TCP bind
失败。本次没有重跑历史 Release SQLLogicTest 40/40，
因为改动不触及单机 SQL 语义。回补仍属于 M7 验收修正，不进入 V2；完成清理门禁后继续等待用户命令。

该轮清理门禁完成时：精确删除 2.4 GiB 的外部 ASan/UBSan、TSan 构建树、进程现场与测试日志；指定的
`/tmp` 前缀、后台 node/client/proxy、源码树 build/cache/core 均为空。当时工作树为 164 项正式交付（61 个
已跟踪修改、103 个新增文件、0 个删除），新增文件仅为实现、已注册测试、复用 harness/故障工具与文档。
测试缺口回补至此完成并停在 M7。

### 最终 production-oracle 复查（2026-08-29，历史记录；由文末收口复核取代）

- 修复三个由真实测试暴露的实现问题：`SessionTable::RecordCommitted` 不再因首个 gap 请求隐式插入空会话；
  `IndexIterator::operator*` 返回拥有生命周期的 key/RID pair，不再返回对叶页访问临时值的引用；正式客户端在
  接受成功响应前同时校验外层 request ID、内层 `WriteResponseV1.request_id` 及请求类型/成功状态的一致性。
- 固定 wire/disk oracle 不再依赖同一套 production encoder/decoder 自证：CommandBatch、client、Raft RPC、
  Log、HardState、Manifest、Catalog、Session 及 BusTub snapshot bundle 均有独立手写 golden；bundle golden
  同时约束 aggregate codec、流式 `EncodeFiles` 和带非零 offset 的 `DecodeFile`。
- 命名故障框架要求事件类型、occurrence、路径、related path 和顺序全部精确匹配；目录 fsync 不再错误发布
  sibling 目录项。进程 harness 会核对 node/proxy/后台 helper 的退出状态，代理协议解析或快照块内容冲突不能
  只写日志后继续变绿。
- 四条聚焦进程场景加一条 distributed 累计链路全部使用有业务含义的数据和字面结果。快照崩溃场景使用 1600 行并分别核对
  完整快照、bridge suffix 和触发被杀 capture 的 Apply 效果；损坏场景持久截断最新代、独立解析前一代 index
  并要求精确选择；传输场景要求实际多块、`Snapshot@S` 精确等于记录值且 `S < suffix`；连续链路从全新
  distributed 目录开始，在同一份 M3–M7 durable state 上穿过准入、响应丢失、切主、快照+后缀、stale replay、
  身份拒绝、全停全启及恢复后继续 DDL/DML。它累计复验早期逻辑性质，但不迁移 term-0 物理状态。
- 该修订当时的本地 Release 严格门禁为 26 个组件二进制、102 个具体测试，全部单进程尝试通过，0 failed、
  0 disabled、`process_retries=0`；原生 `b_plus_tree_insert_test` 3/3；五条正式进程时间线各在全新目录中
  单次通过。该次
  transfer 证据为 3 块/135,485 字节，连续链路为 5 块/266,737 字节、Snapshot@12 term 2。
- 上述 26/102 是该修订当时的 Release 证据。后续 oracle 修正没有冒充新一轮全量 ASan/UBSan、TSan 或
  SQLLogic；旧
  sanitizer/TSan/SQL 结果保留为历史修订证据，CI 会通过 `build-raft-component-gates` 和五条时间线重新验证。
- 该轮清理逐项删除 49 个 `/tmp` 构建/日志/artifact 目录及 6 个零字节日志，共 2,154,049,193 bytes
  （2.006 GiB）。复查后 `/tmp` 候选、后台 node/client/proxy、源码树 build/cache/object/report/core 均为 0；
  当时工作树为 169 项正式交付（64 个已跟踪修改、105 个新增、0 删除），没有中间产物混入交付。

本次只补齐 M0–M7 的测试与实现盲点，完成清理门禁后仍停在 M7，不进入 M8/V2。

### 最终修复收口复核（2026-08-30，历史 M0–M7 全量验收基线）

恢复执行前重新核对方案、工作树、测试注册和上轮现场，确认仍处于 M7 收口而非下一阶段。最终复查并修复了
四类会让测试或运行链路失真的问题：`DiskManager` 的 WAL 双缓冲状态改为实例所有，避免两个数据库实例共享
进程全局缓冲；B+Tree root 发布改正页锁取得顺序；TCP transport `Stop()` 先关闭队列并丢弃陈旧 backlog，再
join worker；测试代理显式处理 `SIGTERM`，即使父进程曾忽略该信号也能按 harness 要求退出，成功场景不再依赖
cleanup 强杀。

InstallSnapshot 的 liveness 修复保持 V1 协议范围不变：Leader 现在为每个 peer 保存独立 snapshot transfer；
heartbeat 只重发同一 in-flight 块，不刷新 request ID；Follower 对重复旧块返回真实 durable high-water；Leader
仅接受匹配活动请求且单调向前的进度。真实 192 KiB 业务值测试预置两个已 fsync 的 64 KiB 块，让下一块 ACK
跨越 heartbeat，逐字节核对重传身份，并进一步丢弃最终 COMPLETE ACK，验证重复末块失败关闭、从 offset 0
重启、已发布快照 stale-complete、旧 ACK 晚到无副作用及后缀继续 Apply。该测试使用真实 `StageChunk`、文件
同步、发布、FSM 安装和独立 KV 结果，不是空参、同源 round-trip 或“输入 A 输出 A”的自证。

基线提交 `ec11bb0f9f15d1e5abaedb64ea44dee5c6606e66` 的最终动态门禁如下，
历史段中的旧数字不再代表该基线：

- Clang 14 ASan/UBSan 组件门禁：26/26 个二进制、122/122 个具体测试，0 failed/errors/disabled/not-run，
  26 份非空 JSON 和日志均可解析，`process_retries=0`，无 sanitizer marker。
- TSan 核心：`tcp_transport_test` 4/4、`raft_node_test` 17/17、`bustub_state_machine_test` 4/4、
  `distributed_node_test` 9/9，合计 34/34，无 data race 或 lock-order-inversion。
- Release SQLLogicTest：40/40，0 failed；长耗时 `leaderboard-q1-index` 自然完成 669.72 秒，未中断或重跑。
- 五条 ASan/UBSan 正式三进程场景与同五条 Release 场景各自单次通过，各覆盖 24 个 timeline。聚焦传输为
  3 块/135,485 bytes/Snapshot@4 term 1；在同一份 M3–M7 distributed durable state 上运行的累计链路为
  5 块/266,737 bytes/Snapshot@12 term 2。两套均无异常 cleanup、残留进程/端口或协议/sanitizer 报告。
- 全部 `src/`、`test/` C/C++ 通过 Clang 14 format dry-run 和仓库 cpplint；6 个 shell 通过 `bash -n`，
  4 个 Python helper 通过 AST 解析，`git diff --check` 和 production 对测试依赖反向扫描通过。

本 WSL 宿主的 Clang 14 ASan/TSan 在随机地址布局下会于 `Running main` 前发生运行库映射失败；原始空日志和
host 的 `overflowed sigaltstack` 证据先保留。最终 ASan 组件 gate 及完整 E2E 父进程树各自在全新目录中整体
单次置于 `setarch x86_64 -R`，仍为 `process_retries=0`，没有逐二进制或逐节点重试。该局部环境处理不用于
Release、原生 Linux CI 或任何进入测试体后的失败，规则已写入 runbook。

结束时精确删除 21 个已逐项盘点的外部构建、组件日志、成功/失败 E2E 现场和零字节诊断日志，共
2,676,096,997 bytes（2.492 GiB），均可按 runbook 重建。复扫 `/tmp` 任务前缀、后台 node/client/proxy、
18,100–31,899 端口、源码树 ignored/generated 文件均为空。基线提交前的 208 项正式交付清单
（103 个 tracked 修改、105 个新增、0 删除）已全部收入 `ec11bb0`，该数字不再是后续工作树状态。
当时把 1,347 文件、36,074,734 bytes 的已跟踪嵌套树判断为课程基线并保留；该判断后来被 M8 的路径来源
审计推翻：它是顶层源码提升后遗留、无构建/测试/CI 引用的过时完整副本。经用户明确授权，`2a1d2ce` 删除
该嵌套树，并同时删除四个被误跟踪的根目录运行/失败诊断产物 `test.bustub`、`test.log`、`expected.log`、
`result.log`；四个根路径使用 anchored ignore 防止复发，嵌套目录不忽略，以便再次生成时立即可见。

在 `ec11bb0` 验收结束时，当时的独立只读审查未发现 blocker；后续审查又发现真实 startup recovery、
live InstallSnapshot preflight 和 owner/fixture 缺口，并由 `1178cdf` 修复。因此本段只能证明 `ec11bb0` 的历史
全量验收，不能继续充当新提交的 “no blocker” 证书。`1178cdf` 的定向验证范围见前文维护段。该历史时点
尚未分配 M8；后续用户只选择了 payload binding，并由 `958fc80` 完成。其余候选仍未获分配，不能自动捆绑执行。

# 合理性复审结论（2026-08-30）

在“教学版 BusTub + 静态三节点 + crash-stop/非 Byzantine”定义下，当前 M0–M8 主线合理；client/Raft wire
仍为 V1，CommandBatch/Session 已按 M8 升为 V2，
不需要因本次复审重写 Raft 核心或替换成第三方库：

- 先用 canonical full-state snapshot + committed suffix 建立单节点 BusTub consumer 恢复闭环，再以 KV FSM
  隔离验证 Raft，最后把 M2 consumer 接入 Raft 并补 M5 SQL producer/完整性强化，依赖顺序正确。
- 日志/HardState 使用同步 durable API，依赖该状态的 RPC 在持久化后才发送；
  这与 etcd/raft `Ready` 所强调的 Entries/HardState/Snapshot 与 Messages 顺序约束一致，
  而不复制一个没有调度器的伪异步 surface。
- 完整状态快照携带 `last_included_index/term`、分块 InstallSnapshot 并在快照后继续追 suffix，
  符合 Raft 论文的状态压缩模型。暂停写、1 GiB 防御上限和非增量快照是课程规模约束，
  不是对大型数据库 production 能力的声称。
- 全局 visibility latch、一次一个 proposal、derived indexes、随机选举区间 + 测试 timeout source、
  稳定 Session response 和分层 production-like E2E 都是适合该实验目标的可验证简化。

`NO-STEAL + NO-FORCE` 在这里是逻辑状态政策：未提交命令不进入公开 working pages，
已提交 working pages 可不立即 FORCE，因为重启丢弃 working state 并从权威 snapshot/log 重建。
它不是对通用 steal-capable page cache 已实现 ARIES 恢复的声称。额外持久化 `commit_index`
是为了简化重启恢复而有意采用的项目约定，不冒充 Raft 原论文的最小必需持久状态。

复审后保留的主要债务均已变成显式边界，而不是阶段间隐式冲突：term-0 与 distributed
物理 Store 不原地互通，且当前只以“集群使用全新目录”的部署前提隔离模式，没有 durable mode marker；
V1 client 只提供 Leader hint 而非通用自动路由；M8 已把最近 Session identity 持久化绑定到 exact payload
fingerprint，但仍不支持 DROP、secondary UNIQUE、多语句或并行 proposal。剩余能力构成有共享前置的候选
DAG，而不是彼此独立或必须顺序执行的 M9、M10；每次只能明确分配一个 ready node。大库低停顿备份、安全
认证、动态成员和成熟库迁移会改变本实验项目定位，已移出普通后续候选。M0–M8 已完成，当前不分配下一节点。

# 已决风险与设计取舍（不构成下一候选 backlog）

## 1. Catalog 恢复比复制协议更早暴露问题

现有 Catalog 是非持久化结构，TableHeap 也缺少清晰的“按首页面重新打开”构造路径。必须先解决这两个问题，否则快照文件即使复制成功也无法恢复数据库语义。

## 2. Snapshot 是否 self-contained 是前置问题

BusTub 的部分 MVCC undo 信息只存在于内存 Transaction 中。M0 必须以 feasibility harness 的等价
exclusive capture barrier 证明：停止准入、排空事务并完成 GC/canonicalization 后，
`db.bustub + catalog.bin + session.bin` 可以在没有旧进程内存的情况下恢复全部 committed state，包括每行
真实的最近提交时间戳、`schema_epoch`、OID 和 SessionTable。M1 才把该屏障固化为 production
`StateVisibilityLatch`。若不能，必须先定义并持久化缺失状态，不能直接复制数据库文件，也不能依赖先 Flush
不权威的 working file 来掩盖缺失元数据。

## 3. 不能依赖会修改页面的 write set 提取命令

最终逻辑 CommandBatch 需要稳定主键、旧 commit timestamp、完整旧 tuple 和完整新 tuple。V1 应实现只读取 committed state、把单条 statement 展开成 mutation buffer 的 `CommandBuilder`。distributed CREATE TABLE 必须在 proposal 前执行 `PrimaryKeyCodecV1` 准入检查，恢复时 Catalog 也必须验证同一 identity；如果只能通过正常写执行器修改 TableHeap 后才能得到 write set，或表没有协议支持的主键，就尚未满足 proposal 前置条件，不能进入 Raft 集成。

## 4. V1 DDL 准备与 V2 overlay 边界

现有 DDL 可能立即修改 Catalog。V1 的单条 DDL prepare 只需验证定义、从 committed allocator 读取但不公开消费确定性 OID，并把 `table_oid/primary_index_oid/index_oid` 显式编码进相应 Catalog command；公开 Catalog 与 allocator 仍只能由 committed-entry Apply 修改。Follower Apply 只校验并使用 command OID，不能调用自己的 `next_oid++` 重新决定。只有 V2 支持 DDL 后继续 DML 的多语句 batch 时，才需要完整私有 Catalog/Table overlay。

## 5. 快照期间暂停写入

这是实验范围内有意接受的正确性简化。测试可记录快照大小、总耗时和暂停时间帮助解释行为，但 V1 不以
低停顿 SLO 为验收门禁，也不因为这些指标增加 copy-on-write、fuzzy checkpoint 或 ARIES。若未来项目目标
发生变化，应先重新立项和定义恢复协议，而不是在本实现中渐进堆叠生产备份机制。

## 6. 物理状态不必逐字节相同

逻辑复制允许 RID、空闲页和所有索引物理布局不同。第一版 snapshot builder 每次生成只含 canonical table heaps 的新数据库文件，避免忽略旧索引 root 后产生代际空间泄漏。监控和测试应比较 Catalog schema、主键到 tuple 的映射、索引查询结果、commit index 和请求去重状态。

# 参考资料

- Diego Ongaro、John Ousterhout，《In Search of an Understandable Consensus Algorithm (Raft)》：<https://raft.github.io/raft.pdf>
- Raft 作者维护的实现与测试资料索引：<https://raft.github.io/>
- HashiCorp Raft 文档与实现：<https://github.com/hashicorp/raft/tree/main/docs>
- rqlite 架构设计：<https://rqlite.io/docs/design/>
- LevelDB implementation notes（WAL、MANIFEST、恢复思路）：<https://github.com/google/leveldb/blob/main/doc/impl.md>
- SQLite atomic commit（文件、同步与原子 rename 的背景）：<https://www.sqlite.org/atomiccommit.html>
- etcd/raft Ready 持久化与消息顺序：<https://github.com/etcd-io/raft/blob/main/README.md>
- etcd robustness 的流量/故障历史与简化模型校验：<https://github.com/etcd-io/etcd/blob/main/tests/robustness/README.md>
- PostgreSQL UNIQUE 与 PRIMARY KEY（PRIMARY KEY 强制 `NOT NULL`）：<https://www.postgresql.org/docs/17/ddl-constraints.html>
- PostgreSQL base backup + 连续 WAL：<https://www.postgresql.org/docs/16/continuous-archiving.html>
- NuRaft 的 Raft、snapshot、可插拔 LogStore/FSM、group commit 与 pipeline 能力：<https://github.com/eBay/NuRaft>

这些资料用于借鉴磁盘状态组织、协议边界和故障测试方法；具体实现仍应服从本项目的 Catalog、MVCC、索引和 CMake/test 结构。

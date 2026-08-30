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

- 不可变全量快照、原子 `CURRENT`/`MANIFEST` 发布，以及“最多两代有效恢复点 + 从最老恢复点开始的 bridge log”保留策略。
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
- 以大型数据集吞吐、最低 RSS 或写停顿 SLO 作为 V1 验收条件；本阶段只验协议、持久化和逻辑恢复正确性。
- 任意索引物理字节持久化；第一版把包括 primary-key index 在内的 B+Tree、Hash、HNSW、IVFFlat 等结构统一视为 derived state，只持久化定义并从表数据重建。
- 以原始 SQL 文本作为最终复制协议。
- secondary UNIQUE index/constraint 和任何需要 deferred constraint checking 的 batch；V1 只保留不可更新的 primary-key uniqueness 与普通 non-unique secondary index。
- 多个 in-flight proposal 和通用 group-commit scheduler；Follower 追赶时一次 `AppendEntries` 携带多条 entry 只属于 batch append。

### V1 范围冻结

V1 功能范围保持冻结。本轮只补 safety contract、确定性细节和已经确认的边界收敛，不引入新的 Snapshot
形态、事务模式或性能调度器。V1 持久化接口明确选择同步 API，不保留“同步执行后包装 ready future”的
伪异步层；真正异步 completion 和可控存储调度器属于未来性能阶段。完整文件快照和分块传输到此为止，
不再以 production scalability 为理由继续增加 base backup/WAL、增量/fuzzy/COW、续传、压缩或 pipeline。
多语句事务、并行 proposal、group commit 和 secondary UNIQUE 同样留到独立后续项目，除非发现 safety 缺口。

## 必须始终成立的不变量

1. 完成启动恢复归一化后，运行态必须满足 `last_applied <= commit_index <= last_log_index`；被快照压缩的日志以 `last_included_index` 作为逻辑日志基点。
2. `published_applied_index` 表示“效果已完整发布的最高连续 Raft log index”，不是“最近一条修改数据的 CommandBatch index”。Apply 每一种 entry（包括不修改数据库的 `NOOP`）都必须推进它；由 `Snapshot@S` 恢复或安装快照后，`published_applied_index` 与 `last_applied` 都初始化为 `S`。
3. Apply 线程只能按连续递增的日志索引执行，不能跳过、并行乱序或重复产生副作用。
4. 节点回复日志持久化成功前，对应日志字节必须已经越过 `fdatasync/fsync` 持久化屏障。
5. `CURRENT` 只能指向已完整写入、已同步并且 checksum 校验通过的不可变 `MANIFEST-N`。
6. 一个快照必须同时描述同一日志索引处的数据库文件、Catalog、OID 分配器、`schema_epoch` 和请求去重表。
7. 未提交日志永远不能 Apply；只复制到少数节点的日志允许被后续 Leader 覆盖。
8. 已提交日志不能丢失。新 Leader 必须包含所有已提交条目。
9. 相同快照和相同已提交日志前缀必须得到相同的逻辑数据库状态。
10. 一个 CommandBatch 是一个原子可见状态转换；表数据、Catalog、全部索引、MVCC commit timestamp 和 SessionTable/去重状态不能向并发读者暴露部分新、部分旧的组合。
11. 若最老的保留快照边界为 `S_old`，本地必须保留从 `S_old + 1` 到当前日志尾的完整连续日志；没有这段 bridge log 的旧快照不能被计为可回退恢复点。
12. 测试不得通过测试专用网络 API、隐藏管理命令或 production 默认分支改变生产行为。
13. InstallSnapshot 只能在安装前的本地逻辑日志满足 `TermAt(S) == T` 时保留 `index > S` 的旧 suffix；否则必须丢弃全部旧 suffix，并建立 `snapshot_base = (S, T)`。
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

# 第一阶段：节点目录、Catalog 持久化与单机恢复闭环

## 背景

Raft 不能弥补状态机自身不能重启恢复的问题。本阶段先让单个 BusTub 节点在任意日志追加、Apply、快照发布阶段崩溃后，都能找到最近一次可信快照并重放已提交命令。

本阶段不是只搭接口。结束时必须有一个可运行的单节点纵向链路：SQL/命令进入、日志持久化、Apply、快照、杀进程、重启、查询验证。

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
  -> 通过正式 FSM Apply 重放 Log[S+1..effective_commit_index]
     每条 Apply 同步增量维护 primary/secondary indexes
  -> 完成一致性校验后开放请求
```

## 节点目录范式

一个节点是一个进程和一个独立目录。单机阶段只启动一个目录；到集群阶段才分别启动 `node-1`、`node-2`、`node-3`，绝不能让三个节点共享同一个 `db.bustub`。

```text
node-1/
├── LOCK
├── node.conf
├── raft/
│   ├── HARD_STATE
│   └── log/
│       ├── LOG-000008
│       └── LOG-000009
├── state/
│   ├── CURRENT
│   ├── MANIFEST-000006
│   ├── MANIFEST-000007
│   ├── SNAPSHOT-000006/
│   │   ├── db.bustub
│   │   ├── catalog.bin
│   │   ├── session.bin
│   │   └── CHECKSUMS
│   └── SNAPSHOT-000007/
│       ├── db.bustub
│       ├── catalog.bin
│       ├── session.bin
│       └── CHECKSUMS
└── working/
    └── db.bustub
```

`CURRENT` 不是数据库内容，而是很小的原子入口，例如只包含 `MANIFEST-000007\n`。`MANIFEST-N` 是第 N 代快照的不可变说明书，记录文件名、边界索引和校验值。它不是不断 append 的单一大文件；每代新建一个小文件。

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
- `last_included_term`：该 index 的 Raft term；单机阶段为 `0`，集群阶段用于快照和日志匹配。
- `schema_epoch`：快照边界处已发布的 Catalog 版本。它也写入 Catalog snapshot，恢复时两份值必须相等，否则该代不可用。
- `database_file`、`catalog_file`、`session_file`：本代快照中各文件的相对路径，禁止逃逸节点目录。
- 三个 checksum：检测半写、误配和静默损坏；校验失败时回退上一代，不允许继续打开可疑文件。
- `next_table_oid`、`next_index_oid`：恢复 OID 分配器，防止重启后复用已有 OID。它们也写入 Catalog 快照，Manifest 中的副本用于交叉校验。

## 本阶段只做

1. 节点数据目录、独占 `LOCK` 和路径合法性校验。
2. 版本化命令日志格式与有效前缀扫描。
3. `CatalogSnapshotCodec`：序列化与恢复表、Schema、首页面 ID、索引定义、`schema_epoch` 与 OID 分配器，并与 Manifest 副本交叉校验。
4. 增加“按 `first_page_id` 打开已有 TableHeap”的明确接口；不能调用创建新表的构造函数代替恢复。
5. 暂停写入的全量快照、原子发布、启动回退和两代保留。
6. 单节点的确定性 `TransactionCommandBatch` 重放。
7. 用 Raft index 兼容的单调序号作为本地 `commit_index/published_applied_index/last_applied`；本阶段 term 固定为 `0`。

## 本阶段不做

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

## 快照发布协议

1. 关闭新的 write prepare/proposal 准入；已经进入 prepare 的请求必须完成构造或取消，不能再产生新的公开状态修改。
2. 等待已经进入复制通道的写请求 commit + Apply；如果在超时内不能排空，则取消本次快照，而不是擅自丢弃一个可能提交的 Raft entry。
3. 记录 `target = commit_index`，等待 `last_applied == target` 且 Apply loop 空闲；然后获取 `StateVisibilityLatch exclusive`。该锁会等待已开始的 reader 结束，并阻止新的 reader、Apply 与 MVCC GC 并发进入逻辑 capture。
4. 在 exclusive lock 内重新确认 `S = target = commit_index = last_applied = published_applied_index`，并断言 working state 中不存在 speculative/uncommitted write。本地创建快照时必须在压缩或删除 index S 的日志前读取 `T = LogStore::TermAt(S)`，将 `last_included_term = T` 写入 Snapshot/Manifest；若无法确定 T，则取消快照而不能猜测。单机阶段 T 固定为 0。
5. 在同一锁内执行必要的 MVCC GC/canonicalization，使快照不依赖只存在于内存 Transaction 中的 undo 信息。
6. 在同一文件系统创建 `SNAPSHOT-N.tmp/`；扫描 `S` 可见的稳定 committed rows，将每行原始 `latest_committed_version_ts` 写入新的 canonical `db.bustub`，并序列化 Catalog definitions、`schema_epoch` 和去重状态。禁止直接把包含旧索引页的 working 文件复制成下一代 canonical snapshot。
7. 完成并封闭上述逻辑 capture 后释放 `StateVisibilityLatch exclusive`；write proposal 准入仍保持关闭，临时快照文件从此只做校验和持久化，不再修改其逻辑内容。
8. 对新的 canonical 快照文件计算 checksum，逐个 `fsync/fdatasync`，再同步临时目录。快照正确性不依赖对旧 working Buffer Pool 执行 Flush；真正需要 durable 的是新快照文件。
9. 将 `SNAPSHOT-N.tmp` rename 为不可变的 `SNAPSHOT-N`，并同步 `state/` 目录。
10. 写入并同步 `MANIFEST-N.tmp`，rename 为 `MANIFEST-N`，再次同步父目录。
11. 写入并同步 `CURRENT.tmp`，原子 rename 覆盖 `CURRENT`，再次同步父目录。
12. 解除 write proposal 阻塞。
13. 新代发布后暂时可能存在三代快照。先删除最老快照及 Manifest 并同步目录，再将日志回收上界推进到新的最老快照边界；删除顺序不能反过来。

任何一步崩溃后，`CURRENT` 必须仍指向旧的完整代或新的完整代。启动时若 `CURRENT` 本身损坏，则按 generation 从新到旧扫描 Manifest；候选快照除了自身 checksum 通过，还必须具备重放到 `effective_commit_index` 所需的连续 bridge log，否则不能作为恢复点。

安装远端快照时可能没有旧快照到新快照之间的 bridge log。这种情况下旧快照应被移出“有效恢复点”集合，只保留新安装快照；等将来再成功生成下一代快照后，才重新形成两个有效恢复点。不能为了满足“两个目录”这一形式要求谎称旧快照可回退。

## 建议修改或新增文件

```text
src/include/recovery/command_log.h
src/recovery/command_log.cpp
src/include/recovery/log_codec.h
src/recovery/log_codec.cpp
src/include/recovery/state_manifest.h
src/recovery/state_manifest.cpp
src/include/recovery/snapshot_manager.h
src/recovery/snapshot_manager.cpp
src/include/catalog/catalog_snapshot.h
src/catalog/catalog_snapshot.cpp
src/include/storage/table/table_heap.h
src/storage/table/table_heap.cpp
src/include/common/bustub_instance.h
src/common/bustub_instance.cpp
```

不要直接把旧的页级 `LogManager` 改造成同时承担命令日志和 Raft 日志的混合类。可以复用文件 I/O 基础设施，但对外语义必须分开，避免未来误把 page LSN 当作 Raft index。

## 测试要求

### 单元测试

- `LogCodecTest`：正常编解码、最大长度、版本拒绝、checksum 错误、半 header、半 payload、segment 边界。
- `LogBaseSentinelTest`：空状态固定为 `snapshot_base=(0,0)`、`TermAt(0)=0`、首条真实 entry 为 1；覆盖第一条 AppendEntries、空数据库 Snapshot@0 和从 sentinel 开始恢复，确认 index 0 永不被编码或 Apply。
- `ManifestTest`：原子发布、CURRENT 损坏、最新代损坏回退、路径逃逸拒绝、两代恢复点与 bridge log 联动回收；Manifest 与 Catalog snapshot 的 `schema_epoch` 或 OID 副本不一致时拒绝该代。
- `CatalogSnapshotTest`：多表、多类型、多索引 definition、OID 连续性、`schema_epoch` round-trip、未知 IndexType/版本拒绝；验证 replicated logical primary key 的列、类型与 codec version round-trip，缺失/不受支持/mismatch 或包含 secondary UNIQUE definition 时 distributed restore 拒绝开放服务；确认第一版格式不依赖任何索引 root/header page。
- `DurableAppendTest`：单条和单次多 entry append 都只能在 `fdatasync` 后成功返回；同步失败时本次调用直接失败，不能确认部分 entry；两个独立 append 调用不要求被调度器合并。

### 单节点集成测试

- 使用真实 `BusTubInstance`、真实临时目录和正式命令 Apply 入口执行 `CREATE TABLE/INDEX`、`INSERT/UPDATE/DELETE`。
- 在快照前、快照后和日志尾存在新 autocommit batch 时重启，查询表、索引定义、下一 OID 和 `schema_epoch`。
- 构造“行最后更新于 index 800、Snapshot 边界为 1000、Log 1001 的 UPDATE 携带 `expected_old_commit_ts = 800`”的恢复场景；先断言 Snapshot@1000 的 primary/secondary indexes 已完成重建，再允许 replay 1001。UPDATE 必须通过 primary index 定位且恢复后 commit timestamp 为 1001；测试配置禁止 fallback 全表扫描，也禁止 replay 后才建索引。
- 让 reader 持有 `StateVisibilityLatch shared` 时启动快照，确认 snapshot capture/GC 在 exclusive lock 处等待；释放 reader 后得到的快照只能对应一个完整 published index。
- 比较 SQL 查询得到的逻辑结果；不要比较 `db.bustub` 的原始字节，因为合法的物理布局可以不同。

### 崩溃点测试

- 复用测试专用的命名故障注入框架，在 `before_write / after_fsync / after_rename / after_dir_fsync` 四类事件及其 occurrence 上逐点模拟掉电；Snapshot、StableStore、LogStore 与 InstallSnapshot 使用同一个“恢复结果只能为完整旧状态或完整新状态”的 oracle。
- 掉电模型同时维护 volatile image 和 durable image；普通 `SIGKILL` 只能证明进程崩溃，不能代替断电后缓存未落盘测试。
- 每个崩溃点重启后只允许恢复旧完整代或新完整代，禁止混合两代文件。
- 构造 Snapshot 7000、Snapshot 8000 和 commit 8500，损坏当前快照后必须由 Snapshot 7000 + Log 7001..8500 恢复；再生成 Snapshot 9000，验证先删 Snapshot 7000、再删除 `<= 8000` 日志的 crash ordering。
- 在 prepare/proposal、commit、Apply、`StateVisibilityLatch` 获取、MVCC canonicalization、逻辑 capture 与新快照文件同步边界请求快照，验证 `Snapshot(S) = Apply(Log[1..S])` 且不含 speculative state。
- 保持 working Buffer Pool 含未 Flush 的 committed dirty pages，完成逻辑扫描与 canonical snapshot 落盘后模拟崩溃；恢复必须只依靠新快照成功，证明 snapshot correctness 不错误依赖 working file FORCE。

## 验收标准

- 单节点真实生产路径完成“提交 autocommit batch -> 杀进程 -> 重启 -> SQL 查询一致”。
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

# 第二阶段：独立 KV 状态机上的完整 Raft

## 背景

在接入 SQL、Catalog 和 MVCC 前，先用很小的 KV FSM 验证 Raft 本身的安全性。这样选举或日志冲突错误不会被复杂数据库行为掩盖。

## 目标

实现持久化的 Raft 核心：

- Follower/Candidate/Leader 状态转换与随机选举超时。
- `RequestVote`、`AppendEntries`、心跳和 term 更新。
- 日志匹配、冲突提示与 `next_index/match_index` 回退。
- 多数提交、严格顺序 Apply、当前 term no-op。
- 快照创建、日志压缩和 `InstallSnapshot`。
- 节点重启、网络分区、丢包、重复包、乱序和旧 Leader 回归。

## 本阶段只做

- 三个静态 voter、单 Raft Group、内存 KV FSM。
- 进程内可控 Transport 做确定性测试，同时实现正式 TCP/RPC Transport。
- `StableStore`、`LogStore`、`SnapshotStore` 与 FSM 明确分层。
- 每个节点仍使用独立真实磁盘目录。
- 生产节点使用相同配置的选举超时区间，并在每次 deadline reset 时独立重新抽样；测试通过固定 seed 或
  timeout source 注入确定性序列。禁止依赖操作者为三个节点手工设置不同固定常量。

## 本阶段不做

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

`HARD_STATE`、`CURRENT/MANIFEST`、Snapshot 和 LogStore 是不同文件，不能假定它们在一次原子写中更新。设当前校验通过的快照边界为 `S = last_included_index`，持久化 HardState 中的提交位置为 `H`：

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
    retained = empty

new snapshot_base = (S, T)
```

新下载或新发布 Snapshot 自己携带的 T 不能反过来充当 `PreInstallLog.TermAt(S)` 的匹配证据。只比较 `index > S` 也不成立。即使保留 suffix，后续 AppendEntries 仍按正常冲突规则校验和修复。

InstallSnapshot 固定采用以下可崩溃顺序：

1. 收到 metadata 时执行首次 stale guard；仅当 `S > published_applied_index` 时下载到临时目录并校验、同步所有文件。
2. 将安装任务提交给单线程 FSM Apply/Install 序列，在其中执行最终 stale guard。若 `S <= published_applied_index`，以 no-op 结束；否则从这里到第 6 步禁止其他 Apply 穿插，并从旧 LogStore view 计算 `retain_old_suffix`。
3. 原子发布 Snapshot、Manifest 与 CURRENT；从这一步起 `S` 已成为 durable commit lower bound。
4. 持久化 `HARD_STATE.commit_index = max(H, S)`。
5. 调用 `LogStore::InstallSnapshotBase(S, T, retain_old_suffix)`，以一个 durable framed mutation 建立 `snapshot_base = (S,T)`，并按决策保留全部 `index > S` entries 或丢弃全部旧 suffix；完成后再按最老有效恢复点规则回收前缀。
6. 获取 `StateVisibilityLatch exclusive`，原子切换 FSM working state，令 `published_applied_index = last_applied = S`，释放锁后按顺序 Apply 保留或由 Leader 补齐的后缀 entry。

在第 3、4、5、6 步任意位置崩溃，重启都按上述 `max(H, S)` 规则解释，而不是把四份文件拼成一个不存在的原子事务。若第 3 步已发布而第 5 步尚未 durable，恢复必须针对仍在磁盘上的 pre-install log material 重做同一 term 比较；若无法证明旧 `TermAt(S) == T`，只能保守丢弃 suffix，绝不能用当前 Snapshot 的新 base 制造“匹配”。若保守丢弃后缺少 `(S, effective_commit_index]` 的 committed entries，则按 committed-range 规则 fail-stop/先向 Leader 补齐，不能降低 commit index。任何正常路径或 crash recovery 都不得令 `last_applied`、`published_applied_index` 或已对读者发布的逻辑状态回退。

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

第一版不采用“原地 truncate 文件，再逐条 append”的两步协议。LogStore 在 append-only segment 尾部写入一条完整 framed mutation record：

```text
REPLACE_SUFFIX { from_index, entry_count, encoded_entries, checksum }
```

恢复扫描只有在整条 record 长度和 checksum 均正确时才将逻辑日志替换为“保留 `< from_index` 前缀 + new_entries”；半条 record 当作未发生，旧逻辑后缀仍有效。物理旧后缀等后续 segment compaction 再回收。`from_index <= effective_commit_index` 必须拒绝并 fail-stop。只有 REPLACE_SUFFIX record 已 `fdatasync` 后该同步调用才可成功返回并回复 `AppendEntries(success=true)`，因此 success 同时证明冲突删除与新后缀已经作为一个逻辑操作 durable。

## 快照安装要求

- 快照就是第一阶段定义的一致状态快照，包含 `last_included_index/term`，不是只复制裸 `db.bustub`。
- 分块传输必须带 snapshot ID、offset、总长度和 checksum；重复块不产生副作用。Leader 从已发布快照文件按 offset 读取固定上限块，Follower 按 offset 追加到临时文件并逐块同步，禁止在任一端把完整 payload 物化为单个内存 vector。
- Leader 为每个 Follower 保留一个活动传输和一个 in-flight 块。heartbeat 重发该块时必须保持 request ID、offset
  和 bytes 不变；只有与活动 request ID 匹配且证明 durable high-water 不小于本块末端的 ACK 才能推进 offset。
  Follower 收到已经 durable 的旧块时报告真实 high-water，而不是简单回显请求末端；最终 COMPLETE ACK 丢失后，
  Leader 必须能通过重复末块失败关闭、从 offset 0 重启和 stale-complete 收敛，且不重新安装或回滚状态。
- Follower 下载到临时目录，完整校验、同步并按“跨文件 crash ordering”发布后，才能更新日志起点和 Apply 状态。
- metadata 到达时和正式发布前各执行一次 `S <= published_applied_index` stale guard；第二次判定在单线程 Apply/Install 序列中完成。stale/duplicate Snapshot 只能被忽略，不能借机压缩日志或替换 working state。
- 对通过最终 stale guard 的 Snapshot，`SnapshotStore/LogStore` contract 明确采用 `PreInstallLog.TermAt(S) == snapshot.last_included_term` 作为唯一 suffix 保留条件：相等时保留全部 `index > S` entries，不相等或不存在时丢弃全部旧 suffix，并建立 `snapshot_base = (S,T)`。
- 如果安装时不存在旧快照到 `S` 的 bridge log，旧快照不得继续标记为 fallback；此时只有新快照是有效恢复点。

## 建议修改或新增文件

```text
src/include/raft/raft_node.h
src/raft/raft_node.cpp
src/include/raft/raft_types.h
src/include/raft/stable_store.h
src/raft/stable_store.cpp
src/include/raft/log_store.h
src/raft/log_store.cpp
src/include/raft/snapshot_store.h
src/raft/snapshot_store.cpp
src/include/raft/transport.h
src/raft/tcp_transport.cpp
src/include/raft/state_machine.h
src/raft/kv_state_machine.cpp
```

可以参考 HashiCorp Raft 的组件边界和存储接口，但协议安全规则以 Raft 论文为准；不要把第三方库的 API 形状直接嵌入 BusTub 执行器。

## 测试要求

### 确定性协议测试

- 由 `ManualClock`、固定随机源和 `InMemoryTransport` 驱动逻辑时间，不通过真实 sleep 猜选举结果。
- 覆盖 sentinel `(0,0)` 上的首次选举、首条 AppendEntries 与首个 current-term NOOP，再覆盖单候选者当选、分票后重新选举、旧 term RPC、重复投票、日志不够新的候选者被拒绝。
- 在 StableStore 的命名持久化事件注入失败，分别触发 Candidate 自增 term、higher-term RequestVote（包括拒绝票）、AppendEntries、InstallSnapshot 和 RPC response；同步调用成功返回前不得发送新 term RPC/response，也不得继续旧 Leader 服务，返回后才能继续状态转换。
- 覆盖快速冲突回退、Follower 多余后缀替换、重复 AppendEntries、乱序响应。
- 对 `ReplaceSuffix` 在写 header、payload、checksum 和同步前后逐点崩溃，恢复结果只能是完整旧后缀或完整新后缀，不能出现混合状态；success reply 只能发生在新逻辑后缀 durable 之后。
- 损坏 uncommitted tail 可以截断；损坏或缺失 committed range 必须 fail-stop，不能把 `effective_commit_index` 从 100 降到 99。
- 覆盖两种关键故障：只复制到少数节点后 Leader 崩溃，该日志被覆盖；复制到多数节点后 Leader 崩溃，新 Leader 必须保留并最终 Apply。
- 在 Leader 本地 LogStore 的命名持久化事件注入失败，断言同步调用未成功返回时不会发送该 entry 的 AppendEntries，也不会推进 commit；本地 durable 后才允许开始复制。
- ReadIndex fresh-round 测试：先让旧 Leader 收到一次完整 heartbeat quorum ACK，再隔离它并选出新 Leader；随后到达旧 Leader 的读必须创建新 context，旧 ACK、错误 context/term ACK 都不能完成该读，最终只能超时或因降级失败。

### 组件故障测试

- 三节点真实 `LogStore`，轮流重启每个节点，验证 term、vote、日志与 commit 恢复。
- 对 HARD_STATE 临时文件写入、文件同步、rename 和目录同步逐点崩溃；任何依赖新增 `current_term` 的 RequestVote/AppendEntries/InstallSnapshot response 或 Candidate RequestVote 都只能在完整可信 generation durable 后发出。
- 双向分区、单向丢包、重复包、消息延迟、旧 Leader 回归、Follower 长时间落后。
- 快照覆盖落后节点所需日志后，Follower 必须经 `InstallSnapshot` 追上。
- 让真实 `StageChunk` 的 fsync ACK 跨越至少一个 heartbeat，断言 Leader 重发同一 request ID/offset/bytes；预置多个
  durable 块后从 offset 0 开始，断言 Follower 返回实际 high-water 且 Leader 直接跳进。再丢弃最终 COMPLETE ACK，
  验证重复末块失败关闭、下一 heartbeat 从 0 重启、已发布 Follower 返回 stale-complete、原 ACK 任意晚到也无副作用。
- 分别构造本地 `TermAt(S) == T` 与 `TermAt(S) != T/不存在`：前者保留完整 `index > S` suffix，后者全部丢弃；禁止用新 Snapshot base 自证匹配。本地创建 Snapshot 时验证 T 在 compact S 前取得。
- stale Snapshot 覆盖三种情况：到达时 `S < P`、重复安装 `S == P`、开始下载时 `S > P` 但发布前 Apply 已推进到 `P >= S`。三者均断言 CURRENT/snapshot base/LogStore/FSM digest/`last_applied/P` 完全不变且索引单调不减；额外用 higher-term stale Snapshot 验证只有 HardState term 先 durable，FSM 仍不回滚。
- 在 InstallSnapshot 发布 Snapshot、更新 HardState、更新日志基点和切换 FSM 的每个间隙崩溃，重启后验证 `effective_commit_index = max(H, S)`、suffix term 决策以及所需日志连续性；无法从 pre-install material 证明匹配时必须走保守丢弃。

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

# 第三阶段：Autocommit Statement 到确定性 CommandBatch 与 BusTub FSM

## 背景

V1 不复制原始 SQL，而是把一条 autocommit statement 编译成确定性的 `TransactionCommandBatch`。一个 `UPDATE/DELETE` statement 可以展开成许多行 mutation，因此 batch 仍然是有意义的原子复制单位。多语句 non-interactive transaction、跨语句私有 Table/Catalog overlay 和跨语句约束验证推迟到 V2，避免其实现复杂度淹没 Raft 主线。

## 目标

建立以下写入链路：

```text
客户端 autocommit DDL/DML statement
  -> Leader 解析、绑定、校验并物化最终 mutations
  -> 生成版本化 TransactionCommandBatch
  -> Raft 复制并在多数节点持久化
  -> commit_index 推进
  -> 各节点 FSM 按 index 串行 Apply
  -> Leader 等待本地 last_applied >= index
  -> 返回客户端成功
```

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

DROP_TABLE { table_oid }                  # 若当前项目语义支持
DROP_INDEX { index_oid, table_oid }          # 若当前项目语义支持
INSERT_ROW
UPDATE_ROW
DELETE_ROW
```

DDL OID 是 Leader prepare 已决定并写入二进制 Command 的状态转换输入，不是 Follower Apply 时的隐式选择。由于 V1 每张表必须有 primary index，`CREATE_TABLE` 同时显式携带 `table_oid` 与 `primary_index_oid`；显式 CREATE_INDEX 携带自己的 `index_oid/table_oid`，DROP 也按 OID 指定目标。V1 单写 prepare 从 committed Catalog allocator 读取候选 OID，但不提前修改公开 allocator；committed Apply 必须校验 command OID 等于本地对应 `next_table_oid/next_index_oid`，使用精确 OID 创建对象，然后逐项推进 allocator。Follower、恢复 replay 和 Leader Apply 都不得调用本地 allocator 重新决定 OID；不匹配表示状态漂移并 fail-stop。未提交 proposal 不消耗 OID，DROP 后也不复用旧 OID。

DML 使用逻辑身份和完整值，而不是复制页面字节：

```text
INSERT_ROW { table_oid, primary_key, complete_tuple }
UPDATE_ROW { table_oid, primary_key, expected_old_commit_ts, expected_old_tuple, complete_new_tuple }
DELETE_ROW { table_oid, primary_key, expected_old_commit_ts, expected_old_tuple }
```

不能把 RID 当作长期逻辑身份，因为不同节点的物理页分配和索引重建不应成为协议正确性的前提。

## V1 replicated table 准入规则

V1 distributed mode 对所有可写用户表强制执行以下协议，不存在“只在本节点写、不进入 Raft”的例外：

- `CREATE TABLE` 必须声明恰好一个单列、`NOT NULL` 的 `PRIMARY KEY`；V1 `PrimaryKeyCodecV1` 白名单固定为 `INTEGER`、`BIGINT` 和 `VARCHAR`。复合主键、nullable key、浮点/向量等不在白名单的 key 留到 V2。
- `INTEGER` wire encoding 固定为 4-byte signed two’s-complement big-endian，`BIGINT` 固定为 8-byte signed two’s-complement big-endian；禁止 `memcpy` host-native C++ value 或依赖 host endian、padding、compiler ABI。其 canonical comparator 使用有符号数值升序。
- `VARCHAR` 主键 wire encoding 固定为“4-byte unsigned big-endian 长度 + 原始字节序列”。identity equality 是 raw-byte equality；primary-index 与 canonical-command comparator 对 raw bytes 做 unsigned lexicographic comparison，区分大小写、尾随空格有意义、不做 Unicode normalization、不使用 locale-dependent collation。该 `PrimaryKeyCodecV1::CanonicalCompare` 必须与 BusTub V1 stable Value equality/primary-index comparator 一致，否则该主键定义在 proposal 前拒绝。
- Parser/Binder/CommandBuilder 在 proposal 前检查该定义。缺少主键、多个主键或类型不受支持时返回稳定的 `UNSUPPORTED_REPLICATED_PRIMARY_KEY`，不得分配公开 OID、修改 Catalog 或追加 Raft log。
- V1 只支持 primary-key uniqueness 和 ordinary non-unique secondary index。`CREATE UNIQUE INDEX`、非主键 `UNIQUE` constraint 或其他需要 deferred constraint checking 的 DDL 返回 `UNSUPPORTED_DEFERRED_UNIQUE_CONSTRAINT`，必须在 proposal 前无副作用地拒绝；unique index 也不能事后充当 replicated table identity。
- `INSERT_ROW/UPDATE_ROW/DELETE_ROW` 中的 `primary_key` 使用版本化 `PrimaryKeyCodecV1` 编码。V1 拒绝修改主键列的 UPDATE；需要改变主键时由客户端显式 DELETE 后 INSERT，并分别遵循 autocommit 语义。
- 节点启动恢复和 InstallSnapshot 在开放服务前验证每个用户表的主键列存在、非空、类型/codec version 受支持且与 Catalog primary-key definition 一致，并拒绝包含 secondary UNIQUE definition 的 V1 Catalog。任一表无法确定 replicated logical key 时 fail-stop，并要求离线迁移或重新安装合法快照，不能等到第一条 UPDATE 才报错。

## V1 prepare 与提交边界

- Leader 同一时刻只允许一个 autocommit write request 进入 prepare/replicate/apply 通道；开始 prepare 前必须追平已提交状态，保证验证基线与日志顺序一一对应。
- prepare 只读取 committed state，在私有 mutation buffer 中解析、绑定、计算表达式、展开本 statement 的全部受影响行并校验约束；不得修改公开 Catalog、TableHeap、索引、Buffer Pool 页面或 MVCC version chain。
- 一条 DML statement 可以生成许多 INSERT/UPDATE/DELETE commands；一条 DDL statement 生成显式携带 Leader 已决定 `table_oid/index_oid` 的确定性 Catalog command，但 V1 不允许在同一 batch 中继续执行下一条 statement。
- 所有可能导致普通业务失败的条件，例如语法、类型、表不存在、primary-key collision、受影响行前置条件和非确定值求值，都必须在 proposal 前解决；secondary UNIQUE/deferred constraint DDL 属于 V1 不支持语义，直接在 proposal 前拒绝。
- 未提交、失去任期或复制失败只需丢弃内存中的构造结果，因为 working state 尚未改变。
- `BusTubStateMachine::Apply(committed_entry)` 是 Leader、Follower、重启恢复和快照后重放修改公开状态的唯一正式路径，不保留“Leader publish 暂存 MVCC 事务”的特殊路径。
- 对格式合法且已完成前置验证的 committed entry，Apply 必须是确定性的、预期不会发生普通业务失败的状态转换。约束、旧 commit timestamp 或旧 tuple 不匹配表示副本漂移、磁盘损坏或实现 bug，节点必须 fail-stop，不能 abort、跳过该 index 或继续服务。
- MVCC `commit_ts` 由 Raft log index 派生或一一映射；所有节点禁止各自分配不同的提交顺序。

## Apply 原子可见性

V1 使用简单的全局 `StateVisibilityLatch` 保证可见性：SQL 读在整个执行期间持有 shared lock，FSM Apply 持有 exclusive lock。Apply 内部使用只服务于 committed entry 的 internal apply transaction，它不是客户端事务，也不会出现在 Raft proposal 之前。`published_applied_index` 是所有 entry 共用的发布水位，而不是 data-change counter。

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

## 请求去重

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

## Catalog 与索引

- DDL 和 DML 共用一个 Raft 序列，不能通过节点本地管理接口绕过日志修改 Catalog。
- 所有索引物理结构在正常运行时由 FSM Apply 同步维护，但都不属于第一版权威快照状态；V1 除 primary-key index 外只允许 ordinary non-unique secondary index。
- B+Tree、Hash、HNSW、IVFFlat 统一由 Catalog definition + canonical table rows 重建；测试比较逻辑查询结果，向量索引比较约定的召回约束，不比较 root page、图结构或文件字节。
- `expected_start_schema_epoch` 表示整个 batch 开始前期望的 Catalog epoch；Apply 开始时只校验一次 `current_epoch == expected_start_schema_epoch`。
- 协议格式规定 batch 内每个 schema-changing command 按顺序将 epoch 递增 1，最终 epoch 等于起始值加 schema-changing command 数。V1 每个 batch 最多包含一条 DDL statement；多 DDL 及 DDL 后 DML 的 batch 仅在 V2 启用。
- epoch 不匹配意味着状态机漂移，节点必须停止服务并报警，不能静默跳过。

## 建议修改或新增文件

```text
src/include/distributed/command.h
src/distributed/command_codec.cpp
src/include/distributed/command_builder.h
src/distributed/command_builder.cpp
src/include/distributed/bustub_state_machine.h
src/distributed/bustub_state_machine.cpp
src/include/distributed/state_visibility.h
src/distributed/state_visibility.cpp
src/include/distributed/session_table.h
src/distributed/session_table.cpp
src/include/concurrency/transaction_manager.h
src/concurrency/transaction_manager.cpp
src/include/common/bustub_instance.h
src/common/bustub_instance.cpp
```

## 测试要求

### 单元测试

- CommandBatch 跨版本编解码、未知命令拒绝、Schema/Value 稳定编码。
- 将同一 mutation 集合以随机 TableHeap、RID 和 unordered-map 顺序多次交给 CommandBuilder，结果必须都按 `(table_oid, PrimaryKeyCodecV1::CanonicalCompare(primary_key))` canonical sort 并产生 byte-identical payload/checksum；重复 logical key 在 proposal 前拒绝。
- `PrimaryKeyCodecV1` 使用 golden bytes 覆盖 `-1`、`42`、INT32/INT64 MIN/MAX，验证 INTEGER=4-byte、BIGINT=8-byte signed two’s-complement big-endian，模拟不同 host endian 后 payload 仍相同；VARCHAR 覆盖 `ABC`/`abc`、尾随空格、空串和多字节序列，验证长度前缀、raw bytes、binary/no-normalization/no-locale 语义与 index comparator 一致。
- prepare 成功和失败都不能改变公开 Catalog、页面、索引或 MVCC version；只有 committed-entry Apply 能产生可见写。
- distributed mode 的 CREATE TABLE 缺少主键、声明复合/nullable 主键或使用非 `PrimaryKeyCodecV1` 类型时，必须在 proposal 前返回 `UNSUPPORTED_REPLICATED_PRIMARY_KEY`；断言无公开 OID/Catalog 变化、无 Raft entry。主键列 UPDATE 同样在 proposal 前拒绝。
- `CREATE UNIQUE INDEX`、secondary UNIQUE constraint 和恢复出的 V1 secondary UNIQUE definition 必须以 `UNSUPPORTED_DEFERRED_UNIQUE_CONSTRAINT` 拒绝且无 Raft/Catalog 副作用；primary-key index 和 ordinary non-unique secondary index 仍可正常创建。
- 用测试侧 blocking storage/index adapter 暂停 Apply 中间步骤，同时启动并发 SELECT；读必须阻塞，释放 Apply 后只能看到整个旧 batch 或整个新 batch，不能看到部分 row/Catalog/index/session 状态。
- Apply 在最终 publish 前发生错误时节点 fail-stop；重启重放整条 committed entry 后只能出现完整结果。
- 一条多行 DML statement 生成的整个 batch 只能整体可见，不能让读者观察到部分 row mutations；合法 committed entry 的业务校验不得在 Apply 阶段重新失败。
- 非确定函数被预求值或拒绝；相同 batch 在两个独立实例上产生相同逻辑 digest。
- 相同 `client_id/request_id` 重放多次只产生一次副作用；首次提交、快照恢复和新 Leader 重试返回 byte-identical encoded `WriteResponseV1`，其中 term 固定为原 entry term；并发、空洞和过旧 request ID 按协议被拒绝。
- `expected_start_schema_epoch = 10` 的 CREATE TABLE batch 显式携带 `table_oid = next_table_oid` 与 `primary_index_oid = next_index_oid` 并结束于 epoch 11；CREATE INDEX 同样携带 `index_oid/table_oid`。两个实例必须使用 command OID 并推进 allocator，伪造任一 OID mismatch 时 fail-stop；后续 INSERT batch 必须期望 epoch 11。
- Apply 一条不含 CommandBatch 的 committed NOOP，断言数据库 digest 与 SessionTable 不变，但 `published_applied_index` 和 `last_applied` 都推进到 NOOP index；由 `Snapshot@S` 新建 FSM 时两者均从 `S` 开始。

### BusTub FSM 集成测试

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
- Leader 在 majority commit 前绝不返回成功；proposal 前的私有 mutation buffer 不修改 working state，可直接丢弃。
- Leader 与 Follower 只能经同一个 `BusTubStateMachine::Apply` 修改公开数据库，不存在双提交路径。
- 任意节点重放同一快照和日志后，Catalog、表数据、索引定义、显式 table/primary-index/secondary-index OID、allocator、commit_ts 顺序和 SessionTable 完整响应一致；每个用户表都能由 Catalog 确定一个受 `PrimaryKeyCodecV1` 支持的逻辑身份。
- 并发读者永远只能观察到 batch publish 前或 publish 后的完整逻辑状态；`published_applied_index/last_applied` 不得提前。

## 输出要求

- CommandBatch 二进制协议与兼容策略，包括整数/VARCHAR wire bytes 和所有 DDL 显式 OID 字段。
- `WriteResponseV1` 与 SessionTable 完整响应重放协议。
- autocommit SQL statement 到逻辑 mutation 的映射表，以及 V2 多语句事务扩展边界。
- 所有拒绝的非确定语义和错误信息清单。

---

# 第四阶段：三节点 BusTub 生产链路集成

## 背景

前两阶段已经分别验证单机状态恢复和 Raft 协议，第三阶段定义了确定性 BusTub FSM。本阶段把它们接成真实的三个进程，而不是只在测试内调用对象方法。

## 目标

```text
正式客户端入口
  -> Leader 路由与请求 ID
  -> TransactionCommandBatch
  -> 正式 TCP/RPC Transport
  -> 三节点 LogStore 持久化
  -> majority commit
  -> 三节点 BustubStateMachine Apply
  -> Leader 等待本地 Apply
  -> 正式客户端响应
```

## 本阶段只做

- 一个正式 `bustub-node` 进程对应一个节点目录。
- 静态配置 `node_id/group_id/peer addresses/client address/data_dir`。
- Leader-only write；Follower 返回结构化 `NOT_LEADER` 和已知 Leader 地址。
- Leader 线性一致读、显式 Follower stale read，二者都使用 Raft 派生的 MVCC read timestamp。
- 节点启动恢复、选举、日志追赶、快照创建与安装。
- 优雅关闭和 `SIGKILL` 后恢复。

## 本阶段不做

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
- 本阶段不使用“Leader lease + 本地时钟”优化，避免时钟假设进入正确性路径。

## 运行与恢复

- 节点启动先完成本地 snapshot + committed log 恢复，再加入 Raft；恢复期间不监听客户端写入口。
- 节点重新加入后先比较快照边界和日志，按 AppendEntries 追赶；日志已压缩则走 InstallSnapshot。
- 工作数据库可以每次启动都从权威状态重建。未来若要复用 working 文件，必须另行设计已持久化的 applied marker 和校验协议，不能默认信任。
- 节点检测到 Catalog epoch、Apply digest 或日志连续性违例时进入 fail-stop 状态，禁止继续对外响应成功。
- 为事务管理器增加显式 `BeginReadAt(timestamp_t)`；生产分布式读路径必须传入 Raft 派生时间戳。

## 建议修改或新增文件

```text
src/include/distributed/node.h
src/distributed/node.cpp
src/include/distributed/client_protocol.h
src/distributed/client_protocol.cpp
tools/bustub-node/CMakeLists.txt
tools/bustub-node/bustub-node.cpp
tools/bustub-client/CMakeLists.txt
tools/bustub-client/bustub-client.cpp
```

## 测试要求

### 三节点进程级 E2E

E2E 必须启动三个正式 `bustub-node` 二进制，使用正式客户端协议和各自的真实临时目录：

1. 等待选出 Leader，先分别提交无主键 CREATE TABLE 和 secondary UNIQUE DDL 并确认都在 proposal 前拒绝，再通过正式客户端依次提交带 `PrimaryKeyCodecV1` 主键及普通 secondary index 的 autocommit DDL/DML；其中至少一条 UPDATE/DELETE 必须影响多行并生成多 command batch。
2. 从 Leader 做线性一致查询，并从三个节点获取带 `last_applied` 的状态后比较逻辑数据。
3. `SIGKILL` Leader，在剩余两节点中选出新 Leader，使用同一 request ID 重试不确定请求；比较新 Leader 返回值与 SessionTable 保存的首次 `WriteResponseV1` encoded bytes。
4. 向新 Leader继续写入，再重启旧 Leader，确认它通过日志或快照追赶。
5. 终止全部节点，再按不同顺序重启，查询 Catalog、`schema_epoch`、OID allocators、数据、索引和去重结果；恢复后立即执行新 DDL/DML，验证 CommandBuilder 使用已恢复 epoch，并把显式 table/primary-index/secondary-index OID 连续写入 Command。
6. 在一个影响大量行的 UPDATE/DELETE batch Apply 期间持续并发读取，结果只能是完整旧集合或完整新集合，并记录 read timestamp 与 published applied index。
7. 每次新 Leader 完成当前 term NOOP 后，通过正式读响应与只读诊断观测其 index；线性一致读必须成功且 `published_applied_index/last_applied` 已越过该 NOOP。
8. 先在 index K 写入目标行，再用无关写把快照边界推进到 S，生成 `Snapshot@S`，随后提交引用 `expected_old_commit_ts = K` 的更新 S+1；杀死节点并强制它由 Snapshot S + Log S+1 恢复，验证后缀可正常 Apply。
9. 网络代理复制并延迟一份完整 `InstallSnapshot@1000`，让原始传输先完成且 Follower 继续追到 index 1200，再释放延迟副本；验证 CURRENT、snapshot base、FSM digest、`last_applied/published_applied_index` 均不回退，并可继续 Apply 1201。

### 必测故障场景

| 编号 | 场景 | 必须证明的性质 |
| --- | --- | --- |
| E2E-01 | 正常三副本 batches 与非法 DDL 准入 | 带 V1 主键及普通 secondary index 的 DDL/多行 DML 三节点一致；无主键和 secondary UNIQUE DDL 在 proposal 前拒绝且无状态副作用 |
| E2E-02 | Leader 只复制给少数节点后崩溃 | 请求未成功，旧未提交后缀可被新 Leader 覆盖 |
| E2E-03 | Leader 完成多数持久化、响应前崩溃 | 新 Leader 保留提交，重复 request ID 不重复写，并返回首次结果对应的 byte-identical WriteResponseV1 |
| E2E-04 | Follower 落后后重启 | 通过 AppendEntries 追上并继续 Apply |
| E2E-05 | Follower 落后超过日志保留范围 | 安装快照后继续接收日志 |
| E2E-06 | 旧 Leader 在网络恢复后回归 | 识别更高 term、降级并截断冲突后缀 |
| E2E-07 | 三节点同时停止再重启 | snapshot + committed log 恢复 Catalog、epoch 与 OID allocators，恢复后 DDL Command 使用连续的显式 OID |
| E2E-08 | Follower stale read | `read_ts = published_applied_index`，响应明确标注该值且不冒充线性一致 |
| E2E-09 | 持有近期 heartbeat ACK 的旧 Leader 与多数失联 | 不再提交写；分区后新到达的线性一致读不能复用旧 ACK，只能超时/非成功 |
| E2E-10 | 快照生成期间杀节点 | 重启选择完整旧代或新代，不能混合状态 |
| E2E-11 | 损坏 CURRENT 指向的最新快照 | 使用上一快照与 bridge log 恢复到当前 committed state |
| E2E-12 | 多行 batch Apply 期间并发读 | 只出现完整旧/新结果，Leader read_ts 不小于 ReadIndex |
| E2E-13 | 新 Leader 当前 term NOOP 后立即线性一致读 | NOOP 不改数据但推进 `published_applied_index/last_applied`，读水位不落后 |
| E2E-14 | 旧版本行的 canonical snapshot + 后缀 UPDATE 重放 | 快照保留行的真实 `latest_committed_version_ts = K`，Log S+1 不因被改写为 S 而 fail-stop |
| E2E-15 | 延迟重复 Snapshot@1000 到达已 Apply 1200 的 Follower | stale guard 把安装视为 no-op，CURRENT/log base/FSM/两个 applied index 不回退且可继续 Apply |

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
- 每次运行保留节点日志、term/index 轨迹和失败诊断产物。

---

# 第五阶段：测试体系、生产隔离与最终验收

## 背景

模块测试通过只能证明局部行为，不能证明“客户端请求 -> Leader -> Raft 持久化 -> majority commit -> FSM Apply -> 崩溃恢复”的生产链路成立；但把同一算法在每一层重复穷举，也会制造高成本、低信息量的测试。本阶段用不变量和风险决定测试位置，并将生产代码与测试控制彻底隔离。

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
- `CommandBuilder + TransactionManager + BustubStateMachine`。

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
- E2E 至少覆盖第四阶段的 E2E-01 至 E2E-15，不允许因各模块测试已通过而删掉完整链路验证。

## 如何决定“测什么”

先建立需求—风险—测试映射，不以代码文件数量决定测试数量：

| 不变量/风险 | 主验证层 | E2E 是否再验证 |
| --- | --- | --- |
| checksum、坏尾识别 | 单元 | 只选一个代表性重启场景 |
| 空日志/snapshot sentinel `(0,0)` | LogStore/SnapshotStore 单元 | 正常集群首次启动自然覆盖 |
| Manifest 原子发布每个崩溃点 | 单元掉电模型 | E2E 在快照期间杀进程一次 |
| 两代快照与 bridge log 回收 | 单元/恢复集成 | E2E 损坏最新快照并从旧代恢复 |
| Snapshot/HardState/Log 跨文件顺序 | 组件崩溃矩阵 | E2E 覆盖一次 InstallSnapshot 后重启 |
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
bustub                 # production library
bustub-node            # production executable
bustub-client          # production client
bustub_test_support    # 仅链接到测试目标，不被 production target 依赖
bustub_cluster_e2e_test# 外部 harness，启动 production executable
```

`bustub_test_support` 可以包含 `ManualClock`、`InMemoryTransport`、`CrashableStorage` 和临时集群管理器。依赖方向必须由测试指向 production，production 不能反向依赖测试。

### 文件和端口隔离

- 每个测试使用唯一临时根目录，并在其下为三个节点创建独立子目录。
- 端口块由 harness 调用方为本次场景独占；所有 listener 和派生 proxy 端口必须避开 Linux 临时端口区
  （通常从 32768 开始），且不得使用跨并发任务共享的单个固定端口。CI 中彼此隔离的 runner 可以复用同一低端口块。
- 用 RAII 清理成功用例；失败时可选择保留目录并输出路径，便于复现。
- 测试绝不读取或覆盖仓库根目录的 `db.bustub`，也不接触真实 production 数据目录。

### 阶段结束清理门禁（M0–M7 强制）

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

1. 现有项目 GTest 与 SQLLogicTest 全部通过，避免 Raft 改造破坏原有单机能力；CI 必须以独立 Release job
   构建 `sqllogictest` 并运行所有已注册 `.slt`，不能由排除 SQLLogic 的公共 GTest job 代替。
2. 第一阶段全部恢复与掉电点测试通过。
3. 第二阶段全部确定性 Raft safety 测试通过。
4. 第三阶段 CommandBatch/FSM 双实例一致性测试通过。
5. 第四阶段 E2E-01 至 E2E-15 通过。
6. 至少一次 ASan/UBSan 全量运行和一次 TSan 并发核心集运行。
7. 重复运行故障 E2E，确认无不可复现的偶发失败；失败必须打印 seed 和节点事件轨迹。
8. 至少一条正式三进程连续链路必须在同一 durable state 上依次穿过 M0–M7：准入拒绝、写入与响应丢失、
   Leader 切换和 exact-once retry、snapshot+suffix、真实多块追赶、stale Snapshot、身份拒绝、全停全启及恢复后继续 DDL/DML；不得把各阶段重置后的独立 green case 冒充跨阶段组合验证。

## 最终验收标准

- 任一已返回成功的 autocommit write request，在任一单节点故障、Leader 切换和全体进程重启后仍可查询。
- 任一未形成多数提交的 CommandBatch，不会因为旧 Leader 恢复而被错误 Apply。
- Catalog、`schema_epoch`、OID、带原始 `latest_committed_version_ts` 的 canonical table rows、索引定义、数据、提交顺序和去重状态能够由权威状态完整重建；primary 与所有 secondary indexes 必须先重建到 Snapshot@S，suffix 才能经正式 FSM Apply 重放并增量维护它们。
- 一个 CommandBatch 对并发读者原子可见，Leader/Follower 的 MVCC read timestamp 与 Raft published index 位于同一时间轴；NOOP 也推进 published index，canonical snapshot 则保留每行真实的最近提交 index。
- HARD_STATE 和 ReplaceSuffix 在任一规定崩溃点恢复后都只能呈现完整旧代或完整新代，不能出现 torn election/log state；任何 higher-term RPC 路径都先持久化新 term。
- InstallSnapshot 仅在 `S > published_applied_index` 时进入发布流程，并仅在 pre-install `TermAt(S) == T` 时保留旧 suffix；stale/duplicate 安装不改变任何状态，无法证明 suffix 匹配时保守丢弃且绝不降低 committed boundary。
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

# 推荐实施顺序与阶段门禁

```text
第一阶段：一个节点 snapshot + replay 可真实恢复
  -> 第二阶段：独立 KV Raft 通过协议与故障测试
  -> 第三阶段：Autocommit CommandBatch + BusTub FSM 确定性
  -> 第四阶段：三个 BusTub 正式进程打通
  -> 第五阶段：完整 E2E、故障矩阵和 CI 门禁
```

任何阶段没有通过自己的持久化和故障验收，不进入下一阶段。尤其不能在第一阶段只有类定义、第二阶段只有正常网络演示时就开始三节点 SQL 集成。

## 建议里程碑

- M0：完成 snapshot self-contained spike，证明无活动事务时 canonical `db.bustub + catalog.bin + session.bin` 可独立表达 committed state，包含每行真实 `latest_committed_version_ts` 与 `schema_epoch`，且不依赖旧索引页、working file Flush 或内存 undo；否则先实现 MVCC canonicalization。
- M1：Catalog 可重新打开，单节点从快照和 bridge log 恢复，最新代损坏时可回退。
- M2：空日志 sentinel、日志 durable batch append、torn tail、两代恢复点联动回收和 Manifest 掉电矩阵通过。
- M3：KV Raft 完成选举、复制、冲突回退和多数提交。
- M4：KV Raft 完成快照安装、stale/duplicate Snapshot 不回滚、节点重启和旧 Leader 回归。
- M5：autocommit statement 到 canonical CommandBatch、固定 wire codec、显式 DDL OID、PrimaryKeyCodecV1/secondary UNIQUE 准入、原子可见 Apply、WriteResponseV1 去重重放和确定性双实例测试通过。
- M6：三个正式 BusTub 节点完成正常链路和 Leader 切换。
- M7：快照追赶、全体重启、全部 E2E 与 sanitizer 门禁通过。

## 执行状态（2026-08-30）

M0–M7 已按上述顺序完成。早期完整验收曾覆盖 ASan/UBSan 有效 GTest 60/60、Release SQLLogicTest 40/40、
M0–M7 目标集 23/23 及 TSan 核心 7/7 + 4/4；这些数字属于当时修订，不能追溯性代表后续测试修正。
当前权威验收见本节“最终修复收口复核（2026-08-30，当前权威）”与 `docs/testing/raft_test_matrix.md`。本方案停在 M7，
不自动进入任何 V2 工作；
后续动作必须等待新的用户命令。

以下所有带 2026-08-29 日期的数字均是对应历史修订的证据，不代表后来源码；只有文末 2026-08-30 收口段
及测试矩阵的同名权威段描述当前工作树。

2026-08-29 选举超时补强：固定单值配置已替换为生产随机区间；Raft 核心接受可注入 timeout source，
确定性测试显式使用固定值或固定 seed。M6/M7 进程 harness 的三个节点使用同一区间，不再按节点编号设置
不同常量。该补强只修正 M3/M6/M7 已有范围，不开启新阶段；针对性验证记录见测试矩阵。

2026-08-29 持久化边界收敛：CommandLog、StableStore、LogStore 的三个 `DurableFuture` 接口改为同步
`void` API，成功返回即表示相应文件持久化屏障已经完成，失败直接抛出；删除同步执行后再包装 ready future、
调用方立刻 `.get()` 的冗余层。本选择借鉴 etcd/raft `Ready` 的关键顺序约束——依赖 HardState/Entries 的
消息不得先于持久化发送——但 V1 不复制其异步 surface，也不引入没有调度器支撑的 completion。测试层统一
使用 `before_write / after_fsync / after_rename / after_dir_fsync` 命名事件，并让 Snapshot、StableStore、
LogStore/CommandLog 和 InstallSnapshot 复用同一 old-or-new 恢复 oracle。该修正仍位于已完成阶段的 durability
验收范围，不进入新阶段。

2026-08-29 架构收敛补强：M6/M7 的进程生命周期与客户端 Leader 重定位已抽到单一测试 harness；当前四条
聚焦脚本和一条 M0–M7 连续链路只保留 E2E 时间线。`magic/version/length/CRC` 校验抽成公共
`VersionedFrame`/`ChecksummedFrame` 骨架，
CommandBatch、客户端协议、Raft RPC、Manifest、HardState、Session、Catalog 与既有 snapshot bundle 保留各自
协议类型和原有线格式，只共享边界检查。正式 BusTub 快照改为 `DurableFileSlice` + 有界块：canonical
文件打包、SnapshotStore 发布/恢复、64 KiB InstallSnapshot 传输、Follower 临时下载及 FSM 安装均不再持有
完整 payload vector；128 MiB 内存上限只留给测试便利 codec/KV 示例，正式文件 payload 使用 1 GiB
实验安全上限。分块用于验证 Raft InstallSnapshot 和崩溃边界，不构成大型数据库容量声明。

V1 继续保留自研 Raft 核心以服务 BusTub 状态机学习和不变量验证，不在本阶段替换库。若目标改成真实生产，
应另立迁移项目比较 NuRaft 等成熟 C++ 实现的选举、压缩、快照、可插拔 LogStore/FSM、group commit 与 pipeline，
并重新完成 wire/disk 兼容、故障语义和整套 E2E；这不是当前实现上的局部重构。Raft 当前的“完整状态快照 +
lastIncludedIndex/Term + 分块 InstallSnapshot”仍遵循论文模型；对更大数据库，base backup + 连续日志是未来独立
演进方向，而不是在本轮偷偷改变恢复协议。针对性 ASan/UBSan 验证通过 16 个二进制/55 个测试及最终 M6/M7
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
正式进程证据，当时改为四条共享 harness 的正式三进程时间线，随后又增加一条连续 M0–M7 链路。M6 增加
无主键/secondary UNIQUE 无副作用拒绝、真实响应丢失、
`WriteResponseV1` 精确字节比较、三节点逻辑结果比较、stale/read/NOOP 水位断言；M7 增加少数派后缀覆盖、旧
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
- 四条聚焦进程场景加一条连续链路全部使用有业务含义的数据和字面结果。快照崩溃场景使用 1600 行并分别核对
  完整快照、bridge suffix 和触发被杀 capture 的 Apply 效果；损坏场景持久截断最新代、独立解析前一代 index
  并要求精确选择；传输场景要求实际多块、`Snapshot@S` 精确等于记录值且 `S < suffix`；连续链路在同一 durable
  state 上穿过准入、响应丢失、切主、快照+后缀、stale replay、身份拒绝、全停全启及恢复后继续 DDL/DML。
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

### 最终修复收口复核（2026-08-30，当前权威）

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

当前源码的最终动态门禁如下，历史段中的旧数字不再代表当前工作树：

- Clang 14 ASan/UBSan 组件门禁：26/26 个二进制、122/122 个具体测试，0 failed/errors/disabled/not-run，
  26 份非空 JSON 和日志均可解析，`process_retries=0`，无 sanitizer marker。
- TSan 核心：`tcp_transport_test` 4/4、`raft_node_test` 17/17、`bustub_state_machine_test` 4/4、
  `distributed_node_test` 9/9，合计 34/34，无 data race 或 lock-order-inversion。
- Release SQLLogicTest：40/40，0 failed；长耗时 `leaderboard-q1-index` 自然完成 669.72 秒，未中断或重跑。
- 五条 ASan/UBSan 正式三进程场景与同五条 Release 场景各自单次通过，各覆盖 24 个 timeline。聚焦传输为
  3 块/135,485 bytes/Snapshot@4 term 1；同一 durable state 的连续链路为 5 块/266,737 bytes/Snapshot@12
  term 2。两套均无异常 cleanup、残留进程/端口或协议/sanitizer 报告。
- 全部 `src/`、`test/` C/C++ 通过 Clang 14 format dry-run 和仓库 cpplint；6 个 shell 通过 `bash -n`，
  4 个 Python helper 通过 AST 解析，`git diff --check` 和 production 对测试依赖反向扫描通过。

本 WSL 宿主的 Clang 14 ASan/TSan 在随机地址布局下会于 `Running main` 前发生运行库映射失败；原始空日志和
host 的 `overflowed sigaltstack` 证据先保留。最终 ASan 组件 gate 及完整 E2E 父进程树各自在全新目录中整体
单次置于 `setarch x86_64 -R`，仍为 `process_retries=0`，没有逐二进制或逐节点重试。该局部环境处理不用于
Release、原生 Linux CI 或任何进入测试体后的失败，规则已写入 runbook。

结束时精确删除 21 个已逐项盘点的外部构建、组件日志、成功/失败 E2E 现场和零字节诊断日志，共
2,676,096,997 bytes（2.492 GiB），均可按 runbook 重建。复扫 `/tmp` 任务前缀、后台 node/client/proxy、
18,100–31,899 端口、源码树 ignored/generated 文件均为空。工作树保持 208 项正式交付（103 个 tracked 修改、
105 个新增、0 删除），未跟踪项只有源码、已注册测试、harness/tool 和文档；1,347 文件、36,074,734 bytes 的
已跟踪嵌套课程基线不是中间产物，继续保留。

独立只读审查在既定 crash-stop、非 Byzantine 模型下未发现 blocker；恶意伪造的超大 RPC 字段属于 V1 明确
边界外，不以测试组合膨胀为由进入新协议阶段。M0–M7 至此完成并停在 M7；不进入 M8/V2，等待用户新命令。

# 风险与提前决策

## 1. Catalog 恢复比复制协议更早暴露问题

现有 Catalog 是非持久化结构，TableHeap 也缺少清晰的“按首页面重新打开”构造路径。必须先解决这两个问题，否则快照文件即使复制成功也无法恢复数据库语义。

## 2. Snapshot 是否 self-contained 是前置问题

BusTub 的部分 MVCC undo 信息只存在于内存 Transaction 中。M0 必须证明：停止准入、在 `StateVisibilityLatch exclusive` 内排空事务并完成 GC/canonicalization 后，`db.bustub + catalog.bin + session.bin` 可以在没有旧进程内存的情况下恢复全部 committed state，包括每行真实的最近提交时间戳、`schema_epoch`、OID 和 SessionTable。若不能，必须先定义并持久化缺失状态，不能直接复制数据库文件，也不能依赖先 Flush 不权威的 working file 来掩盖缺失元数据。

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
- PostgreSQL base backup + 连续 WAL：<https://www.postgresql.org/docs/16/continuous-archiving.html>
- NuRaft 的 Raft、snapshot、可插拔 LogStore/FSM、group commit 与 pipeline 能力：<https://github.com/eBay/NuRaft>

这些资料用于借鉴磁盘状态组织、协议边界和故障测试方法；具体实现仍应服从本项目的 Catalog、MVCC、索引和 CMake/test 结构。

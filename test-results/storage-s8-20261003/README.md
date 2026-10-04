# S8 对象三 Store：最终结果（2026-10-04 测试契约复验）

- 阶段场景 **8/8**；真实 BusTub 与 TCP 三节点，含 InstallSnapshot/重启。
- 相邻回归 **58/58**：SnapshotStore 9、业务 FSM 4、LogStore 10、StableStore 4、RaftNode 31。
- 定向变异 **10/10** 被指定测试 failure 检出，不以编译失败、崩溃或超时计数。
- clang14 Debug + ASan/UBSan/LeakSanitizer，非 PIE，无跳过。阶段耗时 42.242 秒，不是性能评分。

[共同方案](../../docs/storage_redesign/s8_raft_object_stores.md) · [八项审查及失败归因](../../docs/storage_redesign/s8_execution_20261003.md) · [源码包](../../test/archives/S8-raft-object-stores.tar.gz) · [校验](SHA256SUMS) · [清理](cleanup-verification.json)

本次实际改动：T2 以公开候选额度、后继合法追加及重开代替控制根比较；T8 保留小映射预算及中途重开，在 64u Data 区内通过正式对象事务分批写入累计 56u 数据，旧正文+新正文超过总容量，重开验证新正文与存活快照；T6 删除重复的重启前 SELECT，恢复值直接对照字面预期。T6/T7 的 suffix/session 与 TCP 追赶风险不同，继续在同一阶段归档维护。

T8 只对明确 NotCommitted/NoSpace 等待后台释放后重试，重试窗口为 10 秒；它不打断单次票据 Wait，runner 另有整场景进程的 240 秒超时限制，超时记失败。其他错误立即失败。不直接调用 Reclaim 或读取 bitmap。跳过 S7→F14 物理回收的变异只能写入 81920/229376 字节，被容量断言发现。首次尝试将 56u 一次提交，碎片下超过原 16 项元数据预算；gdb 定位后改为每次 4u，未放宽预算或修改生产代码。诊断单独记录，不计通过。

生产及原回归的 489 项哈希与上轮一致，没有新 production hook/getter/API、默认路径或故障分支；共同 C/P 内容不变。正文走 Common/ObjectIO/B/Direct 文件，HardState/Manifest 走 B 控制记录。A 工作库/canonical 文件仍属 S9/S11；不宣称真实掉电、裸设备、长期满盘或性能已验证。

本地 S8-results.tar.gz 包含最终 XML/命令/日志/变异、源码包及 S5 依赖、生产与方案快照、逐成员 SHA256。现有 .gitignore 忽略结果包；仅 clone 不会获得它，应另行保存。源/结果包均原位替换，不保留旧 S8 包或展开测试/设备/二进制。其他有效模块档案保留。该源码归档、方案和结果摘要纳入本次 S8 提交；具体提交号以 Git 历史为准。

本次再次审查仅修正文档计数与时限表述，并更新包内说明和文档快照；生产、测试及 runner 源码未变化，保留上轮执行日志/XML 原字节，没有重新运行并产生另一组通过数。详见审查 §11。

提交前复核另修正主方案风险表残留的变异计数，以及 F26 对 S4/S5 依赖和 A/B 缓存共享的过时描述；未改变生产代码、测试或下一阶段授权范围。见审查 §12。

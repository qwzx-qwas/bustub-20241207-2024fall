# S12 正式对象部署与共同验收

基线 `87f4357` 加本轮工作区。真实 Release CLI / 三节点 TCP / Direct 定容文件；WSL2 Linux、GCC 16，Porcupine v1.3.0。没有使用假数据库，也没有修改共同业务内容与判据。

## 当前结果

- CLI 部署检查 8 项、有界元数据范围 1 项、快照观察器 1 项通过；旧客户端协议 3 项通过；4 个隔离变异命中指定断言。
- 原 C2、C4、C5-short/long 在上一轮最终 C++ 源码上通过；C3-kill/isolate/drop 在本轮此前修复阶段通过，带当时源码差异，未在最后范围调用方修改后重跑。7 个不同场景均有证据，但不能合称最终完整矩阵；C2 一次种子。
- C1 未通过：更新后校验收到 NOT_LEADER；修复扫描范围后仍在更新校验时换主。代码中的 SQL 执行仍持有节点协议锁；旧文件后端同一 C1 通过。没有把健康场景改成自动重试。
- P2 默认部署资格：300 秒准备预算到期，尚未完成加载后校验；未进入正式性能窗口，不可报告吞吐提升。
- 低缓存资格同样未进入窗口，且关闭超时，框架最终终止进程；不能宣称压力推进与关闭通过。最终数据见 [执行记录 §8](../../docs/storage_redesign/s12_node_execution_20261009.md#8-执行证据)。P1/P3/P4、完整基线矩阵、SS/RS/PL/GT 本轮未完成。

## 证据与恢复

唯一源码包 [F34-node-lifecycle.tar.gz](../../test/archives/F34-node-lifecycle.tar.gz) 中的 deployment/ 保存当前阶段测试和变异 runner。原本地 F34 内容保留为历史，未计入本轮通过数。共同 E2E 继续保留在 test/storage_acceptance，不另复制一套常驻测试。

本目录 `F34-node-results.tar.gz` 是被 `.gitignore` 忽略的本地证据，不能仅靠 clone 得到。包含本轮源代码差异/新文件、构建及协议日志、最终 CLI 结果、定向变异、各共同场景原始历史/配置/指标和失败分析；不含设备镜像、构建二进制或工具链。各次运行保存当时的 source.diff/untracked-source，避免将修复前结果冒充最终源码结果。

归档、校验和清理后仅保留一个当前 F34 源码包和一个本轮结果包，不保留错误测试旧包、展开阶段源码及设备文件。其他模块的有效历史依赖继续保留。

[实施协议](../../docs/storage_redesign/s12_node_deployment.md) · [八项测试设计审查](../../docs/storage_redesign/s12_node_execution_20261009.md)。本轮没有 commit/push；S12 整体验收仍未完成，S13 仍未实施。

## 本次追加复查（同日）

修复 B 脏页压力下提交依赖周期维护的等待环：Busy 时由提交角色有界协助同一个 B 写回器，不新增线程/API，不重做 Data IO。ClosePressure、StorePressure 两项真实组件场景通过；删掉协助的一处变异在两个场景均触发指定失败。原共同 C4 在本次最终源码复跑通过（54.215 s，cluster_exit=0，边界 82，恢复后继续 8 次写入）。C1、低缓存 P2 未重跑，保留未通过状态。

新证据位于结果包 `review/`；本次源快照为 `review/source/`。包原先 `source/` 及各次运行是对应历史源码，不冒充本次通过。完整八项设计审查见 [执行记录 §10](../../docs/storage_redesign/s12_node_execution_20261009.md#10-再次复查提交进展不能依赖正在等待它的维护角色)。

## 本次第二次追加复查（同日）

修正 B 页 IO 的错误分类：复用既有 F02 容量计算，单批超限明确拒绝，可容纳批次暂时 Full 返回 Busy，真实 pwrite/Flush 错误保留。MetadataAdmission 一项组合场景及三个指定变异通过；ClosePressure/StorePressure 在本次源码复验通过；原 C4 55.222 s 通过（cluster_exit=0，确认边界 82，恢复后 8 次写入）。本次证据与完整源快照在结果包 `review2/`；既有 `review/`、原 C1/P2 失败记录保留其历史归属。见 [审查 §11](../../docs/storage_redesign/s12_node_execution_20261009.md#11-再次复查b-页-io-暂满永久超限与真实错误)。未运行新的性能矩阵或 sanitizer。

## 同日提交门槛复验（最新）

当前构建仅将无生产调用的旧 DiskScheduler 移至原课程测试目标；运行时 cpp/h 与 review3 相同。重新构建真实节点和原课程测试通过，生产库确认不含旧调度器。原 C1 **51.764 s 失败**：更新后校验收到 NOT_LEADER；原低缓存 P2 **362.419 s 未完成**：加载校验第 125 次未确认，未进入性能窗口，关闭超时、cluster_exit=-9。未改业务模型、原选主或重试规则，未以组件成功替代共同验收。

因用户要求“都没问题再 commit”，本轮未提交；S13 方案待确认。最新证据及恢复源快照改为 **`final-review/`、`final-review/source/`**。原四个阶段场景、变异和 C4 没有在本轮重跑，仍归 review3；§13 只核对证据，没有新执行数。源码包/结果包均原位更新，无旧备份。跨模块去重、测试判据和后续提议见 [执行审查 §14](../../docs/storage_redesign/s12_node_execution_20261009.md#14-提交门槛复查跨模块去重与原场景复验)。

## 同日第三次复查（历史，早于提交门槛复验）

当前修正为 checkpoint Journal 准入分类及 F07 数据+独立 Flush 容量预检。MetadataAdmission 扩展场景、JournalAdmission 新场景、两个指定变异、Close/Store 和原 C4 在当前源码通过；C4 54.164 s、确认边界 82、恢复后 8 次写入、cluster_exit=0。证据和当前源快照为 `review3/`；新 fixture 两次准备错误单独标识，不算通过/变异检出。恢复使用 `review3/source/`，此前目录均为历史源码。C1/低缓存 P2 仍未复验解决，未运行完整性能或 sanitizer。八项设计审查见 [§12](../../docs/storage_redesign/s12_node_execution_20261009.md#12-第三次复查checkpoint-的-journal-准入与-flush-合计容量)。

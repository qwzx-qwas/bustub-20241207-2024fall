# F35 / S13.3 当前证据（2026-10-10复审）

本次 S13.3 交付基于 `69c1427`；具体提交由 git log 查询。唯一源码/结果包原位替换，无旧包备份。

最新复审仅检查代码逻辑、测试断言和证据对应，未修改生产代码/测试源码、未重新执行测试；568个依赖指纹与以下执行版本一致。具体见[提交前总审§8](../../docs/storage_redesign/s133_execution_20261009.md)。本次修正跨模块状态冲突，核查生产职责及归档清理，更新包内审查记录。

## 最近一次实际执行结果（执行审查§6；本次核验）

| 范围 | 结果 |
| --- | --- |
| Release / GCC16.2.0 | 相关生产库与回归目标重新构建成功 |
| 原回归 | 66项通过，无失败/跳过；`regression/`含原XML/log及汇总 |
| F35阶段 | 10组件＋7真实TCP节点通过；`stage/baseline.xml`、`stage/node.xml` |
| 隔离变异 | 22项由指定断言检出：原18＋业务拒绝错误分类4项；`stage/summary.json`及各run日志 |
| 业务判据 | 错误请求4—7持久拒绝、原收据可重试，请求8正常执行，最终三行(27,14)、(43,21)、(59,34) |

生产改动只接续SQL输入错误分类，详见 [复审§6](../../docs/storage_redesign/s133_execution_20261009.md)。未新增同职责模块、fixture或测试专用生产接口。

## 当前包与历史证据

- [阶段源码](../../test/archives/F35-raft-pipeline.tar.gz)：最终两个C++场景、唯一runner、原Direct脚本、恢复说明、审查与568个依赖指纹。仅压缩交付。
- `F35-results.tar.gz`（gitignored）：本次原始结果、构建信息、源指纹、完整工作区源码差异及仅本次的`source/review-change.patch`。
- `prior-20261009/`保留初轮证据和原源快照，包括Direct窗口（64MiB、两次SIGKILL）及共同C1。本次未重跑这两项，不能作为当前代码的新成绩。
- `evidence-sha256.json`逐文件校验；[SHA256SUMS](SHA256SUMS)校验两个当前包；[cleanup-verification.json](cleanup-verification.json)记录实际清理。

没有性能提升、完整C/P矩阵、本轮ASan/TSan或真实设备掉电结论。客户端保存重试输入和确认承诺的职责不变；新窗口首次建立串行，其后跨客户端流水线仍由原依赖登记管理。

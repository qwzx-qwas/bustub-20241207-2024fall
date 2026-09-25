# S3 / F07 单份 Journal 结果

首次执行 2026-09-24；复审 2026-09-25。修正版 8/8 新组件场景、原 F02 8/8、原 F06 2/2 正常测试通过；13 个故意改坏的版本均被指定断言发现（预期失败）。正确生产实现的故障隔离用例另行复核通过。ASan/UBSan、GCC Werror、仓库格式/lint 及 clang-tidy 通过。

- [八项测试设计审查](../../docs/storage_redesign/f07_execution_20260924.md)
- [最终源码包](../../test/archives/F07-journal-service.tar.gz)
- 本地结果包 `F07-results.tar.gz`：日志、XML、变异源码/结果、生产源码快照、最终测试与 manifest、实际方案和环境。哈希见 [SHA256SUMS](SHA256SUMS)。
- 解压到仓库外；历史重建使用 source/ 快照覆盖隔离基准 checkout，然后核对 manifest。结果包沿既有 gitignore 本地保留，普通 clone 不会取得它。

基准 `00a8397332408a0bb95f021789a77d78f121135f` 加生成证据时的工作区改动，生产/测试确切哈希由 manifest 固定；最终提交身份以 Git 历史为准。普通 Journal 单份追加、一次组 Flush；无 mutable A/B 边界。F03 固定引导副本不变。

真实 Direct 文件 IO；受控暂停与 EIO 在测试链接侧。不是裸设备或真实断电，不代表 B WAL、S5 checkpoint/回收、Deferred、节点 E2E 或性能测试已经完成。共同 C/P 内容和旧基线未变。无旧 F07 错误包保留，旧有效模块包未改；清理证据见 cleanup-verification.json。

本次修正故障隔离时机、CRC 独立判据、组恢复内容和 Flush 完成判断。源码与结果包原位替换；旧错误测试源码、旧包、二进制和设备镜像不保留。旧测试漏检的诊断日志/XML 单独标注用途，不作为正常验收结果。

# 存储重构模块验证

本目录保留 S0–S13 阶段风险和测试演进的说明入口。按 2026-09-22 用户要求，开发期小测试验证后删除或按模块压缩，长期主线是 [storage_acceptance](../storage_acceptance/README.md) 的真实生产 C1–C5 / P1–P4 及其工具。跨模块内容复用、模拟到真实接入和阶段退出见[主方案 §1.6](../../docs/storage_redesign/README.md#16-测试演进跨模块复用与阶段退出)。

## S0 / F00

原 `storage_range_contract_test.cpp` 对应 [F00 当轮范围](../../docs/storage_redesign/modules/storage-contracts.md#613-s0-当轮实施范围用户授权后落定)，现保存为 [F00-storage-contracts.tar.gz](../archives/F00-storage-contracts.tar.gz)：

- 范围算术不能溢出，子范围不能逃出父范围，空尾部不授权读取下一字节。
- 使用真实文件和非空快照正文验证公开流式解码的 slice 边界及过大嵌套长度拒绝。
- 测试侧通过已有存储接口观察请求范围，再委托真实文件读取；越界请求即使最后被 OS 拒绝也会判失败。先验证正常输入可被该观察器读取；极大嵌套长度样本保留正确 CRC，避免其他损坏掩盖长度问题。
- 合法格式与业务恢复复用 `raft_state_machine_test` 和 `sql_storage_contract_test`，不重建一套 golden/业务回归。

上述是历史验证范围；只证明范围和受影响路径，不证明三节点或设备掉电安全。当前不再保留这份可执行源码或 `storage-redesign` CTest 标签；历史命令、审查及证据见 [F00 执行记录](../../docs/storage_redesign/s0_execution_20260921.md)。单节点 SQL 回归另在 T0 压缩包中，旧格式 golden 仍在 `legacy_raft`。

本次压缩不代表 C4/C5 已接管恶意长度或溢出边界。后续相关生产入口就绪时，评估能否通过正式链路触发同一风险，复用输入及独立预期，不为每层复制测试；仍未覆盖的部分明确登记。恢复方式和逐文件校验见[模块归档](../archives/README.md)，历史提交前核对见 [复审记录](../../docs/storage_redesign/s0_test_review_20260921.md)。

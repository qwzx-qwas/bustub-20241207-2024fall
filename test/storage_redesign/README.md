# 存储重构模块验证

本目录保存 S0–S13 各阶段新增的、保护项目行为和风险边界的必要测试。共同业务正确性/性能内容仍在 [storage_acceptance](../storage_acceptance/README.md)，既有回归在 [legacy_raft](../legacy_raft/README.md)。本目录不复制 C1–C5/P1–P4，也不把第三方保证或每层实现细节作为测试目标。

## S0 / F00

[storage_range_contract_test.cpp](storage_range_contract_test.cpp) 对应 [F00 当轮范围](../../docs/storage_redesign/modules/storage-contracts.md#613-s0-当轮实施范围用户授权后落定)：

- 范围算术不能溢出，子范围不能逃出父范围，空尾部不授权读取下一字节。
- 使用真实文件和非空快照正文验证公开流式解码的 slice 边界及过大嵌套长度拒绝。
- 测试侧通过已有存储接口观察请求范围，再委托真实文件读取；越界请求即使最后被 OS 拒绝也会判失败。先验证正常输入可被该观察器读取；极大嵌套长度样本保留正确 CRC，避免其他损坏掩盖长度问题。
- 合法格式与业务恢复复用 `raft_state_machine_test` 和 `sql_storage_contract_test`，不重建一套 golden/业务回归。

测试只证明范围和受影响路径；不模拟 durable 标志，不证明设备掉电安全，不测复制数量或内部调用顺序。CTest 用 `storage-redesign` 标签隔离本目录用途；最终命令、审查及证据见 [F00 执行记录](../../docs/storage_redesign/s0_execution_20260921.md)。

保留依据是上述契约仍被生产路径使用，不能因 S0 已结束就删除。未来实现替换时按风险所有者迁移或合并测试；一次性诊断代码、构建与运行中间产物清理，仅保留必要压缩证据。提交前的逐条核对见 [复审记录](../../docs/storage_redesign/s0_test_review_20260921.md)。

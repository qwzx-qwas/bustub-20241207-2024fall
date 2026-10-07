# S0 / F00 首轮历史摘要（旧包已删除）

2026-10-07 全仓归档复查：首轮 `results.tar.gz` 含随后修正过的测试源码，已删除，无备份。当前唯一 S0 结果包见 [提交前复审](../storage-s0-review-20260921/README.md)。本目录 SHA256SUMS 指向该修正版包。

首轮曾报告 10/10 定向场景通过；随后修正极大嵌套长度样本的 CRC 与读取范围观察，不能把首轮结果当作修正后的验收。最新测试和证据由复审入口说明。

首轮的运行、PIE/检测器启动及 LeakSanitizer 环境诊断共 20 份日志已收进修正版结果包的 `history/s0-initial/`，不保留旧 before/、源码快照或源码 diff。原始旧包哈希仅在 cleanup-verification.json 中记录为历史标识。

[首轮实施记录](../../docs/storage_redesign/s0_execution_20260921.md) 保留历史过程；[修正版审查](../../docs/storage_redesign/s0_test_review_20260921.md) 说明修正内容。没有重新运行或新增 S0 验收，构建产物仍已清理。

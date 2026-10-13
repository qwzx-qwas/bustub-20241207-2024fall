# ReadIndex 编号与封闭轮次：2026-10-13

历史九点观测：P1 均匀/热点各三轮，P2 八客户端三轮；使用旧无缓冲 Python 驱动。测量、业务核对及关闭成功。

P1 吞吐中位数为 285.76/281.72 次每秒，P2 八客户端为 33.09 次每秒；测量期 307,541 次实际读取均返回 OK，未出现此前 b709e0a 的 15 次读取 TIMEOUT。不能因超时消失就声称常见请求延迟或吞吐改善。

完整结果见 [analysis-comparison.json](analysis-comparison.json)，当时的检查见 [verification-summary.json](verification-summary.json)。后续使用同一缓冲驱动交错比较的结论见 [新批次](../storage-read-handoff-20261013-122218-tlPmB5/README.md)，不能直接混合两套驱动成绩。

设备与可重建输入已按用户授权删除，结果及逐请求历史保留；清理明细统一记录于新批次的 cleanup-verification.json。SHA256SUMS 保留历史身份，不表示旧工作区路径当前仍匹配。Git 保存摘要及逐点 result.json，大型原始记录仅保留本机。

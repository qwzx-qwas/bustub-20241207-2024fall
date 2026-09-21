# 测试结果压缩归档

本批次全部运行证据已压缩，旧的失败、超时、中断记录也完整保留。完整解压流及逐文件 SHA-256 验证通过后，才删除展开的数据库、历史、构建副本和运行脚本。

- `successful-evidence.tar.xz`：之前已验证的成功证据，原包未改。
- `remaining-evidence.tar.xz`：清理前剩余全部证据，含原 README、配置、结果、源码快照、汇总与诊断。包内 `ARCHIVE-MANIFEST.json` 为逐文件校验清单。
- `cleanup-verification.json`：本次校验与清理记录；`SHA256SUMS`：两个压缩包校验和。

在本目录执行 `sha256sum -c SHA256SUMS` 可校验压缩包。查看时创建新目录：

```bash
mkdir -p /tmp/storage-performance-v2-20260920T143546Z-inspect
tar -xJf successful-evidence.tar.xz -C /tmp/storage-performance-v2-20260920T143546Z-inspect
tar -xJf remaining-evidence.tar.xz -C /tmp/storage-performance-v2-20260920T143546Z-inspect
```

按上述顺序解压，第二个包恢复后续更新的汇总和原始目录结构。当前测试源码仍位于仓库 `test/`，没有删除。性能批次的补充包还包含 `_current-tests/` 源码快照和 `_temporary-build/` 已验证测试二进制，临时目录中的展开构建已清理。

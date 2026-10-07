# S11 / F25 DATA 压缩与正文任务验证（2026-10-07）

**最新提交前复查：原 SnapshotStoreTest 9 项全部通过。** 此次将文件分块读取的保留身份检查复用 `PayloadFile()`；跨模块检查见 [审查记录](../../docs/storage_redesign/design_review_20261007.md)。前次 59 个正常场景与 3 个指定变异证据继续保留，未全部重跑；不据此标记 F35 全面异步化或共同性能对照完成。

- 最新运行：`runs/commit-review-20261007/snapshot_store_test.xml` 9 项，Clang 14、ASan/UBSan/LSan。复用既有测试；没有新增常驻测试、生产接口或 hook。

- 前次复查正常：`runs/review-20261007/deferred.xml` 12 项、`checkpoint.xml` 35 项、`catalog_snapshot_test.xml` 5 项、`rpc_codec_test.xml` 3 项、`tcp_transport_test.xml` 4 项，共 59。既有场景复用，C2/C4 原位补强，不另增套件。
- 前次复查变异：`runs/mutations-review-20261007/mutations.json` 共 3 项：队列上限、协议阻塞、新增 pending 漏心跳。必须指定断言失败，不以编译失败/超时计检出。
- 前次实施的 52/12 及局部编码测量按原目录保留为历史证据，未重跑的变异和 benchmark 不充作最新复查结果。新增源码/结果仅当前一份，不保留旧包。
- ASan/UBSan/LSan，Clang 14。真实 SQL、Direct 普通文件、三 RaftNode/loopback TCP；不是 CLI E2E、真实掉电、裸设备、TSan 或跨机网络性能。
- `runs/benchmark` 记录真实 SQL 混合正文快照的 Raw/LZ4/Zstd 局部比较。LZ4 在该样本约节省 52%，Zstd level 1 约 61%，后者 CPU 更高；不能外推为端到端性能。codec 单独 O3、夹具保留 sanitizer；环境 RSS 含编译/夹具，不是 codec 独占内存。
- `diagnostics/review-runner-first` 保留复查首轮变异判据文字未同步的诊断；修正 runner 后三项均由指定断言检出，首轮不计成功。
- `diagnostics/normal-first` 记录异步化后旧消息驱动测试的进度超时。最终测试补上周期 Tick，增加丢 ACK/重传与旧能力回退，均通过。诊断失败不算验收成功，也未保留错误测试源码的旧包。

[实际改动与八项测试设计审查](../../docs/storage_redesign/s11_compression_execution_20261007.md) · [共同协议与 prompt](../../docs/storage_redesign/s11_snapshot_compression.md) · [唯一测试源码包](../../test/archives/F25-shared-snapshot.tar.gz)。基线 `c20bd98`，结果包包含当前生产源码、方案、命令和原始 XML；恢复命令及内容哈希见源码包 RESTORE/MANIFEST。

旧 F25 源码/结果包已原位替换，没有备份或嵌套旧包。7 个有效依赖模块包不变。阶段测试只保留压缩版，未新增常驻测试目标；原 C/P 内容不变。展开测试、设备、编译产物和临时变异统一清理；结果 tar 沿现有 `.gitignore` 本地保留。源码和记录纳入本次用户授权提交，不 push。

```bash
(cd test-results/storage-f25-s11-20261006 && sha256sum -c SHA256SUMS)
(cd test/archives && sha256sum -c SHA256SUMS)
result_work=$(mktemp -d /tmp/bustub-f25-results-XXXXXX)
tar xzf test-results/storage-f25-s11-20261006/F25-results.tar.gz -C "$result_work"
(cd "$result_work/F25-evidence" && sha256sum -c SHA256SUMS)
```

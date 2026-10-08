# S12 / F30 校验扫描结果（2026-10-08）

最新复核修正 B 扫描由写回/checkpoint 接手时的失败结果，并复用节点错误分类。**本轮全部 41 项正常场景通过、10 个不同变异被指定断言检出**；Debug 构建、ASan/LSan 开启，无跳过。基于 `0b3c870`，未 commit/push。具体改动、测试设计及局限见[执行审查 §13](../../docs/storage_redesign/s12_integrity_execution_20261008.md#13-再次复核2026-10-08写回接手扫描失败的结果与状态)。

## 保留内容

- [唯一源码包](../../test/archives/F30-integrity-scanning.tar.gz)：最终测试源码、三个运行脚本、RESTORE 与 MANIFEST。仅压缩保存，不加入常驻默认目标。
- `F30-results.tar.gz`：本地结果，受 gitignore 忽略；包含 `normal`（F30 11 + F27 12）、`raft-regression`（F29 10 + S8 8）、`mutations`（10 个）、`build`、对应源码/方案快照及校验清单。全部为本轮复验，不混入历史通过次数。
- `SHA256SUMS`：仓库根运行 `sha256sum -c test-results/storage-f30-20261008/SHA256SUMS`。
- `cleanup-verification.json`：包核验与临时产物清理记录。

扩展原 B 冷页测试，分别验证扫描器、Writeback、Checkpoint 处理同一类真实持久损坏。使用现有维护入口保证返回时状态的断言发生在下一轮扫描之前。新增 `scan-handoff-result`、`scan-handoff-condition` 两个变异分别检出结果接口与分类错误；不以编译错误、超时或 sanitizer 崩溃计作检出。

旧 F30 压缩包及分轮结果已替换，没有备份错误测试源码。历史 41/8、三场景复验和额外压力十次仅是先前审查记录，不叠加到本轮 41/10。F27/F29/S8/S5/F31-F22 有效依赖包哈希未变。

## 恢复与边界

在临时目录展开源码包并按 RESTORE 操作，先核对 MANIFEST 中生产、依赖及测试哈希。结果包展开后，在 `F30-results` 内执行 `sha256sum -c FILES.sha256`。生产修改快照和基线用于还原对应版本，不能直接覆盖未来生产树。

旧 S8 低预算适配仍将单批 4U 改为 2U，保留总正文、空间复用及恢复判据；没有放宽生产预算。新格式正常读校验默认生效，后台扫描显式开启，旧格式无依据范围明确未覆盖。

测试基于 Direct 普通文件及既有 SQL/TCP 回归。共同 C/P、SS/RS/PL/GT、裸设备、真实掉电和性能收益均未在本轮验证。

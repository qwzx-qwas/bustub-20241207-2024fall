# S12.2 / F31＋F22 整合结果（2026-10-08）

**82 项正常场景通过（新增 8、复用 74），13 个指定变异被检出。** 沿用原 S12.1 目录，源码/结果原位替换，前版不另保留。

- `run_stage.py`：47 项，含原资源/回收 7、F27/F19/F26/LRU-K 32，以及本轮新增 8；13 个指定变异。
- `run_snapshots.py`：复用 S8 8、F28 7、共享 6、增量 9、压缩 5，总计 35。只适配部署和结果接口，不改变 SQL、会话、恢复、网络输入与预期。
- Clang14 Debug / AddressSanitizer；LSan 因环境限制关闭，未跑 TSan。真实 Direct 文件 IO 和 loopback TCP，不含裸设备、真实断电、原 C/P 或 SS/RS/PL/GT 性能结果。
- [具体改动及八项审查](../../docs/storage_redesign/s12_integration_execution_20261008.md) · [共同协议](../../docs/storage_redesign/s12_resource_gc.md)。

## 问题归属

快照组合首轮 33/35：连续有界 Collect 遇上一笔完成后的内部名额短暂占用，属于本次整合的生产衔接问题；已改为保存候选、结束本轮，下轮继续。修复后原 35 项全部通过，没有改业务 oracle 或扩大限额让它过关。

测试侧问题单列，不计通过：沙箱禁止端口、EIO 测试误等 object_error 而非实际 error、变异编译缺 LZ4 头文件路径、最后变异的异常文本匹配过窄、一次手工定向运行漏设工作目录。最后变异修正 runner 匹配后定向复跑，并核对全部正常/变异 XML；其他生产和测试正文未变，无需因此重复全套。

## 保存与恢复

源码仅留 [F31-F22-resource-gc.tar.gz](../../test/archives/F31-F22-resource-gc.tar.gz)。结果仅留本目录 `F31-F22-results.tar.gz`，含最终日志/XML/命令、13 个指定失败、源摘要、基准上的生产源文件和方案记录。测试正文只在源码包，不另在结果包重复存一份。

`diagnostics/` 保存开发失败的日志，**不保存错误版测试包、设备、二进制或变异源码**。`finalize.py` 是最后一次指定变异复验后的 XML/命令证据核验脚本，不参与 production；fresh runner 已包含正确匹配。源包 `RESTORE.md` 提供完整复现命令。依赖归档有哈希并继续保留，它们是其他模块的有效证据，不是应删除的同模块旧包。

```bash
(cd test/archives && sha256sum -c SHA256SUMS)
(cd test-results/storage-s12-resource-gc-20261007 && sha256sum -c SHA256SUMS)
```

按原 `.gitignore`，结果压缩包仅本地保存；Git 保存文档、摘要和小型源码压缩包。仅 clone 仓库不含运行日志，需要取得同哈希结果包。无新的常驻阶段测试。展开测试、构建目录、设备镜像和变异产物在归档验证后清理，见 cleanup-verification.json。

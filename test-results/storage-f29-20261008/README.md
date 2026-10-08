# S12.3 / F29 日志段整理结果

2026-10-08。基准 `055198eab6a426053c3f5b40b0087cf184caac28` 加 S12.2/S12.3 未提交修改；未 commit。

[共同协议](../../docs/storage_redesign/s12_log_cleaning.md) · [具体代码及八项测试审查](../../docs/storage_redesign/s12_log_cleaning_execution_20261008.md) · [测试源码包](../../test/archives/F29-log-cleaning.tar.gz)

## 最终结果

| 场景 | 结果 |
| --- | --- |
| F29 真实日志/对象集成 | 10/10 |
| S12 共享预算/后台整合回归 | 13/13 |
| Deferred | 14/14 |
| Common 对象事务 | 10/10 |
| 页 IO/缓存 | 9/9 |
| 原 LRU-K | 1/1 |
| 原 S8＋F28/F25 组合（含真实本机节点通信） | 35/35 |
| 总正常场景 | **92/92，无跳过** |
| 故意改坏生产逻辑 | **7/7 命中指定 assertion** |

变异分别跳过 Unmap、忽略新分配成本、禁止整理、跳过目录代次核对、借用完成额度，以及本次新增的误抛 IO 暂满、误抛可选尾部清单容量限制。非预期异常/编译失败/崩溃不算检出。runner 在本次运行直接核对精确失败标记，XML 随包保留。

本机 Clang 14/Debug/ASan；LSan 关闭。10 项新场景使用正式 LogStore/NodeStorage/Direct 文件，其中自动回收场景使用两倍发现值的分配单位。其余 82 项复用原测试内容，仅适配旧夹具显式参数。未执行 TSan、裸设备、真实掉电、共同 C/P 或 SS/RS/PL/GT 性能；不能据此声称提升百分比。进程中断测试只在指定候选写入点退出进程。

## 包与恢复

- `F29-results.tar.gz`：最终命令、日志/XML、变异判据、方案/审查快照、当前生产修改 `source/`、`source.patch` 和完整哈希。大包受既有 `.gitignore` 管理，clone 不会取得结果包。
- `test/archives/F29-log-cleaning.tar.gz`：唯一当前阶段源码包，含 10 项测试、runner、RESTORE、MANIFEST；不注册常驻小测试。
- 核验本目录 SHA256SUMS 后，按源包 RESTORE 在隔离 checkout 覆盖当前生产文件，匹配 MANIFEST，再构建运行。依赖包仍用原同哈希文件。
- `diagnostics/` 只保存初次命名歧义、错误共享 setup、变异判据顺序的历史诊断日志，不含旧版错误测试源码，不算当前证据。
- 本次复审原位替换 F29 两个包，旧 90/5 包不保留备份。F31/F22 与 F25 等有效前置档案保持不变；它们是回归来源，不是应删除的错误版本。

临时 `/tmp/bustub-f29-review`、展开测试、二进制和测试设备文件清理状态见 cleanup-verification.json。保留的是可恢复证据，F29 并未另建重复回收/分配/索引模块。

本次具体修正及新增 T9/T10 的目标、输入、Oracle、非重复性和接口影响见 [复审 §9](../../docs/storage_redesign/s12_log_cleaning_execution_20261008.md#9-再次复审io-暂满与最终发布容量2026-10-08)。

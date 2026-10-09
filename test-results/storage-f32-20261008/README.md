# F32：按需 B 缓存、去重写回与 A/B CLOCK-Pro

2026-10-09 再次复审；沿用 2026-10-08 模块目录，基准 `8b1805c` 加工作树。前次修复 NewImage 计账并完整运行 77/12。本次 T4/T5/T7 改用输入模型、简化预热、收紧变异判据；三个 benchmark 清理旧 LRU-K 头依赖。生产库逻辑未修改，503 个生产清单文件及七个依赖包未变。MANIFEST 同时记录当前源码和沿用回归的原始源码哈希，实测后无生产或测试改动。

## 结果

| 验证 | 结果 |
| --- | --- |
| F32 正式对象/B/策略场景 | 本次重跑 11 通过 |
| 原 F30/F27 | 沿用同源 23 项通过结果 |
| 原 F29/S8，含 SQL/TCP | 沿用同源 18 项通过结果 |
| 原 F26 真实页/SQL | 沿用同源 10 项通过结果 |
| 原课程 BufferPool/PageGuard/B+Tree | 沿用同源 14 项通过结果 |
| 旧 LRU-K 课程兼容 | 沿用 1 项通过结果，不作为新生产策略证据 |
| 定向变异 | 本次重跑 12/12 在指定断言/结果失败 |
| bpm/btree/htable benchmark | 本次重新编译成功，未测性能 |

有效证据共 77 项正常测试，其中本次执行 11 项、沿用 66 项；不能表述为本次重跑全部 77 项。ASan/LSan 开启。新断言以写入前输入为 oracle，旧版重读变异必须报告版本/校验失败，合并加载变异仍须报告重复 IO。策略轨迹两组热点各 900 次访问、各 3 次缺页，仅为特定输入观察。未运行完整 C/P、SS/RS/PL/GT、TSan、裸设备及真实掉电。

## 保留与恢复

- [测试源码包](../../test/archives/F32-metadata-cache.tar.gz)随仓库保存，只含最终测试、三个 runner、RESTORE、MANIFEST；不新增常驻小测试目标。
- 本目录 `F32-results.tar.gz` 由 `.gitignore` 忽略，包含当前 normal/mutations、沿用的 regression 日志/XML/命令、证据沿用核验及源码快照。不能仅靠 clone 获得本地结果包。
- 按包内 RESTORE，在隔离基准 checkout 覆盖结果包 source/，核对哈希并在仓库外解压运行。旧依赖测试从七个原包复用，未复制正文，也未修改原包。
- 原位替换旧 F32 源码/结果包；本轮最终只留这两份不同职责的压缩包，无旧 F32 备份、展开小测试或编译/设备产物。清理核对见 cleanup-verification.json。

[方案与交接](../../docs/storage_redesign/s12_metadata_cache.md) · [逐项测试设计、重复/污染/变异/稳定性审查](../../docs/storage_redesign/s12_cache_execution_20261008.md) · [提交前跨模块核对](../../docs/storage_redesign/design_review_f32_20261009.md)。提交前再次核验生产、测试、依赖及原始结果哈希；未新增 C++ 测试运行。本次结果包仅刷新文档快照并增加核验记录，运行日志不变。

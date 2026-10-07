# S11 / F25 自动增量追赶与去重验证（2026-10-07）

**上次实际复验：1 个增强场景通过、4 个指定变异检出；当前有效证据合计 47 个正常场景、19 个不同变异。** 其余 46 个正常场景和 15 个变异沿用未改动部分的既有结果，不表示全套重跑。 日志不足时由真实 Leader 自动协商旧基础并发送最新目标，已由三节点生产路径验证。S11 压缩、S12/S13、正式性能比较仍未完成。

- 最终默认阶段证据：`runs/clean-final/deferred.xml` 12 项、`checkpoint.xml` 30 项（F25 共享 6、增量 9、F28 7、S8 8）；另直接运行既有 Catalog 5 项（`runs/catalog.xml`）。不把诊断重跑累计成额外覆盖。
- 上次实际定向证据：`runs/design-review-20261007/normal/checkpoint.xml`、`runs/design-review-20261007/mutations/mutations.json`。增强真实业务后缀和 Raft 启动恢复；Offer/Install 立即核对目标。
- `runs/mutations` 原记录 18 项、`runs/mutations-final` 是其中 5 项复验。**原更换目标变异实际反复重建旧会话、依靠进度超时，不能算直接检出。** 最新变异同时更换目标选择和重建条件，已命中身份错误；新增加跳过启动 ApplyCommitted 的变异，命中恢复进度/业务/Session 断言。当前 19 项来自 15 个未变旧变异及本次 4 个，不累计重复执行。旧超时及本次修正过程只作诊断。
- Clang 14 Debug，ASan/UBSan/LSan；真实 SQL、Direct 普通文件、loopback TCP。自动追赶夹具先准备基础数据/日志缺口，随后只控制离线链路与逻辑时钟，Raft 自行选举、发 Offer/Install/Append；旧手工 TCP 场景专门保护乱序/会话/term 风险。不是 CLI/客户端网关 E2E；低收益回退仍为 Store 集成测试。未验证真实掉电、裸设备、TSan 或性能比较。
- 唯一阶段源码：[F25-shared-snapshot.tar.gz](../../test/archives/F25-shared-snapshot.tar.gz)。MANIFEST 固定生产源码、五个包内文件、七个有效依赖包及复用的 Catalog 测试。RESTORE 提供重建运行命令；测试只压缩归档，不新增常驻目标。
- [共同协议](../../docs/storage_redesign/s11_incremental_snapshot.md) · [实际改动、代码逻辑与测试设计审查](../../docs/storage_redesign/s11_automatic_execution_20261007.md)。旧文档的 38/13 为前一轮证据，本包已经原位替换旧包，没有旧版备份。

最新复审只收紧变异脚本，未修改生产代码或 C++ 场景；使用同一判定函数复核 19 份既有原始 XML，全部符合；10 种无效报告及原目标超时 XML 全部拒绝。见 `runs/verdict-review-20261007`。这次没有重编译/重跑 C++，这些判据检查不增加 47/19 的测试数量。

再次审查未修改生产代码。此前本轮生产变化包括 Manifest v2 独立传输基础、轻量协商后规划、按成本代理筛选、固定传输目标与一个重传块；PrepareSnapshot 一次构建后安装；校验遍历/多索引扫描合并；范围引用合批；映射枚举及帧缓冲读取。保持现有发布、恢复校验和引用保护。20% 是初始筛选规则，网络正文减少与逻辑读取次数减少都不代替实际延迟/设备 IO 测量。

过程诊断保留在 `diagnostics`：首次沙箱无法使用 TCP/LSan，后在允许环境复验；测试夹具曾把清单版本写成小端，已按实际大端格式修正；1 KiB 日志段导致大夹具裁剪超出现有提交额度，改用合法 16 KiB 测试部署，未放宽生产额度。编译阶段的缺少头文件及临时值生命周期错误也已修正。最终正常结果均为零失败。

本目录仅保留 README、SHA256SUMS、清理核验及 `F25-results.tar.gz`。结果包包含最终命令/日志/XML、变异、诊断、当前源码及 diff、唯一测试源码包和方案审查，不保留镜像、可执行文件、对象文件或旧测试包。其他模块有效依赖包不变。目录沿用初次验证日期以保持链接有效。

归档校验后删除 `/tmp/f25-next` 、再次审查的 `/tmp/f25-review-20261007` 及判据复审的 `/tmp/f25-proof-review-20261007` 展开和构建产物；结果 tar 沿现有 .gitignore 本地保存。

提交前复核再次核对当前源码及原始 XML：47 个不同正常场景通过，19 个变异符合归档 runner 的指定失败判据；本次没有重跑 C++。仅修正文档的过时进度摘要并同步归档说明，详见执行审查 §9。用户已授权提交本阶段，具体提交身份以 Git 为准；不 push。

```bash
(cd test-results/storage-f25-s11-20261006 && sha256sum -c SHA256SUMS)
(cd test/archives && sha256sum -c SHA256SUMS)
result_work=$(mktemp -d /tmp/bustub-f25-results-XXXXXX)
tar xzf test-results/storage-f25-s11-20261006/F25-results.tar.gz -C "$result_work"
(cd "$result_work/F25-evidence" && sha256sum -c SHA256SUMS)
```

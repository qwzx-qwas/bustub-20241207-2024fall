# S11 / F25 共享与增量接收验证（更新至 2026-10-07）

**本轮 38 项正常测试通过，13 项指定变异全部检出；增量整轮尚未完成。** 当前 `previous` 同时承担恢复入口与日志保留边界，独立传输基础的保留策略仍待选择。因此，实际 TCP Follower 安装已验证，自动 Leader 在自然落后场景选择增量的 E2E 尚未完成。

最新复核见 [审查 §10](../../docs/storage_redesign/s11_incremental_execution_20261007.md#10-再次核对实现与证据范围2026-10-07)：未改生产或测试行为，沿用上述实测结果，没有重新运行。修正“并发退役”的范围表述，并明确重复块断言不能独立证明所有空间已释放；本次实际执行归档哈希、XML/命令、文档链接和 diff 核验。

- 新增增量 5 项、原 F25 6 项、F28 7 项、S8 8 项、F27 12 项。原模块场景从有效归档复用，不另建重复 oracle。
- 最终正常证据：`runs/session-review20261007/deferred.xml` 的 12 项与 `runs/session-review20261007/checkpoint.xml` 的 26 项。`runs/session-mutations20261007` 是十三项定向变异；其余运行仅为过程诊断，不累加通过数量。`runs/selection-review.json` 单独记录变异脚本拒绝空/未知选择的两条命令。
- Clang14 Debug，ASan/UBSan/LSan；真实 SQL、Direct 普通文件、本机 TCP。新 TCP 场景使用实际 Raft Follower，发送端为测试协议驱动；原 S8 三节点继续保护全量传输。未进行真实掉电、裸设备、TSan 或性能对比。
- 唯一阶段源码：[F25-shared-snapshot.tar.gz](../../test/archives/F25-shared-snapshot.tar.gz)。MANIFEST 核对生产、测试及七个依赖包；RESTORE 提供重建与运行命令。
- [增量协议](../../docs/storage_redesign/s11_incremental_snapshot.md)、[实际改动与测试设计审查](../../docs/storage_redesign/s11_incremental_execution_20261007.md)。旧共享阶段证据范围见对应历史文档，本轮未将自动增量标为完成。

此前复审补齐 BeginDelta 的最终清单预算预检和独立会话判据。本次修复迟到 Offer/旧全量块取消新接收、Accepted 重发未续期的问题，并在新 term 清除编号边界。复用原 TCP 测试验证这四种风险，没有增加正常测试函数或生产测试接口；四个新变异逐条验证断言有效。另修正变异脚本静默跳过未知选择的问题。具体逻辑、Oracle 和范围见审查 §9。

测试验证独立 SQL/Session 预期、日志后缀、重启、旧基础退役期间读取、大于数据库页的分配单位、错误基础、冲突重发、错误复用和旧会话；字节对照不是唯一判据。接收阶段观察实际 pwrite，检查未变化正文没有额外复制。观察代码只链接在测试程序，没有 production hook/getter。

本目录仅保留 README、SHA256SUMS、清理核验及 `F25-results.tar.gz`。结果包保留命令/日志/XML、变异说明、当前生产源及 diff、当前唯一测试源码包、方案与审查，不保留设备镜像、执行文件、对象文件或旧源码包。原 F25 源码与结果包原位替换，无旧版/备份；S8/F28/F27 等独立有效依赖包不变。目录沿用初次验证日期以保持既有链接有效。

首次沙箱 LSan 线程检查失败，仅记环境诊断；在允许线程检查和本机网络的环境复验，没有关闭 sanitizer。构建与测试在 `/tmp` 展开，归档校验后删除本轮中间产物。结果 tar 沿既有 .gitignore 本地保存，不自动加入 Git。用户已授权本次审查通过后提交已完成的增量机制；提交范围与下一步边界见 [§11](../../docs/storage_redesign/s11_incremental_execution_20261007.md#11-提交前核验与后续边界2026-10-07)，实际提交以 Git 历史为准。

```bash
(cd test-results/storage-f25-s11-20261006 && sha256sum -c SHA256SUMS)
(cd test/archives && sha256sum -c SHA256SUMS)
result_work=$(mktemp -d /tmp/bustub-f25-results-XXXXXX)
tar xzf test-results/storage-f25-s11-20261006/F25-results.tar.gz -C "$result_work"
(cd "$result_work/F25-evidence" && sha256sum -c SHA256SUMS)
```

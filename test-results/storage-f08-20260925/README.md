# F08 / S4 MetadataEngine B 复审结果（2026-09-26）

[方案](../../docs/storage_redesign/modules/metadata-engine.md) · [执行及八项审查](../../docs/storage_redesign/f08_execution_20260925.md#10-再次复审范围扫描的漏检2026-09-26) · [测试源码包](../../test/archives/F08-metadata-engine.tar.gz)

## 上一轮完整复验结论

先审生产逻辑，未发现新增实现错误；当时生产文件 SHA-256 与首轮完全相同。本轮发现旧测试全从最小键扫描全部记录，不能保护正式 Scan 的起点/上限。已增强原 Mixed 场景：存在/缺失/删除后起点、最后一键及有限返回条数，由输入模型独立计算 expected；未新增生产接口或另一套微测试。

最终 **6 项 F08 组件、12 项未修改的 A 回归通过；12 个改坏版本全部被指定断言发现**，其中两个新增变异分别忽略起点与上限。所有变异编译成功、指定场景运行并退出 1，编译错误/超时/崩溃不计为检出。正常运行启用 ASan/UBSan，未报告错误。

| 场景 | 结果 | 本次耗时（执行证据，非性能指标） |
| --- | --- | --- |
| `MixedValuesSplitMergeAndReopen` | 通过 | 68.088 s |
| `OldViewsConflictAndSpaceReuse` | 通过 | 36.325 s |
| `RejectedPrivateChangesDoNotLeak` | 通过 | 24.952 s |
| `PublicationWaitsForDurabilityAndFaultsOnUnknownResult` | 通过 | 25.221 s |
| `ProcessExitReplaysConfirmedNonemptyTransactions` | 通过 | 26.811 s |
| `ValidJournalCannotHideWrongMetadataRedoBase` | 通过 | 33.676 s |

原 A 回归为插入 3、删除 2、顺序规模 1、规范化快照 2、状态机 4，共 12 项，源码未修改。构建全库/相关目标及增强测试格式检查本轮执行；生产源码未变，原 cpplint/clang-tidy/GCC 证据按哈希沿用，放在 `prior-production-checks/`，不谎称本轮重新执行。

## 范围与版本

真实路径：B→F07→F06→F04→F02→F01 Direct 文件；Flush 暂停/EIO 仅测试侧注入。S4 页全驻留，不含最终 Metadata 页写回、checkpoint、日志复用或业务接入；进程退出不证明物理掉电安全。共同 C/P 未重跑，无新性能结论。

基准 `015f95bf91cb7039d35e9163263f80b61a5d6dd5` 加本轮生产修改，当前清理版哈希见 MANIFEST.json 的 production_sha256，`source/` 保存当前变更；保存的运行对应 runtime_evidence_production_sha256，唯一差异原文件在 `runtime-source/`；基准本身没有 F08。按源码包 RESTORE.md 在隔离目录恢复，不覆盖当前工作树。当前最终命令：

```bash
python3 /tmp/bustub-f08-review-20260925/tests/run_stage.py --repo /home/qwzx/projects/bustub-20241207-2024fall --work /tmp/bustub-f08-review-20260925 --mutations
```

命令/退出码见 commands.json，完整日志在 logs/，XML 在 xml/，变异结果见 mutations.json；输入确定生成，runner 清理 GTEST_* 干扰并核对实际用例，不能用零用例退出 0 冒充通过。

## 归档与清理

- 最终源码包 11,797 字节，SHA-256 `f4441d2ed561ec10006489ed7944e40519b0e987e5ad24a65eddfb6b943d7b84`。
- 本地结果包 75,635 字节，SHA-256 `98f73799a5491fcfdefb494dc48038eefb2cffc063593142ea18effaa3bb7402`，按 [SHA256SUMS](SHA256SUMS) 核对。
- 两包已原位替换，旧 F08 源码/结果包不留备份。测试内容只压缩保存，未注册常驻目标。旧 F01 问题包仍不存在，其他有效包及共同 C/P 不变。
- 包内逐成员比对完成；不含二进制、设备镜像、构建缓存、过时 runner 或修正前测试。临时工作目录最终清理见 [核对记录](cleanup-verification.json)。结果包由既有 .gitignore 忽略，clone 不含本机结果证据；源码包随仓库保存。

后续仍由 S5/F09/F10/F11 补最终页、checkpoint 首改 FULL、裁剪与在途 IO/代次复用，S8/S9 接业务 E2E。本轮六项合并增强，不把变异数量等同于全系统覆盖。

## 2026-09-26 最后一次静态复核

本次先检查生产逻辑和完整归档，未发现新增行为错误。清理 MetadataPager::ReadGuard 未使用的默认构造和 Drop；写守卫对应成员在树分裂/合并中有实际调用，保留。逐字节对照确认只有这两项删除。

Clang/GCC 对当前 metadata_engine.cpp 的模板实例化语法检查及 clang-format 检查通过，命令/输出在 cleanup-review/。测试源码和 runner 字节未改；上面的 6/12/12 为上一轮已保存实测，本次没有重跑或新增测试。runtime-source/ 仅保留那次运行的原生产差异文件作可复现证据，不包含旧测试或旧压缩包。

已补全方案遗留的验收/清理/兼容/参考落点占位，逐项核对测试设计及结果 XML/变异编译、运行退出码。归档原位更新，无备份，其他有效模块包不变；最终清理见 cleanup-verification.json。

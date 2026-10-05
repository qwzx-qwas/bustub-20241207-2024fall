# S10 / F27 验证结果（2026-10-05）

**再次复审：56 个不同正常场景通过，8 个隔离变异检出。**

- S10 共12（本次增加前台读错误与合计预算2项）；已有 S7/Common10、S5循环/兼容8、F36＋其内含F26共18、S8 Store/TCP8。内含F26未重复运行/计数。
- Clang14 Debug、ASan/UBSan/LSan；正式测试无 sanitizer 报错。没有运行 TSan、真实裸设备/断电或共同性能比较。
- 使用真实生产 NodeStorage/B/Journal/ObjectIO/F02/F01 和 Direct 临时文件，测试侧暂停/EIO注入；新方案目前是**新分配目标 Deferred**，原地覆盖未启用。
- [完整方案与八项测试设计审查](../../docs/storage_redesign/s10_execution_20261005.md)。源码：[F27-deferred.tar.gz](../../test/archives/F27-deferred.tar.gz)。

## 保存内容

本目录只保留本说明、SHA256SUMS、`F27-results.tar.gz`。结果包包含最终 XML、分组命令、构建/运行日志、8个变异的实际失败、最终源码包、生产改动源快照/diff、方案和审查。`commands.json` 是最后12项及8个变异，`regression-commands.json` 是同一生产源码的44项相邻回归。最终仅调整F27 fixture/runner后重跑F27，没有虚增计数或重复运行其余套件。

`diagnostics/` 保留本次缺少头文件前置声明的编译日志、恢复指引缺少gtest_main的链接失败、旧变异点失效的日志；这些不计通过。详细修正及测试判断依据见执行审查。最后正常测试和变异均有完整 XML，非凭退出码猜测覆盖数。

旧 F27 源码包和结果包已原位替换，不保留旧包备份；S5/F19/F26/F36/S8 有效依赖不变。构建、镜像、二进制、解压测试及临时变异源码在归档核验后清理，测试源码只留模块包。

## 校验与恢复

```bash
(cd test-results/storage-f27-s10-20261005 && sha256sum -c SHA256SUMS)
(cd test/archives && sha256sum -c SHA256SUMS)
stage_result=$(mktemp -d /tmp/bustub-f27-result-XXXXXX)
tar -xzf test-results/storage-f27-s10-20261005/F27-results.tar.gz -C "$stage_result"
```

源码包 RESTORE.md 给出完整构建/运行命令；需与 MANIFEST 的生产哈希一致。结果包按项目既有规则在本地保存，不包含测试设备镜像或构建缓存。后续共同 E2E 接管这些风险时保持业务 oracle/故障目标，避免复制阶段细节测试。

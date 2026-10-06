# S11 / F28 本地业务 checkpoint 验证（2026-10-06）

第三次复审：仅修正归档 runner 的 `--case` 参数约束并验证错误命令返回2、不创建产物。生产/C++场景/变异未变，本次未重跑生产测试；下列实测数字仍归第二次复审。最新包记录各轮哈希与命令，不把重新归档称为重新测试。

**第二次复审重跑 16 项全部通过，2 个指定隔离变异检出（其中 1 个新增）。** 本次替换 T4，生产代码未变。首轮63项/5个变异、第一次复审16项/2个新变异分别保留原记录；其余48项本次未重跑，当前目录共64个不同场景。

- 当前 F28 8 项＋S8 8 项为本次复验。F27 12、F19 10、S5 8、F36 18（包含 F26 9）共48项沿用首轮，内含测试不重复计数。
- Clang14 Debug、ASan/UBSan/LSan；真实 SQL、Direct 普通文件、已有本机 TCP 三节点。不是跨机、真实掉电或性能比较。
- [方案](../../docs/storage_redesign/s11_business_checkpoint.md)、[实际改动和八项测试设计审查](../../docs/storage_redesign/s11_execution_20261006.md)。
- 唯一阶段源码：[F28-business-checkpoint.tar.gz](../../test/archives/F28-business-checkpoint.tar.gz)。没有新增常驻测试。

## 保存内容

本目录只保留 README、SHA256SUMS 和 `F28-results.tar.gz`。压缩包包括：

- 首轮63项、第一次复审16项、本次16项各自的命令、日志、XML和汇总；
- 首轮五个、第一次复审两个、本次两个指定变异的真实失败、命令和差异；本次一个是已有变异复验，共8种不同变异；
- 测试源码包、生产修改源快照及相对 e4d8703 的 diff、方案与审查；
- 环境限制、错误测试配置及隔离编译缺 include 的诊断日志，它们不计为通过；
- 包内文件 SHA256 校验清单。

上一轮 F28 源码包和结果包在本次复审后原位替换，不保留旧包备份。只保留当前最终测试源码；此前运行日志按历史证据保留，不能当作本次重跑。其他模块有效归档仍是本轮复用依赖。构建库、可执行文件、设备镜像、变异副本和解压源在验证归档后清理，不收入结果包。结果包沿用项目既有规则在本地保留、由 .gitignore 排除；源码包及说明可随仓库保存。

## 校验/恢复

```bash
(cd test-results/storage-f28-20261006 && sha256sum -c SHA256SUMS)
(cd test/archives && sha256sum -c SHA256SUMS)
result_work=$(mktemp -d /tmp/bustub-f28-result-XXXXXX)
tar -xzf test-results/storage-f28-20261006/F28-results.tar.gz -C "$result_work"
(cd "$result_work/F28-evidence" && sha256sum -c SHA256SUMS)
```

完整构建/运行方法见源码包 RESTORE.md；MANIFEST 核对生产源和依赖，不能用旧生产库运行新测试冒充复验。未来共同 C/P 接管本轮风险时保持业务预期与故障范围，精简阶段层测试。

## 两次复审的具体变化

第一次复审修正 `Recover` 的校验顺序：先只读核对本地 index/term，再允许 HardState/日志基点/保留快照变化。新增 T8 并加强 T2 的开始信号，没有生产 API、hook 或默认配置变化。恢复顺序变异实际把原快照2清理成仅余快照3，T8发现；Drain变异实际提前返回，T2发现。

第二次复审没有新增生产修改。旧 T4 自己保存捕获内容，可能漏掉 worker 漏写脏页的错误；现已用80条不同 SQL 记录、真实 checkpoint worker、捕获后的 Apply、重启后旧值及日志接续后的新值验证替换，删除手工保存路径。两个指定变异分别跳过捕获正文与 worker 保存，两者均在真实恢复中报页链成环，除了闸门未命中外还有独立的恢复失败依据。没有把编译失败或 watchdog 超时算作检出。

完整发布故障点尚未穷举；真实掉电、裸设备、TSan、性能仍不在本轮证据范围。

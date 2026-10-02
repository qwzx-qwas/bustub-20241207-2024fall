# F12 分配器验证（2026-09-30）

[方案](../../docs/storage_redesign/modules/allocator.md) · [执行与八项审查](../../docs/storage_redesign/f12_execution_20260930.md) · [最终测试源](../../test/archives/F12-data-allocator.tar.gz)

- 六项真实 F12/B/Journal/Direct 文件集成通过。
- 直接复用原 F34 五项、S5 八项回归，全部通过。
- 十个故意改坏版本均被对应断言检出；不把编译错误、崩溃或超时计为通过。
- Clang 14 Debug/ASan/UBSan；GCC 语法/警告、clang-format、项目配置 cpplint 通过。
- 未测 F13/F14 自动回收、NodeStorage 对象接入、SQL/Raft 新后端 E2E、裸设备、真实掉电或性能改善。

只保留最终 `F12-results.tar.gz`、本说明、SHA256SUMS 和 cleanup-verification.json。结果压缩包沿用已有 .gitignore，仅在本地保存；Git 携带说明及校验信息，需要同哈希结果包才能复验原始证据。

## 恢复

1. 校验本目录和 test/archives/SHA256SUMS。
2. 在隔离目录检出 `3e852f5`，覆盖结果包 source/ 的五份生产文件；核对源码包 MANIFEST 的生产与 S5/F34 依赖哈希。
3. 在仓库外解压 F12 源码包，依 RESTORE 执行 runner；不要将阶段小测试恢复成常驻目标。
4. 独立核对 allocator.xml 六项、lifecycle.xml 五项、s5.xml 八项，以及 mutations.json 的十个指定判据。

结果包含最终命令、日志/XML、变异内容、清理前的最终源码快照、测试源压缩包与原 S5 旧格式镜像。没有 build 或二进制；中间失败版本和展开测试均已清理。旧有效 S5/F34 归档保持原哈希，不因 F12 使用而删除。

复审修正：碎片场景不再拒绝夹具能支持的合法 16 KiB 分配粒度；隔离场景补足旧 snapshot 拒绝后的空间回滚检查。仍为六项，原回归未改，生产哈希与首轮相同；全部重新执行，旧源码和结果包不保留。详见执行记录 §9。

2026-10-01：再次核验源码/依赖/成员哈希、十九项 XML 和十个变异的指定断言，全部对应；本次没有修改代码/测试、替换压缩包或重新运行测试。最新审查见执行记录 §10，结果包内 review.md 保留 2026-09-30 实测时的文档快照。

2026-10-02 提交前再次复核：当前生产/测试与最终归档一致，十九项 XML、十个变异断言及旧包清理均核验通过；没有新运行、代码/测试变更或压缩包替换。最新审查见执行记录 §11，Git 仅提交源码包和结果说明/校验记录，结果压缩包沿用既有忽略规则。

# F19 / S7 Common 对象事务：最终结果

- **56 项通过，无跳过**：新事务路径10、原 F02/S6 15、F34 5、F12/F13/F14 18、S5 8。
- **12 个定向变异**均编译成功，由指定断言发现；崩溃、超时、编译失败不计命中。
- Clang14 ASan/UBSan/LeakSanitizer 开启；生产 clang-format、仓库参数 cpplint、clang-tidy、GCC C++17 Werror 检查通过。本次复审的最终生产/测试源码与完整运行哈希一致。
- 正式 NodeStorage→S7→ObjectIO→F13/F14/F12→F04/F02/F01，元数据走 B/Journal，Linux Direct 文件；暂停/EIO 仅测试链接注入。没有 production 测试 hook。

[方案](../../docs/storage_redesign/s7_common_pipeline.md) · [八项设计审查](../../docs/storage_redesign/s7_execution_20261002.md) · [源码归档](../../test/archives/F19-common-pipeline.tar.gz) · [校验](SHA256SUMS) · [清理](cleanup-verification.json)

本轮 Common/COW 范围完成。原分配尾部复用未启用：F01 对齐信息不能证明掉电隔离；小追加继续使用 COW。不是 SQL/Raft E2E、原始块设备、真实掉电或性能测量；共同 C/P 未改、未重跑。

本地 `F19-results.tar.gz` 保存最终命令/日志/XML、变异源码和判据、生产及构建源码快照、测试源码、方案与审查；`SHA256.json` 提供逐成员校验。源码包 MANIFEST 绑定基准 `e24b5e65d27526a757ff154c14d0552d8a7b9ade`、生产/测试/六个回归依赖档案的哈希。恢复按包内 RESTORE，在隔离目录进行；依赖的历史提交也要可读。

F19 的旧源码/结果包均已原位替换，只保存最终复审版，无旧包或备份。原有效 F02/F12/F13/F14/F34/S5 包继续作回归来源。归档核验后删除专用 `/tmp/bustub-s7-review-20261002` 的构建、设备镜像、展开测试和变异二进制；结果包由现有 `.gitignore` 忽略。Git 保留小型源码包、说明、校验和、清理记录；单独 clone 不会获得本地结果包。

## 本次复审的修正

生产：区分暂时占用和请求自身超限，后者结束为 NotCommitted，避免无限 pending；F12 预留票据暂满单列 Busy，IO 静态容量由私有适配核对。测试：增加永久超限的组合场景，扩展原预留压力与 Journal Flush 故障场景；NotCommitted 必须恢复旧状态，只有 Indeterminate 允许两种完整状态。没有 production 测试钩子。详见审查 §9，最终 56 项、12 个变异结果重新生成，不沿用旧结果。

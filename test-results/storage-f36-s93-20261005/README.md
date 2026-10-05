# F36 / S9.3 当前测试与结果

2026-10-05，基准 `991ced7` 加本轮 source 快照；[实施与八项测试设计审查](../../docs/storage_redesign/s93_execution_20261005.md)。

- **66 项正常测试通过**：F36 九项、复用 F26 九项、课程二十项、向量索引二十项、S8 八项。
- **19 个指定变异检出**：18 个指定单用例断言失败；忽略 pin 的变异先触发映射提前释放断言，再因生产 double-unpin 不变量终止。编译失败/超时/检测器初始化失败不算检出。
- 最终生产与测试内容先逻辑审查再验证。Clang 14 / Debug / ASan / UBSan / LeakSanitizer，非 PIE 独立构建。没有改变生产默认路径、对齐要求或为测试添加 hook。
- 新场景验证 Direct 文件上真实 TableHeap→对象 FS 的释放和物理复用、读者/预取保护、丢失 durable 应答后的重开接续。原 270 行周转改用完整业务行比较。
- 共同 C/P 未改、未重跑；不是 TSan、裸设备、真实掉电或性能提升证据。S11–S13 仍待实施。

## 保留内容与复现

- 源码：[F36-table-space.tar.gz](../../test/archives/F36-table-space.tar.gz)，只有当前压缩版；含九项用例、两个 runner、MANIFEST、RESTORE。
- 本地结果：`F36-results.tar.gz`，继续由既有 `.gitignore` 忽略。普通 git clone 只带源码包、本说明和校验/清理记录，不带本地结果大包。
- 结果包含 `runs/` 最终日志/XML/命令/变异证据，`source/` 执行时的生产/方案快照，以及 `MANIFEST.json`、`review-summary.json` 和最终构建日志。不含设备镜像、可执行文件、目标文件或构建树。
- `SHA256SUMS` 在仓库根执行 `sha256sum -c test-results/storage-f36-s93-20261005/SHA256SUMS`。逐文件生产/依赖/测试哈希在源码包 MANIFEST 内。按包内 RESTORE 在独立目录构建，允许本机 TCP 和 LeakSanitizer 正常运行。
- S9.2 源码包被原位替换，旧结果包、旧校验和清理清单删除；旧 README 仅保留历史跳转，不指向失效压缩包。S5/F26/S8 仍为有效依赖，保留。

本轮未创建 commit。清理记录是执行时事实，不用于推断后续 Git 历史。

2026-10-05 再次审查：生产/测试与归档逐文件哈希一致，核对原始 XML 和变异断言；未改生产或测试、未重跑。仅修正四处方案进度冲突并补充[复审记录](../../docs/storage_redesign/s93_execution_20261005.md#10-再次审查代码测试判据和状态一致性2026-10-05)。压缩包保持原执行证据，后续文档修订不代表新的测试执行。

# F02 / 有界 IO 执行器运行证据

2026-09-22。[当轮方案](../../docs/storage_redesign/modules/object-io.md#65-s1-当轮执行范围用户授权后落定) · [八项审查与结果](../../docs/storage_redesign/f02_execution_20260922.md) · [测试源码归档](../../test/archives/README.md)。

- 最终 6 项阶段验证全部通过，退出码 0，链接真实 CMake `bustub_storage_disk` 对象；Clang 14，ASan/UBSan/LeakSanitizer 全开启。
- 5 个隔离变异均由对应测试断言捕获、退出码 1：漏 Flush、忽略存活预算、忽略成员失败、刷新失败仍标成功、把成功读取错误报告为失败。不是将编译失败/超时/检测器故障计为命中。
- clang-format、仓库参数 cpplint、clang-tidy 通过；GCC C++17 `-Wall -Wextra -Werror -fsyntax-only` 通过。依赖告警被工具过滤不等于全仓库零告警。
- 实际文件 Direct：内存 4 字节、偏移/长度 512 字节，每例 131072 字节；最多两个 IO worker。正常行为是真实 IO，暂停/短写/EIO/分配失败明确为测试侧注入。
- 原 F01 实现、旧正式业务路径及共同 C/P 内容/发现入口未变。未重跑共同性能基线，未验证对象映射、SQL/Raft 接入、裸设备或物理掉电。

本地 `F02-results.tar.gz` 保存构建/运行命令、最终 XML/日志、检测器环境、静态检查日志、5 个变异及命中记录、兼容核对、最终生产与测试源码快照/哈希、方案审查。基准 `2f06f6f42548ec49c53040315f8410b7aea48a27` 必须结合当轮源码快照才是被测版本。

结果包沿用现有规则仅在本地保存；Git 保存本入口、[SHA256SUMS](SHA256SUMS) 和 [清理核对](cleanup-verification.json)，小型测试源码包另行纳入 Git。两个包均逐文件核验后清理专用临时目录，不保留散落阶段测试、构建、镜像或变异二进制。

```sh
cd test-results/storage-f02-20260922
sha256sum -c SHA256SUMS
```

收尾审查补齐了读取成功状态/完整进度的正向 oracle，最终包只保存修正版测试。原 6 个场景保持不变，生产源码未变；复验 6/6 通过，5 个变异由指定断言发现。此前存在断言缺口的源码/结果包已被替换，不另存备份。

再次审查修正 runner 可能受外部 GoogleTest 设置影响、将零项误报为六项通过的问题。原次完整 XML 有六项成功记录，原结果仍有效；本次在过滤为零项/重复零次/启用分片的外部环境下，修正版隔离这些设置并核验完整 XML，6/6 通过、5 个变异命中。`review-evidence/` 记录误报复现及工具自检，不能当作额外组件覆盖。源码/结果包再次直接替换，不保留错误 runner 的旧包；生产和六个 C++ 用例未变。详见[复审结论](../../docs/storage_redesign/f02_execution_20260922.md#10-再次代码与测试审查2026-09-22)。

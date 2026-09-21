# S0 / F00 验证归档

[执行与审查记录](../../docs/storage_redesign/s0_execution_20260921.md) · [模块方案](../../docs/storage_redesign/modules/storage-contracts.md)

- 最终结果：新增 5 项、复用既有 5 项，Debug + ASan/UBSan/LeakSanitizer 下 **10/10 通过**。
- 归档：`results.tar.gz`；校验：`SHA256SUMS`。
- 代码：Git 基准 `52e11ccf552ff527302aab3464038134da0e4e17` 加已有及本轮工作区改动；本轮未提交。压缩包保存实际被测源代码及哈希，不能仅凭 Git 基准复现。
- 失败证据：最初 PIE 检测器启动段错误、沙箱 LeakSanitizer 限制、独立空程序复现和 gdb 栈均保留。最终临时构建使用 `-no-pie`，保留所有检测器，项目默认构建配置未改。
- 范围：字节范围和现有快照/业务恢复定向验证；不是三节点性能、设备掉电或完整 FS 的验收。

## 内容

| 压缩包路径 | 内容 |
| --- | --- |
| `evidence/ctest-nopie.log`、`evidence/results-nopie.xml` | 最终完整约定集合的文本及 JUnit 结果 |
| `evidence/` 其他文件 | 所有配置/构建尝试、检测器故障、静态检查、范围审查、源代码哈希和审查记录 |
| `snapshot/src/`、`snapshot/test/` | 实际生产与测试源代码快照，包含执行前已有改动 |
| `snapshot/build_support/` 及根配置文件 | 构建支持、CMake/格式配置 |
| `snapshot/docs/` | 本轮方案、执行记录及相关设计资料 |
| `before/`、`before.json`、`before-status.txt` | 本轮修改前的目标文件副本、检查范围的哈希和已有工作区状态 |
| `diagnostics/asan_probe.cpp` | 用于复现检测器初始化故障的空程序源码 |

使用 `sha256sum -c SHA256SUMS` 校验后，可解包到独立目录查看。复现构建应从上述基准 checkout 开始，以快照替换对应 `src`、`test`、`build_support` 目录，避免残留基准中的旧测试；第三方目录来自原仓库，不包含在此包。实际命令与环境限制见执行记录。

构建二进制及本轮 `/tmp` 中间目录在归档核验后删除；清理结果见 `cleanup-verification.json`。测试源码、压缩证据和此前基线归档保留。

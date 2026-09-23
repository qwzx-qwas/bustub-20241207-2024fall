# F04 / 固定区域寻址运行证据

2026-09-23。[方案](../../docs/storage_redesign/modules/region-manager.md) · [执行与八项测试审查](../../docs/storage_redesign/f04_execution_20260923.md) · [测试源码归档](../../test/archives/README.md)。

- F04 三项真实组件测试 3/3，通过完整 XML 名称/run/completed/无跳过核验。
- 原 F03 八项从有效源码包原样恢复，回归 8/8；不复制为 F04 新套件。
- 6/6 隔离变异成功编译并由预期断言发现；超时/编译失败/检测器错误不算命中。
- Clang14、ASan/UBSan/LeakSanitizer 开启；clang-format、仓库参数 cpplint、GCC C++17 Werror、clang-tidy 通过。
- 正式 F03 → F04 → F02 → F01 → Linux Direct 文件；两种 16 MiB 布局，IO 对齐实际查询。EIO/Flush 故障在测试链接侧注入；独立全镜像和正文为 oracle。
- 未测裸设备、物理掉电、完整节点/SQL/Raft E2E、TSan 或性能比较；未改共同 C/P 及历史结果。

`F04-results.tar.gz` 保存最终生产/测试快照、命令/环境、XML/日志、6 个变异及审查；基准 `f2dda5b` 加本轮改动，具体字节以 manifest 为准。源码包仅含 F04 三项及 runner；F03 依赖现有 `test/archives/F03-bootstrap.tar.gz`，其 SHA-256 在 runner/manifest 中固定。

阶段测试不注册常驻 CTest，源码和产物仅压缩保留。清理见 [cleanup-verification.json](cleanup-verification.json)。复审已修正测试提前退出时的回调寿命，以及绑定和非对齐判据受范围冲突干扰的问题，并重新运行 3 项 F04、8 项 F03 和 6 个变异；两份 F04 包原位替换，不保留问题旧版或嵌套旧包；F00/F01/F02/F03/T0 有效历史源码包不变，旧错误 F01 结果包仍不存在。

现有 .gitignore 忽略结果压缩包，Git 只保存本目录说明、[SHA256SUMS](SHA256SUMS) 与清理记录；仅 clone 不会得到本地结果原包。小型源码包单独保存在 test/archives。

```sh
cd test-results/storage-f04-20260923
sha256sum -c SHA256SUMS
```

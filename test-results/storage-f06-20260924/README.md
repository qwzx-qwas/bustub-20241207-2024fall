# F06/S2 / Journal 段范围后端运行证据

2026-09-24。[方案](../../docs/storage_redesign/modules/journal-backend.md) · [执行与八项审查](../../docs/storage_redesign/f06_execution_20260924.md) · [源码包](../../test/archives/F06-journal-backend.tar.gz)。

- F06 两项真实组件 2/2；原 F04 三项原样回归 3/3；完整 XML 核验。
- 七个隔离变异成功编译并被指定 assertion 发现，退出码均为 1，无检测器错误冒充检出。
- Debug Clang14 + ASan/UBSan/LSan，无检测器报告；clang-format、仓库 cpplint、GCC C++17 Werror、clang-tidy 通过。
- 本机真实 Direct 文件的 memory=4/offset=512，仅为观察值。测试分别使用 3/5 倍实际偏移对齐的段，没有 production 默认大小或 statx 能力模拟。
- 没有修改共同 C/P 内容或性能基线；未测 SQL/Raft E2E、真实 B/BufferPool、S3/S5 Journal 协议、裸设备、物理掉电、TSan 或性能。

F06-results.tar.gz 保存实际生产快照、测试/runner、命令、环境、XML/日志、七份变异源码、静态检查及文档。基准为 5be34fbfcd18abe14714c192509a312f3874f5a4 加本轮变更，manifest 固定文件哈希；归档时未提交。F04 回归源码按原有效包哈希恢复，结果中记录所用字节身份，不嵌套整个旧包。

F06 本轮首次建包，只有最终版本，没有待删的旧 F06 包。其他模块有效压缩包保持不变；旧错误 F01 结果包仍不存在。源码小包随 Git 保存；大型结果包沿已有 .gitignore 规则只保留本地，clone 不包含原始结果证据。

~~~sh
cd test-results/storage-f06-20260924
sha256sum -c SHA256SUMS
~~~

清理与归档内容核对见 [cleanup-verification.json](cleanup-verification.json)。展开测试、设备镜像和编译产物在哈希验证后删除，没有向常驻 test/CMake 添加测试。


## 再次复审（2026-09-24）

[复审记录](../../docs/storage_redesign/f06_execution_20260924.md#九2026-09-24-再次复审方案代码与归档证据)：未发现 S2 生产或测试缺陷。当前 17 个生产/构建文件与归档一致，91 个结果文件、2+3 项 XML 及七个变异的断言证据已重新核验。本轮未重跑测试或重打包；压缩包保留执行时文档快照，当前仓库文档追加本次审查，二者版本边界明确。

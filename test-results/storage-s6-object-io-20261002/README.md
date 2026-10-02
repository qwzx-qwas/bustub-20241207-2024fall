# F02 / S6 普通对象 IO：最终结果

- **46项通过**：新增对象路径7、原F02 8、F34 5、F14/F13/F12 18、S5 8；无跳过。
- **7个定向变异**均编译成功并在指定断言失败；异常退出不计命中。
- Clang14、ASan/UBSan/LeakSanitizer；生产 clang-format、仓库参数 cpplint、clang-tidy、GCC C++17 Werror 检查通过。依赖告警不等于全仓库零告警。
- 正式 NodeStorage→ObjectIO→F13/F14/F12→F04/F02/F01，Linux Direct 文件；故障门/EIO仅在测试链接侧注入。非 SQL/Raft E2E、裸设备、真实掉电或性能测量。

[方案与八项审查](../../docs/storage_redesign/s6_object_io_execution_20261002.md) · [源码归档](../../test/archives/F02-object-io.tar.gz) · [校验清单](SHA256SUMS) · [清理记录](cleanup-verification.json)

本地 `F02-results.tar.gz` 包含最终日志/XML/命令、七个变异、442个生产/构建文件快照、最终测试源码及测试审查。`SHA256.json`逐成员校验；源码包MANIFEST绑定生产及五个未修改依赖档案。恢复使用包内RESTORE，基准 a739a5c 加结果 source/ 中的准确快照。

旧 F02 源码包已原位替换，`storage-f02-20260922/F02-results.tar.gz` 已删除，不另存备份；其他有效回归档案保留。阶段源码不进入默认测试发现，运行镜像/构建/二进制只在专用临时目录，归档核验后删除。Git保存说明、校验和及小型测试源码包；结果包按现有.gitignore本地保留。

再次复审：修正测试 helper 的失败传播及故障门/future 的清理顺序，重新构建复验仍为46项/7个变异；生产未改。结果包 `review-unwind/` 另存两次有意异常的收尾诊断，不计为生产测试或变异。原静态检查证据经源码哈希核对沿用。见审查 §10。

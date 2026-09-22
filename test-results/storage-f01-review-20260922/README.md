# F01 提交前复审与复验证据

2026-09-22。[八项复审与问题修正](../../docs/storage_redesign/f01_execution_20260922.md#10-提交前代码与测试复审2026-09-22) · [当前阶段测试源码](../../test/archives/README.md) · [首轮结果](../storage-f01-20260922/README.md)。

- 修正测试自身的异常路径：启动 async 任务前为 future 容器预留四个位置，避免扩容失败时等待尚未释放的启动门。未改变生产接口、测试输入、oracle 或用例数量。
- 重新构建真实 CMake 生产对象及阶段验证程序，7/7 通过，进程退出码 0；Clang 14、ASan/UBSan/LeakSanitizer 开启。沿用已确认可运行泄漏检测的沙箱外环境。
- 生产文件与首轮被测快照逐字节相同；核验首轮 6 个 mutation 的源码哈希、对应断言失败和退出码。本次未重跑 mutation，也没有重跑共同 C/P。
- Linux WSL2 真实文件 Direct，实际内存对齐 4 字节，偏移/长度 512 字节；不代表其他后端的固定数值。裸设备、物理掉电、SQL/Raft 接入和性能改善仍未验证，F02 未执行。

本地压缩结果为 `F01-review-results.tar.gz`；哈希见 [SHA256SUMS](SHA256SUMS)。包内保存构建/执行命令、日志、XML、检测器配置与退出码、当前测试及生产源码快照和哈希、审查记录。结果包按已有规则仅在本地保存；Git 保留本入口、哈希、清理记录，小型测试源码包另行随 Git 保存。

包含修正前测试的首轮结果包现已按用户要求删除，首轮 6 个 mutation 只保留历史核验摘要。本复验包中的测试已经修正，继续保留；包内文档/manifest 是复验时的快照，其中涉及旧包保留状态的描述以本页更新为准。临时构建、二进制及设备文件的清理见 [cleanup-verification.json](cleanup-verification.json)。

```sh
cd test-results/storage-f01-review-20260922
sha256sum -c SHA256SUMS
```

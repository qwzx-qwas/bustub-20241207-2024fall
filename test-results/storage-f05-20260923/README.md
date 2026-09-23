# F05/S2 / 元数据页后端运行证据

2026-09-23。[方案](../../docs/storage_redesign/modules/metadata-backend.md) · [执行与八项审查](../../docs/storage_redesign/f05_execution_20260923.md) · [阶段源码包](../../test/archives/F05-metadata-backend.tar.gz)。

- F05 两项正式组件场景 2/2；原 F04 三项原样回归 3/3；XML 核对完整名称、run/completed、无跳过/失败。
- 六个隔离变异成功编译、各自被指定断言捕获；不能把超时或检测器异常算命中。
- Debug Clang14 + ASan/UBSan/LSan；clang-format、仓库参数 cpplint、GCC C++17 Werror、clang-tidy 通过。
- 真实 F03/F04/F05/F02/F01 Direct 文件；不同非零内容、不同起点、首/非连续/末页与独立全文件 oracle。实际 memory=4/offset=512 仅为本机观察；8192 对齐为测试侧能力注入，不是另一台实测设备。
- 未测 B WAL/分配/恢复、真实帧、SQL/Raft E2E、裸设备、真实掉电、TSan、性能；未改共同 C/P 内容和基线。

`F05-results.tar.gz` 保存本轮生产/测试/文档快照、命令/环境、XML/日志、六份隔离变异和静态检查。基准 `69ed1dd` 加本轮变更，具体字节按 manifest 固定。原 F04 回归的源码依赖当前有效 `test/archives/F04-region-manager.tar.gz`，包内记录其哈希和实际解出的源哈希，不重复嵌套整包。

源代码与结果分别压缩，无展开测试、镜像、构建/二进制或错误旧 F05 包保留；原有效模块包不变，旧错误 F01 结果包仍不存在。清理见 [cleanup-verification.json](cleanup-verification.json)。现有 .gitignore 忽略结果 tar.gz，Git 只保存说明/校验/清理记录；源码小包随 Git 保存。仅 clone 不包含本机结果原包。

```sh
cd test-results/storage-f05-20260923
sha256sum -c SHA256SUMS
```

## 本次复审替换

生产与两项 C++ 测试未变。runner 将依赖部分尾页的向上取整变异换为容量多报一页；2/2 F05、3/3 原 F04、6/6 变异重新运行。`geometry-review/` 另存测试侧 4096 对齐实验：正确实现与旧等价变异通过、新容量错误被断言发现；这不是另一台实测设备，也不新增场景计数。前次生产静态检查按相同生产哈希沿用，实际未重跑。源码/结果两包原位替换，不保留旧版本或嵌套备份，校验值以当前 SHA256SUMS 为准。

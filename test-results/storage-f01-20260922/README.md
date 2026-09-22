# F01 / BlockDevice 运行证据

2026-09-22。[方案及八项审查](../../docs/storage_redesign/f01_execution_20260922.md) · [阶段测试源码](../../test/archives/README.md)。

- 7 项阶段测试通过，最终退出码 0；6 个隔离变异均由对应测试断言发现。
- Clang 14，ASan/UBSan/LeakSanitizer；真实 CMake 生产对象。首次沙箱泄漏检测受 ptrace 限制的失败另行保存，不计为通过。
- Linux WSL2 文件 Direct：内存对齐 4 字节，偏移/长度 512 字节；每例 128 KiB。没有使用裸设备，没有真实掉电，没有 SQL/Raft 新后端接入，也没有重跑性能基线。
- 能力缺失/EINTR/短 IO/EIO/零进展部分场景通过测试侧链接注入，明确区别于真实硬件故障。

本地压缩结果：`F01-results.tar.gz`（54506 字节），SHA-256 见 [SHA256SUMS](SHA256SUMS)。按既有规则不提交结果包，Git 只保留此入口、哈希及清理核对；clone 不会自动获得原始日志。小型测试源码包单独随 Git 保存。

包内：`evidence/run-final.log`、`results-final.xml`、`mutations.json`、命令及构建/静态检查日志、沙箱诊断、环境/兼容核对、生产源码快照，另有测试源码、6 个变异源码和相关方案快照。基准提交为 `d8ad4a0666640849f52f12b96ce6bb852e5c4177`，必须结合源码快照/哈希才是被测版本。

```sh
cd test-results/storage-f01-20260922
sha256sum -c SHA256SUMS
```

已逐文件解压并核对源码包和结果包与打包前内容相同。中间产物清理核对见 [cleanup-verification.json](cleanup-verification.json)。原共同 C/P 及其历史归档保持不变。

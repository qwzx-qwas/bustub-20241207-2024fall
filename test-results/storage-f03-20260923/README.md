# F03 / 固定引导与异步修复运行证据

2026-09-23。[当轮方案](../../docs/storage_redesign/modules/bootstrap.md#64-本轮实施规则2026-09-23已获执行授权) · [执行与八项测试审查](../../docs/storage_redesign/f03_execution_20260923.md) · [测试源码归档](../../test/archives/README.md)。

- 8/8 组件验证通过，完整 GoogleTest XML 名称、执行/完成状态和无跳过校验通过。
- 11/11 隔离变异被指定错误判据捕获，退出码 1；不把编译失败、超时或检测器故障当命中。
- Clang14、ASan/UBSan/LeakSanitizer 开启无报错；clang-format、仓库参数 cpplint、clang-tidy、GCC C++17 Werror 语法检查通过。
- 实际 F03 → F02 → F01 → Linux Direct 文件，8 MiB 每例；设备本机查询为内存对齐 4 / 偏移长度对齐 512 字节，生产没有硬编码这些对齐数值。最多两 IO worker 加一个按需修复协调线程，构建 -j2。
- EIO、暂停及验证读损坏在测试侧系统调用边界注入；独立 oracle 检查整个文件和真实 Flush。未进行裸设备、物理掉电、完整节点恢复或性能基线测量。

本地 `F03-results.tar.gz` 保存最终命令/日志/XML、检测器环境、静态检查、十一个变异源码/结果、生产与测试源码快照及 SHA-256、兼容检查和审查文档。基准 `37fa7f2` 加本轮改动；source/ 和 manifest 才是实际被测代码，不用单独基准提交假装含有 F03。

小型源码包 `test/archives/F03-bootstrap.tar.gz` 与结果包中的测试/runner 字节相同。阶段源码不注册 CTest，不常驻展开文件。逐文件核验归档后清理专用 /tmp 目录，包括中间测试源码、构建、设备镜像、变异二进制和 pycache。见 [清理核对](cleanup-verification.json)。

F03 两包已替换为本次复审后的最终版本，旧包不留备份，也不嵌套保存旧测试。review-evidence 仅保留本次来源损坏问题的失败复现日志及说明。先前删除的错误 F01 包未恢复；F01/F02 有效历史模块包保持不变。共同 C/P/旧业务路径字节核对保存，未重跑或重写旧性能结果。

现有 .gitignore 已忽略结果压缩包。Git 仅保存本目录说明、[SHA256SUMS](SHA256SUMS) 与清理记录，小型测试源码包另保存在 test/archives；仅 clone 不会得到本地结果原包。

```sh
cd test-results/storage-f03-20260923
sha256sum -c SHA256SUMS
```

本次复审实测发现旧生产对写后源损坏多重试一次（两种源位置均 attempts=2）；修正后原八项重新通过。关闭场景补齐引用寿命、合法完成顺序和真正进入等待的判据；用漏唤醒变异验证取消责任，未新增生产 hook。完整结果以执行记录 §10 和当前包的 manifest 为准。

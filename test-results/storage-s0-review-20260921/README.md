# S0 测试提交前复审证据

[逐条复审与范围](../../docs/storage_redesign/s0_test_review_20260921.md) · [首轮记录](../../docs/storage_redesign/s0_execution_20260921.md)

- 修正：极大嵌套长度样本保留正确 CRC；测试侧观察读取范围并委托真实 POSIX IO；正常输入先确认可通过。
- 结果：Debug/ASan/UBSan/LeakSanitizer 下 **10/10 C++ 定向测试通过**；**25/25 Python 测试器契约检查通过**。
- 附加检查：格式、cpplint、定向 clang-tidy、Python/Shell 语法、30 份旧文件迁移核对通过。Go 不在当前环境，本轮模型检查未运行；模型源码未变。
- 范围：定向回归和测试器检查；未重跑三节点共同 E2E 或长期性能，不据此新增掉电/性能保证。
- `results.tar.gz` 保存构建/测试/静态检查日志、JUnit、实际源码/文档快照与哈希；`SHA256SUMS` 是包校验和，`cleanup-verification.json` 记录清理。
- 压缩包由 `.gitignore` 排除，保留本地；Git 保存本说明、哈希和清理记录。仅 clone 仓库无法获得原始包，需另取同哈希证据。

校验与查看：

```sh
sha256sum -c SHA256SUMS
tar -tzf results.tar.gz
```

主要结果在包内 `evidence/ctest.log`、`evidence/results.xml`、`evidence/harness-tests.log`。复现构建命令沿用首轮记录 §3.2；快照基准为 `52e11ccf552ff527302aab3464038134da0e4e17` 加当时工作区，实际源文件哈希在包内。

首轮压缩包保持原样；本轮使用独立目录记录修改后的测试结果。临时构建和生成缓存核验后清理，测试源码与历史压缩包保留。

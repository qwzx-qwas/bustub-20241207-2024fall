# S11 / F25 共享快照验证（2026-10-06）

**2026-10-07 复审：33 项全部通过，9 个指定隔离变异全部检出。** Clang14 Debug、ASan/UBSan/LSan；真实 SQL、Direct 普通文件及本机 TCP 三节点。不是裸设备、真实掉电或性能对比。

- 本轮 6 项、F28 7 项、S8 8 项、F27 12 项。已有测试直接从有效归档引入，不复制为多套 oracle。
- 唯一阶段源码：[F25-shared-snapshot.tar.gz](../../test/archives/F25-shared-snapshot.tar.gz)。MANIFEST 校验源码、全部生产源和依赖；RESTORE 给出重建及运行命令。
- [共同方案](../../docs/storage_redesign/s11_shared_snapshot.md)、[具体改动和八项测试设计审查](../../docs/storage_redesign/s11_shared_snapshot_execution_20261006.md)。

本目录只保留 README、SHA256SUMS、清理核验和 `F25-results.tar.gz`。包内保留最终命令/日志/XML、九个变异、生产修改源及 diff、最终测试源码压缩包、方案/审查、环境与中间失败诊断。前几轮失败或通过不冒充最终复验；没有保留旧测试源码版本。

先前 sandbox 拦截 socket/LSan，换允许本机网络及线程检查的环境后复验，未关闭 sanitizer。发现的 term 校验遗漏、回收隔离过宽、分配单位与页大小不等的布局问题均先分析再修正；对应最终测试和变异已通过。

本轮加强 T1：接收端先保存 index 2 的 checkpoint，再接收 index 5 的新快照，执行后缀后重启并核对恢复选择；新增“错误优先旧 checkpoint”变异。本轮未修改生产代码。当前证据为 `runs/review-run` 和 `runs/review-mutations`；先前运行仅作历史诊断。

F25 源码与结果包均原位替换，无旧版/备份；S8/F28 等有效模块包保留原哈希。测试和构建在仓库外展开，校验归档后删除本轮设备镜像/编译产物/变异副本。结果 tar 沿既有 .gitignore 本地保存，不自动加入 Git。

```bash
(cd test-results/storage-f25-s11-20261006 && sha256sum -c SHA256SUMS)
(cd test/archives && sha256sum -c SHA256SUMS)
result_work=$(mktemp -d /tmp/bustub-f25-results-XXXXXX)
tar xzf test-results/storage-f25-s11-20261006/F25-results.tar.gz -C "$result_work"
(cd "$result_work/F25-evidence" && sha256sum -c SHA256SUMS)
```

正文共享仍有目录处理、校验读取及小头部编码成本；同步 Capture 可能等 checkpoint，接收端需要本地写入和索引重建。未宣称增量/压缩、跨 ABI 可移植、TSan 或共同 C/P 性能基线已完成。

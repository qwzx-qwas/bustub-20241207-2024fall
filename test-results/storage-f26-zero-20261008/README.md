# F26 共享零页接续结果

2026-10-08；基线 `3dc23d3` 加本轮变更。已先审逻辑，再编写/执行测试，最后复审契约、接口和冗余。[共同协议](../../docs/storage_redesign/s12_translation_zero_page.md) · [具体改动与八项测试审查](../../docs/storage_redesign/s12_translation_execution_20261008.md) · [唯一测试源码包](../../test/archives/F26-array-buffer-pool.tar.gz)。

| 范围 | 结果 |
| --- | --- |
| 真实 FrameArena/TranslationDirectory | 6/6，ASan/UBSan/LSan |
| 实际文件页、Direct 对象页、SQL、并发淘汰 | 10/10，ASan/UBSan/LSan |
| 原课程 BufferPool/PageGuard/LRU-K/DiskScheduler | 11/11 |
| 原 S8 对象 Store、SQL 恢复、三节点 TCP | 8/8，ASan/UBSan/LSan |
| 同六项组件的 TSan | 6/6，不增加不同场景数 |
| 当前代码的定向 mutation | 8/8，指定测试断言检出 |

接续复审先检查代码逻辑，修正 PathCache 额度申请 bad_alloc 的可选退路和 CMake 最低版本；资源测试补入故障输入（保持六个组件场景），新增一个有效变异。具体见[审查 §8](../../docs/storage_redesign/s12_translation_execution_20261008.md#8-接续复审可选缓存不能阻断已有映射收尾2026-10-08)。

合计 **35 个不同正常场景**。最新复审没有修改生产代码和 C++ 测试正文；补全回收协议文字，并加强八个变异的失败内容判据。本次重跑六项组件及八个变异；其余 29 项正常场景和六项 TSan 沿用上轮、哈希匹配当前代码的结果，未重复运行。另有八个无关失败反例检查，全部被新判据拒绝，不计入业务场景数量。详见[审查 §9](../../docs/storage_redesign/s12_translation_execution_20261008.md#9-再次复审变异失败必须对应预期风险2026-10-08)。

本机真实翻译叶的私有内存观察：稀疏只读 **0 字节** → 修改一个组 **4096 字节** → 安全归还后 **0 字节** → 再写 **4096 字节**。按实际 OS 页计算，4096 不是生产硬编码。观察包含 swap，避免把换出误作归还；不把根目录/组描述符/内核页表计作这段叶正文。

## 归档

提交前再次核验发现源码包及结果包误带 Python `__pycache__` 字节码，现已删除并更新 SHA256；没有改动生产代码、C++ 场景或原始测试结果，本次未重跑 C++。同时核验八个变异的指定断言及无关错误拒绝规则。跨模块检查和剩余成本见[审查 §10](../../docs/storage_redesign/s12_translation_execution_20261008.md#10-提交前复核跨模块职责与归档清理2026-10-08)。

本目录 `F26-results.tar.gz` 包含最终日志、XML、命令、测试源码、变更源码/补丁与工具链说明；不含二进制、设备镜像、构建缓存或旧错误测试。SHA256 见 [SHA256SUMS](SHA256SUMS)。源码包随 Git，结果包按项目原规则仅本机保存，需要另行备份。

旧 `storage-f26-s9e-20261005/F26-results.tar.gz` 已删除，无备份。F26 源码包原位换代，其他模块依赖包保留。校验见 [清理记录](cleanup-verification.json)。

## 限度

- 新构建要求 CMake 至少 3.20、支持 C++23 显式寿命管理的标准库。本机旧 Clang14 的拒绝结果已核验；使用隔离 GCC/libstdc++ 16，设备构建使用本机 Linux 头以取得 STATX_DIOALIGN。
- 初轮 LSan 被 sandbox 限制、旧交叉 sysroot 缺 Direct IO 查询头，均记作环境失败，修正后复验；没有降低校验、禁用 Direct 或固定对齐。
- TSan 范围是翻译组件；完整页/节点路径采用 ASan/UBSan/LSan。有限并发场景不证明所有交错。
- 未跑共同 C/P 前后性能对比、SS/RS/PL/GT、裸设备或真实断电；不宣称取得 Calico 论文中的性能数字。

本轮临时构建、设备镜像及展开测试已删除。开发工具链单独保留在 `/tmp/bustub-gcc16`，不在 Git 工作树内；使用时仍需按包内说明设置本机 sysroot 与 `native-runtime`。

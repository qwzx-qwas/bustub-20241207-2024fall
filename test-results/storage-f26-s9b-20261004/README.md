# F26 / S9.1b 最终验证结果

2026-10-04；基线 `6879753` 加本轮基础组件。对应 S9.1b 的提交见 Git 历史；恢复时以 MANIFEST 的基线及精确文件哈希为准。

- **5/5** RAM 组件场景通过 ASan/UBSan/LeakSanitizer。
- 同五项通过 ThreadSanitizer，不重复计为十项。
- **11/11** 定向变异由指定测试断言发现，不把编译失败、崩溃或超时算检出。
- 新源 Clang/GCC C++17 严格编译、生产 CMake `bustub_buffer` 目标、格式和 diff 检查通过。

[实施与八项设计审查](../../docs/storage_redesign/s9b_execution_20261004.md) · [共同协议](../../docs/storage_redesign/s9_buffer_pool_protocol.md) · [源码包](../../test/archives/F26-array-buffer-pool.tar.gz) · [校验](SHA256SUMS)

真实路径：FrameArena/TranslationDirectory → Linux 匿名 mmap/madvise。验证非零页号跨区间、独立 owner、并发首次创建、条目修改、对齐/正文独立和失败后资源可继续使用。mmap/madvise 失败与首次创建屏障仅在测试链接侧注入，生产没有测试钩子或默认参数改动。旧 BufferPool、PageGuard、B/FS 生产调用链未切换，故没有重新跑无关 E2E，也没有性能比较。

大页观测：此轮正式 ObserveMemory 报告 4 MiB resident / 4 MiB AnonHugePages。该数值仅作环境报告，不是独立测试 Oracle或大页必须生效的通过门槛；THP 建议与实际占用分开。环境为 Linux 6.18.33.2-microsoft-standard-WSL2 x86_64、基础页 4 KiB。

前次实施的环境失败以 historical 日志保留，不属于本次源码通过证据：第一次沙箱运行断言通过，但 LSan 因 ptrace 限制退出失败；沙箱外完整 runner 复验通过。TSan 起初因 unexpected memory mapping 无法启动；仅对测试进程使用 `setarch x86_64 -R` 后通过，没有修改系统设置。测试初次编译的两条类型/复制警告修正于测试，生产接口未因此改变。

本地 `F26-results.tar.gz` 保存最终测试日志/XML、隔离变异日志、环境失败诊断、命令、源码/方案快照、源码包及逐文件校验。它由现有 .gitignore 忽略；只 clone Git 不会获得结果压缩包，需要另行备份。模块源码包进入 Git，测试不注册常驻目标。

本次复审原位替换 F26 包，只有最终一份源码包和一份结果包，无旧 F26 压缩包或备份；其他模块有效归档保留。临时构建、展开阶段测试、变异代码及二进制在归档校验后删除。下一阶段 c 负责真实页生命周期与对象 IO；d 才实现路径缓存/运行期 RAM 归还。

## 前次修正：普通内存回退与测试判断

- 生产：Region 增加共用的基础页策略处理，FrameArena/翻译叶接入。用 Linux 文档规定的零长度能力探测区分 advice 不支持与实际区域错误；仅前者继续普通内存路径。没有新增公开 API 或测试 hook。
- 测试：T1/T4 增加对应输入与正文/映射校验，仍为五项；mincore 改为至多一页，允许内核换出；runner 检查完整名称、无 skip，变异必须在指定场景断言失败。新增两种改坏，共 11 个检出。
- 当前 ASan/UBSan/LSan 与 TSan 全部重新执行通过；旧业务 BufferPool 未切换，未重跑无关 E2E。没有实际启动无 THP 内核，兼容分支证据是测试链接侧模拟。
- 主方案、F26 子方案和共同协议同步，修正“实现验证尚未交付”的过宽措辞，明确 b 已完成、c–e 待接续。小测试继续仅保存在模块压缩包。

## 本轮再次复核

[审查 §10](../../docs/storage_redesign/s9b_execution_20261004.md#10-再次复核实现边界并发寿命与归档证据2026-10-04) 记录并发首次创建、预算、访问寿命、测试去重及阶段交接的复查。未改生产代码、测试或 runner；只澄清子方案中 Header/pin 尚待 c 接入的状态。

核对源码包 7 项生产/构建依赖、3 项测试/恢复文件哈希与当前树、结果包快照一致；复核现有五项 ASan/UBSan/LSan 和 TSan XML、11 项变异 XML。没有重新执行无变化的功能套件，不将历史通过数标为本轮新运行。源码包保持原字节；结果包仅更新文档和审计记录，原位替换且不保留旧包。

## 提交前核对

共同协议及 F26 父/子方案四处变异计数由九统一为十一，详见执行记录 §11。生产和测试代码未因这次核对修改；没有重复执行功能测试。结果包仅同步文档/审计记录，源码包未变。下一步为 S9.1c，等用户确认后实施。

# F26 / S9.1d 当前验证结果

2026-10-04；基线 `bd554af` 加本轮 3 项生产文件修改。再次复审只修改 `buffer_pool_manager.cpp` 的 Flush 异常范围；精确版本见源码包 MANIFEST 和结果包 source/。

[共同协议](../../docs/storage_redesign/s9_buffer_pool_protocol.md) · [实施/八项审查及本轮修复](../../docs/storage_redesign/s9d_execution_20261004.md) · [源码包](../../test/archives/F26-array-buffer-pool.tar.gz) · [校验](SHA256SUMS)。

**后续复核说明：** 再次核对代码逻辑、方案、源码及依赖校验、结果 XML 和变异退出记录，未发现新的需修复问题。本次只更新复核记录与归档说明，生产及测试源码未改，也未重跑测试；下文“本轮重跑”均指之前 Flush 修复后的执行。详见 [复核记录 §10](../../docs/storage_redesign/s9d_execution_20261004.md#10-后续复核代码方案和归档对应关系)。

## 当前结果与本轮执行量

| 范围 | 结果与来源 |
| --- | --- |
| 基础 RAM | 4/4，ASan/UBSan/LSan、TSan；原场景和被测 RAM 组件未改，沿用原证据 |
| 正式页路径 | 6/6，本轮重跑 ASan/UBSan/LSan、TSan |
| 原课程 | 11/11，本轮重跑 |
| 原 S8 SQL/TCP/快照/重启 | 8/8，本轮重跑，内容不变 |
| 定向改坏 | 20/20；基础 9 沿用，页 11 本轮重跑 |

29 个不同场景；本轮实际运行 25 项、6 项 TSan 和 11 个页变异，不能称本轮重跑了全部 29 项。基础日志在 runs/foundation、runs/thread-foundation，当前其他结果在 runs/review-asan、runs/review-tsan。MANIFEST 区分保留与新证据。

## 本轮修复及测试审查

先从代码确认：Flush 组批已保留部分帧时，准备后续页失败没有经过原 IO 收尾，可能遗留 pin/IO 占用。现在准备与 Write 共用错误收尾；异常仍上报，脏版本保留，没有新增生产 API/hook。

复用原 RAM 场景，通过测试侧既有 PageStorage 接口包装在第三项准备时注入 bad_alloc，实际 Read/Write 继续走 FilePageStorage/DiskManager。失败后同时保留另外三页并比较全部正文，验证三个帧都能使用。不增加场景数、不查看内部 pin，也不靠超时判错。新增 leak-prepared-batch 变异被 `g.has_value()` 的帧不可复用断言检出。详细目标、路径、oracle、污染/去重、稳定性和矩阵见审查 §9。

初次 root-alias 变异改点问题及紧预算 TSan 测试预期问题仍以诊断日志保留在 history，不当作通过结果。对应错误源码包与旧页运行记录不保留。

## 归档与限制

唯一源码为 test/archives/F26-array-buffer-pool.tar.gz，唯一当前结果为本目录 F26-results.tar.gz。原位替换，无旧备份；b/c 结果包已删除；其他 19 个有效模块源码包未改。包内保存当前生产/方案快照、源码包、日志/XML、命令和校验，不含编译产物、设备镜像或第二份展开的测试。RESTORE.md 给出复现指引。

源码包随 Git 保存，结果包按现有 .gitignore 仅保留本机，需要另行备份。本轮不是裸设备、真实掉电、跨机或性能验收；共同 C/P 未重跑；S9.1e/F36 未执行。实际 OS 页归还累计次数不等于 RSS 降幅。

提交前复核：已修正主方案 F12 与 F14 阶段表中过时的“owner 待接入”状态，并明确 A 页已在 S9.1c 接入；没有新增生产/测试变更或测试执行。现有证据和归档校验重新核对通过，S9.1e 仍待用户确认。见执行审查 §10 的提交前复核。

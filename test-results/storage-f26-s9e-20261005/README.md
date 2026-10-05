# F26 / S9.1e 当前结果

2026-10-05；基线 `4e9fc47` 加本轮修改，精确源码哈希在 F26 源码包 MANIFEST，结果包 source/ 保存生产和方案快照。

[源码与恢复指引](../../test/archives/F26-array-buffer-pool.tar.gz) · [实施及八项测试设计审查](../../docs/storage_redesign/s9e_execution_20261005.md) · [校验](SHA256SUMS)

| 范围 | 结果 | 包内证据 |
| --- | --- | --- |
| 基础 RAM | 4/4 ASan/UBSan/LSan | runs/foundation-asan |
| 正式页/SQL | 9/9 ASan/UBSan/LSan | runs/review-asan/pages.xml |
| 课程 / S8 | 11/11 + 8/8，原业务内容和判据 | runs/asan-final |
| TSan | 4 + 9，无跳过 | runs/foundation-thread、runs/review-tsan |
| 定向变异 | 9 + 16，指定 assertion failure | runs/foundation-asan、runs/review-asan |

共 32 个不同场景；重复运行不增加覆盖数。真实 SQL 窗口验证 MVCC/集合/顺序，实际 IO 暂停验证同页合并、并行、需求余量及关闭排空；EIO 验证消费时报告错误。没有测试用 production hook/getter；测试包装只在归档源码。

初次名称/签名编译问题、容量 fixture 预期错误、LSan 环境限制及 Close 变异清理挂起只作 history 诊断，不计通过/检出。修正后全部复验。最终源包只包含修正版本。

旧 b/c/d 结果包已删除，旧 F26 源包原位替换，无备份。其他 19 个有效模块源包不改。临时构建、镜像和展开测试清理。源码包进入 Git，结果包按既有 .gitignore 仅本机保存，需要另行备份；包内保留源码快照、运行命令/日志/XML和校验，不含可执行文件或设备镜像。

S9.1a–e 已完成；下一步 S9.2/F36 待讨论确认。共同 C/P 未修改、未在本轮重跑；不是裸设备、真实掉电、跨机或性能收益验收。查询 setup 当前使用缓存 DeletePage 淘汰，F36 赋予物理释放后须换合法冷缓存 setup，原业务输入/Oracle 不变。


## 再次复审（2026-10-05）

生产未改；SQL 测试分开验证两类查询确实预取，完整比较输入派生记录集合。原整体 query 变异被点查/全扫描两个变异替换。本轮重跑页/SQL 9 项、TSan 9 项、页变异 16 个；基础 RAM/课程/S8 源码未变，23 项正常、4 项 TSan、9 个基础变异证据经校验沿用。当前总计 32/13/25，不能称本轮全部重跑。旧弱判据页运行结果被复验记录替换，原测试/结果包不留备份。详情见实施审查 §9。

## 接续复审（同日，未重跑）

再次核对生产交接与关闭逻辑、39 份生产文件和源码/依赖哈希、原始 XML 及变异断言。没有新增生产或测试修改，不增加场景/运行计数；只更新审查说明、方案快照和校验，原始执行日志保持。有限并发测试不证明所有调度时序，SQL integration 不替代共同 C/P；具体边界见 [审查 §10](../../docs/storage_redesign/s9e_execution_20261005.md#s9e-final-review)。

提交前核对修正了共同协议 §7/§10/§11 中 e 的过时状态及 F26 的历史标注；生产/测试及上述执行结果不变，未重跑。用户已授权提交本阶段；结果包仍按既有规则仅本地保留，提交摘要和校验。见 [提交前复审 §11](../../docs/storage_redesign/s9e_execution_20261005.md#s9e-commit-review)。

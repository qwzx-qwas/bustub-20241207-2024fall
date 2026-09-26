# F10/F11 B checkpoint 发布与恢复（2026-09-26 第二次复审）

[方案](../../docs/storage_redesign/modules/metadata-checkpoint.md#64-当轮实现协议已授权) · [最新八项审查](../../docs/storage_redesign/f10_execution_20260926.md#11-第二次复审2026-09-26) · [源码包](../../test/archives/F10-metadata-checkpoint.tar.gz)

## 审查与结果

先复核生产代码和方案。本次未发现新的生产缺陷，六个生产文件及 runner 与上次归档哈希一致。修正两份方案中的阶段残留：F10 prompt 混入未来裁剪，F11 prompt 混入 Deferred 且测试段误称未批准；现已统一当前 B 路径的范围和测试归属。

本次仅补强 DirectRootAndRepeatedCheckpoints：在第一轮记录一个实际溢出页槽，第二轮要求同一槽代次增加且存活，再验证旧视图和恢复集合。仍六项，不复制 F09 在途写回的并发矩阵。上一轮的修复/发布直接断言、不抵消更新输入、删除旧 XML 复用选项继续保留。

基准 `c948797` 加本轮 BootstrapStore、JournalService、MetadataEngine 及头文件。重新构建并完整运行：**6 项新集成、26 项未修改的原回归全部通过，9 个既有故意改坏版本均被指定断言发现**。Clang 14 Debug + ASan/UBSan 正常场景无检测器错误。变异必须成功编译/链接、退出 1 并匹配目标 oracle，超时/崩溃不计检出。九项是既有风险验证，不计作此次新增覆盖。

| 场景组 | 数量 | XML 用例耗时合计（秒；非性能指标） |
| --- | --- | --- |
| F10 | 6 | 66.448 |
| F03 | 8 | 3.991 |
| F07 | 8 | 1.357 |
| F08 | 6 | 197.397 |
| F09 | 4 | 13.457 |

实际执行 `run_stage.py --repo ... --work /tmp/bustub-f10-audit2-20260926 --mutations`。精确命令、退出码、XML、输入/依赖出处、源码快照在包内。本轮未复用旧测试 XML。

六个生产文件未变，格式/cpplint/GCC/clang-tidy 沿用逐文件哈希相同的先前证据，保存在 `prior-production-checks/` 并标明来源，不称作本轮重新运行。测试按仓库格式校验，最终修改在测试对象编译前完成，runner AST 通过，见 review-checks.json。

## 范围和局限

- 最终页→唯一 Journal checkpoint→引导引用的持久顺序；周期首改 FULL；必要后缀与最终页恢复；失败隔离、修复交错和关闭边界。
- 真实 F01 Direct 普通文件链路，暂停/EIO 仅测试链接侧；无新增生产 API、hook、默认参数或常驻测试目标。
- 新的页槽检查确认输入前提成立，不重测分配器全套逻辑；格式解析仍可能存在与生产共同理解错误，保留输入模型和既有最小变异互补。
- 有限等待窗口不证明全部线程交错；不是 TSAN、裸设备掉电、SQL/Raft E2E 或性能证据。日志裁剪/复用、Deferred、A/节点接入未完成。
- 共同 C/P 内容与旧基线未改；无性能改善结论。

## 归档与清理

- 源码包 11,419 字节，SHA-256 `39d04da8de5f09a196b0e5125b0840f6be4a92baa19194cb55eac9ef3f2cf20d`，随仓库保存。
- 本机结果包 45,641 字节，SHA-256 `8c33698c0b1eb8c513db4ddaffa97b3fc758e9980e70ce8c8efabe07dc08b26e`，沿用 `.gitignore` 忽略，clone 不带结果包。
- 两包逐成员核验后原位替换上一版 F10 包，无旧包备份；不含二进制、设备镜像、构建缓存或旧测试执行结果。
- 有效 F03/F07/F08/F09 归档作为回归依赖保留，问题 F01 原包仍不存在。
- 展开测试、临时变异和构建清理，见 cleanup-verification.json。本次未 commit。

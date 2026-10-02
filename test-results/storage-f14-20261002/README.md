# F14 范围引用与细粒度回收验证（2026-10-02）

[方案](../../docs/storage_redesign/modules/reference-manager.md) · [执行与八项审查](../../docs/storage_redesign/f14_execution_20261002.md) · [最终测试源码](../../test/archives/F14-object-references.tar.gz)

- 新增 F14 六项真实组件场景通过；复用 F13/F12 各六项、F34 五项、S5 八项、A 五项，共 36 项。
- 八个隔离变异编译成功且在指定断言失败；未把超时/编译错误当作检出。
- Clang 14 Debug + ASan/UBSan/泄漏检查、GCC 语法、生产/测试格式、项目 lint 通过。
- 没有测试专用 production hook/public getter/默认值/常驻测试目标。数据正文、容量和恢复状态是主要判据。

只保留最终 F14 源码压缩包与本地 F14-results.tar.gz，后者由现有 .gitignore 忽略。Git 保存源码包、说明、校验和清理证据。实际测试运行时尚未 commit；当前提交前核验见下文。

## 恢复

隔离检出 1df3ba1，按结果包 source/ 覆盖本轮八份生产文件，核对源码包 MANIFEST 中生产/依赖 SHA256；在仓库外解压源码包按 RESTORE 运行。原 F13/F12/F34/S5 有效包保持不变。结果包含命令、日志、XML、变异说明、生产源码和方案/审查快照、源码包及成员校验；不含设备镜像、构建目录或可执行文件。

## 范围和后续接管

本轮读取由测试侧适配器将 F14 保护的真实范围交给现有 RegionManager/F02/F01。已覆盖中间复用、IO 持有许可、小退出联合、尾部回收、事务失败与真实旧格式重开。普通对象 owner/统一 ObjectIO 尚未组装；S7 追加复用、S10 PayloadRef、S11 持久共享、SQL/Raft E2E、裸设备、真实掉电和性能仍待后续。阶段通过不能替代共同 C/P 新后端验证。

这是首次创建 F14 归档，没有旧 F14 包或备份；原 F13 等依赖归档保留。实际哈希、成员核验和删除临时产物字节数见 cleanup-verification.json。


## 再次复审

2026-10-02 重新核对生产逻辑、六项测试设计及原始证据，未发现新的阻塞问题。生产/测试/压缩包未改，未重复运行；71 份生产、4 个依赖及 87 个结果成员哈希一致，36 项通过和 8 个指定变异断言经 XML/退出码复核。结果包保留实际运行时快照，最新复审见 [执行审查 §11](../../docs/storage_redesign/f14_execution_20261002.md#11-再次复审方案代码与测试证据2026-10-02)。


## 提交前核验

再次确认代码与测试包未变，原 36 项通过及 8 个变异证据有效；扫描 34 个归档，仅有最终 F14 两包，无同模块旧包。修正 F02 S2 状态残留，补齐 F34/F33 的 S6 待实施归属，未执行下一阶段。源码包和说明/校验随本轮提交，本地结果包仍由 .gitignore 管理。详见 [最终复核](../../docs/storage_redesign/f14_execution_20261002.md#12-提交前最终复核2026-10-02) 和 cleanup-verification.json 的 precommit_review。

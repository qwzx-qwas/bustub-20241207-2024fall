# F34/F33 本地生命周期验证（2026-09-30）

[实现与八项审查](../../docs/storage_redesign/f34_execution_20260930.md) · [当轮方案](../../docs/storage_redesign/modules/node-lifecycle.md#11-2026-09-30-当轮执行协议已授权) · [测试源码包](../../test/archives/F34-node-lifecycle.tar.gz)

- 五项真实 NodeStorage 集成测试通过；复用原 S5 八项通过。
- 再次审查修复 Close 遗漏最终修复状态，收紧 NotReady 和提前关闭判据；最新证据见实现审查 §9。旧同模块包已原位替换。
- 十个定向变异全部命中指定失败判据，不以编译失败、sanitizer 崩溃或 watchdog 超时充数。
- Clang 14、Debug、ASan/UBSan；GCC 警告/语法、clang-format、项目配置 cpplint 通过。
- 真实 Direct 普通文件；未测 SQL/Raft 新后端 E2E、裸设备、真实掉电或性能提升。

本目录只保存最终 `F34-results.tar.gz`、本说明、SHA256SUMS 和清理记录。结果包按既有规则忽略；Git 不携带该结果大包，需保存/取得同哈希副本来复验原始证据。

## 复验

1. 检查本目录及 `test/archives/SHA256SUMS`。
2. 在隔离 checkout 检出 `c004052`，覆盖结果包 source/ 中的三份生产文件；按测试包 MANIFEST 检查生产/依赖哈希。
3. 在仓库外展开 F34-node-lifecycle.tar.gz，按 RESTORE.md 执行 `tests/run_stage.py --repo ... --work ... --mutations`。
4. 独立核对 lifecycle.xml 的 5 项、s5.xml 的 8 项及 10 个变异实际失败判据，不仅看 summary。

包内保存最终运行日志、XML、具体变异及判据、命令、生产源码快照、源码包和原 S5 生成的旧格式镜像。源码包复用未修改的 S5 压缩包，不复制其数据 oracle；两个依赖包均需保留。

本轮五个场景属于开发期真实组件集成；完成后仅压缩保存。S8/S9 接入业务节点时复用输入/独立模型，常驻主线仍为共同生产 C/P，不能把这些结果改称业务 E2E。

# 阶段测试源码归档

2026-09-22：按用户要求，将本轮 F00 / T0 的阶段性小测试退出常驻套件，按模块压缩保存。长期主线是 [C1–C5 / P1–P4](../storage_acceptance/README.md) 的真实生产 E2E 正确性和性能测试；其内容模型、驱动、适配、历史检查器和报告工具仍在原目录。

这是源码归档，不是新的测试通过记录，也不表示现有 E2E 已覆盖被归档测试的所有边界。源提交为 `78c0bdd353036c8907d675ad27b9ed80c253e842`，本轮没有修改包内测试或修复此前审查指出的缺口。既有课程与 `legacy_raft` 回归不在此次归档范围。

## 包与内容

| 模块包 | 原源码 | 历史用途 | 当前状态 |
| --- | --- | --- | --- |
| [F00-storage-contracts.tar.gz](F00-storage-contracts.tar.gz) | `test/storage_redesign/storage_range_contract_test.cpp` | 5 项范围算术及真实文件解码边界测试 | 已压缩，工作源码和用途标签退出常驻入口 |
| [T0-storage-acceptance.tar.gz](T0-storage-acceptance.tar.gz) | `test/storage_acceptance/sql_storage_contract_test.cpp` | 1 项真实单节点 SQL／重放／快照回归 | 已压缩，不再作为单独 CTest 目标 |
| 同上 | `test/storage_acceptance/test_contracts.py`、`test_delivery_contracts.py`、`test_performance_contracts.py` | 25 项测试工具自检 | 已压缩，不再由工作树的 unittest discovery 发现 |
| 同上 | `test/storage_acceptance/linearizability/model_test.go` | 1 个 Go 测试、7 个手写历史子用例 | 已压缩；真实历史检查器 `main.go` 及依赖保留 |

两个包内均保留模块顶层目录、原源码相对路径、`MANIFEST.json` 和 `RESTORE.md`。Manifest 记录逐文件 SHA-256、字节数、源提交、证据入口、依赖及已知限制。包的哈希在 [SHA256SUMS](SHA256SUMS)。仅包含测试源码和恢复说明，没有二进制、节点数据或构建缓存。

## 归档设计审查与风险交接

本轮执行[主方案 §1.5](../../docs/storage_redesign/README.md#15-每次新增或修改测试后的强制设计审查)，并采用 [§1.6 的退出规则](../../docs/storage_redesign/README.md#16-测试演进跨模块复用与阶段退出)：

- 全景及路径：上述六个源码文件；范围和 SQL 测试确有真实生产调用，Python／Go 自检主要检验工具。手写预期不等于假数据库，不能把几类证据混称 E2E。
- Oracle：本轮只验证归档字节、依赖和发现入口，没有更改这些测试的判断逻辑，也没有重新把旧运行标成新的通过。
- 生产／接口污染：未修改 `src/`、生产 API、默认参数或控制流。仅清除已无消费者的两个 CTest 用途标签，并将归档目录排除在 C++ 测试发现外。
- 重复与接管：共同 C/P 内容和工具保留原样。C1/C4 与单节点正文／恢复用例有部分重叠，C4/C5 不完整覆盖恶意快照范围；不宣称已经等价替代。工具自检也不会自动升级为数据库场景。
- 最小错误检查：此前定向变异已发现退避漏计、始终拒绝、只测旧报告分支等自检盲点。压缩不修复它们，详见下表；恢复使用前重新审查，不能仅引用 25 项通过。
- 稳定性／清理：保留此前短实时时窗、调度、PID 临时目录等限制；解压到仓库外的隔离目录，不恢复到常驻测试树。源文件与归档逐字节比对后才移除；没有启动数据库或重跑基线。

| Test / 风险 | invariant 与 Oracle | 新 failure mode／重叠及限制 | Production pollution | 建议与去向 |
| --- | --- | --- | --- | --- |
| F00 范围 5 项 | 人工整数边界；合法 CRC 输入须被安全拒绝；观察真实读取范围 | 地址回绕、越界／嵌套长度；C4/C5 尚未完整接管，catalog/session 恶意长度案例不足 | 无 | 保留：仅在 F00 压缩包中，相关正式入口接入时复核 |
| SQL 恢复 1 项 | 256→1024→64 字节、1025 字节拒绝、schema 和恢复结果 | 与 C1/C4 部分重叠；精确拒绝／恢复边界不等价，不能单独覆盖表页满页下溢 | 无 | 保留：仅在 T0 压缩包中，业务场景扩展另行冻结 |
| Python 工具 25 项 | 人工行、时间线、请求身份与统计预期 | 重试计时断言过弱；空间／前台历史缺正例；P4 自检走旧报告分支；非法 COMMITTED 缺案例；部分实时时窗会 flaky | 无 | 保留：仅在 T0 压缩包中；实际报告／oracle 工具仍使用，缺口继续登记 |
| Go 历史 7 子用例 | 人工 Ok/Illegal 历史与完整行业务模型 | 同身份不同 SQL／结果缺案例；与真实 C2/C3 历史证据不同 | 无 | 保留：仅在 T0 压缩包中；`main.go` 继续支撑真实 C2/C3 |

长期核心是 C1–C5 / P1–P4；阶段小测试不再常驻。是否存在未覆盖风险与是否保留可执行源码分别记录，不能用删除／归档隐藏风险，也不因此在每一层新建替代小套件。

本轮实际核验：两个包共 13,714 字节，完整解压后六个源码文件的哈希及内容与源提交一致；其余 60 个共同套件、工具及历史代码文件逐字节未变。使用当前 CMake 发现表达式确认保留 66 个 C++ 测试源，仅退出上述两个目标；Python 源码 AST、文档链接和 `git diff --check` 通过。未重新运行数据库、性能基线或归档中的自检，不将静态／归档校验称作系统验收。

## 恢复与后续接入

在仓库根目录，先校验包，再恢复到独立临时目录。下面只恢复源码，不构建或执行测试：

```bash
archive_repo=$(pwd)
archive_module=F00-storage-contracts # 或 T0-storage-acceptance
archive_scratch=$(mktemp -d /tmp/bustub-module-tests-XXXXXX)
mkdir "$archive_scratch/package" "$archive_scratch/source"
(cd "$archive_repo/test/archives" && sha256sum -c SHA256SUMS)
tar -xzf "$archive_repo/test/archives/$archive_module.tar.gz" -C "$archive_scratch/package"
git -C "$archive_repo" archive 78c0bdd353036c8907d675ad27b9ed80c253e842 | tar -xf - -C "$archive_scratch/source"
cp -R "$archive_scratch/package/$archive_module/test/." "$archive_scratch/source/test/"
```

包不是独立完整项目：C++ 需要对应提交的生产源码、构建配置和依赖；Python 自检依赖当时的 acceptance 工具；Go 需要包内记录的模块配置／依赖及可用工具链。按 `MANIFEST.json` 核对恢复文件的哈希。历史运行命令见对应记录，在隔离源码目录选择所需目标运行，不默认跑完整旧套件。

生产后续完善时，优先把同一业务输入、故障目标和独立 oracle 接入常驻共同场景；不能仅把旧测试换成真实文件或新类名就标为 E2E。记录新的源版本、实际执行路径、适配差异和证据，按需重新打包；原包和历史结果保持不变。

## 历史运行证据

- [S0 执行](../../docs/storage_redesign/s0_execution_20260921.md)、[S0 复审](../../docs/storage_redesign/s0_test_review_20260921.md)。
- [T0 共同测试运行](../../docs/storage_redesign/testing_execution_20260921.md)。
- [大型结果压缩包入口](../../test-results/README.md)：与本目录的小型源码包不同，结果包按原规则保留本地；本次没有搬动或删除。

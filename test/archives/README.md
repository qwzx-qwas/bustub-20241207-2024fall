# 阶段测试源码归档

2026-09-24（F07/S3；2026-09-25 复审）：新增 [F07-journal-service.tar.gz](F07-journal-service.tar.gz)，仅含最终 8 项单份 Journal 组件测试、runner 和恢复说明；8/8 和原 F02/F06 共 10 项回归通过；13 个故意改坏版本均被指定断言发现。详见 [八项审查](../../docs/storage_redesign/f07_execution_20260924.md)。

2026-09-23（F05）：新增 [F05-metadata-backend.tar.gz](F05-metadata-backend.tar.gz)，仅含两项正式页 IO 组件场景和 runner；2/2、6 个变异通过，复用原 F04 三项回归。见 [八项审查](../../docs/storage_redesign/f05_execution_20260923.md)。

2026-09-23（F04）：新增 [F04-region-manager.tar.gz](F04-region-manager.tar.gz)，含 3 项真实区域寻址测试和 runner；3/3、6 个隔离变异通过，另复用 F03 原八项回归。只压缩保存，见 [审查](../../docs/storage_redesign/f04_execution_20260923.md)。

2026-09-23（F03 复审）：更新 [F03-bootstrap.tar.gz](F03-bootstrap.tar.gz)，包含 8 项真实 F03→F02→F01 Direct 文件引导/选源/修复/关闭验证和 runner；8 项通过，11 个隔离变异被指定判据捕获。只压缩保存，不注册常驻测试。详见 [执行与八项审查](../../docs/storage_redesign/f03_execution_20260923.md)。

2026-09-23（F02 复审更新）：[F02-object-io.tar.gz](F02-object-io.tar.gz) 已替换为当前 8 项真实 F01/F02 文件 IO 检查和 runner；8 项通过，10 个隔离变异由对应断言发现，旧版包不留备份。测试仅压缩保存，不注册常驻目标。详见 [F02 执行与八项审查](../../docs/storage_redesign/f02_execution_20260922.md)。

2026-09-22（F01）：新增 [F01-block-device.tar.gz](F01-block-device.tar.gz)，包含 7 项设备后端阶段检查和隔离变异 runner。已按八项要求审查，测试通过、6 个改坏版本被发现；源码在临时目录编写和执行，未加入常驻测试树。详见 [F01 执行与审查](../../docs/storage_redesign/f01_execution_20260922.md)。下述 F00/T0 历史事实与旧包保持不变。

2026-09-22：按用户要求，将本轮 F00 / T0 的阶段性小测试退出常驻套件，按模块压缩保存。长期主线是 [C1–C5 / P1–P4](../storage_acceptance/README.md) 的真实生产 E2E 正确性和性能测试；其内容模型、驱动、适配、历史检查器和报告工具仍在原目录。

这是源码归档，不是新的测试通过记录，也不表示现有 E2E 已覆盖被归档测试的所有边界。源提交为 `78c0bdd353036c8907d675ad27b9ed80c253e842`，本轮没有修改包内测试或修复此前审查指出的缺口。既有课程与 `legacy_raft` 回归不在此次归档范围。

## 包与内容

| 模块包 | 原源码 | 历史用途 | 当前状态 |
| --- | --- | --- | --- |
| [F05-metadata-backend.tar.gz](F05-metadata-backend.tar.gz) | 包内 `test/storage_redesign/metadata_backend_test.cpp`、`run_stage.py` | 2 项页地址/容量/IO 适配，6 个隔离 mutation；复用原 F04 包回归 | S2 完成，仅压缩保留；S4 页分配/WAL/业务接入待完成 |
| [F04-region-manager.tar.gz](F04-region-manager.tar.gz) | 包内 `test/storage_redesign/region_manager_test.cpp`、`run_stage.py` | 3 项区域绑定/包含/IO 接入，6 个隔离 mutation；依赖 F03 原包做回归 | 固定区域完成，仅压缩保留；页/日志后端与节点业务待接入 |
| [F03-bootstrap.tar.gz](F03-bootstrap.tar.gz) | 包内 `test/storage_redesign/bootstrap_store_test.cpp`、`run_stage.py` | 8 项基础引导与异步修复组件场景，11 个隔离 mutation | 仅压缩归档；F34 节点/业务接入及 S5 可变恢复根未完成 |
| [F02-object-io.tar.gz](F02-object-io.tar.gz) | 包内 `test/storage_redesign/io_executor_test.cpp`、`run_stage.py`；原工作目录为专用 `/tmp` | 8 项批次/预算/外部许可/失败/生命周期验证，10 个隔离 mutation | 仅压缩归档；FrameArena、S2/S6 对象寻址及业务接入待完成 |
| [F01-block-device.tar.gz](F01-block-device.tar.gz) | 包内 `test/storage_redesign/block_device_test.cpp`、`run_stage.py`；工作源码原在专用 `/tmp` 目录 | 7 项真实文件/边界注入验证，6 个隔离 mutation | 已压缩，不注册常驻目标；F02 已使用 F01，S8/S9 业务接入仍待完成 |
| [F00-storage-contracts.tar.gz](F00-storage-contracts.tar.gz) | `test/storage_redesign/storage_range_contract_test.cpp` | 5 项范围算术及真实文件解码边界测试 | 已压缩，工作源码和用途标签退出常驻入口 |
| [T0-storage-acceptance.tar.gz](T0-storage-acceptance.tar.gz) | `test/storage_acceptance/sql_storage_contract_test.cpp` | 1 项真实单节点 SQL／重放／快照回归 | 已压缩，不再作为单独 CTest 目标 |
| 同上 | `test/storage_acceptance/test_contracts.py`、`test_delivery_contracts.py`、`test_performance_contracts.py` | 25 项测试工具自检 | 已压缩，不再由工作树的 unittest discovery 发现 |
| 同上 | `test/storage_acceptance/linearizability/model_test.go` | 1 个 Go 测试、7 个手写历史子用例 | 已压缩；真实历史检查器 `main.go` 及依赖保留 |

原 F00/T0 两个包内均保留模块顶层目录、原源码相对路径、`MANIFEST.json` 和 `RESTORE.md`。Manifest 记录逐文件 SHA-256、字节数、源提交、证据入口、依赖及已知限制。包的哈希在 [SHA256SUMS](SHA256SUMS)。仅包含测试源码和恢复说明，没有二进制、节点数据或构建缓存。

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

## F01 恢复与风险交接

F01 当前包为 9,316 字节；包内 MANIFEST.json 记录两个源码的哈希、基准提交与生产文件哈希、依赖及未覆盖项，RESTORE.md 提供外部目录恢复/运行命令。基准 d8ad4a0 本身没有 F01，需匹配后续生产源码哈希或使用本地结果包中的 source/ 快照。不要将其自动恢复到长期测试目录。

提交前复审修正并发测试的 future 容器扩容异常路径，修正版 7 项重新通过；6 个 mutation 的对应用例/生产逻辑未变，提交前核验后沿用其历史结果，未重跑。当前包为修正版；包含问题测试的首轮结果包已按用户要求删除，首轮 mutation 原始日志不再保留。恢复生产源码及核验现存运行结果请使用[修正版复验包](../../test-results/storage-f01-review-20260922/README.md)。

风险标识：F01/range-io、reject-invalid、concurrent、durability、failure-boundary。正常路径经过真实 BlockDevice 和 Direct 文件 IO；缺失能力/EINTR/EIO/短操作的部分边界为测试链接注入；独立真实文件截短单独记录。程序重开和刷新观察均不证明掉电安全。

八项审查与逐例 oracle/最小 mutation/最终矩阵见 [F01 记录](../../docs/storage_redesign/f01_execution_20260922.md)。无生产测试 hook、内部 fd getter、测试默认路径或常驻注册；新正式 BlockDevice 接口由授权的 F01 职责需要，现已供 F02 使用，业务接入仍待 S8/S9，不能用这组通过冒称共同 E2E 已覆盖新后端。后续按共同 C/P 内容接入和去重；本轮小测试全部仅归档保留。

现存结果包及清理证据见 [F01 修正版复验](../../test-results/storage-f01-review-20260922/README.md)，旧包删除见[首轮摘要](../../test-results/storage-f01-20260922/README.md)。原共同 C/P 源码和原归档没有被修改。

旧包删除后仅同步了当前包的 manifest/恢复说明，测试源码和 runner 字节未变；当前包以本目录 SHA256SUMS 为准，复验清理记录内的包哈希描述复验当时版本。

## F02 恢复与风险交接

当前包包含 8 项组件场景、runner、MANIFEST.json、RESTORE.md；源码和结果均为外部缓冲扩展后的复验版本。旧版源码/结果压缩包原位替换，没有备份。之前“零用例误报成功”的 runner 修正保留：隔离外部 GTEST_* 配置，并核验完整 XML。当前在有干扰配置的环境下仍实际运行全部八项。

基准 `a4a6062` 需要结合本轮改动；按 MANIFEST.json 的生产哈希核对，或在隔离基准 checkout 使用本地结果包 source/ 快照。按 RESTORE.md 在仓库外恢复，不永久恢复到测试树。当前包大小和哈希以 [SHA256SUMS](SHA256SUMS) 及结果清理清单为准，不用旧历史包大小描述当前文件。

风险标识为 F02/batch-barrier、budget、admission、failure-drain、flush-error、lifetime-close、external-buffer。正常分支是真实 F01/F02 Direct 文件 IO；暂停、EIO、短写及 ENOMEM 为测试侧注入。外部内存由测试 owner 提供，不是 FrameArena 或真实 BufferPool。无生产 hook、测试默认路径或私有计数 getter。8 项通过，10 个改坏版本均有指定失败断言与退出码 1；不推定全项目覆盖率。

来源、输入/oracle、最小变异、接口污染/重复/稳定性及矩阵见 [F02 当前审查 §14](../../docs/storage_redesign/f02_execution_20260922.md#14-2026-09-23-方案落实与测试复审)；命令、日志、源码版本及清理见 [本地压缩证据](../../test-results/storage-f02-20260922/README.md)。F31/F32/F26/F35 引用同一风险，不另写镜像套件。S9.1c 仍须验证真实帧许可、BufferPool 默认切换及旧页路径清理；共同 C/P 尚未经过新后端，裸设备、掉电及整体性能仍未验证。

2026-09-22 修正外部准入场景的名额泄漏漏检；2026-09-23 又对自有缓冲回滚作同类修正。两条准备路径均按满额度重试，当前八项/十个变异通过。旧包原位替换，不保留含缺口的旧测试版本；两次复现日志仅为缺陷证据。

## F03 恢复与风险交接

当前包只包含最终八项测试、runner、MANIFEST.json、RESTORE.md，没有设备镜像/编译产物。基准 `37fa7f2` 加本轮 F03 生产变更，恢复时必须核对 manifest 中的生产哈希，或在隔离的该提交 checkout 中覆盖本地结果包 `source/` 快照。严格按 RESTORE.md 解压到仓库外，不恢复成常驻小测试。

风险标识为 F03/bootstrap-format、create-durable、copy-selection、repair-lifecycle；F02/F04/F10/F34 引用同一归属。真实路径是默认 Direct 文件、正式 BootstrapStore/IOExecutor/BlockDevice；暂停/EIO/读回损坏在测试侧注入。不是裸设备、真实掉电或 SQL/Raft E2E。共同 C/P 内容和基线未改；后续正式接入时讨论等价场景，不复制一套性能测试。

[结果与清理](../../test-results/storage-f03-20260923/README.md) 保留八项/十一个变异和检测器证据。本次已将复审前 F03 源码包和结果包原位替换，不保留旧版备份；F01/F02 的有效历史包未被误删，原错误 F01 结果包未恢复。

F03 本次复审补齐写后固定源检查，修正关闭测试的寿命/调度及实际等待确认；源码包仍为原八项场景，等待观察仅在 Linux/libstdc++ 的测试链接侧。当前精确哈希以 SHA256SUMS 为准，见执行记录 §10。

## F04 恢复与风险交接

包内 RESTORE.md 要求在仓库外恢复、先核对 production 哈希；结果包 source/ 保存 `f2dda5b` 加本轮的实际源码。runner 核验 F03 源码包哈希后恢复并运行原八项，F04 包不再复制它们。

风险为 F04/region-binding、region-containment、io-adaptation，F05/F06/F12/F34 引用；正式 F03/F04/F02/F01 Direct 路径与测试侧故障注入分别标明。没有测试生产 hook，不把三项组件结果视为节点 E2E；后续由共同 C/P 的真实接入接管适用风险，不建重复性能套件。

F04 复审修正了测试提前退出时的回调寿命，并使绑定及非对齐场景免受范围冲突判据干扰；仅保留修正版，不保留旧包、失败的变异构造或备份。原 F03 有效包保留作回归依赖，不能因本轮使用其测试而误删。详见 [结果与清理](../../test-results/storage-f04-20260923/README.md)。

## F05 恢复与风险交接

包内仅两个阶段场景、runner、MANIFEST.json、RESTORE.md；按 RESTORE 在仓库外恢复。基准 `69ed1dd` 本身没有 F05，需匹配 manifest 的生产哈希，或在隔离基准 checkout 覆盖本地结果包 `source/` 快照。原 F04 包由 runner 校验哈希后复用，不重复打包旧测试。

风险 `F05/page-address、page-capacity、page-io-adaptation` 归 F05 维护，F08/F09/F26 引用。真实 Direct 文件及整个物理镜像 oracle、8 KiB 测试侧能力注入分别标明；未经过真实 B/BufferPool 或业务 E2E。两项/六个变异及原 F04 三项通过；详见 [审查](../../docs/storage_redesign/f05_execution_20260923.md) 与 [结果和清理](../../test-results/storage-f05-20260923/README.md)。没有 production 测试 hook、默认路径改动或常驻小测试。后续真实接入沿风险标识复用，不复制测试矩阵。

F05 再次复审仅修正 runner：旧容量向上取整变异在整页对齐环境可能与正确实现等价，已改为可观察的多报一页错误。两个 C++ 场景不变，2/3/6 重新通过；两份 F05 压缩包原位替换，旧版本不保留。


## F06 恢复与风险交接

[F06-journal-backend.tar.gz](F06-journal-backend.tar.gz) 仅含两个阶段场景、runner、MANIFEST.json、RESTORE.md；按 RESTORE 解压到仓库外，不恢复成常驻小测试。基准 5be34fb 加本轮固定段后端，实际字节按 production_sha256 核对；历史恢复可用本地结果包 source/ 覆盖隔离基准目录。原 F04 包按哈希校验后复用三项，F06 包不复制其测试。

风险 F06/segment-address、segment-boundary、io-adaptation 归 F06，F07/F10/F11 引用。正式 F03/F04/F06/F02/F01 Direct 文件、不同段大小和物理起点、两种缓冲、独立全文件 oracle；不是完整 Journal 事务、B 或 Raft/SQL E2E。两项场景、原 F04 三项和七个隔离变异通过，见 [八项审查](../../docs/storage_redesign/f06_execution_20260924.md) 与 [结果/清理](../../test-results/storage-f06-20260924/README.md)。

没有 production 测试 hook、默认路径改动或常驻注册。后续真实 F07/B 接入沿风险标识接管，不复制同义套件；S3/S5 协议、裸设备、物理掉电和性能未测。F06 本轮首次建包，仅最终版本；F00–F05/T0 有效包未变，原错误 F01 包没有恢复。

## F07 恢复与风险交接

当前包含最终 8 项 C++ 场景、runner、MANIFEST.json、RESTORE.md。基准 `00a8397` 加 S3 生产改动，恢复需匹配 production_sha256，或在隔离基准 checkout 覆盖本地结果包 source/ 快照。原 F02/F06 源码包按哈希读取作回归，本包不复制它们。

风险 `F07/format-chain`、`group-durable`、`admission-lifetime`、`failure-isolation`、`reopen`；S4/S5/S10 与共同 C/P 真实接入时沿这些风险接管，不另建同义套件。正式 F07→F06→F04→F02→F01 Direct 文件路径；暂停/EIO 为测试链接注入，没有 production hook。不是 B/SQL/Raft E2E、裸设备或真实掉电。

[设计审查](../../docs/storage_redesign/f07_execution_20260924.md) 与 [结果/清理](../../test-results/storage-f07-20260924/README.md) 记录正常实现 8 项、原回归 10 项通过，13 个故意改坏版本按预期失败及工具检查。只保留最终压缩源码；临时修正前用例、构建和镜像均不常驻。2026-09-25 复审修正了 CRC 判据并补强 Flush 等待、组内容和及时隔离验证，原位替换 F07 包，无旧 F07 错误包保留；旧有效模块包及共同 C/P 未修改。

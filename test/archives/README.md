# 阶段测试源码归档

2026-10-09（F32 再次复审）：唯一 [F32-metadata-cache.tar.gz](F32-metadata-cache.tar.gz)，仍含 11 项阶段场景及复用/变异 runner。T4/T5/T7 改用输入模型、简化 T5；11 项重跑通过、12 个变异检出，66 项既有同源回归沿用，合计 77 项证据（含 1 项旧 LRU-K 课程兼容）。ASan/LSan 开启；三个 benchmark 清理旧头依赖后编译通过。[八项审查 §10](../../docs/storage_redesign/s12_cache_execution_20261008.md#10-2026-10-09-再次复审预期正文必须来自独立输入) · [结果/清理](../../test-results/storage-f32-20261008/README.md)。旧 F32 源码/结果包原位替换，不留备份；阶段源码仅压缩保存，七个有效依赖包保持原哈希。完整性能比较未完成。

2026-10-08（F30）：唯一 [F30-integrity-scanning.tar.gz](F30-integrity-scanning.tar.gz)。最新修正 B 扫描由写回/checkpoint 接手时的结果与损坏分类；原 B 场景扩展三个完成入口，无新增正常场景。本轮全部 41 项正常场景通过、10 个变异检出，ASan/LSan 开启；旧同模块包及旧分轮结果已由完整复验替换。阶段源码仅压缩保存，无常驻新增目标；五个有效依赖包保持原哈希。见 [八项审查 §13](../../docs/storage_redesign/s12_integrity_execution_20261008.md#13-再次复核2026-10-08写回接手扫描失败的结果与状态) 与 [结果](../../test-results/storage-f30-20261008/README.md)。

2026-10-08（F26 共享零页接续）：[唯一源码包](F26-array-buffer-pool.tar.gz) 原位替换，基础 6 项、实际页/SQL 10 项，复用课程 11 项与 S8 8 项，共 35 个不同正常场景；组件 TSan 6 项、当前变异 8 个。旧 S9.1e 的 32/25 是历史结果，不作为本轮证据。旧 F26 结果包已删除，其他模块有效依赖保留；见[审查](../../docs/storage_redesign/s12_translation_execution_20261008.md)和[当前结果](../../test-results/storage-f26-zero-20261008/README.md)。 最新复审仅加强变异失败内容判据，重跑组件六项和八个变异；其余结果沿用源码匹配的上轮证据，见审查 §9。


2026-10-08：[F29-log-cleaning.tar.gz](F29-log-cleaning.tar.gz) 为 S12.3 唯一源码包：10 项新集成、82 项复用回归、7 个指定变异；测试仅压缩保存。先 Unmap、净收益连续尾部、旧格式及完成额度隔离，见 [八项审查](../../docs/storage_redesign/s12_log_cleaning_execution_20261008.md) 和 [结果/清理](../../test-results/storage-f29-20261008/README.md)。本次复审原位替换旧 F29 包、无备份；前置模块有效依赖保留。

2026-10-08（S12.2）：原位替换唯一 [F31-F22-resource-gc.tar.gz](F31-F22-resource-gc.tar.gz)。82 项正常场景（8 新增、74 复用）通过，13 个指定变异检出；原 S12.1 风险仍在同包，旧版不另留。新增帧/目录/快照/网络共享预算、Store 进展与关闭/错误；没有新常驻目标。其他模块有效依赖包保留。见 [八项审查](../../docs/storage_redesign/s12_integration_execution_20261008.md) 与 [结果](../../test-results/storage-s12-resource-gc-20261007/README.md)。

2026-10-07（F25 当前压缩接续）：唯一 [F25-shared-snapshot.tar.gz](F25-shared-snapshot.tar.gz) 原位更新，本次复查 59 个正常场景、3 个定向变异；含真实 SQL/TCP 压缩/旧协议追赶与阶段队列/锁外读取证据。旧 F25 包无备份，测试仅压缩保存。详见 [八项审查](../../docs/storage_redesign/s11_compression_execution_20261007.md)。下文旧日期的 47/19 等保留历史事实。

2026-10-07（F25 增量接续，整体未完成）：同一 F25 源码包包含五个真实 SQL/对象/接收节点场景，38 项正常场景通过，十三个相关变异检出；此前补齐提交预算拒绝和独立会话判据，本次扩展原 TCP 场景验证旧协商/旧全量块隔离、重复协商续期及新 term 编号重置。变异脚本拒绝空选择/未知名称。旧 F25 包原位替换。基础保留策略及 Leader 自动增量 E2E 待确认/接续。[当前审查](../../docs/storage_redesign/s11_incremental_execution_20261007.md)。下条 33/9 仅记录原共享轮，当前包/结果以本条为准。

2026-10-06（F25 / S11 共享快照）：新增唯一 [F25-shared-snapshot.tar.gz](F25-shared-snapshot.tar.gz)。6 个新生产路径场景，复用 F28 7、S8 8、F27 12，2026-10-07 复审后 33 项通过、9 个定向变异检出。F28 原 portable/local 场景不再重复执行；加强原 T1，以接收端旧 checkpoint、新快照和后缀重启真正接管其恢复选择风险；原依赖归档不修改。测试只压缩保存，无生产测试 hook，错误/中间版本没有单独保留旧包。[逐项审查](../../docs/storage_redesign/s11_shared_snapshot_execution_20261006.md)、[结果](../../test-results/storage-f25-s11-20261006/README.md)。增量/压缩、真实掉电和性能对比未在本轮完成。


2026-10-06（F28 / S11，第三次复审）：同一源码包原位更新 runner/RESTORE/MANIFEST，要求 `--case` 与 `--checkpoint-only` 一起使用，防止静默漏跑目标。生产/C++场景/变异未改，实际只检查错误命令拒绝；最近16项/2个指定变异结果沿用第二次复审，见 [审查 §11](../../docs/storage_redesign/s11_execution_20261006.md#11-第三次复审定向测试命令不能漏跑目标2026-10-06)。

2026-10-06（F28 / S11，第二次复审换代）：唯一 [F28-business-checkpoint.tar.gz](F28-business-checkpoint.tar.gz) 已原位替换，旧包不保留备份。F28 仍8项；T4 由手工保存改为真实 SQL/F28 worker/重启验证，生产未改。最新重跑 F28＋S8 共16项通过、2个指定变异检出（1个新增）；另48项沿用首轮，覆盖目录共64项，不冒称全部重跑。首轮63项/5变异、第一次复审16项/2新变异按轮保存。[逐项审查及具体修复](../../docs/storage_redesign/s11_execution_20261006.md)、[压缩结果](../../test-results/storage-f28-20261006/README.md)。共同C/P和有效依赖包不变。

2026-10-05（F27 / S10，再次复审）：更新 [F27-deferred.tar.gz](F27-deferred.tar.gz)。12 项新目标 Deferred 集成场景，复用44项已有回归（F36内含F26只计一次），56项通过、8个定向变异检出；无常驻新阶段测试。旧F27源码及结果包原位替换，不保留旧包备份；依赖包原哈希不变。见 [八项审查与修正](../../docs/storage_redesign/s10_execution_20261005.md) 与 [结果及限制](../../test-results/storage-f27-s10-20261005/README.md)。

2026-10-05（F36 / S9.3）：[F36-table-space.tar.gz](F36-table-space.tar.gz) 原位替换 S9.2 包。含 9 项 F36 集成场景和 runner：保留/适配原六项，新增空页/物理复用、扫描与在途访问、持久退役恢复三项；SQL 周转使用完整行模型。复用 F26 9、课程 20、向量索引 20、S8 8，最终 66 项正常场景通过，19 个定向变异检出。见 [八项测试设计审查](../../docs/storage_redesign/s93_execution_20261005.md) 和 [当前结果](../../test-results/storage-f36-s93-20261005/README.md)。旧 F36 包不保留，S5/F26/S8 有效依赖不改；没有常驻新增阶段测试。TSan、共同性能比较、真实裸设备和物理掉电不在本轮证据内。

2026-10-05（F26 / S9.1e）：[F26-array-buffer-pool.tar.gz](F26-array-buffer-pool.tar.gz) 原位更新，4 项 RAM＋9 项真实页/SQL、25 个断言变异；复用课程 11 项和 S8 8 项。32 个不同场景通过，13 项通过 TSan。真实索引窗口接通有界预取；F36 未执行。仅压缩保存，不注册常驻阶段目标；旧 b/c/d 结果包删除，其他 19 个有效模块源码包不改。见 [八项审查](../../docs/storage_redesign/s9e_execution_20261005.md) 与 [结果/恢复](../../test-results/storage-f26-s9e-20261005/README.md)。

2026-10-03（S8；2026-10-04 测试契约复验）：更新 [S8-raft-object-stores.tar.gz](S8-raft-object-stores.tar.gz)。F23/F24/F25 共用 8 个真实对象后端场景，含 SQL/会话恢复、TCP 三节点 InstallSnapshot/重启；58 个相邻回归、10 个定向变异通过。T2 已删除控制根观察，验证公开额度下的合法后继写入；T8 在受限 Data 容量内验证 Store 退役→真实后台回收→新数据写入与重开，不再仅依赖删除计数；T6 去掉重复的重启前查询。生产代码未为本轮测试改动。见 [八项审查](../../docs/storage_redesign/s8_execution_20261003.md) 与 [结果](../../test-results/storage-s8-20261003/README.md)。只保留最终 S8 压缩版本，不注册常驻目标，不保留旧包/备份；S5 fixture 和其他有效模块包保持原样。

2026-10-02（F02/S6 对象接入）：更新 [F02-object-io.tar.gz](F02-object-io.tar.gz)，当前含七项真实 NodeStorage 对象路径＋原八项执行器测试；runner 复用 F34/F12/F13/F14/S5，合计46项通过、7个定向变异命中。旧 F02 包原位替换，旧结果包删除；见 [八项审查](../../docs/storage_redesign/s6_object_io_execution_20261002.md) 和 [最终结果](../../test-results/storage-s6-object-io-20261002/README.md)。

同日再次复审：修正辅助函数失败传播和异常关闭清理；46项/7个变异重新通过，源码与结果包原位更新，生产未改，见上述审查 §10。

2026-10-02（F13 对象映射）：[F13-object-mapping.tar.gz](F13-object-mapping.tar.gz)。六项真实组件场景，直接复用 F12/F34/S5 和 A 原测试；见 [八项审查](../../docs/storage_redesign/f13_execution_20261002.md) 与 [结果](../../test-results/storage-f13-20261002/README.md)。F14、完整对象服务和业务接入未完成。

2026-09-30（F12 分配器）：[F12-data-allocator.tar.gz](F12-data-allocator.tar.gz)，六项新集成＋原 F34 五项/S5 八项回归、十个指定变异；见 [八项审查](../../docs/storage_redesign/f12_execution_20260930.md) 与 [结果](../../test-results/storage-f12-20260930/README.md)。仅最终压缩源码；F13/F14、自动 GC 和节点对象接入未完成。

2026-09-30（F34/F33 本地生命周期）：[F34-node-lifecycle.tar.gz](F34-node-lifecycle.tar.gz)，5 项新集成＋8 项原 S5 回归、10 个指定变异；见[八项审查](../../docs/storage_redesign/f34_execution_20260930.md)及[结果](../../test-results/storage-f34-20260930/README.md)。仅最终压缩包，S5 为原哈希依赖；未接入 DistributedNode 业务后端。

2026-09-30（S5 基础 Journal 回收）：新增 [S5-journal-recycling.tar.gz](S5-journal-recycling.tar.gz)。8 项新集成、23 项复用回归通过，8 个指定变异检出；新建 v2，旧 v1 真实生产镜像继续读写恢复且不迁移。见 [实现与八项审查](../../docs/storage_redesign/s5_execution_20260930.md) 和 [结果](../../test-results/storage-s5-20260930/README.md)。仅一个最终 S5 包，不注册常驻目标。F03/F08/F09/F10 是有效历史依赖，保留原哈希；F10 旧根回退组合不原样套到已退役的新格式，风险由 S5 新用例接管。

2026-09-26（F10/F11 两轮复审）：更新 [F10-metadata-checkpoint.tar.gz](F10-metadata-checkpoint.tar.gz)，统一六项直接入口/周期 FULL/故障/并发恢复集成场景；6 项、原 F03/F07/F08/F09 共 26 项重新执行通过，9 个变异被指定断言发现；补强修复/发布断言和更新输入，删除旧 XML 复用选项，第二轮补足同一页槽跨 checkpoint 复用的输入判据，旧同模块包再次原位替换。先逻辑审查再测试，见 [八项审查 §11](../../docs/storage_redesign/f10_execution_20260926.md#11-第二次复审2026-09-26)。仅压缩保留；有效依赖包不删除，不注册常驻目标。

2026-09-26（F09）：新增 [F09-metadata-page-writer.tar.gz](F09-metadata-page-writer.tar.gz)，仅最终四项真实页写回测试与 runner；4 项/原 F08 六项通过，8 个指定变异检出。先审生产逻辑再测试，见 [八项审查](../../docs/storage_redesign/f09_execution_20260926.md)。不注册常驻测试，F08 有效包保留作回归来源。

2026-09-25（F08/S4）：新增 [F08-metadata-engine.tar.gz](F08-metadata-engine.tar.gz)，仅最终六项真实 B 组件测试和 runner；6 项/原 A 12 项通过，12 个定向变异检出，详见 [八项审查](../../docs/storage_redesign/f08_execution_20260925.md)。没有常驻测试目标。

2026-09-24（F07/S3；2026-09-25 复审）：新增 [F07-journal-service.tar.gz](F07-journal-service.tar.gz)，仅含最终 8 项单份 Journal 组件测试、runner 和恢复说明；8/8 和原 F02/F06 共 10 项回归通过；13 个故意改坏版本均被指定断言发现。详见 [八项审查](../../docs/storage_redesign/f07_execution_20260924.md)。

2026-09-23（F05）：新增 [F05-metadata-backend.tar.gz](F05-metadata-backend.tar.gz)，仅含两项正式页 IO 组件场景和 runner；2/2、6 个变异通过，复用原 F04 三项回归。见 [八项审查](../../docs/storage_redesign/f05_execution_20260923.md)。

2026-09-23（F04）：新增 [F04-region-manager.tar.gz](F04-region-manager.tar.gz)，含 3 项真实区域寻址测试和 runner；3/3、6 个隔离变异通过，另复用 F03 原八项回归。只压缩保存，见 [审查](../../docs/storage_redesign/f04_execution_20260923.md)。

2026-09-23（F03 复审）：更新 [F03-bootstrap.tar.gz](F03-bootstrap.tar.gz)，包含 8 项真实 F03→F02→F01 Direct 文件引导/选源/修复/关闭验证和 runner；8 项通过，11 个隔离变异被指定判据捕获。只压缩保存，不注册常驻测试。详见 [执行与八项审查](../../docs/storage_redesign/f03_execution_20260923.md)。

2026-09-23（F02 复审更新）：[F02-object-io.tar.gz](F02-object-io.tar.gz) 当时替换为 8 项真实 F01/F02 文件 IO 检查和 runner；当前包已由上方 S6 接续；8 项通过，10 个隔离变异由对应断言发现，旧版包不留备份。测试仅压缩保存，不注册常驻目标。详见 [F02 执行与八项审查](../../docs/storage_redesign/f02_execution_20260922.md)。

2026-09-22（F01）：新增 [F01-block-device.tar.gz](F01-block-device.tar.gz)，包含 7 项设备后端阶段检查和隔离变异 runner。已按八项要求审查，测试通过、6 个改坏版本被发现；源码在临时目录编写和执行，未加入常驻测试树。详见 [F01 执行与审查](../../docs/storage_redesign/f01_execution_20260922.md)。下述 F00/T0 历史事实与旧包保持不变。

2026-09-22：按用户要求，将本轮 F00 / T0 的阶段性小测试退出常驻套件，按模块压缩保存。长期主线是 [C1–C5 / P1–P4](../storage_acceptance/README.md) 的真实生产 E2E 正确性和性能测试；其内容模型、驱动、适配、历史检查器和报告工具仍在原目录。

这是源码归档，不是新的测试通过记录，也不表示现有 E2E 已覆盖被归档测试的所有边界。源提交为 `78c0bdd353036c8907d675ad27b9ed80c253e842`，本轮没有修改包内测试或修复此前审查指出的缺口。既有课程与 `legacy_raft` 回归不在此次归档范围。

## 包与内容

| 模块包 | 原源码 | 历史用途 | 当前状态 |
| --- | --- | --- | --- |
| [F19-common-pipeline.tar.gz](F19-common-pipeline.tar.gz) | 包内 `tests/transaction_test.cpp`、`run_stage.py` | 10 项 S7 正式对象事务集成、原 46 项回归、12 个定向变异 | S7 Common/COW 已完成；原分配尾部复用、S8/S9 业务接入未启用 |
| [S5-journal-recycling.tar.gz](S5-journal-recycling.tar.gz) | 包内 `recycling_test.cpp`、fixture、旧生产镜像生成器和 runner | 8 项 S5 + 23 项复用回归；8 个变异 | 基础回收完成；节点编排/Deferred/业务接入未完成 |
| [F10-metadata-checkpoint.tar.gz](F10-metadata-checkpoint.tar.gz) | 包内 `test/storage_redesign/metadata_checkpoint_test.cpp`、`run_stage.py` | 6 项真实 B checkpoint/恢复，9 个变异；执行原 26 项回归 | 本轮 B 路径完成；日志裁剪/复用、Deferred、A 接入未完成 |
| [F09-metadata-page-writer.tar.gz](F09-metadata-page-writer.tar.gz) | 包内 `test/storage_redesign/metadata_page_writer_test.cpp`、`run_stage.py` | 4 项最终页/并发/失败，8 个变异；原 F08 六项回归 | F09 当轮完成，仅压缩；checkpoint/最终页恢复由 F10 接续，回收待后续 |
| [F08-metadata-engine.tar.gz](F08-metadata-engine.tar.gz) | 包内 `test/storage_redesign/metadata_engine_test.cpp`、`run_stage.py` | 6 项 B 事务/恢复/空间复用，12 个变异；原 A 回归 12 项 | S4 已完成，仅压缩；最终页写回由 F09 接续，checkpoint 由 F10 接续；业务接入未完成 |
| [F05-metadata-backend.tar.gz](F05-metadata-backend.tar.gz) | 包内 `test/storage_redesign/metadata_backend_test.cpp`、`run_stage.py` | 2 项页地址/容量/IO 适配，6 个隔离 mutation；复用原 F04 包回归 | S2 完成，仅压缩保留；S4 页分配/WAL 已由 F08 接续，业务接入未完成 |
| [F04-region-manager.tar.gz](F04-region-manager.tar.gz) | 包内 `test/storage_redesign/region_manager_test.cpp`、`run_stage.py` | 3 项区域绑定/包含/IO 接入，6 个隔离 mutation；依赖 F03 原包做回归 | 固定区域完成，仅压缩保留；页/日志后端与节点业务待接入 |
| [F03-bootstrap.tar.gz](F03-bootstrap.tar.gz) | 包内 `test/storage_redesign/bootstrap_store_test.cpp`、`run_stage.py` | 8 项基础引导与异步修复组件场景，11 个隔离 mutation | 仅压缩归档；S5 可变恢复根由 F10 接续，F34 本地编排已接续，业务节点接入未完成 |
| [F02-object-io.tar.gz](F02-object-io.tar.gz) | 包内 `tests/object_io_test.cpp`、`io_executor_test.cpp`、`run_stage.py` | 7项对象组合＋8项原执行器；另复用31项回归，共46项、7个定向变异 | 仅压缩归档；S6已接入，FrameArena/业务接入待完成 |
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

当前入口为模块包 `F02-object-io/RESTORE.md`，运行 `tests/run_stage.py`；生产/测试/依赖哈希以当前 MANIFEST 为准。旧目录形式和旧哈希只属于历史版本，不能用于恢复当前包。新结果见 [S6 证据](../../test-results/storage-s6-object-io-20261002/README.md)，八项设计审查和跨模块接管见 [执行记录](../../docs/storage_redesign/s6_object_io_execution_20261002.md)。原八项执行器测试原样保留，无常驻目标；S6新增组合风险后续交由 S7/S8/S9 的真实业务路径接管。

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

## F08 恢复与风险交接

最终包包含六项组件测试、runner、MANIFEST.json 和 RESTORE.md；基准 `015f95b`＋本轮生产改动，恢复必须核对 production_sha256，历史重建可用本地结果包的 source/。原 A 五个目标的十二项测试直接从对应源码树运行，源码未修改，不重复打包。

风险为 `F08/atomic-view、mixed-values、private-rollback、page-reuse、redo-base、reopen`。正式 B→F07→F06→F04→F02→F01 Direct 文件路径；fdatasync 暂停/EIO 仅测试链接注入。六项、十二项原回归通过；十二个隔离改坏版本在指定断言失败。独立输入模型、调用路径、污染/重复/稳定性与范围限制见 [八项审查](../../docs/storage_redesign/f08_execution_20260925.md)；[结果与清理](../../test-results/storage-f08-20260925/README.md) 保存最终日志、XML、源码和哈希。

本轮首次建包，只保留修复尾部空槽、增强旧读者/版本预算后的版本；没有展开测试/构建/镜像或旧 F08 包。其他模块有效包未变。S5 最终页/checkpoint/回收、S8/S9 业务 E2E 尚未完成，不能把阶段测试作为共同性能基线。

2026-09-26 再次复审补强原 Mixed 的 lower/limit 判断，加入两个对应变异，仍为六个组件场景。旧 F08 两包已原位替换，无旧版备份；生产未改，六项/十二项原回归/十二个变异复验结果见 [审查 §10](../../docs/storage_redesign/f08_execution_20260925.md#10-再次复审范围扫描的漏检2026-09-26)。

2026-09-26 静态收尾仅删除两个未调用的 B 私有守卫成员，测试和 runner 未改。当前包 manifest 分别记录清理版 production_sha256 与最近实测 runtime_evidence_production_sha256；恢复说明区分两者。本次 Clang/GCC/格式检查通过，未重跑完整套件。旧包原位替换，无备份，见 [§11](../../docs/storage_redesign/f08_execution_20260925.md#11-静态收尾复核2026-09-26)。

## F09 恢复与风险交接

按包内 MANIFEST/RESTORE，在隔离源码树检出基准并恢复结果包 source/ 中三份当前生产修改，再在仓库外解压测试执行 runner。精确测试/生产/原 F08 依赖哈希与证据见 [结果](../../test-results/storage-f09-20260926/README.md)。完整链路测试接入后复用业务输入与独立 Oracle，不把本轮 Direct 文件场景自动升级为 E2E。F09 再次复审已增强原场景中的同一在途槽换代判据，4 项/原 F08 六项/8 个变异全部重新验证；源码和结果包原位替换，不保留旧 F09 包或备份。有效 F08 包仍作为真实回归来源保留，见 [复审 §10](../../docs/storage_redesign/f09_execution_20260926.md#10-再次复审2026-09-26)。

## F12 恢复与风险交接

包内仅最终六项集成测试、runner、MANIFEST.json 和 RESTORE.md。基准 `3e852f5` 加本轮五份生产修改，恢复以 production_sha256 为准；本地结果包 source/ 可覆盖隔离基准 checkout。原 S5/F34 包为同哈希依赖，直接复用而不复制或修改历史来源。

风险 `F12/reservation-lifetime、bounded-search、atomic-allocation、quarantine-race、failure-recovery、open-format` 由本模块维护。F13/F14/F22/F34 后续接入时复用输入/独立范围 oracle 并补真实引用/owner 条件，不另造每层位图测试。共同 C/P 内容没有改动，尚未经过新后端。

首轮发现隔离提交窗口的代码漏洞，先修生产逻辑再写测试。测试审查补强了失败申请的完整容量回收和无位图时的格式匹配判据；只保留最终版本，没有把先前漏检计为通过。首轮创建 F12 压缩包，本次复审已原位更新，不保留旧 F12 包或展开的常驻测试。源码/结果哈希及清理记录见结果目录。

F12 再次复审修正合法分配粒度的夹具限制，补强隔离被旧 snapshot 拒绝后的空间归还判据。生产未改，六项/十三项回归/十个指定变异重新通过，旧 F12 两包均替换，见 [§9](../../docs/storage_redesign/f12_execution_20260930.md#9-2026-09-30-再次审查隔离拒绝与测试粒度)。

## F13 恢复与风险交接

只保存最终 `F13-object-mapping/tests/mapping_test.cpp`、runner、MANIFEST.json 与 RESTORE.md。原 F12 六项由 include 原文件复用；F34/S5 原包不复制。源码树按基准 `954769f` 加结果包 source/ 恢复，并核对生产和依赖哈希。A 五项回归取原课程源码，不修改测试条件。

风险 `F13/identity、range-read、truncate-retire、floor-query、bounded-publication、atomic-recovery` 的场景、独立预期和局限见执行审查。未来 F14、F21、F26、F25/S11 接管适用风险，替换测试侧 ReadBody 为正式对象服务；共同 C/P 保持原内容，不复制每层同义测试。

前驱查询首版输入未触发左侧回退，已通过逻辑分析补强原场景，并验证删除回退的变异确实失败；没有保留先前漏检版测试/结果包。本轮首次创建 F13 包，只留最终版；有效 F12/F34/S5 包仍是回归来源。没有新增常驻 CMake/CTest 目标或 production 测试入口。

2026-10-02 F13 非零起点复审：原六项中的读取与恢复输入改以非零映射起点为主，保留两次必要零边界；同一生产哈希下三十项复验、九个定向变异通过。只保留修正后的 F13 源码与结果包，原包原位替换，无备份；原 F12/F34/S5 有效包不动。见 [审查 §9](../../docs/storage_redesign/f13_execution_20261002.md#9-非零起点覆盖调整2026-10-02)。


## F14 恢复与风险交接

只保留最终 F14-object-references.tar.gz；基准 1df3ba1 加结果包 source/ 本轮八份生产修改，核对 MANIFEST 精确生产和依赖哈希。六项新场景走真实 F14/F13/F12/B/Journal/Direct 文件及实际在途 IO；三十项原回归内容不变，八个指定变异检出。测试仍通过适配器执行受保护的范围，不是已完成的普通对象服务。

风险 `F14/range-lifetime、partial-reclaim、ownership-reuse、atomic-recovery、legacy-format` 在 S6 正式 ObjectIO、S7/F21/F26/S11 接入时接管，保留独立正文/容量 oracle，不复制各层同义矩阵。[八项审查](../../docs/storage_redesign/f14_execution_20261002.md) 与 [结果/清理](../../test-results/storage-f14-20261002/README.md) 记录全部证据。

首次创建本模块包，仅最终源码/结果各一份，无旧 F14 或展开常驻测试。F13/F12/F34/S5 的有效包为未改回归依赖，保留。共同 C/P 未改、未重跑。

## F19 / S7 恢复与风险交接

本模块仅保留再次复审后的最终源码包，旧 F19 包已原位替换，未展开到常驻测试目录。基准 `e24b5e6`，最终生产快照和本次完整运行的逐文件哈希见本地结果包和 MANIFEST；按包内 RESTORE 在隔离目录恢复。S6/F02、F12/F13/F14、F34、S5 六个有效档案为原样回归依赖，不复制测试正文，也不将它们当作错误旧包删除。

风险 `F19/handoff、object-order、atomic-publication、cow-content、capacity-lifetime、failure-recovery、automatic-reclaim` 由 S8/S9/S10/S11/S12/S13 正式业务接入时按职责接管；不为各层另建同义矩阵。[八项设计审查](../../docs/storage_redesign/s7_execution_20261002.md) 和 [结果/清理](../../test-results/storage-s7-common-20261002/README.md) 记录 56 项通过、12 个指定变异命中及局限。

正式 NodeStorage/S7/B/Direct 文件路径，没有 production 测试 hook。小尾部追加采用 COW，F01 的 IO 对齐不能证明原地尾部写的掉电隔离。当前证据不是业务 SQL/Raft E2E、裸设备/真实掉电或性能基线；共同 C/P 内容未改、未重跑。

F19 本次复审修复永久超限被无限重试的问题，并补强提交结果与恢复内容的一致性判据；10 项新集成＋46 项原回归、12 个变异完整重跑。F19 源码/结果包均原位替换，无旧包备份；原回归依赖保持不变。见 [审查 §9](../../docs/storage_redesign/s7_execution_20261002.md#9-再次复审进度保证与恢复判断2026-10-02)。

2026-10-04 F26/d 再次审查：修复批量准备失败遗漏帧收尾，原 RAM 场景增加确定性准备失败和全部帧再使用判据；本轮重跑页 6、课程/S8 19、TSan 页 6、页变异 11。基础 4/9 个变异未改沿用；源码与结果原位替换，旧页运行证据不再作为当前结果保留。详见 [复审 §9](../../docs/storage_redesign/s9d_execution_20261004.md#9-再次审查批量准备失败的完整收尾)。

2026-10-07 F25 提交前复查：文件快照分块读取复用既有定位检查，原 SnapshotStoreTest 9 项通过；前次 59 项/3 个变异沿用证据，未全部重跑。唯一包更新生产与原测试哈希，不增加模块测试；[跨模块去重审查](../../docs/storage_redesign/design_review_20261007.md)。

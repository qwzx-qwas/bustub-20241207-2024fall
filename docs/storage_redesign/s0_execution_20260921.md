# S0 / F00 执行与审查记录

后续状态（2026-09-22）：本文保留原运行事实。阶段小测试现已按用户要求压缩至[模块源码归档](../../test/archives/README.md)，旧路径和命令仅供在对应版本的隔离源码目录恢复使用；共同 C/P 套件及原结果包保留。

日期：2026-09-21（Asia/Shanghai）

[主方案](README.md) · [F00 方案及实现 prompt](modules/storage-contracts.md) · [压缩结果](../../test-results/storage-s0-20260921/README.md)

本文记录首次执行。当时测试已通过；后续审查发现负面用例存在校验和干扰及读取范围观察不足，已在 [提交前复审](s0_test_review_20260921.md) 修正并重新验证，形成独立归档。首轮日志及原始结果保持不变。

## 1. 完成范围

用户以“执行方案”授权执行已评审的 S0/F00。本轮完成 F00 §6.13：共同语义契约、现有接口兼容说明、确有生产消费者的最小范围类型，以及必要验证。S1–S13 没有在本轮实施。

代码基准为 `52e11ccf552ff527302aab3464038134da0e4e17` 加执行前已有的未提交改动；本轮也未提交。归档保存执行前后文件哈希、实际源代码快照和本轮差异，不能只用该 Git 提交号代表被测代码。

## 2. 实现及兼容审查

| 改动 | 作用与边界 |
| --- | --- |
| `src/include/storage/byte_range.h` | `StorageByteRange` 检查 uint64_t 字节区间溢出和子范围包含关系；不表示身份、所有权或 durable |
| `src/distributed/raft_state_machine.cpp` | `DecodeFile` 先验证输入 slice，再由父范围派生正文、长度字段和校验范围；保留 V1 格式、公开签名和拒绝损坏输入的异常类别 |
| `test/storage_redesign/storage_range_contract_test.cpp` | 5 项范围风险验证，真实文件、非空容器；与共同 C/P 负载隔离 |
| `test/CMakeLists.txt`、测试 README | 使用已有自动发现机制；增加 `storage-redesign` 用途标签及入口说明 |
| F00、主方案 | 更新当轮范围、已执行 prompt、完成状态、证据与下一步 |

算术审查：创建时先检查 `size <= max - offset`；子范围先检查相对偏移及剩余长度；解码游标只在所需范围验证成功后前移。这样随后的绝对偏移加法、剩余长度减法不会回绕。空范围允许表示 EOF，但不授权读取终点后的字节。

冗余清理：移除流式解码中被统一子范围检查覆盖的重复判断；固定字段短读复用已有 `ReadExact`。保留 catalog/session 大小上限、恰好 4 字节的 CRC 尾部规则和实际 CRC 检查。新类型的 4 个公开方法均有生产调用，没有添加仅供测试使用的生产接口。

没有改变现有 Append/Sync、Raft Store 的返回前持久化承诺、快照协议、默认生产路径或课程页格式。保留现有回归；本轮不存在因实现替代而失效、需要删除的旧测试。执行前哈希对比确认共同 `storage_acceptance` 测试内容与其他生产源文件未被本轮改写。

开源参考仍按 F00 §4 落在共同语义：Linux 的 IO/持久化区分、PostgreSQL 的日志先行、etcd/raft 的协议与 IO 边界。范围类型是本项目实现，本轮没有复制外部代码或引入依赖；设备、WAL 和 Raft 流水线能力不据此标记完成。

## 3. 测试审查与结果

运行前先审查 `pretest-review.md`，运行后核对实际结果：

| 测试 | 数量 | 保护的承诺 | 结果 |
| --- | --- | --- | --- |
| StorageRangeContractTest | 2 | 地址不能溢出；子范围不能越界；空尾部语义 | 通过 |
| SnapshotRangeContractTest | 3 | 不得越过授予的文件 slice；拒绝范围回绕与超大嵌套长度 | 通过 |
| 既有 BusTubRaftStateMachineTest | 4 | 独立 V1 golden、非零偏移解析、canonical 安装/后缀应用、损坏或错误边界拒绝且不替换原状态 | 通过 |
| 既有 SqlStorageContractTest | 1 | 实际 SQL 内容、长度边界和变长更新经重放/快照后保持 | 通过 |

**最终 10/10 通过**，Debug、Clang 14、ASan + UBSan + LeakSanitizer。没有新写重复的格式 golden 或业务恢复套件；没有断言内部调用次数、错误字符串、线程安排或第三方行为。新增测试中的零长度只验证范围的 EOF 定义，不作为空业务输入。

新头文件/测试通过 clang-format 检查及项目配置的 cpplint；变动生产函数按范围格式检查；生产变更行、新头文件与新测试通过定向 clang-tidy。构建使用项目 Debug `-Werror`。clang-tidy 日志中的依赖告警由过滤规则抑制，没有把它作为整个仓库零告警的证明。

### 3.1 启动故障的诊断与处理

最初 PIE 测试进程有时在测试枚举或进入 gtest 前段错误；沙箱内另出现 LeakSanitizer 的 ptrace 限制。离开沙箱运行后仍有一次 9/10、一个进程无正文输出的失败，因此没有把它算作全部通过。

用不含项目代码的 `int main() { return 0; }` 编译同样的 Clang 14 ASan/UBSan 程序，启动 3 次即复现 SIGSEGV；gdb 栈位于 `__sanitizer::internal_mmap` → allocator 初始化 → `__asan::AsanInitInternal` → 动态链接器初始化。该证据确认独立于本次项目业务代码的检测器启动问题，未据此推断所有潜在代码错误均可忽略。

只在本轮临时构建传入 `-DCMAKE_EXE_LINKER_FLAGS=-no-pie`，重新链接测试，并在允许 LeakSanitizer 工作的环境执行完整约定集合，得到最终 10/10。ASan、UBSan 和泄漏检测均保留；没有修改项目默认编译配置、关闭检测器或修改系统 ASLR。失败日志、空程序源码、诊断栈和成功结果一同归档。

### 3.2 可复现命令

在本轮实际源代码和相同构建依赖上执行；`S0_BUILD_DIR` 指向专用构建目录：

```sh
cmake -S . -B "$S0_BUILD_DIR" -G 'Unix Makefiles' \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_C_COMPILER=clang-14 -DCMAKE_CXX_COMPILER=clang++-14 \
  -DBUSTUB_SANITIZER=address,undefined -DCMAKE_EXE_LINKER_FLAGS=-no-pie
cmake --build "$S0_BUILD_DIR" \
  --target storage_range_contract_test raft_state_machine_test sql_storage_contract_test -j2
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
ctest --test-dir "$S0_BUILD_DIR" --output-on-failure \
  -R '^(StorageRangeContractTest\.|SnapshotRangeContractTest\.|BusTubRaftStateMachineTest\.|SqlStorageContractTest\.)' -j1
```

本机没有 Ninja，因此使用已安装的 Unix Makefiles。CTest 最终日志和 JUnit 在压缩包的 `evidence/ctest-nopie.log`、`evidence/results-nopie.xml`；配置、构建、lint、静态检查、运行前后审查也一并保存。

## 4. 证据和清理

归档入口为 `test-results/storage-s0-20260921/`，包含压缩结果、SHA-256 和阅读说明。压缩包保留日志、版本/哈希清单、被测 `src`/`test` 及构建支持快照；第三方依赖仍来自基准仓库。恢复时应在独立 checkout 中以快照替换对应目录，不能叠加基准中的旧测试目录导致重复注册。

核验压缩包可读和哈希后，清理本轮专用 `/tmp/bustub-s0-jmd4q3co` 下的构建、程序与中间数据；既有测试源码和此前基线归档保留。清理核对结果见归档入口的 `cleanup-verification.json`。

## 5. 结论边界与后续

本轮证明范围契约和受影响的既有快照/业务恢复路径通过定向验证。没有重跑长期性能基线，没有实现裸设备、WAL、异步队列、GC、增量快照或多提案流水线，也不把本轮结果解释为掉电安全或 FS 性能收益。

下一步是 **S1/F01 BlockDevice 方案讨论**：先明确首个后端和支持的设备能力、定位读写、对齐、短 IO/错误、缓冲生命周期及持久化屏障，再确定 F02 与最小预算/状态支撑的衔接和验收。详细接口与测试经讨论后再实施。

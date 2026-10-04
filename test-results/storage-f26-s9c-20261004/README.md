# F26 / S9.1c 真实页路径验证

2026-10-04；基线 `8e16074` 加本轮生产/构建修改，精确恢复以源码包 MANIFEST 和本地结果包 source/ 为准。[执行与八项审查](../../docs/storage_redesign/s9c_execution_20261004.md) · [共同协议](../../docs/storage_redesign/s9_buffer_pool_protocol.md) · [当前源码包](../../test/archives/F26-array-buffer-pool.tar.gz) · [校验](SHA256SUMS)。

## 结果

| 范围 | 不重复计数的结果 |
| --- | --- |
| 保留的 RAM 基础组件 | 4/4；ASan/UBSan/LSan、TSan |
| 新真实页/对象场景 | 5/5；ASan/UBSan/LSan、TSan |
| 原课程回归 | BufferPool 7、PageGuard 2、LRUK 1、DiskScheduler 1，共 11 |
| 原 S8 内容适配 | 8/8；正式 SQL、对象 Store、三节点 TCP/快照/重启 |
| 定向变异 | 8 个基础＋6 个页路径，全部由指定断言发现 |

共 28 个不同场景；并发检查、重复运行不增加场景数。b 中人工拼接目录/Arena 的身份场景已经删除，由真实 BufferPool 跨 root/middle、相同 suffix、非零页正文与换库场景替代。

## 本次复审和重跑（2026-10-04）

- 生产/构建文件与原 MANIFEST 的 33 项一致，没有新增生产改动。先复核锁、任务、IO 及正文寿命，再补强既有测试。
- 关闭场景改用真实 pread 退出与 Close 返回的事件顺序，并用正式缓存读确认已经停止接纳；删除排空等待的变异被准确检出（Close 序号 1，pread 退出序号 2）。不是靠短暂未返回推定安全。
- 修正稀疏文件 fixture 的日志清理名；runner 对课程执行量要求精确 7/2/1/1 且不跳过。
- 本次重跑 5 项页测试：ASan/UBSan/LSan 和 TSan 均通过；6 个页变异均断言失败。基础 4 项、课程 11 项、S8 8 项及基础 8 个变异沿用原归档证据，**本次没有重跑 28 项**。课程历史 XML 已核验符合收紧后的数量检查。
- 测试和结果均原位替换，清除已被本次替代的页测试旧运行记录；未修改模块的证据继续保留，来源由包内 SUMMARY/REVIEW 区分。完整八项复审见 [执行记录 §9](../../docs/storage_redesign/s9c_execution_20261004.md#9-再次审查关闭判据清理和方案状态2026-10-04)。

## 实际发现及修复

- root/middle 变异首次漏检：输入 suffix 也变化，测试无法区分中间层别名。改为相同 suffix、不同 prefix，两个改坏版本都由正文断言发现。
- 三节点测试曾 TIMEOUT/UNAVAILABLE。独立运行仍复现，临时诊断定位到 Snapshot Validate 创建新 A 工作区与后台 GC 更新 B 的竞争；不能全部归因于慢机器。B 增加明确的提交前视图冲突类型；Create/Common 刷新视图、重新计算并核验，语义冲突和不确定提交不盲目重试。修复后完整 S8 通过，原 TCP 按相同超时连续三轮通过。
- 初次新测试有编译/链接及关闭竞跑问题，均先修正，失败不计通过。ASan 曾在 WSL2 地址布局下启动退出 -11、无测试输出；只给测试进程使用 setarch，不改系统设置。运行环境/失败日志与最终通过记录分开保存。
- 最后清理删除无消费者的 PageIOCapabilities 分配单位字段，并保留原项目版权说明。TSan 覆盖清理后的可执行代码，随后补回的版权注释不改变语义；ASan 完整复验在最终树运行。

## 证明边界

对象 C++ 节点的 A 已经使用数组 BufferPool → 普通对象/Common → B/F02/Direct；文件部署和规范化快照构建仍有正式文件消费者。工作页不是业务 checkpoint，恢复仍由 Snapshot + Raft 决定。S8 测试只改测试侧部署/接口和失败诊断，原输入、断言、超时不变。

本轮是 Linux Direct 文件及真实本机 TCP。不是裸设备、物理掉电、非 Linux 或性能验收。没有重跑/修改共同 C/P，也不能从正确性通过推定性能收益。测试链接侧暂停/EIO 标明其模拟边界，不向 production 增加 hook。

## 归档与恢复

只保留一份 F26 源码压缩包和本目录的一份 `F26-results.tar.gz`；旧 `storage-f26-s9b-20261004/F26-results.tar.gz` 已删除，无备份。b 的文字审查作为历史记录保留并指向本目录。其他模块有效源码包原字节保留。

源码包进入 Git；结果包按现有 `.gitignore` 保存在本机，push 不会上传它，需要独立备份。结果压缩包包含最终源码/方案快照、源码包、日志/XML、命令、失败分析及校验，**不含**构建树、二进制、设备镜像和展开阶段测试副本。阶段测试不加入常驻 CMake/CTest。

# 测试入口

| 目录 | 用途 | 是否作为新旧存储改造的共同基线 |
| --- | --- | --- |
| [storage_acceptance](storage_acceptance/README.md) | 新旧系统共用的真实链路正确性及性能对比：C1–C5、P1–P4 | 本轮旧系统观测已归档；P2 超时保留、P3 长期空间稳定性未覆盖，见最新执行记录 |
| [storage_redesign](storage_redesign/README.md) | 模块风险与阶段验证的说明入口；当前 S0 小测试已压缩 | 否；后续能力优先接入共同 E2E |
| [archives](archives/README.md) | 按模块压缩的 F00 / T0 阶段测试源码、清单和恢复说明 | 否；不参与常驻发现或执行 |
| [legacy_raft](legacy_raft/README.md) | 归档的既有 Raft/复制/恢复回归，以及旧探索测量工具 | 否；仍保留现有回归门禁 |
| [support](support/README.md) | 两套测试共用的进程管理、链路代理及丢响应工具 | 工具不单独代表系统测试通过 |
| 其余课程目录 | 既有课程组件、SQLLogic 等测试 | 保持原职责，本轮没有整体迁移课程测试 |

本轮重构长期主线是共同生产 E2E 正确性／性能套件及其实际工具，显式构建、显式运行，不加入默认 CTest。2026-09-22 按用户要求，S0 范围、单节点 SQL 回归、Python 工具自检和 Go 人工历史测试均按模块压缩，退出常驻测试入口；源码归档的逐文件哈希和恢复说明见 `archives/`。已有课程及 `legacy_raft` 保持原职责；其历史“归档”仍是源码分类，不与本次阶段压缩混淆。

共同测试的执行、观测限制和归档见 [最新执行记录](../docs/storage_redesign/testing_execution_20260921.md)。模块验证须说明自身承诺和未覆盖能力，不能把定向通过扩展成三节点/掉电/性能结论。

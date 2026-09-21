# 测试入口

| 目录 | 用途 | 是否作为新旧存储改造的共同基线 |
| --- | --- | --- |
| [storage_acceptance](storage_acceptance/README.md) | 新旧系统共用的真实链路正确性及性能对比：C1–C5、P1–P4 | 本轮旧系统观测已归档；P2 超时保留、P3 长期空间稳定性未覆盖，见最新执行记录 |
| [storage_redesign](storage_redesign/README.md) | S0–S13 模块新增行为与风险的必要验证 | 否；不替代共同业务对比内容 |
| [legacy_raft](legacy_raft/README.md) | 归档的既有 Raft/复制/恢复回归，以及旧探索测量工具 | 否；仍保留现有回归门禁 |
| [support](support/README.md) | 两套测试共用的进程管理、链路代理及丢响应工具 | 工具不单独代表系统测试通过 |
| 其余课程目录 | 既有课程组件、SQLLogic 等测试 | 保持原职责，本轮没有整体迁移课程测试 |

“归档”表示按用途隔离源码，不表示关闭仍有价值的回归测试。旧 C++ 目标名称保持，CTest 增加 `legacy-regression` 分类；新 E2E 套件显式构建、显式运行，不加入默认 CTest。`storage_acceptance/sql_storage_contract_test.cpp` 是本轮缺陷的定向恢复回归，单独注册为 CTest，不启动三节点。

共同测试的执行、观测限制和归档见 [最新执行记录](../docs/storage_redesign/testing_execution_20260921.md)。模块验证须说明自身承诺和未覆盖能力，不能把定向通过扩展成三节点/掉电/性能结论。

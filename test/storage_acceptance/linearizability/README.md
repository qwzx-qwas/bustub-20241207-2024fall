# 单键业务历史检查器

本项目模型：一行完整业务数据、Bump、ReadPoint、请求身份去重及重试结果稳定性。

[Porcupine v1.3.0](https://github.com/anishathalye/porcupine/tree/v1.3.0) 只提供历史搜索和可视化；本目录不复制其数据库模型。`go.mod`、`go.sum` 固定依赖，MIT 许可保留在 `LICENSE.porcupine`。模块提交为 `55508eb201b314c218d7e8412c3ea4b9499a5f53`。

`history.py` 从未删减的业务尝试生成 `linearizability.json`。未知写（包括可能先追加后失去 Leader 身份的 NOT_LEADER）保留原始响应记录；检查时将允许生效的上界延至观察结束，允许没有生效或恰好生效一次。原身份重试不增加第二次。用每次尝试独立的展示 ID 表示重叠，生产 client/request 身份保留在输入内。搜索不使用 Raft index 排定业务顺序。

最多 128 次业务网络尝试；搜索 30 秒，进程包含输入与可视化最多 45 秒，独立观测 RSS 上限 1 GiB，`GOMEMLIMIT=768MiB`。结果为 `Ok`、`Illegal`、`Unknown`；Unknown、超预算、输入不完整不能通过。Ok 还需与有效工作量、并发和故障证据一起判定。

构建（不会执行测试或启动节点）：

```bash
go build -C test/storage_acceptance/linearizability -mod=readonly -p=1 -o /tmp/bustub-check-history .
```

2026-09-22：`model_test.go` 的七份手写历史已按用户要求移入 [T0 源码压缩归档](../../archives/README.md)，不再常驻本目录。真实 C2/C3 使用的 `main.go`、依赖和模型保持不变；手写业务 oracle 仍是 E2E 必需组件，不等于模拟数据库。需要复核模型自检时，在记录版本的隔离源码目录恢复、审查并运行，不能将当前目录下“没有测试文件”当作自检通过。

本轮编译使用临时 Go 1.27.1 工具链，官方 Linux amd64 压缩包 SHA256：`63d339f0da5ab53635a56f2490a7984dfe12dfcff22ad749f63edaf590168445`。本环境可用 `/tmp/bustub-t0-toolchain/go/bin/go` 代替上述 `go`，依赖缓存位于 `/tmp/bustub-t0-gopath`（通过 GOPATH 指定）。没有安装到系统路径或修改生产依赖。编译成功不是模型样例运行通过。

2026-09-20 追加复审：上述七份模型历史已实际运行并通过，结果见 [A/B 审查](../../../docs/storage_redesign/testing_review_ab.md)。三节点历史仍未运行；不能由模型样例通过推出真实集群通过。

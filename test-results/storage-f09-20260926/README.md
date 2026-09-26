# F09 / 元数据页写回复审（2026-09-26）

[方案](../../docs/storage_redesign/modules/metadata-page-writer.md) · [执行及八项审查](../../docs/storage_redesign/f09_execution_20260926.md#10-再次复审2026-09-26) · [源码包](../../test/archives/F09-metadata-page-writer.tar.gz)

## 本轮结论

先复核生产逻辑，未发现新增生产错误，三个生产文件与上一轮实测哈希完全一致。发现原并发场景可能暂停根页，不能确定覆盖同一在途槽换代。已增强原场景：明确暂停溢出页，捕获真实页号/代次，删除 160 条并插入 320 条后验证同一页号确实成为新代次存活页；并行完成计数从暂停握手处重置。未新增 production API、getter 或测试用例，只增强原场景。

**本轮重新运行 4 项 F09、6 项未修改的 F08 回归，全部通过；8 个隔离变异全部编译/链接成功，并失败于预定断言。** 正常场景 ASan/UBSan 未报告错误。

| 场景 | 结果 | 执行耗时（非性能指标） |
| --- | --- | --- |
| CanonicalPagesAndReopenWriteback | 通过 | 7.374s |
| UncommittedChangesStayOutOfPageRegion | 通过 | 2.285s |
| ReusedSlotsKeepNewerWorkPending | 通过 | 2.201s |
| FailuresRetainWorkAndCloseDrains | 通过 | 2.331s |

F09 合计约 14.191s；F08 合计约 215.566s。完整生产库在本轮重新构建。生产 GCC/cpplint/tidy 证据按不变哈希沿用，置于 prior-production-checks/，不计作重跑。测试格式本轮显式使用仓库配置核验；review-checks.json 记录区分。原 A 十二项与共同 C/P 未重跑，无新性能结论。

实际命令：

```bash
python3 /tmp/bustub-f09-review-20260926/tests/run_stage.py --repo /home/qwzx/projects/bustub-20241207-2024fall --work /tmp/bustub-f09-review-20260926 --mutations
```

基准 `b043695796b342b1e523e61ae440bcc864058e7d` 加 manifest 中三份生产修改；source/ 保存精确源码。命令/退出码见 commands.json，XML/日志见 xml/、logs/，变异判据见 mutations.json，原 F08 依赖哈希见 regression-provenance.json。

## 证据限制

真正经过 B→F05→F04→F02→F01 Direct 文件链路，独立解析 Metadata 区；暂停/EIO 仅测试链接侧。不是裸设备/掉电证明、checkpoint 恢复或 SQL/Raft E2E。Open 仍从完整 Journal 恢复，F09 不授权裁剪 Journal。Close 的有限时间观察有调度局限，已保留对应实际变异检验。

## 归档与清理

- 修正版源码包 11,155 字节，SHA-256 `f64fb7a55b4f13db64673adfce9846091b3638ed240f425821991dd7f5620913`，随仓库保存。
- 本地结果包 38,828 字节，SHA-256 `3ee641dbdfd08bcdc08963fd8f83eec7b43b7a9e20a51019f0cb3f2b57468bb6`，按既有 .gitignore 忽略，clone 不含该结果包。
- 两包已原位替换、逐成员字节核验；旧 F09 包不留备份，不保留旧测试或旧 runner。历史静态生产证据只对应未改代码，不混入旧测试运行结果。
- 不含二进制、设备镜像或构建缓存。有效 F08 包作为回归依赖保留，其他有效包与共同 C/P 不变；问题 F01 包仍不存在。
- 核验后删除本轮展开测试和构建目录；最终记录见 cleanup-verification.json。

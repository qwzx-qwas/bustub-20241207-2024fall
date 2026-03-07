SortKey 定义在 execution_common.h (line 27) ，本质上是 std::vector<Value>。它表示“一个 tuple 用来参与排序的键”。因为 ORDER BY 可能不止一列，比如 ORDER BY a ASC, b DESC，所以不能只用一个值，而是要把每个排序列对应的值按顺序收集起来，组成一个 key。这个 key 是“比较依据”，不是完整数据。

SortEntry 定义在 execution_common.h (line 29) ，本质上是 std::pair<SortKey, Tuple>。意思是把“排序键”和“原始 tuple”绑在一起。原因很直接：排序时按 SortKey 比较，但最后输出或者写回 run 的是完整 Tuple，所以需要同时保存“怎么比”和“真正的数据”。你可以把它理解成“排序记录项”。

TupleComparator 定义在 execution_common.h (line 32) ，它是 SortEntry 的比较器，真正决定两个记录谁排前面。它内部保存 order_bys_，然后在实现里按每一列依次比较两个 SortKey 里的值：前一列相等就继续看下一列；如果遇到不等，就根据该列是 ASC 还是 DESC 决定返回结果。实现可以看 execution_common.cpp (line 24)。

整体配合关系就是：

GenerateSortKey(tuple, order_bys, schema) 从 tuple 里提取排序列，生成 SortKey。
把它和原 tuple 组成一个 SortEntry。
std::sort(..., TupleComparator) 或归并时用 TupleComparator 比较两个 SortEntry。
排序依据是 key，最终保留下来的是 tuple。
所以这套设计的核心意义是：把“排序依据”和“原始数据”分离，但又绑定在一起，方便排序执行器统一处理。






select v2 from t1 order by l2_distance(v1, ARRAY [1.0, 1.0, 1.0]) limit 2;的主链路：

流程是：
1.入口： bustub_instance.cpp (line 254) 的 ExecuteSqlTxn 进去。
Binder binder(*catalog_)
binder.ParseAndSave(sql)
statement = binder.BindStatement(stmt)
Planner planner(*catalog_); planner.PlanQuery(*statement);
Optimizer optimizer(*catalog_, IsForceStarterRule());
optimized_plan = optimizer.Optimize(planner.plan_);
execution_engine_->Execute(optimized_plan, &result_set, txn, exec_ctx.get());

2.Binder阶段
目标：把 SQL 变成绑定后的 AST。

关键点：

t1 会被绑定成 base table ref
v2 / v1 会绑定到 t1 的 schema 列
ARRAY [1.0, 1.0, 1.0] 会被解析成内部函数 construct_array
l2_distance(...) 会被绑定成函数调用
ORDER BY 和 LIMIT 2 会挂在 SelectStatement 上

3.Planner阶段
目标：把绑定后的 AST 变成执行计划树。

这条 SQL 理论上的初始计划大概是：

SeqScan(t1) 读出表
一个 Projection 之类只保留 v2
Sort，排序键是 l2_distance(v1, ARRAY [...])
Limit(2)


4.Optimizer阶段：
目标：把 Limit(Sort(...)) 改写成 VectorKnnScanPlanNode

你的规则在：
vector_knn_scan.cpp
调用入口在 optimizer_custom_rules.cpp (line 17)
它现在做的是：

递归优化子计划
如果当前节点是 Limit
child 是 Sort
Sort 只有一个 ORDER BY
该排序表达式是 VectorDistanceExpression
就重写成：VectorKnnScanPlanNode(output_schema, sort_child, distance_expr, limit)

也就是说优化后的计划应类似：
VectorKnnScan(child=原Sort的child, distance_expr=l2_distance(...), k=2)

这里的数据流变化是：
原本 SortExecutor 负责算距离和排序
现在变成 VectorKnnScanExecutor 负责从 child 拉 tuple、算距离、排序、取 top-k


5.Execution阶段：
执行器工厂在 executor_factory.cpp (line 55)

如果 plan type 是 VectorKnnScan，会构造：

VectorKnnScanExecutor
执行数据流如下：

VectorKnnScanExecutor::Init() 先创建 child executor
child_executor_->Init()
循环 child_executor_->Next(&tuple, &rid)
对每个 tuple 用
plan_->GetDistanceExpr()->Evaluate(&tuple, child_schema)
计算距离
把 (distance, tuple, rid) 放进 candidates
全部读完后排序
取前 k
Next() 再逐个吐出结果 tuple

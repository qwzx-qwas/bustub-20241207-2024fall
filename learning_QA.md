# 查询/DML链路：
sql string

## -> 

PG Parse tree(PostgreSQL 特定实现里的内部树)

## -> 

进行sql语句类别分类，分为Create/Select/Index/Delete /.... 

## -> 

binder 把PG AST 变为bustub自己的语义对象，对于Select来说这个语义对象就是SelectStatement:
 他把
 FROM 绑定成 table_
 WHERE 绑定成 where_
 GROUP BY 绑定成 group_by_
 HAVING 绑定成 having_
 LIMIT/OFFSET 绑定成 limit_count_ / limit_offset_
 ORDER BY 绑定成 sort_
 然后一并塞进SelectStatement

 select,create，insert，index,delete,update等使用BindStatement分发到对应的绑定函数，他的输入是parser的statement AST节点，输出是BusTub自己的“已绑定语句对象”

binder把函数调用变为 BoundFuncCall（普通函数） / BoundAggCall（聚合函数） / BoundWindow（窗口函数）等语义对象


CREATE TABLE / CREATE INDEX / EXPLAIN / SET / SHOW / TRANSACTION等语句会走另外的分支，在binder阶段先产出BoundStatement,然后按statement->type分流处理，不走planner主链路

## ->

planner将binder产出的语义对象翻译成执行层中间表示，它分为两类：
BoundStatement -> PlanNode tree：执行计划树的骨架，表示做什么算子
BoundExpression -> AbstractExpression tree：算子内部用来算值/判条件的表达式
以及某些特殊的表达式，它不会只落成expression，而是会“抬升”为专门的算子（因为他们时会影响算子形态的表达式）：
BoundAggCall -> AggregationPlanNode（所有BoundAggCall先被planner收集到context，再统一生成AggreagtionPlanNode）
BoundWindow -> WindowFunctionPlanNode


Q:
PlanNode称为“做什么算子”而“AbstractExpression"却被称为”算子内部用来算值/判条件的表达式树“,我对此有点疑惑：我原以为算子是能够执行某些功能的表达式，类似于函数，我能够理解select等确实能执行某种功能，但为什么他们不是某种表达式，同样，为什么能执行某些功能的函数不能作为算子，而是放到表达式树中

A：
算子处理"一批"tuple的流动方式
表达式处理的是"一条"tuple上某个值怎么计算


DDL语句不会进入planner


## ->
planner产出初始plan后，optimizer再按规则优化plan

这个阶段也是将seqscan转换为indexscan的阶段:
做模式匹配（目前支持的功能有限）：
当前节点必须是SeqScan且它必须带谓词
谓词必须长得像column = constant 或 constant = column，即看谓词是不是固定的列对应常数，常数对应列
还支持col = 1 OR col = 2 OR col = 3 这种，将他们转化为谓词树进行递归处理左右子树
然后去catalog中找这个表的索引
如果发现正好有就把SeqScanPlanNode 改写成 IndexScanPlanNode


## ->
ExecutorFactory根据已经确定好的PlanNode tree,递归地实例化对应的Executor tree

## ->
接下来就是执行Executor，然后不断Next()，这里是pull model，上游executor主动拉下游的数据
注意：
ExecutionEngine是从plan到execute的中间角色:
它接收最终plan并创建root executor
然后调root executor的Init()
再循环调Next()，把tuple一条条拉出来，
期间是try catch来捕获ExecutionException
如果有捕获到就清空result集


## ->
最后就是结果整理和输出了(fmt层)
由BusTubInstance::ExecuteSqlTxn(...)（其实ExecutionEngine也是它调用的）接入结果集，
接着先去取schema（ planner.plan_->OutputSchema();）
根据schema将每个tuple的内部格式转化为人看到的文本
再通过ResultWriter完成最终输出（他会开始一张结果表，进入header区域，接着一列列写列名，接着一行行写，直到完成）
writer提供多种输出接口，但决定用哪种接口取决于上层根据语句类型来选择


# 注意在功能完成时需要将文件在对应模块的CMakeLists.txt中声明



# DDL/特殊控制语句分支链路：
SQL string

## ->
PG AST

## ->
binder阶段变为对应的BoundStatement

## -> 
BusTubInstance::ExecuteSqlTxn 按 statement->type_ 分流，
直接进入对应的handler：
CREATE_STATEMENT -> HandleCreateStatement
INDEX_STATEMENT -> HandleIndexStatement
VARIABLE_SHOW_STATEMENT -> HandleVariableShowStatement
VARIABLE_SET_STATEMENT -> HandleVariableSetStatement
EXPLAIN_STATEMENT -> HandleExplainStatement
TRANSACTION_STATEMENT -> HandleTxnStatement
这些分支后面跟着continue，表示到这里就处理完了，不会再往下走planner那条主链路了

CREATE TABLE：要改 catalog，创建表
CREATE INDEX：要建索引对象并注册
SET / SHOW：要改系统变量或读系统变量
TRANSACTION：要切换事务状态
EXPLAIN：要把 plan 打印出来

## ->
看handlre做什么，要分类看：

对于CREATE TABLE:
加锁
调用catalog_ -> CreateTable()
有主键就调用catalog_->CreateIndex()来创建主键索引
用WriteOneCell来返回建表消息给write



对于CREATE INDEX:
从stmt(就是IndexStatement)中找出col_ids
构造key_schema
根据index_type来选择是去hash,bplustree,ivfflat还是默认分支
接着对对应的类型调用catalog_->CreateIndex()或catalog_->CreateIVFFlatIndex()
最后把索引创建成功的信息传给write


走index plan时，真正执行索引查找发生在executor中：
普通索引扫描
比如 IndexScanPlanNode，executor 一般会调用 index 的 ScanKey、迭代器，或者别的点查/范围查接口。

向量索引扫描
比如 VectorIndexScanPlanNode，executor 调的是 SearchKnn。
......




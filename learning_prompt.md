# 任务：
## 目标：
为当前基准版本增添向量数据库功能，使他支持:
精准ANN功能
向量距离表达式l2_distance,cosine_distance、ip_distance
基于IVFFlat的近似路径

## 输入
单表，vector类型，sort+limit,可以指定向量距离算法
暂不支持多表
支持`ORDER BY <vector_distance_expr> [ASC|DESC] LIMIT k` 的执行；其中只有单排序键且 ASC/默认时才可能触发索引改写

## 输出
按语句正常输出

## 成功路径
对于精准knn查询：
输入一个sql语句
先拆成sql string解析为PG Parse tree
再对sql语句进行分类
接着binder把sql语句变成对应的语义对象
而后在planner生成plan
这个plan在optimizer阶段走topn或命中索引后就改写成VectorIndexScanPlanNode
ExecutorFactory调用最终生成好的plan进行executor
在executor中要完成逐条计算距离，排序并取top-k

topn优化：
对于非向量输入，支持Sort+Limit -> TopN
在正常流程走到optimizer时检查是否存在Limit(Sort(child))
有就进入topn

IVFFlat索引查询：
当前optimizer先识别Limit(Sort(SeqScan)) + vector_distance的特定模式
再检查索引列和metric是否匹配
命中才改写为VectorIndexScan

## 失败路径:
非法输入：抛异常
不满足rewrite条件： 就fallback，按原plan
DDL成功信息：writer输出

## 边界：
处理类型不匹配问题
要求输入到向量距离算法中的是两个等维度vector
目前只支持单表的vector查询
向量维度不一致
NULL输入
cosine分母为0
topk中的输入的k为0
空表/空索引
IVFFlat中的一次查询簇数nprobe > 总簇数nlist
不满足改写plan的条件就fallback
索引改写场景目前只支持`ORDER BY <vector_distance_expr> ASC LIMIT k` 的knn查询
（即 ORDER BY 只有一个排序键，且只能 ASC 或默认）
用户输入的metric不匹配目前已有的metric就不rewrite plan
nlist / nprobe 要是正整数
nprobe 要裁剪到 nlist
metric 只允许 l2 / cosine / ip

## 什么算完成：
每完成一个阶段就写一份测试，跑通才能去下一个阶段
本次任务分为三个阶段：
SQL logic test：向量表达式、KNN SQL、rewrite
storage/index test：IVFFlat 构建、插入、删除、metric、multi-probe
explain / plan-level 验证：确认是否真的走了 VectorIndexScan 或 TopN
目标当我输入正确的sql语句能返回正确的答案





# prompt:
以下 prompt 为最终生效规范；若与上文“任务/目标/边界”的摘要表述冲突，以本节为准。
在当前基准版本需要添加：
精准ANN功能
向量距离表达式l2_distance,cosine_distance、ip_distance
向量索引类型
IVFFlat功能
打通从语法输入，注册到 catalog / metadata / type system / factory，到计划生成，执行注册，可观测输出，构建连线的工程链
查询链：sql -> binder ->planner -> optimizer -> execution factory -> executor

建索引：sql -> binder -> HandleIndexStatement -> DDL/catalog -> create/build index
完善其中的index,DDL,
注册对应的factory等，使得执行期能正常查找到对应的executor
补充对应的输出方法
将新添加的参与构建的文件在对应模块的CMakeList中声明



## 精准KNN功能 & 向量距离表达式
新增三种向量距离表达式
对候选向量进行遍历查找，返回最小的几个向量。TopNExecutor 使用堆数据结构，堆顶为堆中满足条件但是情况最差的一个。当前阶段 exact path 的最低可行实现以 Sort/Limit -> TopN 为主；VectorKnnScanExecutor 若实现，只作为可选增强，不是本阶段必须路径。

此时可阶段性写测试验证

## topn优化
先尝试向量化索引改写为OptimizeVectorKnnScan：Limit(Sort(child)) + sort的order_by是“一个”VectorDistanceExpression + child是SeqScan + 没有filter + 单表 + 一边为常量（目前先保守写法）

否则非向量输入，走OptimizeSortLimitAsTopN，支持Sort+Limit -> TopN
在正常流程走到optimizer时检查是否存在Limit(Sort(child))
有就进入topn

对于向量化输入

## ivfflat
iVFFLat是利用分桶作为索引功能，来大幅度减少精准knn的遍历搜索的时间复杂度

建立索引：
如果在sql语句指明using ivfflat,
那么到Index时，会识别用户所要创建的是IVFFlat索引，
然后解析nlist,nprobe,metric等参数
构造对应的metadata，catalog信息
CREATE INDEX 属于 DDL/特殊语句分支：binder 产出 IndexStatement 后，直接进入 HandleIndexStatement，不经过 planner/optimizer/executor 主查询链。

查询：
在optimizer进行判断是否要走ivfflat优化：
识别Limit(Sort(child)) + vector_distance
检查是否有匹配的单列vector
检查metric是否与之匹配
如果都满足，就说明命中了，optimizer 会把原plan改写为 VectorIndexScanPlanNode
然后在 execution factory 创建并调用 VectorIndexScanExecutor
executor 会先通过统一接口 index->SearchKnn() 拿到一批候选 RID
（因为这个 index 的实际类型是 IVFFlatIndex，所以底层会调用 IVFFlatIndex::SearchKnn()）
但 executor 不会拿到 RID 就直接返回，而是会进一步回表
按当前事务视角做 tuple visibility / MVCC 检查，必要时重建可见版本
如果 plan 中还带有 filter predicate，则会继续对可见 tuple 做谓词判断
只有同时通过最近邻命中、可见性检查和可选谓词过滤的 tuple，才会作为最终结果返回
当前阶段的语义约定：index path 先向索引请求至多 k 个候选 RID，再对这些候选做可见性和可选谓词过滤；过滤后结果条数可以少于 k，不要求继续补召回。


一些细节：
需要完成在建桶环节的选取预训练数据，进行训练中心向量，正式分桶逻辑的解耦

支持nlist/nprobe/metric选项
支持InsertEntry / DeleteEntry / SearchKnn(第一阶段可以做个简单的遍历，后续再调整)
支持metric影响

## 更细节补充：
### 基准版本
基准版本已经具备：VECTOR(n) 列类型建表能力、普通 CREATE INDEX DDL 分支、Sort+Limit -> TopN 的空壳优化入口。

基准版本尚不具备：向量距离函数注册、向量距离表达式执行、向量索引接口、IVFFlat 索引类型、向量 KNN 的优化器改写、对应 executor 注册。

本任务是在基准版本上“补齐缺口”，不是重写整条 SQL 主链。

### 支持范围
l2_distance / cosine_distance / ip_distance 可用于投影和排序；
支持单表、单个向量列；
支持 ORDER BY <vector_distance_expr> [ASC|DESC] LIMIT k 执行；
不支持多表 KNN 语义优化。

### rewrite范围
仅当命中 Limit(Sort(SeqScan))；
ORDER BY 只有一个键；
排序键为一个 VectorDistanceExpression；
顺序必须是 ASC 或默认；
SeqScan 无 filter；
一边是本表向量列，另一边是不含列引用的 query expression；
存在单列向量索引且 metric 匹配；
才允许改写成 VectorIndexScanPlanNode；
否则必须 fallback 到原计划，再由通用 TopN 规则处理。


### 三个向量距离函数细节
l2_distance(a, b)：返回欧氏距离，值越小越近。
cosine_distance(a, b)：返回 1 - cosine_similarity(a, b)，值越小越近；若任一向量范数为 0，当前阶段返回固定距离 1.0，不抛异常。
ip_distance(a, b)：为了兼容 ORDER BY ... ASC 的 KNN 形式，返回 -dot(a, b)，这样内积越大，距离值越小，排序方向无需反转。

### 把exact path 和 index path 分层

精确路径的最低可行实现：向量距离表达式先算出来，SQL 仍走 Sort/Limit -> TopN。
VectorKnnScanExecutor 若实现，只作为专用 exact path 的可选增强，不是本阶段必须路径。
近似路径：只有索引命中时，优化器才把计划改成 VectorIndexScanPlanNode，再由 VectorIndexScanExecutor 调 index->SearchKnn()。

### DDL链路
CREATE INDEX 属于 DDL/特殊语句分支：binder 产出 IndexStatement 后，直接进入 HandleIndexStatement，不经过 planner/optimizer/executor 主查询链。
SELECT ... ORDER BY vector_distance ... LIMIT k 才走 planner/optimizer/executor 链。


### ivfflat约束
仅支持单列 VECTOR 索引。
WITH 选项只允许 nlist、nprobe、metric。
nlist、nprobe 必须是正整数。
nprobe > nlist 时裁剪到 nlist。
metric 只允许 l2 / cosine / ip。
建索引时需要从已有表数据构建初始索引。
索引创建后新增插入也必须维护索引。
SearchKnn() 是索引统一对外接口，executor 不应直接依赖 IVFFlatIndex 具体实现细节。

### 测试目标
表达式层：三种距离函数的返回值、维度不一致、NULL、零范数 cosine。
SQL 层：ORDER BY distance LIMIT k 的正确结果；投影中直接输出距离值。
优化器层：命中时 EXPLAIN/检查项确认走 VectorIndexScan；不命中时确认 fallback 到 TopN 或原计划。
存储层：IVFFlat 的 build-from-existing-tuples、insert after create、metric、生效的 nprobe、SearchKnn。
一致性层：删除/更新后查询结果仍符合可见性语义。
这会比“返回正确答案”更抗跑偏。



------------------------------------------------------------
更多信息可参考/mnt/d/cmu15445/bustub-20241207-2024fall/learning_QA.md

以我提供的信息为主，但是也必须参考成熟的向量数据库架构来完成
 
以我提供的信息和当前仓库接口约束为主。
可以参考成熟向量数据库的设计思路，但不要引入超出当前 BusTub 架构边界的复杂机制。
若外部设计与当前仓库约束冲突，以当前仓库接口、事务语义和执行模型优先。

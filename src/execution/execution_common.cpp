//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_common.cpp
//
// Identification: src/execution/execution_common.cpp
//
// Copyright (c) 2024-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/execution_common.h"

#include "catalog/catalog.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value_factory.h"

namespace bustub {

TupleComparator::TupleComparator(std::vector<OrderBy> order_bys) : order_bys_(std::move(order_bys)) {}
// 排序时比较两个元组的排序键，按照 order_bys_ 中指定的列和顺序进行比较
auto TupleComparator::operator()(const SortEntry &entry_a, const SortEntry &entry_b) const -> bool {
  const auto &key_a = entry_a.first;
  const auto &key_b = entry_b.first;
  size_t n = order_bys_.size();

  for (size_t i = 0; i < n; i++) {
    const auto &val_a = key_a[i];
    const auto &val_b = key_b[i];
    const auto &order_by = order_bys_[i];

    if (val_a.CompareEquals(val_b) == CmpBool::CmpTrue) {
      continue;
    }

    if (order_by.first == OrderByType::DESC) {
      return val_a.CompareGreaterThan(val_b) == CmpBool::CmpTrue;
    }

    // Default and ASC
    return val_a.CompareLessThan(val_b) == CmpBool::CmpTrue;
  }
  return false;
}

auto GenerateSortKey(const Tuple &tuple, const std::vector<OrderBy> &order_bys, const Schema &schema) -> SortKey {
  SortKey key;
  key.reserve(order_bys.size());
  for (const auto &order_by : order_bys) {
    key.push_back(order_by.second->Evaluate(&tuple, schema));
  }
  return key;
}

/**
 * Above are all you need for P3.
 * You can ignore the remaining part of this file until P4.
 */

/**
 * @brief Reconstruct a tuple by applying the provided undo logs from the base tuple. All logs in the undo_logs are
 * applied regardless of the timestamp
 *
 * @param schema The schema of the base tuple and the returned tuple.
 * @param base_tuple The base tuple to start the reconstruction from.
 * @param base_meta The metadata of the base tuple.
 * @param undo_logs The list of undo logs to apply during the reconstruction, the front is applied first.
 * @return An optional tuple that represents the reconstructed tuple. If the tuple is deleted as the result, returns
 * std::nullopt.
 */
//  元组模式（Schema，表长什么样）、
//  基础元组（Base Tuple，表堆中的最新版本）
//  及其元数据（包含is_delete，表示当前是否被删除)（两者均存储在表堆中），
//  以及一个按修改时间从近到远排序的撤销日志（Undo Logs）列表

//  Undo Log表示“我这一步，改了哪些列，原来是什么”
//  modified_fields_ —— 改了哪几列，长度是 schema 的列数，bool值表示该列是否被修改
//  tuple_ —— 被改列的“旧值”
//  is_deleted_ —— “在那次修改之前，这个元组是否存在”

//  要做的是：从现在的Base Tuple开始，逆序应用这些Undo Log，最终得到“在某个时间点，这个元组是什么样子”
//  （输入的Undo Log实际上是该给定时间戳之后发生的所有修改）

//  再次提醒： 在 ReconstructTuple 中，你不需要使用甚至不需要检查时间戳（ts_）字段
//  和前一版本（prev_version_）字段。prev_version_ 应当由 ReconstructTuple 的调用方使用，
//  用于确定哪些 UndoLog 应该放入输入向量中。

// 根据modified_fields_从原始Schema中抽出一个Partial Schema,用它去读Undo Log中的tuple_

// 注意ReconstructTuple的主体是一条tuple的各列，而不是表中的若干行列
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  std::vector<Value> current_values;
  // 表示该元组在当前最新版本中的状态
  bool exists = !base_meta.is_deleted_;
  if (exists) {
    for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
      current_values.push_back(base_tuple.GetValue(schema, i));
    }
  } else {
    // 如果 base 是删掉的，数组填空占位，exists 为 false
    // 一个一个列地填充 Null 值，使其长度合法
    for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
      //用ValueFactory生成对应类型的Null值
      current_values.push_back(ValueFactory::GetNullValueByType(schema->GetColumn(i).GetType()));
    }
  }

  // 2. 正序遍历：从最新到最旧应用 UndoLog (这是剥洋葱)
  for (const auto &log : undo_logs) {
    //倒带操作：如果遍历到某个 undo_log 时，
    // 如果 log.is_deleted_ 为 true，
    // 意味着在这个 undo_log 所代表的时间点（即回滚操作应用之后），
    // 该元组变成了“已删除”状态。
    // 那就让当前版本的 exists 标记为 false（完成倒带），表示该元组在那个时间点不存在
    if (log.is_deleted_) {
      exists = false;
      continue;
    }
    // 如果 log 不是 deleted，说明在这个日志的时间点，元组是存在的
    exists = true;
    // 提取日志中的部分列，覆盖到 current_values 中
    // 遍历该undolog中的modified_fields_，发现哪些列曾经被改过
    uint32_t partial_idx = 0;
    std::vector<uint32_t> attrs;
    for (uint32_t i = 0; i < log.modified_fields_.size(); ++i) {
      if (log.modified_fields_[i]) {
        attrs.push_back(i);
      }
    }

    // 用CopySchema根据记录的已更改的列的索引，生成一个新的 Partial Schema，这个 Partial Schema 只包含被修改过的列
    auto partial_schema = Schema::CopySchema(schema, attrs);
    // 依据log.modified_fields_,导航到 current_values 中对应的列索引，使用 Partial Schema 进行覆写
    // schema->GetColumnCount()返回该tuple有多少列
    for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
      if (log.modified_fields_[i]) {
        current_values[i] = log.tuple_.GetValue(&partial_schema, partial_idx++);
      }
    }
  }

  //返回目标时间点的元组状态
  if (!exists) {
    return std::nullopt;
  }

  return Tuple(current_values, schema);
}

/**
 * @brief Collects the undo logs sufficient to reconstruct the tuple w.r.t. the txn.
 *
 * @param rid The RID of the tuple.
 * @param base_meta The metadata of the base tuple.
 * @param base_tuple The base tuple.
 * @param undo_link The undo link to the latest undo log.
 * @param txn The transaction.
 * @param txn_mgr The transaction manager.
 * @return An optional vector of undo logs to pass to ReconstructTuple(). std::nullopt if the tuple did not exist at the
 * time.
 */
// 该函数负责返回根据给定事务读取时间戳重建元组所需的所有撤销日志
auto CollectUndoLogs(RID rid, const TupleMeta &base_meta, const Tuple &base_tuple, std::optional<UndoLink> undo_link,
                     Transaction *txn, TransactionManager *txn_mgr) -> std::optional<std::vector<UndoLog>> {
  std::vector<UndoLog> result_logs;
  // 1.最新版本即所需版本，返回空vector
  // 2.当前tuple较新或被其他人使用，需要遍历版本链，收集所有在读取时间戳之后的撤销日志。
  // 3.当前元组由本事务修改

  //获取当前事务的读取时间戳（事务视角的现在时刻）（不是以TXN_START_ID起始的事务ID）(利用二者的巨大差异来区分是否已经提交)
  auto read_ts = txn->GetReadTs();
  //获取当前事务ID（实际上是以TXN_START_ID起始，然后不断++，这里还未提交）
  auto my_id = txn->GetTransactionId();
  //当前存储在table heap中的元组的时间戳(最新版本)
  auto meta_ts = base_meta.ts_;

  // 该tuple已经提交且提交时间早于当前事务的读取时间戳，
  // 或者该tuple由当前事务修改（不管提交与否），都说明当前版本就是我们需要的版本，
  // 不需要任何undo log
  if ((meta_ts < TXN_START_ID && meta_ts <= read_ts) || meta_ts == my_id) {
    // 如果 is_deleted_ 是 true，说明在当前事务视角下该行不存在
    if (base_meta.is_deleted_) {
      return std::nullopt;
    }
    // 否则返回空 vector，表示直接读取主表 tuple
    return std::vector<UndoLog>{};
  }
  //情况2 加 情况3中的“别人修改的”情况
  if ((meta_ts < TXN_START_ID && meta_ts > read_ts) || (meta_ts >= TXN_START_ID && meta_ts != my_id)) {
    //遍历版本链（因为此时的tuple对当前事务不可见，必须回退到第一个对read_ts可见的版本）
    std::optional<UndoLink> current_undo_link = undo_link;
    while (current_undo_link.has_value() && current_undo_link->IsValid()) {
      UndoLog log = txn_mgr->GetUndoLog(current_undo_link.value());

      //检查该日志的时间戳
      //因为log.ts_记录的是旧版本的时间戳
      //所以总是收集这个 log，因为当前版本太新，必须回退到上一个版本
      //即只能旧不能新
      result_logs.push_back(log);

      // 检查回退后的版本（即这个 log 代表的旧版本）是否对 read_ts 可见
      if (log.ts_ <= read_ts) {
        //如果是小于等于读取时间戳，说明后续的日志都不需要了，可以直接返回
        //无论是否删除，都要返回当前收集到的日志
        // if (log.is_deleted_) return std::nullopt;
        return std::make_optional(result_logs);
      }

      //继续往前找
      current_undo_link = log.prev_version_;
    }
    return std::nullopt;
  }
  return std::nullopt;
}

/**
 * @brief Generates a new undo log as the transaction tries to modify this tuple at the first time.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param ts The timestamp of the base tuple.
 * @param prev_version The undo link to the latest undo log of this tuple.
 * @return The generated undo log.
 *
 * @brief 当事务第一次尝试修改该元组时，生成一个新的撤销日志（undo log）。
 *
 * @param schema 表的模式（Schema）。
 * @param base_tuple 更新前的基础元组（从表堆中获取），如果元组被删除则为 nullptr。
 * @param target_tuple 更新后的目标元组，如果是删除操作则为 nullptr。
 * @param ts 基础元组的时间戳。
 * @param prev_version 指向该元组最新撤销日志的撤销链（undo link）。
 * @return 生成的撤销日志。
 */
/*假设一个事务多次更新同一个元组，我们希望在特定事务内的每次更新只产生一个 UndoLog。
GenerateNewUndoLog 用于每个元组的首次修改。
之后，使用 GenerateUpdatedUndoLog 将后续修改合并到现有的那一个 UndoLog 中。
*/
auto GenerateNewUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple, timestamp_t ts,
                        UndoLink prev_version) -> UndoLog {
  //对比 base_tuple（旧）和 target_tuple（新），找出哪些列变了，并把旧值存进 UndoLog。

  //创建新的 UndoLog
  UndoLog log;
  log.ts_ = ts;
  log.prev_version_ = prev_version;

  //初始化modified_fields_为false，表示所有列都未修改
  std::vector<bool> modified_fields(schema->GetColumnCount(), false);
  std::vector<Value> old_values;
  //记录哪些列被修改了
  std::vector<uint32_t> changed_indices;
  // GenerateNewUndoLog 是用来创建新日志的，
  //  不需要像 GenerateUpdatedUndoLog 那样检查旧日志的状态。
  //   if (log.is_deleted_) {
  //   return log;
  // }

  // task4.2中的“当向一个已删除的元组插入新版本时”
  if (base_tuple == nullptr) {
    log.is_deleted_ = true;
    //当base_tuple为空时，让undolog的modified_fields_全为false
    log.modified_fields_ = std::vector<bool>(schema->GetColumnCount(), false);
    return log;
  }

  log.is_deleted_ = false;
  //判断是否是删除操作
  bool is_deleted_opt = (target_tuple == nullptr);
  //遍历 Schema，对比两元组每一列的值，将结果写入modified_fields_
  for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
    Value old_value = base_tuple->GetValue(schema, i);
    if (is_deleted_opt) {
      //如果是删除操作，所有列都被修改了,意味着保存所有旧值
      modified_fields[i] = true;
      changed_indices.push_back(i);
      old_values.push_back(old_value);
      continue;
    }

    Value new_value = target_tuple->GetValue(schema, i);
    if (!old_value.CompareExactlyEquals(new_value)) {
      //该列被修改了
      modified_fields[i] = true;
      //记录对应的列索引和旧值
      changed_indices.push_back(i);
      old_values.push_back(old_value);
    }
  }

  // log.is_deleted_ = is_deleted_opt;
  log.modified_fields_ = std::move(modified_fields);
  if (changed_indices.empty()) {
    //没有任何修改
    log.tuple_ = Tuple();  //空tuple
    return log;
  }
  Schema changed_schema = Schema::CopySchema(schema, changed_indices);
  log.tuple_ = Tuple(old_values, &changed_schema);

  return log;
}

/**
 * @brief Generate the updated undo log to replace the old one, whereas the tuple is already modified by this txn once.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param log The original undo log.
 * @return The updated undo log.
 */
auto GenerateUpdatedUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple,
                            const UndoLog &log) -> UndoLog {
  if (log.is_deleted_) {
    return log;
  }
  UndoLog new_log = log;

  if (base_tuple == nullptr) {
    //当base_tuple为空时，让undolog的modified_fields_全为false
    new_log.modified_fields_ = std::vector<bool>(schema->GetColumnCount(), false);
    new_log.is_deleted_ = true;
    new_log.tuple_ = Tuple();  // 空 tuple
    return new_log;
  }

  bool is_deleted = (target_tuple == nullptr);
  // new_log.is_deleted_ = is_deleted;
  //如果是删除操作
  if (is_deleted) {
    //表示这条undo log描述的是删除之前的有效状态
    new_log.is_deleted_ = false;

    // 标记所有列都被修改（即都需要恢复）
    new_log.modified_fields_ = std::vector<bool>(schema->GetColumnCount(), true);

    // 构建旧 UndoLog 的 schema 用于读取（复用后续 UPDATE 逻辑中的变量）
    std::vector<uint32_t> old_log_attrs;
    for (uint32_t i = 0; i < log.modified_fields_.size(); ++i) {
      if (log.modified_fields_[i]) {
        old_log_attrs.push_back(i);
      }
    }
    Schema old_log_partial_schema = Schema::CopySchema(schema, old_log_attrs);
    uint32_t old_log_val_idx = 0;

    // 保存所有列的旧值：如果之前修改过从 log 取，否则从 base_tuple 取
    std::vector<Value> all_values;
    for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
      if (log.modified_fields_[i]) {
        // 从旧的 UndoLog 获取该列的原始值
        all_values.push_back(log.tuple_.GetValue(&old_log_partial_schema, old_log_val_idx++));
      } else {
        // 该列在本次事务中未被修改过，从 base_tuple 获取（这也是原始值）
        all_values.push_back(base_tuple->GetValue(schema, i));
      }
    }
    new_log.tuple_ = Tuple(all_values, schema);

    return new_log;
  }
  new_log.is_deleted_ = false;

  std::vector<bool> current_modified(schema->GetColumnCount(), false);
  std::vector<Value> all_old_values;
  std::vector<uint32_t> all_changed_indices;

  // 获取旧 UndoLog 内部 Tuple 的 Schema 指针，用于高效读取 Value
  std::vector<uint32_t> old_log_attrs;
  for (uint32_t i = 0; i < log.modified_fields_.size(); ++i) {
    if (log.modified_fields_[i]) {
      old_log_attrs.push_back(i);
    }
  }
  Schema old_log_partial_schema = Schema::CopySchema(schema, old_log_attrs);
  // 核心优化：使用偏移量指针同步遍历旧的 Partial Tuple 内容
  //因为旧的log.tuple_只存储被修改的列，所以需要专门一个指针来追踪读取位置，
  // 这个指针只在modified_fields_为true时递增
  uint32_t old_log_val_idx = 0;

  for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
    //原有undo log中该列是否被修改过
    bool previously_modified = log.modified_fields_[i];
    bool currently_modified = false;

    Value old_val = base_tuple->GetValue(schema, i);
    Value new_val = target_tuple->GetValue(schema, i);
    currently_modified = (!old_val.CompareExactlyEquals(new_val));

    //只要之前改了或现在改了，modified就算改了,计为true
    if (previously_modified || currently_modified) {
      current_modified[i] = true;
      all_changed_indices.push_back(i);
      //获取旧值
      Value old_value;
      //所要记录的是这个事务第一次修改该列之前的值
      if (!previously_modified) {
        //之前没改过，从base_tuple取旧值
        //从base_tuple中获取旧值
        all_old_values.push_back(base_tuple->GetValue(schema, i));

      } else {
        //从原有log中获取旧值
        //需要构造Partial Schema来读取log.tuple_

        all_old_values.push_back(log.tuple_.GetValue(&old_log_partial_schema, old_log_val_idx++));
      }
    }
  }
  new_log.modified_fields_ = std::move(current_modified);
  if (all_changed_indices.empty()) {
    //没有任何修改
    new_log.tuple_ = Tuple();  //空tuple
    return new_log;
  }
  Schema changed_schema = Schema::CopySchema(schema, all_changed_indices);
  new_log.tuple_ = Tuple(all_old_values, &changed_schema);
  return new_log;
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);
  /*fmt::println(
      stderr,
      "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
      "finished task 2. Implementing this helper function will save you a lot of time for debugging in later tasks.");
*/

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1

  //遍历 TableHeap 中的每一个 Tuple，并沿着 UndoLink 追溯其所有的历史版本（Undo Logs）

  auto iter = table_info->table_->MakeIterator();
  while (!iter.IsEnd()) {
    RID rid = iter.GetRID();
    auto [meta, tuple] = iter.GetTuple();

    // 1. 打印当前存储在 TableHeap 中的最新版本
    std::string ts_str =
        ((meta.ts_ & TXN_START_ID) != 0) ? fmt::format("txn{}", meta.ts_ ^ TXN_START_ID) : std::to_string(meta.ts_);
    std::string tuple_data = meta.is_deleted_ ? "<del marker>" : tuple.ToString(&table_info->schema_);
    fmt::println(stderr, "RID={}/{} ts={} {} tuple={}", rid.GetPageId(), rid.GetSlotNum(), ts_str,
                 meta.is_deleted_ ? "<del marker>" : "", tuple_data);

    // 2. 追溯 Undo Logs 形成的版本链
    std::optional<UndoLink> link = txn_mgr->GetUndoLink(rid);
    while (link.has_value() && link->IsValid()) {
      auto undo_log_opt = txn_mgr->GetUndoLogOptional(*link);
      if (!undo_log_opt.has_value()) {
        break;
      }
      const auto &undo_log = *undo_log_opt;

      // 格式化当前 undo log 的 timestamp
      std::string log_ts_str = ((undo_log.ts_ & TXN_START_ID) != 0) ? fmt::format("txn{}", undo_log.ts_ ^ TXN_START_ID)
                                                                    : std::to_string(undo_log.ts_);
      // 辅助函数：根据 modified_fields 构建 Partial Schema 并打印
      auto get_partial_tuple_string = [&](const UndoLog &undo_log, const Schema &full_schema) -> std::string {
        std::vector<Column> partial_columns;
        std::vector<uint32_t> column_indices;

        // 找出哪些列被修改了
        for (uint32_t i = 0; i < full_schema.GetColumnCount(); ++i) {
          if (undo_log.modified_fields_[i]) {
            partial_columns.push_back(full_schema.GetColumn(i));
            column_indices.push_back(i);
          }
        }

        Schema partial_schema(partial_columns);
        std::string result = "(";
        for (uint32_t i = 0; i < column_indices.size(); ++i) {
          // 使用 partial_schema 解析 partial tuple 中的 value
          Value val = undo_log.tuple_.GetValue(&partial_schema, i);
          result += fmt::format("{}={}", full_schema.GetColumn(column_indices[i]).GetName(), val.ToString());
          if (i < column_indices.size() - 1) {
            result += ", ";
          }
        }
        result += ")";
        return result;
      };

      // 如果是删除标记，打印 <del>，否则打印修改后的列
      if (undo_log.is_deleted_) {
        fmt::println(stderr, "   txn{}@{} <del> ts={}", link->prev_txn_ ^ TXN_START_ID, link->prev_log_idx_,
                     log_ts_str);
      } else {
        // 注意：不再直接对 undo_log.tuple_ 使用 table_info->schema_
        std::string partial_data = get_partial_tuple_string(undo_log, table_info->schema_);
        fmt::println(stderr, "   txn{}@{} {} ts={}", link->prev_txn_ ^ TXN_START_ID, link->prev_log_idx_, partial_data,
                     log_ts_str);
      }

      // 移动到下一个 undo link
      link = undo_log.prev_version_;
    }

    ++iter;
  }
}

}  // namespace bustub

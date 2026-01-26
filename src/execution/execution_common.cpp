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
 // 元组模式（Schema，表长什么样）、
 // 基础元组（Base Tuple，表堆中的最新版本）
 // 及其元数据（包含is_delete，表示当前是否被删除)（两者均存储在表堆中），
 // 以及一个按修改时间从近到远排序的撤销日志（Undo Logs）列表

 // Undo Log表示“我这一步，改了哪些列，原来是什么”
 // modified_fields_ —— 改了哪几列，长度是 schema 的列数，bool值表示该列是否被修改
 // tuple_ —— 被改列的“旧值”
 // is_deleted_ —— “在那次修改之前，这个元组是否存在”

 // 要做的是：从现在的Base Tuple开始，逆序应用这些Undo Log，最终得到“在某个时间点，这个元组是什么样子”
 // （输入的Undo Log实际上是该给定时间戳之后发生的所有修改）

 // 再次提醒： 在 ReconstructTuple 中，你不需要使用甚至不需要检查时间戳（ts_）字段
 // 和前一版本（prev_version_）字段。prev_version_ 应当由 ReconstructTuple 的调用方使用，
 // 用于确定哪些 UndoLog 应该放入输入向量中。

 //根据modified_fields_从原始Schema中抽出一个Partial Schema,用它去读Undo Log中的tuple_
        
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  std::vector<Value> current_values;
  bool exists = !base_meta.is_deleted_;
  if (exists) {
    for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
      current_values.push_back(base_tuple.GetValue(schema, i));
    }
  } else {
    // 如果 base 是删掉的，数组填空占位，exists 为 false
    //current_values.resize(schema->GetColumnCount());
    for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
      //用ValueFactory生成对应类型的Null值
      current_values.push_back(ValueFactory::GetNullValueByType(schema->GetColumn(i).GetType()));
    }
  }

  // 2. 正序遍历：从最新到最旧应用 UndoLog (这是剥洋葱)
  for (const auto &log : undo_logs) {
    if (log.is_deleted_) {
      exists = false;
      continue;
    }
    
    // 如果 log 不是 deleted，说明在这个日志的时间点，元组是存在的
    exists = true; 
    
    // 提取日志中的部分列，覆盖到 current_values 中
    uint32_t partial_idx = 0;
    std::vector<uint32_t> attrs;
    for (uint32_t i = 0; i < log.modified_fields_.size(); ++i) {
      if (log.modified_fields_[i]) {
        attrs.push_back(i);
      }
    }

    // 2. 此时再调用 CopySchema 就匹配了
    auto partial_schema = Schema::CopySchema(schema, attrs);

    for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
      if (log.modified_fields_[i]) {
        current_values[i] = log.tuple_.GetValue(&partial_schema, partial_idx++);
      }
    }
  }

  if (!exists) return std::nullopt;

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
  
  //获取当前事务的读取时间戳
  auto read_ts = txn->GetReadTs();
  //当前事务的临时时间戳
  auto my_ts = txn->GetTransactionTempTs();
  auto meta_ts = base_meta.ts_;

  //情况1 加 情况3中的“自己修改的”情况
  if ((meta_ts < TXN_START_ID && meta_ts <= read_ts) || (meta_ts >= TXN_START_ID &&
                                                    meta_ts == my_ts)) {
    return base_meta.is_deleted_ ? std::nullopt : std::make_optional(result_logs);
  }
  //情况2 加 情况3中的“别人修改的”情况
  if((meta_ts < TXN_START_ID && meta_ts > read_ts) || (meta_ts >= TXN_START_ID &&
                                                    meta_ts != my_ts)) {
    //遍历版本链
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
        //if (log.is_deleted_) return std::nullopt;
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
 */
auto GenerateNewUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple, timestamp_t ts,
                        UndoLink prev_version) -> UndoLog {
  UNIMPLEMENTED("not implemented");
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
  UNIMPLEMENTED("not implemented");
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
    std::string ts_str = (meta.ts_ & TXN_START_ID) ? fmt::format("txn{}", meta.ts_ ^ TXN_START_ID) 
                                                   : std::to_string(meta.ts_);
    
    std::string tuple_data = meta.is_deleted_ ? "<del marker>" : tuple.ToString(&table_info->schema_);
    fmt::println(stderr, "RID={}/{} ts={} {} tuple={}", rid.GetPageId(), rid.GetSlotNum(), 
                 ts_str, meta.is_deleted_ ? "<del marker>" : "", tuple_data);

    // 2. 追溯 Undo Logs 形成的版本链
    std::optional<UndoLink> link = txn_mgr->GetUndoLink(rid);
    while (link.has_value() && link->IsValid()) {
      auto undo_log_opt = txn_mgr->GetUndoLogOptional(*link);
      if (!undo_log_opt.has_value()) {
        break;
      }
      const auto &undo_log = *undo_log_opt;

      // 格式化当前 undo log 的 timestamp
      std::string log_ts_str = (undo_log.ts_ & TXN_START_ID) ? fmt::format("txn{}", undo_log.ts_ ^ TXN_START_ID) 
                                                             : std::to_string(undo_log.ts_);
      // 辅助函数：根据 modified_fields 构建 Partial Schema 并打印
      auto GetPartialTupleString = [&](const UndoLog &undo_log, const Schema &full_schema) -> std::string {
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
        if (i < column_indices.size() - 1) result += ", ";
  }   
      result += ")";
      return result;
    };

      // 如果是删除标记，打印 <del>，否则打印修改后的列
      if (undo_log.is_deleted_) {
        fmt::println(stderr, "   txn{}@{} <del> ts={}", link->prev_txn_ ^ TXN_START_ID, link->prev_log_idx_, log_ts_str);
      } else {
        // 注意：不再直接对 undo_log.tuple_ 使用 table_info->schema_
        std::string partial_data = GetPartialTupleString(undo_log, table_info->schema_);
        fmt::println(stderr, "   txn{}@{} {} ts={}", link->prev_txn_ ^ TXN_START_ID, 
                     link->prev_log_idx_, partial_data, log_ts_str);
      }

      // 移动到下一个 undo link
      link = undo_log.prev_version_;
    }

    ++iter;
  }
}



}  // namespace bustub

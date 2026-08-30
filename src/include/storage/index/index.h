//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index.h
//
// Identification: src/include/storage/index/index.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

enum class VectorIndexDistanceMetric { L2, Cosine, InnerProduct };

/** 作用：统一表达 ANN 查询预算，避免执行器直接依赖某个索引实现的私有术语。 */
struct AnnSearchOptions {
  std::size_t top_k_{0};
  std::size_t candidate_budget_{0};
  std::size_t search_budget_{0};
};

/** 作用：携带 ANN 候选的 RID 与索引键版本，便于执行器做 MVCC / stale 复查。 */
struct VectorIndexCandidate {
  RID rid_{};
  Tuple index_key_{};
  std::uint64_t candidate_id_{0};
};

class Transaction;

/**
 * class IndexMetadata - Holds metadata of an index object.
 *
 * The metadata object maintains the tuple schema and key attribute of an
 * index, since the external callers does not know the actual structure of
 * the index key, so it is the index's responsibility to maintain such a
 * mapping relation and does the conversion between tuple key and index key
 */
class IndexMetadata {
 public:
  IndexMetadata() = delete;

  /**
   * Construct a new IndexMetadata instance.
   * @param index_name The name of the index
   * @param table_name The name of the table on which the index is created
   * @param tuple_schema The schema of the indexed key
   * @param key_attrs The mapping from indexed columns to base table columns
   */
  IndexMetadata(std::string index_name, std::string table_name, const Schema *tuple_schema,
                std::vector<uint32_t> key_attrs, bool is_primary_key)
      : name_(std::move(index_name)),
        table_name_(std::move(table_name)),
        key_attrs_(std::move(key_attrs)),
        is_primary_key_(is_primary_key) {
    key_schema_ = std::make_shared<Schema>(Schema::CopySchema(tuple_schema, key_attrs_));
  }

  ~IndexMetadata() = default;

  /** @return The name of the index */
  inline auto GetName() const -> const std::string & { return name_; }

  /** @return The name of the table on which the index is created */
  inline auto GetTableName() -> const std::string & { return table_name_; }

  /** @return A schema object pointer that represents the indexed key */
  inline auto GetKeySchema() const -> Schema * { return key_schema_.get(); }

  /**
   * @return The number of columns inside index key (not in tuple key)
   *
   * NOTE: this must be defined inside the cpp source file because it
   * uses the member of catalog::Schema which is not known here.
   */
  auto GetIndexColumnCount() const -> std::uint32_t { return static_cast<uint32_t>(key_attrs_.size()); }

  /** @return The mapping relation between indexed columns and base table columns */
  inline auto GetKeyAttrs() const -> const std::vector<uint32_t> & { return key_attrs_; }

  /** @return is primary key */
  inline auto IsPrimaryKey() const -> bool { return is_primary_key_; }

  /** @return A string representation for debugging */
  auto ToString() const -> std::string {
    std::stringstream os;

    os << "IndexMetadata["
       << "Name = " << name_ << ", "
       << "Type = B+Tree, "
       << "Table name = " << table_name_ << "] :: ";
    os << key_schema_->ToString();

    return os.str();
  }

 private:
  /** The name of the index */
  std::string name_;
  /** The name of the table on which the index is created */
  std::string table_name_;
  /** The mapping relation between key schema and tuple schema */
  const std::vector<uint32_t> key_attrs_;
  /** The schema of the indexed key */
  std::shared_ptr<Schema> key_schema_;
  /** Is primary key? */
  bool is_primary_key_;
};

/////////////////////////////////////////////////////////////////////
// Index class definition
/////////////////////////////////////////////////////////////////////

/**
 * class Index - Base class for derived indices of different types
 *
 * The index structure majorly maintains information on the schema of the
 * underlying table and the mapping relation between index key
 * and tuple key, and provides an abstracted way for the external world to
 * interact with the underlying index implementation without exposing
 * the actual implementation's interface.
 *
 * Index object also handles predicate scan, in addition to simple insert,
 * delete, predicate insert, point query, and full index scan. Predicate scan
 * only supports conjunction, and may or may not be optimized depending on
 * the type of expressions inside the predicate.
 */
class Index {
 public:
  /**
   * Construct a new Index instance.
   * @param metadata An owning pointer to the index metadata
   */
  explicit Index(std::unique_ptr<IndexMetadata> &&metadata) : metadata_{std::move(metadata)} {}

  virtual ~Index() = default;

  /** @return A non-owning pointer to the metadata object associated with the index */
  auto GetMetadata() const -> IndexMetadata * { return metadata_.get(); }

  /** @return The number of indexed columns */
  auto GetIndexColumnCount() const -> std::uint32_t { return metadata_->GetIndexColumnCount(); }

  /** @return The index name */
  auto GetName() const -> const std::string & { return metadata_->GetName(); }

  /** @return The index key schema */
  auto GetKeySchema() const -> Schema * { return metadata_->GetKeySchema(); }

  /** @return The index key attributes */
  auto GetKeyAttrs() const -> const std::vector<uint32_t> & { return metadata_->GetKeyAttrs(); }

  /** @return A string representation for debugging */
  auto ToString() const -> std::string {
    std::stringstream os;
    os << "INDEX: (" << GetName() << ")";
    os << metadata_->ToString();
    return os.str();
  }

  ///////////////////////////////////////////////////////////////////
  // Point Modification
  ///////////////////////////////////////////////////////////////////

  /**
   * Insert an entry into the index.
   * @param key The index key
   * @param rid The RID associated with the key
   * @param transaction The transaction context
   * @returns whether insertion is successful
   */
  virtual auto InsertEntry(const Tuple &key, RID rid, Transaction *transaction) -> bool = 0;

  /**
   * Delete an index entry by key.
   * @param key The index key
   * @param rid The RID associated with the key (unused)
   * @param transaction The transaction context
   */
  virtual void DeleteEntry(const Tuple &key, RID rid, Transaction *transaction) = 0;

  /**
   * Search the index for the provided key.
   * @param key The index key
   * @param result The collection of RIDs that is populated with results of the search
   * @param transaction The transaction context
   */
  virtual void ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) = 0;

  // 用query区别于key,强调这是“查询向量”，而不是“索引键”，虽然它们的类型都是Tuple
  virtual auto SearchKnn(const Tuple &query, size_t k, std::vector<RID> *result, Transaction *transaction) -> void {
    throw NotImplementedException("KNN search is not supported for this index type");
  }

  /** 作用：统一对外暴露 ANN 搜索接口，`search_budget_` 由具体索引自行解释。 */
  virtual auto SearchVector(const Tuple &query, const AnnSearchOptions &options,
                            std::vector<VectorIndexCandidate> *result, Transaction *transaction) -> void {
    throw NotImplementedException("Approximate vector search is not supported for this index type");
  }

  /** 作用：兼容旧的 KNN 调用方，并复用新的通用 ANN 搜索接口。 */
  virtual auto GetDefaultAnnSearchOptions(std::size_t top_k) const -> std::optional<AnnSearchOptions> {
    return std::nullopt;
  }

  /** 作用：向执行器暴露搜索预算上限，但不泄露具体实现名词。 */
  virtual auto GetMaxAnnSearchBudget() const -> std::optional<std::size_t> { return std::nullopt; }

  /** 作用：兼容旧接口，内部转发到通用 ANN 接口。 */
  virtual auto SearchKnnWithProbe(const Tuple &query, size_t k, std::size_t probe_count, std::vector<RID> *result,
                                  Transaction *transaction) -> void {
    const auto options = AnnSearchOptions{k, k, probe_count};
    std::vector<VectorIndexCandidate> candidates;
    SearchVector(query, options, &candidates, transaction);
    result->clear();
    result->reserve(candidates.size());
    for (const auto &candidate : candidates) {
      result->push_back(candidate.rid_);
    }
  }

  virtual auto GetVectorDistanceMetric() const -> std::optional<VectorIndexDistanceMetric> { return std::nullopt; }

  /** 作用：兼容旧接口，避免影响未迁移的调用方。 */
  virtual auto GetDefaultKnnProbeCount() const -> std::optional<std::size_t> {
    auto options = GetDefaultAnnSearchOptions(1);
    if (!options.has_value()) {
      return std::nullopt;
    }
    return options->search_budget_;
  }

  /** 作用：兼容旧接口，避免影响未迁移的调用方。 */
  virtual auto GetMaxKnnProbeCount() const -> std::optional<std::size_t> { return GetMaxAnnSearchBudget(); }

 protected:
  /** The Index structure owns its metadata */
  std::unique_ptr<IndexMetadata> metadata_;
};

}  // namespace bustub

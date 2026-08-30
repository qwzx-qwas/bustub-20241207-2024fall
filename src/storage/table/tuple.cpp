//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// tuple.cpp
//
// Identification: src/storage/table/tuple.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <sstream>
#include <string>
#include <vector>

#include "common/exception.h"
#include "storage/table/tuple.h"

namespace bustub {

namespace {

/** 作用：在所有写入路径统一校验 VECTOR 列的类型、NULL 与维度是否合法。 */
void ValidateVectorColumns(const std::vector<Value> &values, const Schema *schema) {
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    const auto &column = schema->GetColumn(i);
    if (column.GetType() != TypeId::VECTOR) {
      continue;
    }

    const auto &value = values[i];
    if (value.IsNull()) {
      throw Exception("NULL VECTOR value is not supported");
    }
    if (value.GetTypeId() != TypeId::VECTOR) {
      throw Exception("vector column expects VECTOR value");
    }

    const auto expected_dim = column.GetStorageSize() / sizeof(double);
    const auto actual_size = value.GetStorageSize();
    if (actual_size % sizeof(double) != 0) {
      throw Exception("vector value storage is corrupted");
    }
    const auto actual_dim = actual_size / sizeof(double);
    if (expected_dim != actual_dim) {
      throw Exception("vector dimension mismatch");
    }
  }
}

}  // namespace

// TODO(Amadou): It does not look like nulls are supported. Add a null bitmap?
Tuple::Tuple(std::vector<Value> values, const Schema *schema) {
  assert(values.size() == schema->GetColumnCount());
  ValidateVectorColumns(values, schema);

  // 1. Calculate the size of the tuple.
  uint32_t tuple_size = schema->GetInlinedStorageSize();
  for (auto &i : schema->GetUnlinedColumns()) {
    auto len = values[i].GetStorageSize();
    if (len == BUSTUB_VALUE_NULL) {
      len = 0;
    }
    tuple_size += sizeof(uint32_t) + len;
  }

  // 2. Allocate memory.
  data_.resize(tuple_size);
  std::fill(data_.begin(), data_.end(), 0);

  // 3. Serialize each attribute based on the input value.
  uint32_t column_count = schema->GetColumnCount();
  uint32_t offset = schema->GetInlinedStorageSize();

  for (uint32_t i = 0; i < column_count; i++) {
    const auto &col = schema->GetColumn(i);
    if (!col.IsInlined()) {
      // Serialize relative offset, where the actual varchar data is stored.
      std::memcpy(data_.data() + col.GetOffset(), &offset, sizeof(offset));
      // Serialize varchar value, in place (size+data).
      values[i].SerializeTo(data_.data() + offset);
      auto len = values[i].GetStorageSize();
      if (len == BUSTUB_VALUE_NULL) {
        len = 0;
      }
      offset += sizeof(uint32_t) + len;
    } else {
      values[i].SerializeTo(data_.data() + col.GetOffset());
    }
  }
}

Tuple::Tuple(RID rid, const char *data, uint32_t size) {
  rid_ = rid;
  data_.resize(size);
  memcpy(data_.data(), data, size);
}

auto Tuple::GetValue(const Schema *schema, const uint32_t column_idx) const -> Value {
  if (schema == nullptr || column_idx >= schema->GetColumnCount()) {
    throw Exception("tuple value lookup uses an invalid schema column");
  }
  const auto &column = schema->GetColumn(column_idx);
  const TypeId column_type = column.GetType();
  const auto offset = static_cast<size_t>(column.GetOffset());
  if (column.IsInlined()) {
    if (offset > data_.size() || column.GetStorageSize() > data_.size() - offset) {
      throw Exception("inline tuple value exceeds the tuple boundary");
    }
  } else {
    if (offset > data_.size() || sizeof(uint32_t) > data_.size() - offset) {
      throw Exception("tuple varlen offset field exceeds the tuple boundary");
    }
    uint32_t value_offset = 0;
    std::memcpy(&value_offset, data_.data() + offset, sizeof(value_offset));
    if (value_offset > data_.size() || sizeof(uint32_t) > data_.size() - value_offset) {
      throw Exception("tuple varlen value offset exceeds the tuple boundary");
    }
    uint32_t value_size = 0;
    std::memcpy(&value_size, data_.data() + value_offset, sizeof(value_size));
    if (value_size != BUSTUB_VALUE_NULL && value_size > data_.size() - value_offset - sizeof(uint32_t)) {
      throw Exception("tuple varlen value exceeds the tuple boundary: column=" + std::to_string(column_idx) +
                      " tuple_size=" + std::to_string(data_.size()) + " inline_offset=" + std::to_string(offset) +
                      " value_offset=" + std::to_string(value_offset) + " value_size=" + std::to_string(value_size));
    }
  }
  const char *data_ptr = GetDataPtr(schema, column_idx);
  // the third parameter "is_inlined" is unused
  return Value::DeserializeFrom(data_ptr, column_type);
}

auto Tuple::KeyFromTuple(const Schema &schema, const Schema &key_schema, const std::vector<uint32_t> &key_attrs) const
    -> Tuple {
  std::vector<Value> values;
  values.reserve(key_attrs.size());
  for (auto idx : key_attrs) {
    values.emplace_back(this->GetValue(&schema, idx));
  }
  return {values, &key_schema};
}

auto Tuple::GetDataPtr(const Schema *schema, const uint32_t column_idx) const -> const char * {
  assert(schema);
  const auto &col = schema->GetColumn(column_idx);
  bool is_inlined = col.IsInlined();
  // For inline type, data is stored where it is.
  if (is_inlined) {
    return (data_.data() + col.GetOffset());
  }
  // We read the relative offset from the tuple data.
  uint32_t offset = 0;
  std::memcpy(&offset, data_.data() + col.GetOffset(), sizeof(offset));
  // And return the beginning address of the real data for the VARCHAR type.
  return (data_.data() + offset);
}

auto Tuple::ToString(const Schema *schema) const -> std::string {
  std::stringstream os;

  int column_count = schema->GetColumnCount();
  bool first = true;
  os << "(";
  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    if (first) {
      first = false;
    } else {
      os << ", ";
    }
    if (IsNull(schema, column_itr)) {
      os << "<NULL>";
    } else {
      Value val = (GetValue(schema, column_itr));
      os << val.ToString();
    }
  }
  os << ")";

  return os.str();
}

void Tuple::SerializeTo(char *storage) const {
  int32_t sz = data_.size();
  memcpy(storage, &sz, sizeof(int32_t));
  memcpy(storage + sizeof(int32_t), data_.data(), sz);
}

void Tuple::DeserializeFrom(const char *storage) {
  uint32_t size = 0;
  std::memcpy(&size, storage, sizeof(size));
  this->data_.resize(size);
  memcpy(this->data_.data(), storage + sizeof(int32_t), size);
}

}  // namespace bustub

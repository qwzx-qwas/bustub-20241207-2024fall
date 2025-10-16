#include "primer/hyperloglog_presto.h"

#include <algorithm>
#include <bitset>
#include <cmath>
#include <cstdint>
#include <limits>
#include <mutex>

namespace bustub {

// 对负值进行移位操作是未定义行为, 因为输入的 n_leading_bits 是决定尺寸的
template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits)
    : dense_bucket_(), overflow_bucket_(), cardinality_(0) {
  int16_t effective_b;
  if (n_leading_bits < 0) {
    effective_b = 0;
  } else if (n_leading_bits > 16) {
    effective_b = 16;
  } else {
    effective_b = n_leading_bits;
  }

  b_ = effective_b;
  m_ = 1 << b_;
  dense_bucket_.resize(m_, std::bitset<DENSE_BUCKET_SIZE>(0));
  overflow_bucket_.reserve(m_);
}

template <typename KeyType>
auto HyperLogLogPresto<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  return std::bitset<BITSET_CAPACITY>(hash);
}

template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  // 使用 RAII 自动锁管理来处理多线程
  std::lock_guard<std::mutex> lock(mtx_);

  // 1. 计算哈希并提取桶索引。
  auto hash = CalculateHash(val);
  // 将 hash 整体右移，再用 & 运算取出低 b_ 位作为桶索引 (也即高位 b_ 位)
  auto index = (hash >> (BITSET_CAPACITY - b_)) & (m_ - 1);

  // 2. 提取用于计数尾部零的低位（BITSET_CAPACITY - b_ 位）。
  uint64_t tail_mask;
  uint64_t num_tail_bits = BITSET_CAPACITY - b_;

  // 需要考虑到 num_tail_bits 等于 BITSET_CAPACITY 的情况
  // 对 64 位无符号整数进行 1<<64 的操作会导致未定义行为
  if (num_tail_bits == BITSET_CAPACITY) {
    tail_mask = ~0ULL;
  } else {
    tail_mask = (1ULL << num_tail_bits) - 1;
  }

  uint64_t tail_bits = hash & tail_mask;

  // 3. 统计尾随零的个数（Z）。
  uint64_t trailing_zeros = (tail_bits == 0) ? (BITSET_CAPACITY - b_) : (__builtin_ctzll(tail_bits));

  // 4. 读取当前存储的 Z（将溢出桶的高位与稠密桶的低位合并）。
  uint64_t current_msbs = 0;
  auto it = overflow_bucket_.find(index);
  // 如果溢出桶中存在该索引，则读取其值。
  if (it != overflow_bucket_.end()) {
    current_msbs = it->second.to_ullong();
  }

  uint64_t current_lsbs = dense_bucket_[index].to_ullong();
  // 合并高位和低位以获取当前的 Z 值。
  uint64_t current_z = (current_msbs << DENSE_BUCKET_SIZE) | current_lsbs;

  // 5. 如果新的 Z 更大则更新。
  if (trailing_zeros > current_z) {
    uint64_t lsbs = trailing_zeros & ((1ULL << DENSE_BUCKET_SIZE) - 1);
    uint64_t msbs = trailing_zeros >> DENSE_BUCKET_SIZE;

    // 更新稠密桶（低位）。
    dense_bucket_[index] = std::bitset<DENSE_BUCKET_SIZE>(static_cast<unsigned long long>(lsbs));

    // 更新溢出桶（高位）。
    if (msbs > 0) {
      uint64_t max_overflow = (1ULL << OVERFLOW_BUCKET_SIZE) - 1;
      msbs = std::min(msbs, max_overflow);
      overflow_bucket_[index] = std::bitset<OVERFLOW_BUCKET_SIZE>(static_cast<unsigned long long>(msbs));
    } else if (it != overflow_bucket_.end()) {
      // 如果 MSBs 变为 0，则移除之前的溢出条目。
      overflow_bucket_.erase(it);
    }
  }
}

template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  double sum = 0.0;
  // 计算 2^{-register} 的调和和。
  for (size_t idx = 0; idx < m_; ++idx) {
    uint64_t dense_val = dense_bucket_[idx].to_ullong();

    uint64_t total = dense_val;

    auto it = overflow_bucket_.find(idx);
    if (it != overflow_bucket_.end()) {
      uint64_t overflow_val = it->second.to_ullong();
      total = (overflow_val << DENSE_BUCKET_SIZE) | dense_val;
    }

    // 不要对无符号数取负数，会变成一个很大的数！要先转换为有符号数后再取负数，
    // 要么就少用无符号数！
    sum += 1.0 / std::pow(2.0, static_cast<double>(total));
  }

  double estimate = CONSTANT * m_ * m_ / sum;
  cardinality_ = static_cast<uint64_t>(std::floor(estimate));
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;

}  // namespace bustub

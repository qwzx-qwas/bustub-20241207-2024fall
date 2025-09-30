#include "primer/hyperloglog.h"

#include <algorithm>
#include <cmath>

namespace bustub {

template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits)
    : cardinality_(0),
      b_(n_bits),
      m_(1 << n_bits),
      registers_(m_, 0) {}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const
    -> std::bitset<BITSET_CAPACITY> {
  /** @TODO(student) Implement this function! */
  return std::bitset<BITSET_CAPACITY>(hash);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(
    const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  /** @TODO(student) Implement this function! */
  size_t effective_bits = BITSET_CAPACITY - b_;
  for (size_t i = 0; i < effective_bits; ++i) {
    if (bset[effective_bits - 1 - i]) {
      return i + 1;
    }
  }
  return static_cast<uint64_t>(BITSET_CAPACITY - b_ + 1);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  // 获取对应的哈希值。
  auto hash = CalculateHash(val);

  // 计算桶的索引。
  auto index =
      static_cast<size_t>((hash >> (BITSET_CAPACITY - b_)) & (m_ - 1));

  // 计算 hash 的低位部分用于前导 1 的检测。
  auto low_bits = hash & ((1ULL << (BITSET_CAPACITY - b_)) - 1);

  // 将低位转换为二进制并计算前导 1 的位置（等价于前导零计数 + 1）。
  auto binary_low = ComputeBinary(low_bits);
  auto leading_zeros = PositionOfLeftmostOne(binary_low);

  // 更新寄存器（取更大值）。
  uint8_t current_value = registers_[index];
  uint8_t max_value =
      static_cast<uint8_t>(std::max(current_value, static_cast<uint8_t>(leading_zeros)));
  registers_[index] = max_value;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  double sum = 0.0;
  for (const auto &reg : registers_) {
    sum += std::pow(2.0, -static_cast<int>(reg));
  }

  auto zero_count = static_cast<double>(std::count(registers_.begin(), registers_.end(), 0));
  double estimate = CONSTANT * m_ * m_ / sum;

  // 小基数时做空桶修正。
  if (estimate <= 2.5 * m_ && zero_count > 0) {
    estimate = m_ * std::log(static_cast<double>(m_) / zero_count);
  }

  cardinality_ = static_cast<uint64_t>(estimate);
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub

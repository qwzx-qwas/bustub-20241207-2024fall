#include "primer/hyperloglog.h"

#include <algorithm>
#include <cmath>

namespace bustub {

template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits)
    : cardinality_(0) {
       
  int16_t effective_b;
  if (n_bits < 0) {
    effective_b = 0;
  } else if (n_bits > 16) {
    effective_b = 16;
  } else {
    effective_b = n_bits;
  }

  b_ = effective_b;
  m_ = 1 << b_;
  registers_.resize(m_, 0);
      }

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
  //
  //计算除去桶索引位后的前导1位置
  size_t effective_bits = BITSET_CAPACITY - b_;
  for (size_t i = 0; i < effective_bits; ++i) {
    if (bset[effective_bits - 1 - i]) {
    //if (bset[i]) {
      return i + 1;
    }
  }
  return static_cast<uint64_t>(BITSET_CAPACITY - b_ + 1);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  //使用RSII自动锁管理来处理多线程
  std::lock_guard<std::mutex> lock(mtx_);
  // 获取对应的哈希值。
  hash_t hash = CalculateHash(val);

  // 将hash右移b_位，取高位作为桶索引。(考虑到鲁棒性，使用&运算确保索引在范围内)
  auto index =
         static_cast<size_t>((hash >> (BITSET_CAPACITY - b_)) & (m_ - 1));

  // 将hash转换为二进制并计算前导 1 的位置（等价于前导零计数 + 1）。
  auto binary_low = ComputeBinary(hash);
  auto leading_zeros = PositionOfLeftmostOne(binary_low);

  // 更新寄存器（取更大值）。
  uint8_t current_value = registers_[index];
  //避免std::max会提升类型（int)导致错误
  uint8_t new_value = static_cast<uint8_t>(leading_zeros);
  uint8_t max_value =
      static_cast<uint8_t>(std::max(current_value, new_value));
  registers_[index] = max_value;

}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  double sum = 0.0;
  for (const auto &reg : registers_) {
    // 不要对无符号数取负数，会变成一个很大的数！要先转换为有符号数后再取负数，要么就少用无符号数！
    sum += 1.0 / std::pow(2.0, static_cast<double>(reg));
  }
  
  double estimate = CONSTANT * m_ * m_ / sum;
  cardinality_ = static_cast<uint64_t>(std::floor(estimate));
}
template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub

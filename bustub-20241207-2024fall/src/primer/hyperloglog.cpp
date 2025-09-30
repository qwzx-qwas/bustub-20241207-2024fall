#include "primer/hyperloglog.h"

namespace bustub {

template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) :
              cardinality_(0), b_(n_bits), m_(1 << n_bits), registers_(m_, 0) {}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  /** @TODO(student) Implement this function! */
  return std::bitset<BITSET_CAPACITY>(hash);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  /** @TODO(student) Implement this function! */
  size_t effective_bits = BITSET_CAPACITY - b_;
  for (size_t i = 0; i < effective_bits; i++) {
    if (bset[effective_bits - 1 - i]) {
      return i + 1;
    }
  }
  return BITSET_CAPACITY - b_ + 1;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  //获取对应的hash值
  auto hash = CalculateHash(val);
  //计算桶的索引
  //auto index = hash >> (BITSET_CAPACITY - b_);
  auto index = static_cast<size_t>(hash >> (BITSET_CAPACITY - b_)) & (m_ - 1);
  //计算hash值的低位部分
  auto low_bits = hash & ((1ULL << (BITSET_CAPACITY - b_)) - 1);
  //low_bits是hash值的低位部分，不是二进制形式，所以要转化为二进制
  auto binary_low = ComputeBinary(low_bits);
  //计算前导零的数量
  auto leading_zeros = PositionOfLeftmostOne(binary_low);
  //更新寄存器
  auto current_value = registers_[index];
  auto max_value = std::max(current_value, static_cast<uint8_t>(leading_zeros));
  registers_[index] = max_value;
  std::cout << "index: " << index << " leading_zeros: " << leading_zeros << " max_value: " << static_cast<int>(max_value) << std::endl;
  return;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  double sum = 0.0;
  //求寄存器中的调和平均值
  for (const auto &reg : registers_) {
    //static_cast<int>(reg)把 reg 从无符号强制转成有符号的 int，这样 -reg 才会真的变成负数。
    sum += pow(2.0, -static_cast<int>(reg));
  }
  //带入公式计算基数,并将其转化为小于或等于的整数赋值给cardinality_
  //cardinality_ = static_cast<uint64_t>(std::floor(CONSTANT * m_ * m_ / sum));
  auto v = std::count(registers_.begin(), registers_.end(), 0);
double estimate = CONSTANT * m_ * m_ / sum;

// 空桶修正
if (estimate <= 2.5 * m_ && v > 0) {
  estimate = m_ * std::log(static_cast<double>(m_) / v);
}

// 估计结果更新
cardinality_ = static_cast<uint64_t>(estimate);

  std::cout << "cardinality: " << cardinality_ << std::endl;
  return;
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
/*#include "primer/hyperloglog.h"

namespace bustub {

template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : cardinality_(0), b_(n_bits), m_(1 << n_bits), registers_(m_, 0) {}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  return std::bitset<BITSET_CAPACITY>(hash);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
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
  auto hash = CalculateHash(val);
  size_t index = static_cast<size_t>((hash >> (BITSET_CAPACITY - b_)) & (m_ - 1));

  uint64_t low_bits = hash & ((1ULL << (BITSET_CAPACITY - b_)) - 1);
  auto binary_low = ComputeBinary(low_bits);
  auto leading_zeros = PositionOfLeftmostOne(binary_low);

  uint8_t current_value = registers_[index];
  uint8_t new_value = static_cast<uint8_t>(std::max(current_value, static_cast<uint8_t>(leading_zeros)));
  registers_[index] = new_value;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  double sum = 0.0;
  for (const auto &reg : registers_) {
    sum += std::pow(2.0, -static_cast<int>(reg));
  }

  auto zero_count = static_cast<double>(std::count(registers_.begin(), registers_.end(), 0));
  double estimate = CONSTANT * m_ * m_ / sum;

  if (estimate <= 2.5 * m_ && zero_count > 0) {
    estimate = m_ * std::log(static_cast<double>(m_) / zero_count);
  }

  cardinality_ = static_cast<uint64_t>(estimate);
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
*/

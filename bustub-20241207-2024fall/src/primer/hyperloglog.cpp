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
  for (size_t i = 0; i < BITSET_CAPACITY; i++) {
    if (bset[BITSET_CAPACITY - 1 - i]) {
      return i + 1;
    }
  }
  return 0;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  //获取对应的hash值
  auto hash = CalculateHash(val);
  //将hash值转换为二进制
  auto binary = ComputeBinary(hash);
  //计算桶的索引
  auto index = binary.to_ullong() >> (BITSET_CAPACITY - b_);
  //计算前导零的数量
  auto leading_zeros = PositionOfLeftmostOne(binary << b_);
  //更新寄存器
  auto current_value = registers_[index].to_ulong();
  auto max_value = std::max(current_value, static_cast<uint8_t>(leading_zeros));
  registers_[index] = std::bitset<uint8_t>(max_value);

  return;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> uint64_t {
  /** @TODO(student) Implement this function! */
  double sum = 0.0;
  //求寄存器中的调和平均值
  for (const auto &reg : registers_) {
    //static_cast<int>(reg)把 reg 从无符号强制转成有符号的 int，这样 -reg 才会真的变成负数。
    sum += pow(2.0, -static_cast<int>(reg));
  }
  //带入公式计算基数,并将其转化为小于或等于的整数返回
  return static_cast<uint64_t>(std::floor(CONSTANT * m_ * m_ / sum));
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub

#include "primer/hyperloglog_presto.h"

namespace bustub {

template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits) :
      dense_bucket_(1 << n_leading_bits,0), overflow_bucket_(), cardinality_(0),  b_(n_leading_bits), m_(1 << n_leading_bits) {}



template <typename KeyType>
auto HyperLogLogPresto<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  /** @TODO(student) Implement this function! */
  return std::bitset<BITSET_CAPACITY>(hash);
}

template <typename KeyType> 
auto HyperLogLogPresto<KeyType>::NumberOfTrailingZeros(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  /** @TODO(student) Implement this function! */
  size_t effective_bits = BITSET_CAPACITY - b_;
  for (size_t i = 0; i < effective_bits; i++) {
    if (bset[i]) {
      return i + 1;
    }
  }
  return effective_bits;
}


template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  //先转化为hash值
  auto hash = CalculateHash(val);
  //计算桶的索引
  auto index = hash >> (BITSET_CAPACITY - b_);
  //计算hash值的低位部分
  auto low_bits = hash & ((1ULL << (BITSET_CAPACITY - b_)) - 1);
  //low_bits是hash值的低位部分，不是二进制形式，所以要转化为二进制
  auto binary_low = ComputeBinary(low_bits);
  //计算后导零的数量
  auto trailing_zeros = NumberOfTrailingZeros(binary_low);
  //当后导零小于16时，更新密集桶
  /*if (trailing_zeros < denseNum) {
    auto current_value = dense_bucket_[index].to_ulong();
    auto max_value = std::max(current_value, static_cast<size_t>(trailing_zeros));
    dense_bucket_[index] = std::bitset<DENSE_BUCKET_SIZE>(max_value);

  } else if (trailing_zeros >= denseNum && trailing_zeros < totalNum) {
    //否则更新溢出桶,同时将密集桶对应位置取最大
    dense_bucket_[index] = std::bitset<DENSE_BUCKET_SIZE>(denseNum - 1);
    //构造或更新溢出桶
    overflow_bucket_[index] = std::max(overflow_bucket_[index].to_ulong(), static_cast<size_t>(trailing_zeros - denseNum + 1));  
  } else {
    //否则不做任何更新
    */
    //将数据拆分为低位和高位
  auto LSB = trailing_zeros & ((1 << DENSE_BUCKET_SIZE) - 1);
  auto MSB = trailing_zeros >> DENSE_BUCKET_SIZE;
  //更新溢出桶
  auto current_dense = dense_bucket_[index].to_ulong();
  auto new_dense = std::max(current_dense, LSB);
  dense_bucket_[index] = std::bitset<DENSE_BUCKET_SIZE>(new_dense);
  //如果MSB大于0，说明需要更新溢出桶
  if (MSB > 0) {
    auto current_overflow = overflow_bucket_[index].to_ulong();
    auto new_overflow = std::max(current_overflow, static_cast<size_t>(MSB));
    overflow_bucket_[index] = std::bitset<OVERFLOW_BUCKET_SIZE>(new_overflow);
  }

  return;
}


template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  double sum = 0.0;
  //求调和平均值
  for(size_t idx = 0;idx < m_; idx++) {
    auto reg = dense_bucket_[idx].to_ulong();
    if (reg < denseNum) {
      sum += std::pow(2.0, -static_cast<int>(reg));
    } else {
      //获取对应索引的溢出桶
      auto it = overflow_bucket_.find(idx);
      //如果溢出桶中有数据
      if (it != overflow_bucket_.end()) {
        uint64_t stored = it->second.to_ulong();
        sum += std::pow(2.0, -static_cast<int>(denseNum - 1 + stored));
      } else {
        sum += std::pow(2.0, -static_cast<int>(denseNum - 1));
      }
    }
  }
  cardinality_ = static_cast<uint64_t>(std::floor(CONSTANT * m_ * m_ / sum));
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub

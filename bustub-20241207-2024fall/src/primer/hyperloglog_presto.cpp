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
  //分离出低位和高位
  auto LSBs = trailing_zeros & ((1 << DENSE_BUCKET_SIZE) - 1);
  auto MSBs = trailing_zeros >> DENSE_BUCKET_SIZE;
  //更新溢出桶
  auto current_dense = dense_bucket_[index].to_ulong();
  auto new_dense = std::max(current_dense, LSBs);
  dense_bucket_[index] = std::bitset<DENSE_BUCKET_SIZE>(new_dense);
  //如果MSB大于0，说明需要更新溢出桶
  if (MSBs > 0) {
    auto it = overflow_bucket_.find(index);
    if (it == overflow_bucket_.end()) {
      overflow_bucket_.emplace(index, std::bitset<OVERFLOW_BUCKET_SIZE>(static_cast<unsigned long>(MSBs)));
    } else {
      unsigned long cur_over = it->second.to_ulong();
      if (MSBs > cur_over) {
        it->second = std::bitset<OVERFLOW_BUCKET_SIZE>(static_cast<unsigned long>(MSBs));
      }
    }
  }
  std::cout << "index: " << index << " trailing_zeros: " << trailing_zeros << " LSBs: " << LSBs << " MSBs: " << MSBs << std::endl;
  return;
}


template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  double sum = 0.0;
  //求调和平均值
  for(size_t idx = 0;idx < m_; idx++) {
    uint64_t dense_val = dense_bucket_[idx].to_ulong();

    // 2) overflow part if exists
    uint64_t total = dense_val;
    auto it = overflow_bucket_.find(idx);
    if (it != overflow_bucket_.end()) {
      uint64_t overflow_val = it->second.to_ulong();
      total = (overflow_val << DENSE_BUCKET_SIZE) | dense_val;
    }

    // 3) accumulate 2^{-reg}
    // ensure reg isn't absurdly large for pow() exponent
    sum += std::pow(2.0, -static_cast<int>(total));
  }

  if (sum <= 0.0) {
    cardinality_ = 0;
  } else {
    cardinality_ = static_cast<uint64_t>(std::floor(CONSTANT * m_ * m_ / sum));
  }
  
  
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub

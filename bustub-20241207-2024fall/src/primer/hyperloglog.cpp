#include "primer/hyperloglog.h"

#include <algorithm>
#include <cmath>

namespace bustub {

template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : cardinality_(0) {
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
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  // Convert the hash to a BITSET_CAPACITY-bit bitset.
  return std::bitset<BITSET_CAPACITY>(hash);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  // Compute the position of the leftmost 1 bit after excluding the index bits.
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
  // Use RAII lock guard to be thread-safe.
  std::lock_guard<std::mutex> lock(mtx_);

  // Compute hash.
  hash_t hash = CalculateHash(val);

  // Extract bucket index from the high b_ bits.
  auto index = static_cast<size_t>((hash >> (BITSET_CAPACITY - b_)) & (m_ - 1));

  // Convert hash to binary and compute the leading-one position.
  auto binary_low = ComputeBinary(hash);
  auto leading_zeros = PositionOfLeftmostOne(binary_low);

  // Update register with the max value.
  uint8_t current_value = registers_[index];
  uint8_t new_value = static_cast<uint8_t>(leading_zeros);
  uint8_t max_value = static_cast<uint8_t>(std::max(current_value, new_value));
  registers_[index] = max_value;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  double sum = 0.0;
  for (const auto &reg : registers_) {
    sum += 1.0 / std::pow(2.0, static_cast<double>(reg));
  }

  double estimate = CONSTANT * m_ * m_ / sum;
  cardinality_ = static_cast<uint64_t>(std::floor(estimate));
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub

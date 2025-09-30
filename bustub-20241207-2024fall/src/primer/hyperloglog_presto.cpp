#include "primer/hyperloglog_presto.h"

namespace bustub {

template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits)
    : dense_bucket_(1 << n_leading_bits, std::bitset<DENSE_BUCKET_SIZE>(0)),
      overflow_bucket_(),
      cardinality_(0),
      b_(n_leading_bits),
      m_(1 << n_leading_bits) {
  overflow_bucket_.reserve(m_);
}

template <typename KeyType>
auto HyperLogLogPresto<KeyType>::ComputeBinary(const hash_t &hash) const
    -> std::bitset<BITSET_CAPACITY> {
  return std::bitset<BITSET_CAPACITY>(hash);
}

template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  // 1. Compute hash and extract bucket index.
  auto hash = CalculateHash(val);
  auto index = (hash >> (BITSET_CAPACITY - b_)) & (m_ - 1);

  // 2. Extract low bits for zero-counting (BITSET_CAPACITY - b_ bits).
  uint64_t tail_mask = (1ULL << (BITSET_CAPACITY - b_)) - 1;
  uint64_t tail_bits = hash & tail_mask;

  // 3. Count trailing zeros (Z).
  uint64_t trailing_zeros;
  if (tail_bits == 0) {
    trailing_zeros = BITSET_CAPACITY - b_;
  } else {
    trailing_zeros = static_cast<uint64_t>(__builtin_ctzll(tail_bits));
  }

  // Clamp Z to the maximum representable value.
  uint64_t max_z_value = BITSET_CAPACITY - b_;
  trailing_zeros = std::min(trailing_zeros, max_z_value);

  // 4. Read current stored Z (combine overflow MSBs and dense LSBs).
  uint64_t current_msbs = 0;
  auto it = overflow_bucket_.find(index);
  if (it != overflow_bucket_.end()) {
    current_msbs = it->second.to_ulong();
  }

  uint64_t current_lsbs = dense_bucket_[index].to_ulong();
  uint64_t current_z = (current_msbs << DENSE_BUCKET_SIZE) | current_lsbs;

  // 5. Compare and update if new Z is larger.
  if (trailing_zeros > current_z) {
    uint64_t lsbs = trailing_zeros & ((1ULL << DENSE_BUCKET_SIZE) - 1);
    uint64_t msbs = trailing_zeros >> DENSE_BUCKET_SIZE;

    // Update dense bucket (LSBs).
    dense_bucket_[index] =
        std::bitset<DENSE_BUCKET_SIZE>(static_cast<unsigned long>(lsbs));

    // Update overflow bucket (MSBs).
    if (msbs > 0) {
      uint64_t max_overflow = (1ULL << OVERFLOW_BUCKET_SIZE) - 1;
      msbs = std::min(msbs, max_overflow);
      overflow_bucket_[index] =
          std::bitset<OVERFLOW_BUCKET_SIZE>(static_cast<unsigned long>(msbs));
    } else if (it != overflow_bucket_.end()) {
      // If MSBs become 0, remove any previous overflow entry.
      overflow_bucket_.erase(it);
    }
  }
}

template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  double sum = 0.0;

  // Harmonic mean of 2^{-register}.
  for (size_t idx = 0; idx < m_; ++idx) {
    uint64_t dense_val = dense_bucket_[idx].to_ulong();

    uint64_t total = dense_val;
    auto it = overflow_bucket_.find(idx);
    if (it != overflow_bucket_.end()) {
      uint64_t overflow_val = it->second.to_ulong();
      total = (overflow_val << DENSE_BUCKET_SIZE) | dense_val;
    }

    sum += std::pow(2.0, -static_cast<double>(total));
  }

  // Compute cardinality without additional zero correction.
  cardinality_ =
      static_cast<uint64_t>(std::floor(CONSTANT * m_ * m_ / sum));
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;

}  // namespace bustub

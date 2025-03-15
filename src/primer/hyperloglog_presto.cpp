//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog_presto.cpp
//
// Identification: src/primer/hyperloglog_presto.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog_presto.h"
#include <bitset>
#include <cstddef>
#include <string>
#include "primer/hyperloglog.h"

namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits)
    : cardinality_(0), b(std::max<int16_t>(n_leading_bits, 0)), m(1 << b) {
  dense_bucket_.resize(m, 0);
}

/** @brief Element is added for HLL calculation. */
template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  hash_t hashVal = CalculateHash(val);
  std::bitset<64> hashB(hashVal);
  hash_t hash = hashB.to_ulong();

  hash_t bucketIdx = hash >> (64 - b);
  uint64_t remainingBits = hash & ((1LL << (64 - b)) - 1);

  if (b == 0) {
    bucketIdx = 0, remainingBits = hash;
  }

  int leadingZeros = GetLeadingZeros(remainingBits);

  UpdateBucket(bucketIdx, leadingZeros);
}

template <typename KeyType>
auto HyperLogLogPresto<KeyType>::UpdateBucket(uint16_t bucketId, int leadingZeros) -> void {
  uint16_t denseValue = dense_bucket_[bucketId].to_ulong();
  uint16_t overflowValue = 0;

  auto overflowIt = overflow_bucket_.find(bucketId);
  if (overflowIt != overflow_bucket_.end()) {
    overflowValue = overflowIt->second.to_ulong();
  }

  uint16_t current_value = denseValue + (overflowValue << DENSE_BUCKET_SIZE);

  if (leadingZeros > current_value) {
    if (leadingZeros < (1 << DENSE_BUCKET_SIZE)) {
      dense_bucket_[bucketId] = std::bitset<DENSE_BUCKET_SIZE>(leadingZeros);
    } else {
      dense_bucket_[bucketId] = std::bitset<DENSE_BUCKET_SIZE>(leadingZeros);
      overflow_bucket_[bucketId] = std::bitset<OVERFLOW_BUCKET_SIZE>((leadingZeros >> DENSE_BUCKET_SIZE));
    }
  }
}

/** @brief Function to compute cardinality. */
template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  double sum = 0.0;

  for (int i = 0; i < m; i++) {
    auto r = dense_bucket_[i];
    int rInt = r.to_ulong();
    if (overflow_bucket_.find(i) != overflow_bucket_.end()) {
      int overflowInt = overflow_bucket_[i].to_ulong();
      rInt += (overflowInt << DENSE_BUCKET_SIZE);
    }
    sum += pow(2, (-1 * rInt));
  }

  double estimate = CONSTANT * m * (m / sum);
  cardinality_ = static_cast<uint64_t>(std::floor<uint64_t>(estimate));
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub

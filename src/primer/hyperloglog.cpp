//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog.cpp
//
// Identification: src/primer/hyperloglog.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog.h"
#include <bitset>
#include <cmath>
#include <string>

namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : b(std::max<int16_t>(n_bits, 0)), m(1 << b), registers(m, 0) {}

/**
 * @brief Function that computes binary.
 *
 * @param[in] hash
 * @returns binary of a given hash
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  /** @TODO(student) Implement this function! */
  std::bitset<BITSET_CAPACITY> b(hash);

  std::string test = b.to_string();
  return b;
}

/**
 * @brief Function that computes leading zeros.
 *
 * @param[in] bset - binary values of a given bitset
 * @returns leading zeros of given binary set
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  std::string t = bset.to_string();

  for (size_t i = 0; i < t.length(); i++) {
    if (t[i] == '1') return i + 1;
  }
  return 0;
}

/**
 * @brief Adds a value into the HyperLogLog.
 *
 * @param[in] val - value that's added into hyperloglog
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  hash_t hashVal = CalculateHash(val);
  std::bitset<BITSET_CAPACITY> binary = ComputeBinary(hashVal);
  int registerId = (binary >> (BITSET_CAPACITY - b)).to_ullong();
  uint64_t p = PositionOfLeftmostOne(binary << b);

  registers[registerId] = std::max<long long>(registers[registerId], p);
}

/**
 * @brief Function that computes cardinality.
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  double sum = 0.0;

  for (const int &r : registers) {
    sum += pow(2, -r);
  }

  double T = CONSTANT * (double)m * (double)((double)m / sum);
  cardinality_ = std::floor(T);
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub

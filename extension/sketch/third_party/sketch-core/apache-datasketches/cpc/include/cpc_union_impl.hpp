/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef CPC_UNION_IMPL_HPP_
#define CPC_UNION_IMPL_HPP_

#include "count_zeros.hpp"

#include <stdexcept>

namespace datasketches {

template<typename A>
cpc_union_alloc<A>::cpc_union_alloc(uint8_t lg_k, uint64_t seed, const A& allocator):
lg_k(lg_k),
seed(seed),
accumulator(nullptr),
bit_matrix(allocator)
{
  if (lg_k < CPC_MIN_LG_K || lg_k > CPC_MAX_LG_K) {
    throw std::invalid_argument("lg_k must be >= " + std::to_string(CPC_MIN_LG_K) + " and <= " + std::to_string(CPC_MAX_LG_K) + ": " + std::to_string(lg_k));
  }
  accumulator = new (AllocCpc(allocator).allocate(1)) cpc_sketch_alloc<A>(lg_k, seed, allocator);
}

template<typename A>
cpc_union_alloc<A>::cpc_union_alloc(const cpc_union_alloc<A>& other):
lg_k(other.lg_k),
seed(other.seed),
accumulator(other.accumulator),
bit_matrix(other.bit_matrix)
{
  if (accumulator != nullptr) {
    accumulator = new (AllocCpc(accumulator->get_allocator()).allocate(1)) cpc_sketch_alloc<A>(*other.accumulator);
  }
}

template<typename A>
cpc_union_alloc<A>::cpc_union_alloc(cpc_union_alloc<A>&& other) noexcept:
lg_k(other.lg_k),
seed(other.seed),
accumulator(other.accumulator),
bit_matrix(std::move(other.bit_matrix))
{
  other.accumulator = nullptr;
}

template<typename A>
cpc_union_alloc<A>::~cpc_union_alloc() {
  if (accumulator != nullptr) {
    AllocCpc allocator(accumulator->get_allocator());
    accumulator->~cpc_sketch_alloc<A>();
    allocator.deallocate(accumulator, 1);
  }
}

template<typename A>
cpc_union_alloc<A>& cpc_union_alloc<A>::operator=(const cpc_union_alloc<A>& other) {
  cpc_union_alloc<A> copy(other);
  std::swap(lg_k, copy.lg_k);
  seed = copy.seed;
  std::swap(accumulator, copy.accumulator);
  bit_matrix = std::move(copy.bit_matrix);
  return *this;
}

template<typename A>
cpc_union_alloc<A>& cpc_union_alloc<A>::operator=(cpc_union_alloc<A>&& other) noexcept {
  std::swap(lg_k, other.lg_k);
  seed = other.seed;
  std::swap(accumulator, other.accumulator);
  bit_matrix = std::move(other.bit_matrix);
  return *this;
}

template<typename A>
void cpc_union_alloc<A>::update(const cpc_sketch_alloc<A>& sketch) {
  internal_update(sketch);
}

template<typename A>
void cpc_union_alloc<A>::update(cpc_sketch_alloc<A>&& sketch) {
  internal_update(std::forward<cpc_sketch_alloc<A>>(sketch));
}

template<typename A>
template<typename S>
void cpc_union_alloc<A>::internal_update(S&& sketch) {
  const uint16_t seed_hash_union = compute_seed_hash(seed);
  const uint16_t seed_hash_sketch = compute_seed_hash(sketch.seed);
  if (seed_hash_union != seed_hash_sketch) {
    throw std::invalid_argument("Incompatible seed hashes: " + std::to_string(seed_hash_union) + ", "
        + std::to_string(seed_hash_sketch));
  }
  const auto src_flavor = sketch.determine_flavor();
  if (cpc_sketch_alloc<A>::flavor::EMPTY == src_flavor) return;

  if (sketch.get_lg_k() < lg_k) reduce_k(sketch.get_lg_k());
  if (sketch.get_lg_k() < lg_k) throw std::logic_error("sketch lg_k < union lg_k");

  if (accumulator == nullptr && bit_matrix.size() == 0) throw std::logic_error("both accumulator and bit matrix are absent");

  if (cpc_sketch_alloc<A>::flavor::SPARSE == src_flavor && accumulator != nullptr)  { // Case A
    if (bit_matrix.size() > 0) throw std::logic_error("union bit_matrix is not expected");
    const auto initial_dest_flavor = accumulator->determine_flavor();
    if (cpc_sketch_alloc<A>::flavor::EMPTY != initial_dest_flavor &&
        cpc_sketch_alloc<A>::flavor::SPARSE != initial_dest_flavor) throw std::logic_error("wrong flavor");

    // The following partially fixes the snowplow problem provided that the K's are equal.
    if (cpc_sketch_alloc<A>::flavor::EMPTY == initial_dest_flavor && lg_k == sketch.get_lg_k()) {
      *accumulator = std::forward<S>(sketch);
      return;
    }

    walk_table_updating_sketch(sketch.surprising_value_table);
    const auto final_dst_flavor = accumulator->determine_flavor();
    // if the accumulator has graduated beyond sparse, switch to a bit matrix representation
    if (final_dst_flavor != cpc_sketch_alloc<A>::flavor::EMPTY && final_dst_flavor != cpc_sketch_alloc<A>::flavor::SPARSE) {
      switch_to_bit_matrix();
    }
    return;
  }

  if (cpc_sketch_alloc<A>::flavor::SPARSE == src_flavor && bit_matrix.size() > 0)  { // Case B
    if (accumulator != nullptr) throw std::logic_error("union accumulator != null");
    or_table_into_matrix(sketch.surprising_value_table);
    return;
  }

  if (cpc_sketch_alloc<A>::flavor::HYBRID != src_flavor && cpc_sketch_alloc<A>::flavor::PINNED != src_flavor
      && cpc_sketch_alloc<A>::flavor::SLIDING != src_flavor) throw std::logic_error("wrong flavor");

  // source is past SPARSE mode, so make sure that dest is a bit matrix
  if (accumulator != nullptr) {
    if (bit_matrix.size() > 0) throw std::logic_error("union bit matrix is not expected");
    const auto dst_flavor = accumulator->determine_flavor();
    if (cpc_sketch_alloc<A>::flavor::EMPTY != dst_flavor && cpc_sketch_alloc<A>::flavor::SPARSE != dst_flavor) {
      throw std::logic_error("wrong flavor");
    }
    switch_to_bit_matrix();
  }
  if (bit_matrix.size() == 0) throw std::logic_error("union bit_matrix is expected");

  if (cpc_sketch_alloc<A>::flavor::HYBRID == src_flavor || cpc_sketch_alloc<A>::flavor::PINNED == src_flavor) { // Case C
    or_window_into_matrix(sketch.sliding_window, sketch.window_offset, sketch.get_lg_k());
    or_table_into_matrix(sketch.surprising_value_table);
    return;
  }

  // SLIDING mode involves inverted logic, so we can't just walk the source sketch.
  // Instead, we convert it to a bitMatrix that can be OR'ed into the destination.
  if (cpc_sketch_alloc<A>::flavor::SLIDING != src_flavor) throw std::logic_error("wrong flavor"); // Case D
  vector_u64<A> src_matrix = sketch.build_bit_matrix();
  or_matrix_into_matrix(src_matrix, sketch.get_lg_k());
}

template<typename A>
cpc_sketch_alloc<A> cpc_union_alloc<A>::get_result() const {
  if (accumulator != nullptr) {
    if (bit_matrix.size() > 0) throw std::logic_error("bit_matrix is not expected");
    return get_result_from_accumulator();
  }
  if (bit_matrix.size() == 0) throw std::logic_error("bit_matrix is expected");
  return get_result_from_bit_matrix();
}

template<typename A>
cpc_sketch_alloc<A> cpc_union_alloc<A>::get_result_from_accumulator() const {
  if (lg_k != accumulator->get_lg_k()) throw std::logic_error("lg_k != accumulator->lg_k");
  if (accumulator->get_num_coupons() == 0) {
    return cpc_sketch_alloc<A>(lg_k, seed, accumulator->get_allocator());
  }
  if (accumulator->determine_flavor() != cpc_sketch_alloc<A>::flavor::SPARSE) throw std::logic_error("wrong flavor");
  cpc_sketch_alloc<A> copy(*accumulator);
  copy.was_merged = true;
  return copy;
}

template<typename A>
cpc_sketch_alloc<A> cpc_union_alloc<A>::get_result_from_bit_matrix() const {
  const uint32_t k = 1 << lg_k;
  const uint32_t num_coupons = count_bits_set_in_matrix(bit_matrix.data(), k);

  const auto flavor = cpc_sketch_alloc<A>::determine_flavor(lg_k, num_coupons);
  if (flavor != cpc_sketch_alloc<A>::flavor::HYBRID && flavor != cpc_sketch_alloc<A>::flavor::PINNED
      && flavor != cpc_sketch_alloc<A>::flavor::SLIDING) throw std::logic_error("wrong flavor");

  const uint8_t offset = cpc_sketch_alloc<A>::determine_correct_offset(lg_k, num_coupons);

  vector_u8<A> sliding_window(k, 0, bit_matrix.get_allocator());
  // don't need to zero the window's memory

  // dynamically growing caused snowplow effect
  uint8_t table_lg_size = lg_k - 4; // K/16; in some cases this will end up being oversized
  if (table_lg_size < 2) table_lg_size = 2;
  u32_table<A> table(table_lg_size, 6 + lg_k, bit_matrix.get_allocator());

  // the following should work even when the offset is zero
  const uint64_t mask_for_clearing_window = (static_cast<uint64_t>(0xff) << offset) ^ UINT64_MAX;
  const uint64_t mask_for_flipping_early_zone = (static_cast<uint64_t>(1) << offset) - 1;
  uint64_t all_surprises_ored = 0;

  // The snowplow effect was caused by processing the rows in order,
  // but we have fixed it by using a sufficiently large hash table.
  for (uint32_t i = 0; i < k; i++) {
    uint64_t pattern = bit_matrix[i];
    sliding_window[i] = (pattern >> offset) & 0xff;
    pattern &= mask_for_clearing_window;
    pattern ^= mask_for_flipping_early_zone; // this flipping converts surprising 0's to 1's
    all_surprises_ored |= pattern;
    while (pattern != 0) {
      const uint8_t col = count_trailing_zeros_in_u64(pattern);
      pattern = pattern ^ (static_cast<uint64_t>(1) << col); // erase the 1
      const uint32_t row_col = (i << 6) | col;
      bool is_novel = table.maybe_insert(row_col);
      if (!is_novel) throw std::logic_error("is_novel != true");
    }
  }

  // at this point we could shrink an oversized hash table, but the relative waste isn't very big

  uint8_t first_interesting_column = count_trailing_zeros_in_u64(all_surprises_ored);
  if (first_interesting_column > offset) first_interesting_column = offset; // corner case

  // HIP-related fields will contain zeros, and that is okay
  return cpc_sketch_alloc<A>(lg_k, num_coupons, first_interesting_column, std::move(table), std::move(sliding_window), false, 0, 0, seed);
}

template<typename A>
void cpc_union_alloc<A>::switch_to_bit_matrix() {
  bit_matrix = accumulator->build_bit_matrix();
  AllocCpc allocator(accumulator->get_allocator());
  accumulator->~cpc_sketch_alloc<A>();
  allocator.deallocate(accumulator, 1);
  accumulator = nullptr;
}

template<typename A>
void cpc_union_alloc<A>::walk_table_updating_sketch(const u32_table<A>& table) {
  const uint32_t* slots = table.get_slots();
  const uint32_t num_slots = 1 << table.get_lg_size();
  const uint64_t dst_mask = (((1 << accumulator->get_lg_k()) - 1) << 6) | 63; // downsamples when dst lgK < src LgK

  // Using a golden ratio stride fixes the snowplow effect.
  const double golden = 0.6180339887498949025;
  uint32_t stride = static_cast<uint32_t>(golden * static_cast<double>(num_slots));
  if (stride < 2) throw std::logic_error("stride < 2");
  if (stride == ((stride >> 1) << 1)) stride += 1; // force the stride to be odd
  if (stride < 3 || stride >= num_slots) throw std::out_of_range("stride out of range");

  for (uint32_t i = 0, j = 0; i < num_slots; i++, j += stride) {
    j &= num_slots - 1;
    const uint32_t row_col = slots[j];
    if (row_col != UINT32_MAX) {
      accumulator->row_col_update(row_col & dst_mask);
    }
  }
}

template<typename A>
void cpc_union_alloc<A>::or_table_into_matrix(const u32_table<A>& table) {
  const uint32_t* slots = table.get_slots();
  const uint32_t num_slots = 1 << table.get_lg_size();
  const uint64_t dest_mask = (1 << lg_k) - 1;  // downsamples when dst lgK < sr LgK
  for (uint32_t i = 0; i < num_slots; i++) {
    const uint32_t row_col = slots[i];
    if (row_col != UINT32_MAX) {
      const uint8_t col = row_col & 63;
      const uint32_t row = row_col >> 6;
      bit_matrix[row & dest_mask] |= static_cast<uint64_t>(1) << col; // set the bit
    }
  }
}

template<typename A>
void cpc_union_alloc<A>::or_window_into_matrix(const vector_u8<A>& sliding_window, uint8_t offset, uint8_t src_lg_k) {
  if (lg_k > src_lg_k) throw std::logic_error("dst LgK > src LgK");
  const uint64_t dst_mask = (1 << lg_k) - 1; // downsamples when dst lgK < src LgK
  const uint32_t src_k = 1 << src_lg_k;
  for (uint32_t src_row = 0; src_row < src_k; src_row++) {
    bit_matrix[src_row & dst_mask] |= static_cast<uint64_t>(sliding_window[src_row]) << offset;
  }
}

template<typename A>
void cpc_union_alloc<A>::or_matrix_into_matrix(const vector_u64<A>& src_matrix, uint8_t src_lg_k) {
  if (lg_k > src_lg_k) throw std::logic_error("dst LgK > src LgK");
  const uint64_t dst_mask = (1 << lg_k) - 1; // downsamples when dst lgK < src LgK
  const uint32_t src_k = 1 << src_lg_k;
  for (uint32_t src_row = 0; src_row < src_k; src_row++) {
    bit_matrix[src_row & dst_mask] |= src_matrix[src_row];
  }
}

template<typename A>
void cpc_union_alloc<A>::reduce_k(uint8_t new_lg_k) {
  if (new_lg_k >= lg_k) throw std::logic_error("new LgK >= union lgK");
  if (accumulator == nullptr && bit_matrix.size() == 0) throw std::logic_error("both accumulator and bit_matrix are absent");

  if (bit_matrix.size() > 0) { // downsample the unioner's bit matrix
    if (accumulator != nullptr) throw std::logic_error("accumulator is not null");
    vector_u64<A> old_matrix = std::move(bit_matrix);
    const uint8_t old_lg_k = lg_k;
    const uint32_t new_k = 1 << new_lg_k;
    bit_matrix = vector_u64<A>(new_k, 0, old_matrix.get_allocator());
    lg_k = new_lg_k;
    or_matrix_into_matrix(old_matrix, old_lg_k);
    return;
  }

  if (accumulator != nullptr) { // downsample the unioner's sketch
    if (bit_matrix.size() > 0) throw std::logic_error("bit_matrix is not expected");
    if (!accumulator->is_empty()) {
      cpc_sketch_alloc<A> old_accumulator(*accumulator);
      *accumulator = cpc_sketch_alloc<A>(new_lg_k, seed, old_accumulator.get_allocator());
      walk_table_updating_sketch(old_accumulator.surprising_value_table);
    }
    lg_k = new_lg_k;

    const auto final_new_flavor = accumulator->determine_flavor();
    // if the new sketch has graduated beyond sparse, convert to bit_matrix
    if (final_new_flavor != cpc_sketch_alloc<A>::flavor::EMPTY &&
        final_new_flavor != cpc_sketch_alloc<A>::flavor::SPARSE) {
      switch_to_bit_matrix();
    }
    return;
  }

  throw std::logic_error("invalid state");
}

} /* namespace datasketches */

#endif

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

#ifndef KLL_HELPER_IMPL_HPP_
#define KLL_HELPER_IMPL_HPP_

#include <algorithm>
#include <stdexcept>

#include "common_defs.hpp"

namespace datasketches {

bool kll_helper::is_even(uint32_t value) {
  return (value & 1) == 0;
}

bool kll_helper::is_odd(uint32_t value) {
  return (value & 1) > 0;
}

uint8_t kll_helper::floor_of_log2_of_fraction(uint64_t numer, uint64_t denom) {
  if (denom > numer) return 0;
  uint8_t count = 0;
  while (true) {
    denom <<= 1;
    if (denom > numer) return count;
    count++;
  }
}

uint8_t kll_helper::ub_on_num_levels(uint64_t n) {
  if (n == 0) return 1;
  return 1 + floor_of_log2_of_fraction(n, 1);
}

uint32_t kll_helper::compute_total_capacity(uint16_t k, uint8_t m, uint8_t num_levels) {
  uint32_t total = 0;
  for (uint8_t h = 0; h < num_levels; h++) {
    total += level_capacity(k, num_levels, h, m);
  }
  return total;
}

uint16_t kll_helper::level_capacity(uint16_t k, uint8_t numLevels, uint8_t height, uint8_t min_wid) {
  if (height >= numLevels) throw std::invalid_argument("height >= numLevels");
  const uint8_t depth = numLevels - height - 1;
  return std::max<uint16_t>(min_wid, int_cap_aux(k, depth));
}

uint16_t kll_helper::int_cap_aux(uint16_t k, uint8_t depth) {
  if (depth > 60) throw std::invalid_argument("depth > 60");
  if (depth <= 30) return int_cap_aux_aux(k, depth);
  const uint8_t half = depth / 2;
  const uint8_t rest = depth - half;
  const uint16_t tmp = int_cap_aux_aux(k, half);
  return int_cap_aux_aux(tmp, rest);
}

uint16_t kll_helper::int_cap_aux_aux(uint16_t k, uint8_t depth) {
  if (depth > 30) throw std::invalid_argument("depth > 30");
  const uint64_t twok = k << 1; // for rounding, we pre-multiply by 2
  const uint64_t tmp = (uint64_t) (((uint64_t) twok << depth) / powers_of_three[depth]);
  const uint64_t result = (tmp + 1) >> 1; // then here we add 1 and divide by 2
  if (result > k) throw std::logic_error("result > k");
  return static_cast<uint16_t>(result);
}

uint64_t kll_helper::sum_the_sample_weights(uint8_t num_levels, const uint32_t* levels) {
  uint64_t total = 0;
  uint64_t weight = 1;
  for (uint8_t lvl = 0; lvl < num_levels; lvl++) {
    total += weight * (levels[lvl + 1] - levels[lvl]);
    weight *= 2;
  }
  return total;
}

template <typename T>
void kll_helper::randomly_halve_down(T* buf, uint32_t start, uint32_t length) {
  if (!is_even(length)) throw std::invalid_argument("length must be even");
  const uint32_t half_length = length / 2;
#ifdef KLL_VALIDATION
  const uint32_t offset = deterministic_offset();
#else
  const uint32_t offset = random_bit();
#endif
  uint32_t j = start + offset;
  for (uint32_t i = start; i < (start + half_length); i++) {
    if (i != j) buf[i] = std::move(buf[j]);
    j += 2;
  }
}

template <typename T>
void kll_helper::randomly_halve_up(T* buf, uint32_t start, uint32_t length) {
  if (!is_even(length)) throw std::invalid_argument("length must be even");
  const uint32_t half_length = length / 2;
#ifdef KLL_VALIDATION
  const uint32_t offset = deterministic_offset();
#else
  const uint32_t offset = random_bit();
#endif
  uint32_t j = (start + length) - 1 - offset;
  for (uint32_t i = (start + length) - 1; i >= (start + half_length); i--) {
    if (i != j) buf[i] = std::move(buf[j]);
    j -= 2;
  }
}

// this version moves objects within the same buffer
// assumes that destination has initialized objects
// does not destroy the originals after the move
template <typename T, typename C>
void kll_helper::merge_sorted_arrays(T* buf, uint32_t start_a, uint32_t len_a, uint32_t start_b, uint32_t len_b, uint32_t start_c) {
  const uint32_t len_c = len_a + len_b;
  const uint32_t lim_a = start_a + len_a;
  const uint32_t lim_b = start_b + len_b;
  const uint32_t lim_c = start_c + len_c;

  uint32_t a = start_a;
  uint32_t b = start_b;

  for (uint32_t c = start_c; c < lim_c; c++) {
    if (a == lim_a) {
      if (b != c) buf[c] = std::move(buf[b]);
      b++;
    } else if (b == lim_b) {
      if (a != c) buf[c] = std::move(buf[a]);
      a++;
    } else if (C()(buf[a], buf[b])) {
      if (a != c) buf[c] = std::move(buf[a]);
      a++;
    } else {
      if (b != c) buf[c] = std::move(buf[b]);
      b++;
    }
  }
  if (a != lim_a || b != lim_b) throw std::logic_error("inconsistent state");
}

// this version is to merge from two different buffers into a third buffer
// initializes objects is the destination buffer
// moves objects from buf_a and destroys the originals
// copies objects from buf_b
template <typename T, typename C>
void kll_helper::merge_sorted_arrays(const T* buf_a, uint32_t start_a, uint32_t len_a, const T* buf_b, uint32_t start_b, uint32_t len_b, T* buf_c, uint32_t start_c) {
  const uint32_t len_c = len_a + len_b;
  const uint32_t lim_a = start_a + len_a;
  const uint32_t lim_b = start_b + len_b;
  const uint32_t lim_c = start_c + len_c;

  uint32_t a = start_a;
  uint32_t b = start_b;

  for (uint32_t c = start_c; c < lim_c; c++) {
    if (a == lim_a) {
      new (&buf_c[c]) T(buf_b[b++]);
    } else if (b == lim_b) {
      new (&buf_c[c]) T(std::move(buf_a[a]));
      buf_a[a++].~T();
    } else if (C()(buf_a[a], buf_b[b])) {
      new (&buf_c[c]) T(std::move(buf_a[a]));
      buf_a[a++].~T();
    } else {
      new (&buf_c[c]) T(buf_b[b++]);
    }
  }
  if (a != lim_a || b != lim_b) throw std::logic_error("inconsistent state");
}

/*
 * Here is what we do for each level:
 * If it does not need to be compacted, then simply copy it over.
 *
 * Otherwise, it does need to be compacted, so...
 *   Copy zero or one guy over.
 *   If the level above is empty, halve up.
 *   Else the level above is nonempty, so...
 *        halve down, then merge up.
 *   Adjust the boundaries of the level above.
 *
 * It can be proved that general_compress returns a sketch that satisfies the space constraints
 * no matter how much data is passed in.
 * All levels except for level zero must be sorted before calling this, and will still be
 * sorted afterwards.
 * Level zero is not required to be sorted before, and may not be sorted afterwards.
 */
template <typename T, typename C>
kll_helper::compress_result kll_helper::general_compress(uint16_t k, uint8_t m, uint8_t num_levels_in, T* items,
        uint32_t* in_levels, uint32_t* out_levels, bool is_level_zero_sorted)
{
  if (num_levels_in == 0) throw std::invalid_argument("num_levels_in == 0"); // things are too weird if zero levels are allowed
  const uint32_t starting_item_count = in_levels[num_levels_in] - in_levels[0];
  uint8_t current_num_levels = num_levels_in;
  uint32_t current_item_count = starting_item_count; // decreases with each compaction
  uint32_t target_item_count = compute_total_capacity(k, m, current_num_levels); // increases if we add levels
  bool done_yet = false;
  out_levels[0] = 0;
  uint8_t current_level = 0;
  while (!done_yet) {

    // If we are at the current top level, add an empty level above it for convenience,
    // but do not increment num_levels until later
    if (current_level == (current_num_levels - 1)) {
      in_levels[current_level + 2] = in_levels[current_level + 1];
    }

    const auto raw_beg = in_levels[current_level];
    const auto raw_lim = in_levels[current_level + 1];
    const auto raw_pop = raw_lim - raw_beg;

    if ((current_item_count < target_item_count) || (raw_pop < level_capacity(k, current_num_levels, current_level, m))) {
      // move level over as is
      // make sure we are not moving data upwards
      if (raw_beg < out_levels[current_level]) throw std::logic_error("wrong move");
      std::move(items + raw_beg, items + raw_lim, items + out_levels[current_level]);
      out_levels[current_level + 1] = out_levels[current_level] + raw_pop;
    } else {
      // The sketch is too full AND this level is too full, so we compact it
      // Note: this can add a level and thus change the sketches capacities

      const auto pop_above = in_levels[current_level + 2] - raw_lim;
      const bool odd_pop = is_odd(raw_pop);
      const auto adj_beg = odd_pop ? 1 + raw_beg : raw_beg;
      const auto adj_pop = odd_pop ? raw_pop - 1 : raw_pop;
      const auto half_adj_pop = adj_pop / 2;

      if (odd_pop) { // move one guy over
        items[out_levels[current_level]] = std::move(items[raw_beg]);
        out_levels[current_level + 1] = out_levels[current_level] + 1;
      } else { // even number of items
        out_levels[current_level + 1] = out_levels[current_level];
      }

      // level zero might not be sorted, so we must sort it if we wish to compact it
      if ((current_level == 0) && !is_level_zero_sorted) {
        std::sort(items + adj_beg, items + adj_beg + adj_pop, C());
      }

      if (pop_above == 0) { // Level above is empty, so halve up
        randomly_halve_up(items, adj_beg, adj_pop);
      } else { // Level above is nonempty, so halve down, then merge up
        randomly_halve_down(items, adj_beg, adj_pop);
        merge_sorted_arrays<T, C>(items, adj_beg, half_adj_pop, raw_lim, pop_above, adj_beg + half_adj_pop);
      }

      // track the fact that we just eliminated some data
      current_item_count -= half_adj_pop;

      // adjust the boundaries of the level above
      in_levels[current_level + 1] = in_levels[current_level + 1] - half_adj_pop;

      // increment num_levels if we just compacted the old top level
      // this creates some more capacity (the size of the new bottom level)
      if (current_level == (current_num_levels - 1)) {
        current_num_levels++;
        target_item_count += level_capacity(k, current_num_levels, 0, m);
      }

    } // end of code for compacting a level

    // determine whether we have processed all levels yet (including any new levels that we created)

    if (current_level == (current_num_levels - 1)) done_yet = true;
    current_level++;
  } // end of loop over levels

  if ((out_levels[current_num_levels] - out_levels[0]) != current_item_count) throw std::logic_error("inconsistent state");

  for (uint32_t i = current_item_count; i < starting_item_count; i++) items[i].~T();

  compress_result result;
  result.final_num_levels = current_num_levels;
  result.final_capacity = target_item_count;
  result.final_num_items = current_item_count;
  return result;
}

template<typename T>
void kll_helper::copy_construct(const T* src, size_t src_first, size_t src_last, T* dst, size_t dst_first) {
  while (src_first != src_last) {
    new (&dst[dst_first++]) T(src[src_first++]);
  }
}

template<typename T>
void kll_helper::move_construct(T* src, size_t src_first, size_t src_last, T* dst, size_t dst_first, bool destroy) {
  while (src_first != src_last) {
    new (&dst[dst_first++]) T(std::move(src[src_first]));
    if (destroy) src[src_first].~T();
    src_first++;
  }
}

#ifdef KLL_VALIDATION
uint32_t kll_helper::deterministic_offset() {
  const uint32_t result(kll_next_offset);
  kll_next_offset = 1 - kll_next_offset;
  return result;
}
#endif

} /* namespace datasketches */

#endif // KLL_HELPER_IMPL_HPP_

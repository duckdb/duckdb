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

// author Kevin Lang, Oath Research

#ifndef U32_TABLE_IMPL_HPP_
#define U32_TABLE_IMPL_HPP_

#include <stdexcept>
#include <algorithm>
#include <climits>

namespace datasketches {

template<typename A>
u32_table<A>::u32_table(const A& allocator):
lg_size(0),
num_valid_bits(0),
num_items(0),
slots(allocator)
{}

template<typename A>
u32_table<A>::u32_table(uint8_t lg_size, uint8_t num_valid_bits, const A& allocator):
lg_size(lg_size),
num_valid_bits(num_valid_bits),
num_items(0),
slots(1ULL << lg_size, UINT32_MAX, allocator)
{
  if (lg_size < 2) throw std::invalid_argument("lg_size must be >= 2");
  if (num_valid_bits < 1 || num_valid_bits > 32) throw std::invalid_argument("num_valid_bits must be between 1 and 32");
}

template<typename A>
uint32_t u32_table<A>::get_num_items() const {
  return num_items;
}

template<typename A>
const uint32_t* u32_table<A>::get_slots() const {
  return slots.data();
}

template<typename A>
uint8_t u32_table<A>::get_lg_size() const {
  return lg_size;
}

template<typename A>
void u32_table<A>::clear() {
  std::fill(slots.begin(), slots.end(), UINT32_MAX);
  num_items = 0;
}

template<typename A>
bool u32_table<A>::maybe_insert(uint32_t item) {
  const uint32_t index = lookup(item);
  if (slots[index] == item) return false;
  if (slots[index] != UINT32_MAX) throw std::logic_error("could not insert");
  slots[index] = item;
  num_items++;
  if (U32_TABLE_UPSIZE_DENOM * num_items > U32_TABLE_UPSIZE_NUMER * (1 << lg_size)) {
    rebuild(lg_size + 1);
  }
  return true;
}

template<typename A>
bool u32_table<A>::maybe_delete(uint32_t item) {
  const uint32_t index = lookup(item);
  if (slots[index] == UINT32_MAX) return false;
  if (slots[index] != item) throw std::logic_error("item does not exist");
  if (num_items == 0) throw std::logic_error("delete error");
  // delete the item
  slots[index] = UINT32_MAX;
  num_items--;

  // re-insert all items between the freed slot and the next empty slot
  const size_t mask = (1 << lg_size) - 1;
  size_t probe = (index + 1) & mask;
  uint32_t fetched = slots[probe];
  while (fetched != UINT32_MAX) {
    slots[probe] = UINT32_MAX;
    must_insert(fetched);
    probe = (probe + 1) & mask;
    fetched = slots[probe];
  }
  // shrink if necessary
  if (U32_TABLE_DOWNSIZE_DENOM * num_items < U32_TABLE_DOWNSIZE_NUMER * (1 << lg_size) && lg_size > 2) {
    rebuild(lg_size - 1);
  }
  return true;
}

// this one is specifically tailored to be a part of fm85 decompression scheme
template<typename A>
u32_table<A> u32_table<A>::make_from_pairs(const uint32_t* pairs, uint32_t num_pairs, uint8_t lg_k, const A& allocator) {
  uint8_t lg_num_slots = 2;
  while (U32_TABLE_UPSIZE_DENOM * num_pairs > U32_TABLE_UPSIZE_NUMER * (1 << lg_num_slots)) lg_num_slots++;
  u32_table<A> table(lg_num_slots, 6 + lg_k, allocator);
  // Note: there is a possible "snowplow effect" here because the caller is passing in a sorted pairs array
  // However, we are starting out with the correct final table size, so the problem might not occur
  for (size_t i = 0; i < num_pairs; i++) {
    table.must_insert(pairs[i]);
  }
  table.num_items = num_pairs;
  return table;
}

template<typename A>
uint32_t u32_table<A>::lookup(uint32_t item) const {
  const uint32_t size = 1 << lg_size;
  const uint32_t mask = size - 1;
  const uint8_t shift = num_valid_bits - lg_size;
  uint32_t probe = item >> shift;
  if (probe > mask) throw std::logic_error("probe out of range");
  while (slots[probe] != item && slots[probe] != UINT32_MAX) {
    probe = (probe + 1) & mask;
  }
  return probe;
}

// counts and resizing must be handled by the caller
template<typename A>
void u32_table<A>::must_insert(uint32_t item) {
  const uint32_t index = lookup(item);
  if (slots[index] == item) throw std::logic_error("item exists");
  if (slots[index] != UINT32_MAX) throw std::logic_error("could not insert");
  slots[index] = item;
}

template<typename A>
void u32_table<A>::rebuild(uint8_t new_lg_size) {
  if (new_lg_size < 2) throw std::logic_error("lg_size must be >= 2");
  const uint32_t old_size = 1 << lg_size;
  const uint32_t new_size = 1 << new_lg_size;
  if (new_size <= num_items) throw std::logic_error("new_size <= num_items");
  vector_u32<A> old_slots = std::move(slots);
  slots = vector_u32<A>(new_size, UINT32_MAX, old_slots.get_allocator());
  lg_size = new_lg_size;
  for (uint32_t i = 0; i < old_size; i++) {
    if (old_slots[i] != UINT32_MAX) {
      must_insert(old_slots[i]);
    }
  }
}

// While extracting the items from a linear probing hashtable,
// this will usually undo the wrap-around provided that the table
// isn't too full. Experiments suggest that for sufficiently large tables
// the load factor would have to be over 90 percent before this would fail frequently,
// and even then the subsequent sort would fix things up.
// The result is nearly sorted, so make sure to use an efficient sort for that case
template<typename A>
vector_u32<A> u32_table<A>::unwrapping_get_items() const {
  if (num_items == 0) return vector_u32<A>(slots.get_allocator());
  const uint32_t table_size = 1 << lg_size;
  vector_u32<A> result(num_items, 0, slots.get_allocator());
  size_t i = 0;
  size_t l = 0;
  size_t r = num_items - 1;

  // special rules for the region before the first empty slot
  uint32_t hi_bit = 1 << (num_valid_bits - 1);
  while (i < table_size && slots[i] != UINT32_MAX) {
    const uint32_t item = slots[i++];
    if (item & hi_bit) { result[r--] = item; } // this item was probably wrapped, so move to end
    else               { result[l++] = item; }
  }

  // the rest of the table is processed normally
  while (i < table_size) {
    const uint32_t item = slots[i++];
    if (item != UINT32_MAX) result[l++] = item;
  }
  if (l != r + 1) throw std::logic_error("unwrapping error");
  return result;
}

// This merge is safe to use in carefully designed overlapping scenarios.
template<typename A>
void u32_table<A>::merge(
  const uint32_t* arr_a, size_t start_a, size_t length_a, // input
  const uint32_t* arr_b, size_t start_b, size_t length_b, // input
  uint32_t* arr_c, size_t start_c // output
) {
  const size_t length_c = length_a + length_b;
  const size_t lim_a = start_a + length_a;
  const size_t lim_b = start_b + length_b;
  const size_t lim_c = start_c + length_c;
  size_t a = start_a;
  size_t b = start_b;
  size_t c = start_c;
  for ( ; c < lim_c ; c++) {
    if      (b >= lim_b)          { arr_c[c] = arr_a[a++]; }
    else if (a >= lim_a)          { arr_c[c] = arr_b[b++]; }
    else if (arr_a[a] < arr_b[b]) { arr_c[c] = arr_a[a++]; }
    else                          { arr_c[c] = arr_b[b++]; }
  }
  if (a != lim_a || b != lim_b) throw std::logic_error("merging error");
}

// In applications where the input array is already nearly sorted,
// insertion sort runs in linear time with a very small constant.
// This introspective version of insertion sort protects against
// the quadratic cost of sorting bad input arrays.
// It keeps track of how much work has been done, and if that exceeds a
// constant times the array length, it switches to a different sorting algorithm.

template<typename A>
void u32_table<A>::introspective_insertion_sort(uint32_t* a, size_t l, size_t r) { // r points past the rightmost element
  const size_t length = r - l;
  const size_t cost_limit = 8 * length;
  size_t cost = 0;
  for (size_t i = l + 1; i < r; i++) {
    size_t j = i;
    uint32_t v = a[i];
    while (j >= l + 1 && v < a[j - 1]) {
      a[j] = a[j - 1];
      j--;
    }
    a[j] = v;
    cost += i - j; // distance moved is a measure of work
    if (cost > cost_limit) {
      knuth_shell_sort3(a, l, r);
      return;
    }
  }
}

template<typename A>
void u32_table<A>::knuth_shell_sort3(uint32_t* a, size_t l, size_t r) {
  size_t h;
  for (h = 1; h < (r - l) / 9; h = 3 * h + 1);
  for ( ; h > 0; h /= 3) {
    for (size_t i = l + h; i < r; i++) {
      size_t j = i;
      const uint32_t v = a[i];
      while (j >= l + h && v < a[j - h]) {
        a[j] = a[j - h];
        j -= h;
      }
      a[j] = v;
    }
  }
}

} /* namespace datasketches */

#endif

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

#ifndef U32_TABLE_HPP_
#define U32_TABLE_HPP_

// This is a highly specialized hash table that was designed
// to be a part of the library's CPC sketch implementation

#include "cpc_common.hpp"

namespace datasketches {

static const uint32_t U32_TABLE_UPSIZE_NUMER = 3LL;
static const uint32_t U32_TABLE_UPSIZE_DENOM = 4LL;

static const uint32_t U32_TABLE_DOWNSIZE_NUMER = 1LL;
static const uint32_t U32_TABLE_DOWNSIZE_DENOM = 4LL;

template<typename A>
class u32_table {
public:

  u32_table(const A& allocator);
  u32_table(uint8_t lg_size, uint8_t num_valid_bits, const A& allocator);

  inline uint32_t get_num_items() const;
  inline const uint32_t* get_slots() const;
  inline uint8_t get_lg_size() const;
  inline void clear();

  // returns true iff the item was new and was therefore added to the table
  inline bool maybe_insert(uint32_t item);
  // returns true iff the item was present and was therefore removed from the table
  inline bool maybe_delete(uint32_t item);

  static u32_table make_from_pairs(const uint32_t* pairs, uint32_t num_pairs, uint8_t lg_k, const A& allocator);

  vector_u32<A> unwrapping_get_items() const;

  static void merge(
    const uint32_t* arr_a, size_t start_a, size_t length_a, // input
    const uint32_t* arr_b, size_t start_b, size_t length_b, // input
    uint32_t* arr_c, size_t start_c                         // output
  );

  static void introspective_insertion_sort(uint32_t* a, size_t l, size_t r);
  static void knuth_shell_sort3(uint32_t* a, size_t l, size_t r);

private:

  uint8_t lg_size; // log2 of number of slots
  uint8_t num_valid_bits;
  uint32_t num_items;
  vector_u32<A> slots;

  inline uint32_t lookup(uint32_t item) const;
  inline void must_insert(uint32_t item);
  inline void rebuild(uint8_t new_lg_size);
};

} /* namespace datasketches */

#include "u32_table_impl.hpp"

#endif

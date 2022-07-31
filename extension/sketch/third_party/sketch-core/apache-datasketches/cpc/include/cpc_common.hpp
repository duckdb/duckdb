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

#ifndef CPC_COMMON_HPP_
#define CPC_COMMON_HPP_

#include <memory>

#include "MurmurHash3.h"

namespace datasketches {

namespace cpc_constants {
    const uint8_t MIN_LG_K = 4;
    const uint8_t MAX_LG_K = 26;
    const uint8_t DEFAULT_LG_K = 11;
}

// TODO: Redundant and deprecated. Will be removed in next major version release.
static const uint8_t CPC_MIN_LG_K = cpc_constants::MIN_LG_K;
static const uint8_t CPC_MAX_LG_K = cpc_constants::MAX_LG_K;
static const uint8_t CPC_DEFAULT_LG_K = cpc_constants::DEFAULT_LG_K;

template<typename A> using AllocU8 = typename std::allocator_traits<A>::template rebind_alloc<uint8_t>;
template<typename A> using AllocU16 = typename std::allocator_traits<A>::template rebind_alloc<uint16_t>;
template<typename A> using AllocU32 = typename std::allocator_traits<A>::template rebind_alloc<uint32_t>;
template<typename A> using AllocU64 = typename std::allocator_traits<A>::template rebind_alloc<uint64_t>;

template<typename A> using vector_u8 = std::vector<uint8_t, AllocU8<A>>;
template<typename A> using vector_u32 = std::vector<uint32_t, AllocU32<A>>;
template<typename A> using vector_u64 = std::vector<uint64_t, AllocU64<A>>;

// forward declaration
template<typename A> class u32_table;

template<typename A>
struct compressed_state {
  explicit compressed_state(const A& allocator): table_data(allocator), table_data_words(0), table_num_entries(0),
      window_data(allocator), window_data_words(0) {}
  vector_u32<A> table_data;
  uint32_t table_data_words;
  uint32_t table_num_entries; // can be different from the number of entries in the sketch in hybrid mode
  vector_u32<A> window_data;
  uint32_t window_data_words;
};

template<typename A>
struct uncompressed_state {
  explicit uncompressed_state(const A& allocator): table(allocator), window(allocator) {}
  u32_table<A> table;
  vector_u8<A> window;
};

} /* namespace datasketches */

#endif

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

#ifndef COMPACT_THETA_SKETCH_PARSER_HPP_
#define COMPACT_THETA_SKETCH_PARSER_HPP_

#include <stdint.h>

namespace datasketches {

template<bool dummy>
class compact_theta_sketch_parser {
public:
  struct compact_theta_sketch_data {
    bool is_empty;
    bool is_ordered;
    uint16_t seed_hash;
    uint32_t num_entries;
    uint64_t theta;
    const uint64_t* entries;
  };

  static compact_theta_sketch_data parse(const void* ptr, size_t size, uint64_t seed, bool dump_on_error = false);

private:
  // offsets are in sizeof(type)
  static const size_t COMPACT_SKETCH_PRE_LONGS_BYTE = 0;
  static const size_t COMPACT_SKETCH_SERIAL_VERSION_BYTE = 1;
  static const size_t COMPACT_SKETCH_TYPE_BYTE = 2;
  static const size_t COMPACT_SKETCH_FLAGS_BYTE = 5;
  static const size_t COMPACT_SKETCH_SEED_HASH_U16 = 3;
  static const size_t COMPACT_SKETCH_NUM_ENTRIES_U32 = 2;
  static const size_t COMPACT_SKETCH_SINGLE_ENTRY_U64 = 1;
  static const size_t COMPACT_SKETCH_ENTRIES_EXACT_U64 = 2;
  static const size_t COMPACT_SKETCH_THETA_U64 = 2;
  static const size_t COMPACT_SKETCH_ENTRIES_ESTIMATION_U64 = 3;

  static const uint8_t COMPACT_SKETCH_IS_EMPTY_FLAG = 2;
  static const uint8_t COMPACT_SKETCH_IS_ORDERED_FLAG = 4;

  static const uint8_t COMPACT_SKETCH_SERIAL_VERSION = 3;
  static const uint8_t COMPACT_SKETCH_TYPE = 3;

  static std::string hex_dump(const uint8_t* ptr, size_t size);
};

} /* namespace datasketches */

#include "compact_theta_sketch_parser_impl.hpp"

#endif

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

#ifndef _COUNT_ZEROS_HPP_
#define _COUNT_ZEROS_HPP_

#include <cstdint>

#include <stdio.h>

namespace datasketches {

static const uint8_t byte_leading_zeros_table[256] = {
    8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4,
    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

static const uint8_t byte_trailing_zeros_table[256] = {
    8, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0
};

static const uint64_t FCLZ_MASK_56 = 0x00ffffffffffffff;
static const uint64_t FCLZ_MASK_48 = 0x0000ffffffffffff;
static const uint64_t FCLZ_MASK_40 = 0x000000ffffffffff;
static const uint64_t FCLZ_MASK_32 = 0x00000000ffffffff;
static const uint64_t FCLZ_MASK_24 = 0x0000000000ffffff;
static const uint64_t FCLZ_MASK_16 = 0x000000000000ffff;
static const uint64_t FCLZ_MASK_08 = 0x00000000000000ff;

static inline uint8_t count_leading_zeros_in_u64(uint64_t input) {
  if (input > FCLZ_MASK_56)
    return      byte_leading_zeros_table[(input >> 56) & FCLZ_MASK_08];
  if (input > FCLZ_MASK_48)
    return  8 + byte_leading_zeros_table[(input >> 48) & FCLZ_MASK_08];
  if (input > FCLZ_MASK_40)
    return 16 + byte_leading_zeros_table[(input >> 40) & FCLZ_MASK_08];
  if (input > FCLZ_MASK_32)
    return 24 + byte_leading_zeros_table[(input >> 32) & FCLZ_MASK_08];
  if (input > FCLZ_MASK_24)
    return 32 + byte_leading_zeros_table[(input >> 24) & FCLZ_MASK_08];
  if (input > FCLZ_MASK_16)
    return 40 + byte_leading_zeros_table[(input >> 16) & FCLZ_MASK_08];
  if (input > FCLZ_MASK_08)
    return 48 + byte_leading_zeros_table[(input >>  8) & FCLZ_MASK_08];
  if (true)
    return 56 + byte_leading_zeros_table[(input      ) & FCLZ_MASK_08];
}

static inline uint8_t count_trailing_zeros_in_u32(uint32_t input) {
  for (int i = 0; i < 4; i++) {
    const int byte = input & 0xff;
    if (byte != 0) return static_cast<uint8_t>((i << 3) + byte_trailing_zeros_table[byte]);
    input >>= 8;
  }
  return 32;
}

static inline uint8_t count_trailing_zeros_in_u64(uint64_t input) {
  for (int i = 0; i < 8; i++) {
    const int byte = input & 0xff;
    if (byte != 0) return static_cast<uint8_t>((i << 3) + byte_trailing_zeros_table[byte]);
    input >>= 8;
  }
  return 64;
}

} /* namespace datasketches */

#endif // _COUNT_ZEROS_HPP_

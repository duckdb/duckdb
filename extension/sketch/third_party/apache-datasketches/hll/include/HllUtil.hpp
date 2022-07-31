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

#ifndef _HLLUTIL_HPP_
#define _HLLUTIL_HPP_

#include "MurmurHash3.h"
#include "RelativeErrorTables.hpp"
#include "count_zeros.hpp"
#include "common_defs.hpp"
#include "ceiling_power_of_2.hpp"

#include <cmath>
#include <stdexcept>
#include <string>

namespace datasketches {

enum hll_mode { LIST = 0, SET, HLL };

namespace hll_constants {

// preamble stuff
static const uint8_t SER_VER = 1;
static const uint8_t FAMILY_ID = 7;

static const uint8_t EMPTY_FLAG_MASK          = 4;
static const uint8_t COMPACT_FLAG_MASK        = 8;
static const uint8_t OUT_OF_ORDER_FLAG_MASK   = 16;
static const uint8_t FULL_SIZE_FLAG_MASK      = 32;

static const uint32_t PREAMBLE_INTS_BYTE = 0;
static const uint32_t SER_VER_BYTE       = 1;
static const uint32_t FAMILY_BYTE        = 2;
static const uint32_t LG_K_BYTE          = 3;
static const uint32_t LG_ARR_BYTE        = 4;
static const uint32_t FLAGS_BYTE         = 5;
static const uint32_t LIST_COUNT_BYTE    = 6;
static const uint32_t HLL_CUR_MIN_BYTE   = 6;
static const uint32_t MODE_BYTE          = 7; // lo2bits = curMode, next 2 bits = tgtHllMode

// Coupon List
static const uint32_t LIST_INT_ARR_START = 8;
static const uint8_t LIST_PREINTS = 2;
// Coupon Hash Set
static const uint32_t HASH_SET_COUNT_INT             = 8;
static const uint32_t HASH_SET_INT_ARR_START         = 12;
static const uint8_t HASH_SET_PREINTS         = 3;
// HLL
static const uint8_t HLL_PREINTS = 10;
static const uint32_t HLL_BYTE_ARR_START = 40;
static const uint32_t HIP_ACCUM_DOUBLE = 8;
static const uint32_t KXQ0_DOUBLE = 16;
static const uint32_t KXQ1_DOUBLE = 24;
static const uint32_t CUR_MIN_COUNT_INT = 32;
static const uint32_t AUX_COUNT_INT = 36;

static const uint32_t EMPTY_SKETCH_SIZE_BYTES = 8;

// other HllUtil stuff
static const uint8_t KEY_BITS_26 = 26;
static const uint8_t VAL_BITS_6 = 6;
static const uint32_t KEY_MASK_26 = (1 << KEY_BITS_26) - 1;
static const uint32_t VAL_MASK_6 = (1 << VAL_BITS_6) - 1;
static const uint32_t EMPTY = 0;
static const uint8_t MIN_LOG_K = 4;
static const uint8_t MAX_LOG_K = 21;

static const double HLL_HIP_RSE_FACTOR = 0.8325546; // sqrt(ln(2))
static const double HLL_NON_HIP_RSE_FACTOR = 1.03896; // sqrt((3 * ln(2)) - 1)
static const double COUPON_RSE_FACTOR = 0.409; // at transition point not the asymptote
static const double COUPON_RSE = COUPON_RSE_FACTOR / (1 << 13);

static const uint8_t LG_INIT_LIST_SIZE = 3;
static const uint8_t LG_INIT_SET_SIZE = 5;
static const uint32_t RESIZE_NUMER = 3;
static const uint32_t RESIZE_DENOM = 4;

static const uint8_t loNibbleMask = 0x0f;
static const uint8_t hiNibbleMask = 0xf0;
static const uint8_t AUX_TOKEN = 0xf;

/**
* Log2 table sizes for exceptions based on lgK from 0 to 26.
* However, only lgK from 4 to 21 are used.
*/
static const uint8_t LG_AUX_ARR_INTS[] = {
  0, 2, 2, 2, 2, 2, 2, 3, 3, 3,   // 0 - 9
  4, 4, 5, 5, 6, 7, 8, 9, 10, 11, // 10-19
  12, 13, 14, 15, 16, 17, 18      // 20-26
};

} // namespace hll_constants


// template provides internal consistency and allows static float values
// but we don't use the template parameter anywhere
template<typename A = std::allocator<uint8_t> >
class HllUtil final {
public:

  static uint32_t coupon(const uint64_t hash[]);
  static uint32_t coupon(const HashState& hashState);
  static void hash(const void* key, size_t keyLen, uint64_t seed, HashState& result);
  static uint8_t checkLgK(uint8_t lgK);
  static void checkMemSize(uint64_t minBytes, uint64_t capBytes);
  static inline void checkNumStdDev(uint8_t numStdDev);
  static uint32_t pair(uint32_t slotNo, uint8_t value);
  static uint32_t getLow26(uint32_t coupon);
  static uint8_t getValue(uint32_t coupon);
  static double invPow2(uint8_t e);
  static uint8_t ceilingPowerOf2(uint32_t n);
  static uint8_t simpleIntLog2(uint32_t n); // n must be power of 2
  static uint8_t computeLgArrInts(hll_mode mode, uint32_t count, uint8_t lgConfigK);
  static double getRelErr(bool upperBound, bool unioned, uint8_t lgConfigK, uint8_t numStdDev);
};

template<typename A>
inline uint32_t HllUtil<A>::coupon(const uint64_t hash[]) {
  uint32_t addr26 = hash[0] & hll_constants::KEY_MASK_26;
  uint8_t lz = count_leading_zeros_in_u64(hash[1]);
  uint8_t value = ((lz > 62 ? 62 : lz) + 1); 
  return (value << hll_constants::KEY_BITS_26) | addr26;
}

template<typename A>
inline uint32_t HllUtil<A>::coupon(const HashState& hashState) {
  uint32_t addr26 = (int) (hashState.h1 & hll_constants::KEY_MASK_26);
  uint8_t lz = count_leading_zeros_in_u64(hashState.h2);  
  uint8_t value = ((lz > 62 ? 62 : lz) + 1); 
  return (value << hll_constants::KEY_BITS_26) | addr26;
}

template<typename A>
inline void HllUtil<A>::hash(const void* key, size_t keyLen, uint64_t seed, HashState& result) {
  MurmurHash3_x64_128(key, keyLen, seed, result);
}

template<typename A>
inline double HllUtil<A>::getRelErr(bool upperBound, bool unioned,
                                    uint8_t lgConfigK, uint8_t numStdDev) {
  return RelativeErrorTables<A>::getRelErr(upperBound, unioned, lgConfigK, numStdDev);
}

template<typename A>
inline uint8_t HllUtil<A>::checkLgK(uint8_t lgK) {
  if ((lgK >= hll_constants::MIN_LOG_K) && (lgK <= hll_constants::MAX_LOG_K)) {
    return lgK;
  } else {
    throw std::invalid_argument("Invalid value of k: " + std::to_string(lgK));
  }
}

template<typename A>
inline void HllUtil<A>::checkMemSize(uint64_t minBytes, uint64_t capBytes) {
  if (capBytes < minBytes) {
    throw std::invalid_argument("Given destination array is not large enough: " + std::to_string(capBytes));
  }
}

template<typename A>
inline void HllUtil<A>::checkNumStdDev(uint8_t numStdDev) {
  if ((numStdDev < 1) || (numStdDev > 3)) {
    throw std::invalid_argument("NumStdDev may not be less than 1 or greater than 3.");
  }
}

template<typename A>
inline uint32_t HllUtil<A>::pair(uint32_t slotNo, uint8_t value) {
  return (value << hll_constants::KEY_BITS_26) | (slotNo & hll_constants::KEY_MASK_26);
}

template<typename A>
inline uint32_t HllUtil<A>::getLow26(uint32_t coupon) {
  return coupon & hll_constants::KEY_MASK_26;
}

template<typename A>
inline uint8_t HllUtil<A>::getValue(uint32_t coupon) {
  return coupon >> hll_constants::KEY_BITS_26;
}

template<typename A>
inline double HllUtil<A>::invPow2(uint8_t e) {
  union {
    long long longVal;
    double doubleVal;
  } conv;
  conv.longVal = (1023L - e) << 52;
  return conv.doubleVal;
}

template<typename A>
inline uint8_t HllUtil<A>::simpleIntLog2(uint32_t n) {
  if (n == 0) {
    throw std::logic_error("cannot take log of 0");
  }
  return count_trailing_zeros_in_u32(n);
}

template<typename A>
inline uint8_t HllUtil<A>::computeLgArrInts(hll_mode mode, uint32_t count, uint8_t lgConfigK) {
  // assume value missing and recompute
  if (mode == LIST) { return hll_constants::LG_INIT_LIST_SIZE; }
  uint32_t ceilPwr2 = ceiling_power_of_2(count);
  if ((hll_constants::RESIZE_DENOM * count) > (hll_constants::RESIZE_NUMER * ceilPwr2)) { ceilPwr2 <<= 1;}
  if (mode == SET) {
    return std::max(hll_constants::LG_INIT_SET_SIZE, HllUtil<A>::simpleIntLog2(ceilPwr2));
  }
  //only used for HLL4
  return std::max(hll_constants::LG_AUX_ARR_INTS[lgConfigK], HllUtil<A>::simpleIntLog2(ceilPwr2));
}

}

#endif /* _HLLUTIL_HPP_ */

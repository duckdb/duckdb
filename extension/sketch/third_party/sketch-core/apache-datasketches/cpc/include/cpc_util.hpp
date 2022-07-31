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

#ifndef CPC_UTIL_HPP_
#define CPC_UTIL_HPP_

#include <stdexcept>

namespace datasketches {

static inline uint64_t divide_longs_rounding_up(uint64_t x, uint64_t y) {
  if (y == 0) throw std::invalid_argument("divide_longs_rounding_up: bad argument");
  const uint64_t quotient = x / y;
  if (quotient * y == x) return (quotient);
  else return quotient + 1;
}

static inline uint8_t floor_log2_of_long(uint64_t x) {
  if (x < 1) throw std::invalid_argument("floor_log2_of_long: bad argument");
  uint8_t p = 0;
  uint64_t y = 1;
  while (true) {
    if (y == x) return p;
    if (y > x) return p - 1;
    p += 1;
    y <<= 1;
  }
}

// This place-holder code was inadequate because it caused
// the cost of the post-merge get_result() operation to be O(C)
// instead of O(K). It did have the advantage of being
// very simple and trustworthy during initial testing.
static inline uint64_t wegner_count_bits_set_in_matrix(const uint64_t* array, size_t length) {
  uint64_t pattern = 0;
  uint64_t count = 0;
  //  clock_t t0, t1;
  //  t0 = clock();
  // Wegner's Bit-Counting Algorithm, CACM 3 (1960), p. 322.
  for (uint64_t i = 0; i < length; i++) {
    pattern = array[i];
    while (pattern != 0) {
      pattern &= (pattern - 1);
      count++;
    }
  }
  //  t1 = clock();
  //  printf ("\n(Wegner CountBitsTime %.1f)\n", ((double) (t1 - t0)) / 1000.0);
  //  fflush (stdout);
  return count;
}

// Note: this is an adaptation of the Java code,
// which is apparently a variation of Figure 5-2 in "Hacker's Delight"
// by Henry S. Warren.
static inline uint32_t warren_bit_count(uint64_t i) {
  i = i - ((i >> 1) & 0x5555555555555555ULL);
  i = (i & 0x3333333333333333ULL) + ((i >> 2) & 0x3333333333333333ULL);
  i = (i + (i >> 4)) & 0x0f0f0f0f0f0f0f0fULL;
  i = i + (i >> 8);
  i = i + (i >> 16);
  i = i + (i >> 32);
  return i & 0x7f;
}

static inline uint32_t warren_count_bits_set_in_matrix(const uint64_t* array, uint32_t length) {
  uint32_t count = 0;
  for (uint32_t i = 0; i < length; i++) {
    count += warren_bit_count(array[i]);
  }
  return count;
}

// This code is Figure 5-9 in "Hacker's Delight" by Henry S. Warren.

#define CSA(h,l,a,b,c) {uint64_t u = a ^ b; uint64_t v = c; h = (a & b) | (u & v); l = u ^ v;}

static inline uint32_t count_bits_set_in_matrix(const uint64_t* a, uint32_t length) {
  if ((length & 0x7) != 0) throw std::invalid_argument("the length of the array must be a multiple of 8");
  uint32_t total = 0;
  uint64_t ones, twos, twos_a, twos_b, fours, fours_a, fours_b, eights;
  fours = twos = ones = 0;

  for (uint32_t i = 0; i <= length - 8; i += 8) {
    CSA(twos_a, ones, ones, a[i+0], a[i+1]);
    CSA(twos_b, ones, ones, a[i+2], a[i+3]);
    CSA(fours_a, twos, twos, twos_a, twos_b);

    CSA(twos_a, ones, ones, a[i+4], a[i+5]);
    CSA(twos_b, ones, ones, a[i+6], a[i+7]);
    CSA(fours_b, twos, twos, twos_a, twos_b);

    CSA(eights, fours, fours, fours_a, fours_b);

    total += warren_bit_count(eights);
  }
  total = 8 * total + 4 * warren_bit_count(fours) + 2 * warren_bit_count(twos) + warren_bit_count(ones);

  // Because I still don't fully trust this fancy version
  // assert(total == wegner_count_bits_set_in_matrix(A, length));
  //if (total != wegner_count_bits_set_in_matrix(a, length)) throw std::logic_error("count_bits_set_in_matrix error");

  return total;
}

// Here are some timings made with quickTestMerge.c
// for the "5 5" case:

// Wegner CountBitsTime 29.3
// Warren CountBitsTime  5.3
// CSA    CountBitsTime  4.3

} /* namespace datasketches */

#endif

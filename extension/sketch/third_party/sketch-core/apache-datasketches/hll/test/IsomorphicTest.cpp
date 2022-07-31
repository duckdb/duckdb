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

#include <catch.hpp>

#include <cstdio>

#include "hll.hpp"
#include "HllUtil.hpp"

namespace datasketches {

/*
// hex format for comparing serialized bytes
// previously used with cppunit testing to display results upon mismatch.
// catch2 testing framework provides such output, but this may be easier for debugging
// with long vectors. keeping the code for now.
static std::string toString(const datasketches::hll_sketch::vector_bytes& v) {
   std::ostringstream s;
   s << std::hex << std::setfill('0');
   int cnt = 0;
   for (uint8_t byte: v) {
     if (cnt == 8) { // insert space after each 8 bytes for readability
       s << ' ';
       cnt = 0;
     } else {
       ++cnt;
     }
     s << std::setw(2) << static_cast<int>(byte);
   }
   return s.str();
}
*/

// if lg_k >= 8, mode != SET!
static int get_n(int lg_k, hll_mode mode) {
  if (mode == LIST) return 4;
  if (mode == SET) return 1 << (lg_k - 4);
  return ((lg_k < 8) && (mode == HLL)) ? (1 << lg_k) : 1 << (lg_k - 3);
}

static long v = 0;

static hll_sketch build_sketch(uint8_t lg_k, target_hll_type hll_type, hll_mode mode) {
  hll_sketch sk(lg_k, hll_type);
  int n = get_n(lg_k, mode);
  for (int i = 0; i < n; i++) sk.update(static_cast<uint64_t>(i + v));
  v += n;
  return sk;
}

// merges a sketch to an empty union and gets result of the same type, checks binary equivalence
static void union_one_update(bool compact) {
  for (uint8_t lg_k = 4; lg_k <= 21; lg_k++) { // all lg_k
    for (int mode = 0; mode <= 2; mode++) { // List, Set, Hll
      if ((lg_k < 8) && (mode == 1)) continue; // lg_k < 8 list transitions directly to HLL
      for (int t = 0; t <= 2; t++) { // HLL_4, HLL_6, HLL_8
        target_hll_type hll_type = (target_hll_type) t;
        hll_sketch sk1 = build_sketch(lg_k, hll_type, (hll_mode) mode);
        hll_union u(lg_k);
        u.update(sk1);
        hll_sketch sk2 = u.get_result(hll_type);
        auto bytes1 = compact ? sk1.serialize_compact() : sk1.serialize_updatable();
        auto bytes2 = compact ? sk2.serialize_compact() : sk2.serialize_updatable();
        auto msg = "LgK=" + std::to_string(lg_k)
          + ", Mode=" + std::to_string(mode)
          + ", Type=" + std::to_string(hll_type)
          + "\n" + sk1.to_string(true, true, true, true)
          + "\n" + sk2.to_string(true, true, true, true);
        if (bytes1 != bytes2) {
          std::cerr << msg << std::endl;
          REQUIRE(bytes1 == bytes2);
        }
      }
    }
  }
}

TEST_CASE("hll isomorphic: union one update serialize updatable", "[hll_isomorphic]") {
  union_one_update(false);
}

TEST_CASE("hll isomorphic: union one update serialize compact", "[hll_isomorphic]") {
  union_one_update(true);
}

// converts a sketch to a different type and converts back to the original type to check binary equivalence
static void convert_back_and_forth(bool compact) {
  for (uint8_t lg_k = 4; lg_k <= 21; lg_k++) { // all lg_k
    for (int mode = 0; mode <= 2; mode++) { // List, Set, Hll
      if ((lg_k < 8) && (mode == 1)) continue; // lg_k < 8 list transitions directly to HLL
      for (int t1 = 0; t1 <= 2; t1++) { // HLL_4, HLL_6, HLL_8
        target_hll_type hll_type1 = (target_hll_type) t1;
        hll_sketch sk1 = build_sketch(lg_k, hll_type1, (hll_mode) mode);
        auto bytes1 = compact ? sk1.serialize_compact() : sk1.serialize_updatable();
        for (int t2 = 0; t2 <= 2; t2++) { // HLL_4, HLL_6, HLL_8
          if (t2 == t1) continue;
          target_hll_type hll_type2 = (target_hll_type) t2;
          hll_sketch sk2(hll_sketch(sk1, hll_type2), hll_type1);
          auto bytes2 = compact ? sk2.serialize_compact() : sk2.serialize_updatable();
          auto msg = "LgK=" + std::to_string(lg_k)
            + ", Mode=" + std::to_string(mode)
            + ", Type1=" + std::to_string(hll_type1)
            + ", Type2=" + std::to_string(hll_type2)
            + "\n" + sk1.to_string(true, true, true, true)
            + "\n" + sk2.to_string(true, true, true, true);
          if (bytes1 != bytes2) {
            std::cerr << msg << std::endl;
            REQUIRE(bytes1 == bytes2);
          }
        }
      }
    }
  }
}

TEST_CASE("hll isomorphic: convert back and forth serialize updatable", "[hll_isomorphic]") {
  convert_back_and_forth(false);
}

TEST_CASE("hll isomorphic: convert back and forth serialize compact", "[hll_isomorphic]") {
  convert_back_and_forth(true);
}

} /* namespace datasketches */

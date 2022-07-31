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
#include <sstream>
#include <stdexcept>

#include "hll.hpp"

namespace datasketches {

static void println(std::string& str) {
  unused(str);
  //std::cout << str << "\n";
}

static void basicUnion(uint64_t n1, uint64_t n2,
		                   uint8_t lgk1, uint8_t lgk2, uint8_t lgMaxK,
                       target_hll_type type1, target_hll_type type2, target_hll_type resultType) {
  uint64_t v = 0;
  //int tot = n1 + n2;

  hll_sketch h1(lgk1, type1);
  hll_sketch h2(lgk2, type2);
  uint8_t lgControlK = std::min(std::min(lgk1, lgk2), lgMaxK);
  hll_sketch control(lgControlK, resultType);

  for (uint64_t i = 0; i < n1; ++i) {
    h1.update(v + i);
    control.update(v + i);
  }
  v += n1;
  for (uint64_t i = 0; i < n2; ++i) {
    h2.update(v + i);
    control.update(v + i);
  }
  v += n2;

  hll_union u(lgMaxK);
  u.update(std::move(h1));
  u.update(h2);

  hll_sketch result = u.get_result(resultType);

  // force non-HIP estimates to avoid issues with in- vs out-of-order
  double uEst = result.get_composite_estimate();
  double uUb = result.get_upper_bound(2);
  double uLb = result.get_lower_bound(2);
  //double rerr = ((uEst/tot) - 1.0) * 100;

  double controlEst = control.get_composite_estimate();
  double controlUb = control.get_upper_bound(2);
  double controlLb = control.get_lower_bound(2);

  REQUIRE((controlUb - controlEst) >= 0.0);
  REQUIRE((uUb - uEst) >= 0.0);
  REQUIRE((controlEst - controlLb) >= 0.0);
  REQUIRE((uEst - uLb) >= 0.0);

  REQUIRE(controlEst == uEst);
}

/**
 * The task here is to check the transition boundaries as the sketch morphs between LIST to
 * SET to HLL modes. The transition points vary as a function of lgConfigK. In addition,
 * this checks that the union operation is operating properly based on the order the
 * sketches are presented to the union.
 */
TEST_CASE("hll union: check unions", "[hll_union]") {
  target_hll_type type1 = HLL_8;
  target_hll_type type2 = HLL_8;
  target_hll_type resultType = HLL_8;

  uint8_t lgK1 = 7;
  uint8_t lgK2 = 7;
  uint8_t lgMaxK = 7;
  uint64_t n1 = 7;
  uint64_t n2 = 7;
  basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
  n1 = 8;
  n2 = 7;
  basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
  n1 = 7;
  n2 = 8;
  basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
  n1 = 8;
  n2 = 8;
  basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
  n1 = 7;
  n2 = 14;
  basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);

  uint8_t i = 0;
  for (i = 7; i <= 13; ++i) {
    lgK1 = i;
    lgK2 = i;
    lgMaxK = i;
    {
      n1 = ((1 << (i - 3)) * 3)/4; // compute the transition point
      n2 = n1;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      n1 += 2;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      n1 -= 2;
      n2 += 2;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      n1 += 2;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
    }
    lgK1 = i;
    lgK2 = i + 1;
    lgMaxK = i;
    {
      n1 = ((1 << (i - 3)) * 3)/4;
      n2 = n1;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      n1 += 2;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      n1 -= 2;
      n2 += 2;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      n1 += 2;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
    }
    lgK1 = i + 1;
    lgK2 = i;
    lgMaxK = i;
    {
      n1 = ((1 << (i - 3)) * 3)/4;
      n2 = n1;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      n1 += 2;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      n1 -= 2;
      n2 += 2;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      n1 += 2;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
    }
    lgK1 = i + 1;
    lgK2 = i + 1;
    lgMaxK = i;
    {
      n1 = ((1 << (i - 3)) * 3)/4;
      n2 = n1;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      n1 += 2;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      n1 -= 2;
      n2 += 2;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      n1 += 2;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
    }
  }
}

TEST_CASE("hll union: check composite estimate", "[hll_union]") {
  hll_union u(12);
  REQUIRE(u.is_empty());
  REQUIRE(u.get_composite_estimate() == Approx(0.0).margin(0.03));
  for (int i = 1; i <= 15; ++i) { u.update(i); }
  REQUIRE(u.get_composite_estimate() == Approx(15.0).margin(15 * 0.03));
  for (int i = 16; i <= 1000; ++i) { u.update(i); }
  REQUIRE(u.get_composite_estimate() == Approx(1000.0).margin(1000 * 0.03));
}

TEST_CASE("hll union: check config k limits", "[hll_union]") {
  REQUIRE_THROWS_AS(hll_union(hll_constants::MIN_LOG_K - 1), std::invalid_argument);

  REQUIRE_THROWS_AS(hll_union(hll_constants::MAX_LOG_K + 1), std::invalid_argument);
}

static double getBound(int lgK, bool ub, bool oooFlag, int numStdDev, double est) {
  double re = RelativeErrorTables<>::getRelErr(ub, oooFlag, lgK, numStdDev);
  return est / (1.0 + re);
}

TEST_CASE("hll union: check ub lb", "[hll_union]") {
  uint8_t lgK = 4;
  int n = 1 << 20;
  bool oooFlag = false;
  
  double bound;
  std::string str;

  bound = (getBound(lgK, true, oooFlag, 3, n) / n) - 1;
  str = "LgK=" + std::to_string(lgK) + ", UB3: " + std::to_string(bound);
  println(str);
  bound = (getBound(lgK, true, oooFlag, 2, n) / n) - 1;
  str = "LgK=" + std::to_string(lgK) + ", UB2: " + std::to_string(bound);
  println(str);
  bound = (getBound(lgK, true, oooFlag, 1, n) / n) - 1;
  str = "LgK=" + std::to_string(lgK) + ", UB1: " + std::to_string(bound);
  println(str);
  bound = (getBound(lgK, false, oooFlag, 1, n) / n) - 1;
  str = "LgK=" + std::to_string(lgK) + ", LB1: " + std::to_string(bound);
  println(str);
  bound = (getBound(lgK, false, oooFlag, 2, n) / n) - 1;
  str = "LgK=" + std::to_string(lgK) + ", LB2: " + std::to_string(bound);
  println(str);
  bound = (getBound(lgK, false, oooFlag, 3, n) / n) - 1;
  str = "LgK=" + std::to_string(lgK) + ", LB3: " + std::to_string(bound);
  println(str);
}

TEST_CASE("hll union: check conversions", "[hll_union]") {
  uint8_t lgK = 4;
  hll_sketch sk1(lgK, HLL_8);
  hll_sketch sk2(lgK, HLL_8);
  int n = 1 << 20;
  for (int i = 0; i < n; ++i) {
    sk1.update(i);
    sk2.update(i + n);
  }
  hll_union hllUnion(lgK);
  hllUnion.update(sk1);
  hllUnion.update(sk2);

  hll_sketch rsk1 = hllUnion.get_result(HLL_4);
  hll_sketch rsk2 = hllUnion.get_result(HLL_6);
  hll_sketch rsk3 = hllUnion.get_result(HLL_8);
  double est1 = rsk1.get_estimate();
  double est2 = rsk2.get_estimate();
  double est3 = rsk3.get_estimate();
  REQUIRE(est1 == est2);
  REQUIRE(est1 == est3);
}

TEST_CASE("hll union: check input types", "[hll_union]") {
  hll_union u(8);

  // inserting the same value as a variety of input types
  u.update((uint8_t) 102);
  u.update((uint16_t) 102);
  u.update((uint32_t) 102);
  u.update((uint64_t) 102);
  u.update((int8_t) 102);
  u.update((int16_t) 102);
  u.update((int32_t) 102);
  u.update((int64_t) 102);
  REQUIRE(u.get_estimate() == Approx(1.0).margin(0.01));

  // identical binary representations
  // no unsigned in Java, but need to sign-extend both as Java would do
  u.update((uint8_t) 255);
  u.update((int8_t) -1);

  u.update((float) -2.0);
  u.update((double) -2.0);

  std::string str = "input string";
  u.update(str);
  u.update(str.c_str(), str.length());
  REQUIRE(u.get_estimate() == Approx(4.0).margin(0.01));

  u = hll_union(8);
  u.update((float) 0.0);
  u.update((float) -0.0);
  u.update((double) 0.0);
  u.update((double) -0.0);
  REQUIRE(u.get_estimate() == Approx(1.0).margin(0.01));

  u = hll_union(8);
  u.update(std::nanf("3"));
  u.update(std::nan("12"));
  REQUIRE(1.0 == Approx(u.get_estimate()).margin(0.01));
  REQUIRE(u.get_result().get_estimate() == Approx(u.get_estimate()).margin(0.01));

  u = hll_union(8);
  u.update(nullptr, 0);
  u.update("");
  REQUIRE(u.is_empty());
}

static void union_two_sketches_with_overlap(int num, uint8_t lg_k, target_hll_type type) {
  hll_sketch sketch1(lg_k, type);
  for (int key = 0; key < num; key++) sketch1.update(key);

  const int overlap = num / 2;
  hll_sketch sketch2(lg_k, type);
  for (int key = overlap; key < num + overlap; key++) sketch2.update(key);

  hll_union u(lg_k);
  u.update(sketch1);
  u.update(sketch2);
  hll_sketch sketch = u.get_result(type);
  REQUIRE(sketch.get_estimate() == Approx(num * 1.5).margin(num * 1.5 * 0.02));
}

TEST_CASE("hll union: check hll to hll", "[hll_union]") {
  union_two_sketches_with_overlap(1000000, 11, HLL_4);
}

} /* namespace datasketches */

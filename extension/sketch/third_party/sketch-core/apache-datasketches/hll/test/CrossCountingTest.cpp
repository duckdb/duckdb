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
#include <ostream>
#include <cmath>
#include <string>

#include "hll.hpp"

namespace datasketches {

static hll_sketch buildSketch(const int n, const uint8_t lgK, const target_hll_type tgtHllType) {
  hll_sketch sketch(lgK, tgtHllType);
  for (int i = 0; i < n; ++i) {
    sketch.update(i);
  }
  return sketch;
}

static void crossCountingCheck(const uint8_t lgK, const int n) {
  hll_sketch sk4 = buildSketch(n, lgK, HLL_4);
  const double est = sk4.get_estimate();
  const double lb = sk4.get_lower_bound(1);
  const double ub = sk4.get_upper_bound(1);

  hll_sketch sk6 = buildSketch(n, lgK, HLL_6);
  REQUIRE(sk6.get_estimate() == est);
  REQUIRE(sk6.get_lower_bound(1) == lb);
  REQUIRE(sk6.get_upper_bound(1) == ub);

  hll_sketch sk8 = buildSketch(n, lgK, HLL_8);
  REQUIRE(sk8.get_estimate() == est);
  REQUIRE(sk8.get_lower_bound(1) == lb);
  REQUIRE(sk8.get_upper_bound(1) == ub);

  // Conversions
  hll_sketch sk4to6(sk4, HLL_6);
  REQUIRE(sk4to6.get_estimate() == est);
  REQUIRE(sk4to6.get_lower_bound(1) == lb);
  REQUIRE(sk4to6.get_upper_bound(1) == ub);

  hll_sketch sk4to8(sk4, HLL_8);
  REQUIRE(sk4to8.get_estimate() == est);
  REQUIRE(sk4to8.get_lower_bound(1) == lb);
  REQUIRE(sk4to8.get_upper_bound(1) == ub);

  hll_sketch sk6to4(sk6, HLL_4);
  REQUIRE(sk6to4.get_estimate() == est);
  REQUIRE(sk6to4.get_lower_bound(1) == lb);
  REQUIRE(sk6to4.get_upper_bound(1) == ub);

  hll_sketch sk6to8(sk6, HLL_8);
  REQUIRE(sk6to8.get_estimate() == est);
  REQUIRE(sk6to8.get_lower_bound(1) == lb);
  REQUIRE(sk6to8.get_upper_bound(1) == ub);

  hll_sketch sk8to4(sk8, HLL_4);
  REQUIRE(sk8to4.get_estimate() == est);
  REQUIRE(sk8to4.get_lower_bound(1) == lb);
  REQUIRE(sk8to4.get_upper_bound(1) == ub);

  hll_sketch sk8to6(sk8, HLL_6);
  REQUIRE(sk8to6.get_estimate() == est);
  REQUIRE(sk8to6.get_lower_bound(1) == lb);
  REQUIRE(sk8to6.get_upper_bound(1) == ub);
}

TEST_CASE("cross counting: cross counting checks", "[cross_counting]") {
  crossCountingCheck(4, 100);
  crossCountingCheck(4, 10000);
  crossCountingCheck(12, 7);
  crossCountingCheck(12, 384);
  crossCountingCheck(12, 10000);
}

} /* namespace datasketches */

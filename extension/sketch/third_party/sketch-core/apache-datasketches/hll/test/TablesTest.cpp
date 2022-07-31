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

#include <stdexcept>

#include <catch.hpp>

#include "CubicInterpolation.hpp"

namespace datasketches {

TEST_CASE("hll tables: interpolation exception", "[hll_tables]") {
  REQUIRE_THROWS_AS(CubicInterpolation<>::usingXAndYTables(-1.0), std::invalid_argument);

  REQUIRE_THROWS_AS(CubicInterpolation<>::usingXAndYTables(1e12), std::invalid_argument);
}

TEST_CASE("hll tables: check corner case", "[hll_tables]") {
  int len = 10;
  double xArr[] = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
  double yArr[] = {2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0};
  double x = xArr[len - 1];
  double y = CubicInterpolation<>::usingXAndYTables(xArr, yArr, len, x);
  double yExp = yArr[len - 1];
  REQUIRE(y == yExp);
}

} /* namespace datasketches */


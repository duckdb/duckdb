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

#include <random>

#include <kll_sketch.hpp>
#include <kolmogorov_smirnov.hpp>

namespace datasketches {

TEST_CASE("kolmogorov-smirnov empty", "[kll_sketch]") {
  const uint16_t k = 200;
  kll_sketch<double> sketch1(k);
  kll_sketch<double> sketch2(k);
  REQUIRE(kolmogorov_smirnov::delta(sketch1, sketch2) == 0);
  REQUIRE_FALSE(kolmogorov_smirnov::test(sketch1, sketch2, 0.01));
}

TEST_CASE("kolmogorov-smirnov same distribution", "[kll_sketch]") {
  const uint16_t k = 200;
  kll_sketch<double> sketch1(k);
  kll_sketch<double> sketch2(k);
  std::default_random_engine rand;
  std::normal_distribution<double> distr;
  const int n = k * 3 - 1;
  for (int i = 0; i < n; ++i) {
    const double x = distr(rand);
    sketch1.update(x);
    sketch2.update(x);
  }
  REQUIRE(kolmogorov_smirnov::delta(sketch1, sketch2) == Approx(0).margin(0.02));
  REQUIRE_FALSE(kolmogorov_smirnov::test(sketch1, sketch2, 0.01));
}

TEST_CASE("kolmogorov-smirnov very different distributions", "[kll_sketch]") {
  const uint16_t k = 200;
  kll_sketch<double> sketch1(k);
  kll_sketch<double> sketch2(k);
  std::default_random_engine rand;
  std::normal_distribution<double> distr;
  const int n = k * 3 - 1;
  for (int i = 0; i < n; ++i) {
    const double x = distr(rand);
    sketch1.update(x + 100.0);
    sketch2.update(x);
  }
  const auto delta = kolmogorov_smirnov::delta(sketch1, sketch2);
  REQUIRE(delta == Approx(1.0).margin(1e-6));
  REQUIRE(delta <= 1);
  REQUIRE(kolmogorov_smirnov::test(sketch1, sketch2, 0.05));
}

TEST_CASE("kolmogorov-smirnov slightly different distributions", "[kll_sketch]") {
  const uint16_t k = 2000;
  kll_sketch<double> sketch1(k);
  kll_sketch<double> sketch2(k);
  std::default_random_engine rand;
  std::normal_distribution<double> distr;
  const int n = k * 3 - 1;
  for (int i = 0; i < n; ++i) {
    const double x = distr(rand);
    sketch1.update(x + 0.05);
    sketch2.update(x);
  }
  const double delta = kolmogorov_smirnov::delta(sketch1, sketch2);
  REQUIRE(delta == Approx(0.02).margin(0.01));
  const double threshold = kolmogorov_smirnov::threshold(sketch1, sketch2, 0.05);
  //std::cout << "delta=" << delta << ", threshold=" << threshold << "\n";
  REQUIRE_FALSE(delta > threshold);
  REQUIRE_FALSE(kolmogorov_smirnov::test(sketch1, sketch2, 0.05));
}

TEST_CASE("kolmogorov-smirnov slightly different distributions high resolution", "[kll_sketch]") {
  const uint16_t k = 8000;
  kll_sketch<double> sketch1(k);
  kll_sketch<double> sketch2(k);
  std::default_random_engine rand;
  std::normal_distribution<double> distr;
  const int n = k * 3 - 1;
  for (int i = 0; i < n; ++i) {
    const double x = distr(rand);
    sketch1.update(x + 0.05);
    sketch2.update(x);
  }
  const double delta = kolmogorov_smirnov::delta(sketch1, sketch2);
  REQUIRE(delta == Approx(0.02).margin(0.01));
  const double threshold = kolmogorov_smirnov::threshold(sketch1, sketch2, 0.05);
  //std::cout << "delta=" << delta << ", threshold=" << threshold << "\n";
  REQUIRE(delta > threshold);
  REQUIRE(kolmogorov_smirnov::test(sketch1, sketch2, 0.05));
}

} /* namespace datasketches */

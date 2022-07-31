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
#include <cmath>
#include <cstring>
#include <sstream>
#include <fstream>
#include <stdexcept>

#include <kll_sketch.hpp>
#include <test_allocator.hpp>

namespace datasketches {

static const double RANK_EPS_FOR_K_200 = 0.0133;
static const double NUMERIC_NOISE_TOLERANCE = 1E-6;

#ifdef TEST_BINARY_INPUT_PATH
static std::string testBinaryInputPath = TEST_BINARY_INPUT_PATH;
#else
static std::string testBinaryInputPath = "test/";
#endif

// typical usage would be just kll_sketch<float> or kll_sketch<std::string>, but here we use test_allocator
using kll_float_sketch = kll_sketch<float, std::less<float>, serde<float>, test_allocator<float>>;
// let std::string use the default allocator for simplicity, otherwise we need to define "less" and "serde"
using kll_string_sketch = kll_sketch<std::string, std::less<std::string>, serde<std::string>, test_allocator<std::string>>;

TEST_CASE("kll sketch", "[kll_sketch]") {

  // setup
  test_allocator_total_bytes = 0;

  SECTION("k limits") {
    kll_float_sketch sketch1(kll_float_sketch::MIN_K, 0); // this should work
    kll_float_sketch sketch2(kll_float_sketch::MAX_K, 0); // this should work
    REQUIRE_THROWS_AS(new kll_float_sketch(kll_float_sketch::MIN_K - 1, 0), std::invalid_argument);
    // MAX_K + 1 makes no sense because k is uint16_t
  }

  SECTION("empty") {
    kll_float_sketch sketch(200, 0);
    REQUIRE(sketch.is_empty());
    REQUIRE_FALSE(sketch.is_estimation_mode());
    REQUIRE(sketch.get_n() == 0);
    REQUIRE(sketch.get_num_retained() == 0);
    REQUIRE(std::isnan(sketch.get_rank(0)));
    REQUIRE(std::isnan(sketch.get_min_value()));
    REQUIRE(std::isnan(sketch.get_max_value()));
    REQUIRE(std::isnan(sketch.get_quantile(0.5)));
    const double fractions[3] {0, 0.5, 1};
    REQUIRE(sketch.get_quantiles(fractions, 3).size() == 0);
    const float split_points[1] {0};
    REQUIRE(sketch.get_PMF(split_points, 1).size() == 0);
    REQUIRE(sketch.get_CDF(split_points, 1).size() == 0);

    for (auto it: sketch) {
      (void) it; // to suppress "unused" warning
      FAIL("should be no iterations over an empty sketch");
    }
  }

  SECTION("get bad quantile") {
    kll_float_sketch sketch(200, 0);
    sketch.update(0); // has to be non-empty to reach the check
    REQUIRE_THROWS_AS(sketch.get_quantile(-1), std::invalid_argument);
  }

  SECTION("one item") {
    kll_float_sketch sketch(200, 0);
    sketch.update(1.0f);
    REQUIRE_FALSE(sketch.is_empty());
    REQUIRE_FALSE(sketch.is_estimation_mode());
    REQUIRE(sketch.get_n() == 1);
    REQUIRE(sketch.get_num_retained() == 1);
    REQUIRE(sketch.get_rank(1.0f) == 0.0);
    REQUIRE(sketch.get_rank<true>(1.0f) == 1.0);
    REQUIRE(sketch.get_rank(2.0f) == 1.0);
    REQUIRE(sketch.get_rank(std::numeric_limits<float>::infinity()) == 1.0);
    REQUIRE(sketch.get_min_value() == 1.0);
    REQUIRE(sketch.get_max_value() == 1.0);
    REQUIRE(sketch.get_quantile(0.5) == 1.0);
    const double fractions[3] {0, 0.5, 1};
    auto quantiles = sketch.get_quantiles(fractions, 3);
    REQUIRE(quantiles.size() == 3);
    REQUIRE(quantiles[0] == 1.0);
    REQUIRE(quantiles[1] == 1.0);
    REQUIRE(quantiles[2] == 1.0);

    int count = 0;
    for (auto it: sketch) {
      REQUIRE(it.second == 1);
      ++count;
    }
    REQUIRE(count == 1);
  }

  SECTION("NaN") {
    kll_float_sketch sketch(200, 0);
    sketch.update(std::numeric_limits<float>::quiet_NaN());
    REQUIRE(sketch.is_empty());

    sketch.update(0);
    sketch.update(std::numeric_limits<float>::quiet_NaN());
    REQUIRE(sketch.get_n() == 1);
  }

  SECTION("many items, exact mode") {
    kll_float_sketch sketch(200, 0);
    const uint32_t n = 200;
    for (uint32_t i = 0; i < n; i++) {
      sketch.update(static_cast<float>(i));
      REQUIRE(sketch.get_n() == i + 1);
    }
    REQUIRE_FALSE(sketch.is_empty());
    REQUIRE_FALSE(sketch.is_estimation_mode());
    REQUIRE(sketch.get_num_retained() == n);
    REQUIRE(sketch.get_min_value() == 0.0);
    REQUIRE(sketch.get_quantile(0) == 0.0);
    REQUIRE(sketch.get_max_value() == n - 1);
    REQUIRE(sketch.get_quantile(1) == n - 1);

    const double fractions[3] {0, 0.5, 1};
    auto quantiles = sketch.get_quantiles(fractions, 3);
    REQUIRE(quantiles.size() == 3);
    REQUIRE(quantiles[0] == 0.0);
    REQUIRE(quantiles[1] == n / 2);
    REQUIRE(quantiles[2] == n - 1 );

    for (uint32_t i = 0; i < n; i++) {
      const double true_rank = (double) i / n;
      REQUIRE(sketch.get_rank(static_cast<float>(i)) == true_rank);
      const double true_rank_inclusive = (double) (i + 1) / n;
      REQUIRE(sketch.get_rank<true>(static_cast<float>(i)) == true_rank_inclusive);
    }

    // the alternative method must produce the same result
    auto quantiles2 = sketch.get_quantiles(3);
    REQUIRE(quantiles2.size() == 3);
    REQUIRE(quantiles[0] == quantiles2[0]);
    REQUIRE(quantiles[1] == quantiles2[1]);
    REQUIRE(quantiles[2] == quantiles2[2]);
  }

  SECTION("10 items") {
    kll_float_sketch sketch(200, 0);
    sketch.update(1.0f);
    sketch.update(2.0f);
    sketch.update(3.0f);
    sketch.update(4.0f);
    sketch.update(5.0f);
    sketch.update(6.0f);
    sketch.update(7.0f);
    sketch.update(8.0f);
    sketch.update(9.0f);
    sketch.update(10.0f);
    REQUIRE(sketch.get_quantile(0) == 1.0);
    REQUIRE(sketch.get_quantile(0.5) == 6.0);
    REQUIRE(sketch.get_quantile(0.99) == 10.0);
    REQUIRE(sketch.get_quantile(1) == 10.0);
  }

  SECTION("100 items") {
    kll_float_sketch sketch(200, 0);
    for (int i = 0; i < 100; ++i) sketch.update(static_cast<float>(i));
    REQUIRE(sketch.get_quantile(0) == 0);
    REQUIRE(sketch.get_quantile(0.01) == 1);
    REQUIRE(sketch.get_quantile(0.5) == 50);
    REQUIRE(sketch.get_quantile(0.99) == 99.0);
    REQUIRE(sketch.get_quantile(1) == 99.0);
  }

  SECTION("many items, estimation mode") {
    kll_float_sketch sketch(200, 0);
    const int n = 1000000;
    for (int i = 0; i < n; i++) {
      sketch.update(static_cast<float>(i));
      REQUIRE(sketch.get_n() == static_cast<uint64_t>(i + 1));
    }
    REQUIRE_FALSE(sketch.is_empty());
    REQUIRE(sketch.is_estimation_mode());
    REQUIRE(sketch.get_min_value() == 0.0); // min value is exact
    REQUIRE(sketch.get_quantile(0) == 0.0); // min value is exact
    REQUIRE(sketch.get_max_value() == n - 1); // max value is exact
    REQUIRE(sketch.get_quantile(1) == n - 1); // max value is exact

    // test rank
    for (int i = 0; i < n; i++) {
      const double trueRank = (double) i / n;
      REQUIRE(sketch.get_rank(static_cast<float>(i)) == Approx(trueRank).margin(RANK_EPS_FOR_K_200));
    }

    // test quantiles at every 0.1 percentage point
    double fractions[1001];
    double reverse_fractions[1001]; // check that ordering does not matter
    for (int i = 0; i < 1001; i++) {
      fractions[i] = (double) i / 1000;
      reverse_fractions[1000 - i] = fractions[i];
    }
    auto quantiles = sketch.get_quantiles(fractions, 1001);
    auto reverse_quantiles = sketch.get_quantiles(reverse_fractions, 1001);
    float previous_quantile(0);
    for (int i = 0; i < 1001; i++) {
      // expensive in a loop, just to check the equivalence here, not advised for real code
      const float quantile = sketch.get_quantile(fractions[i]);
      REQUIRE(quantiles[i] == quantile);
      REQUIRE(reverse_quantiles[1000 - i] == quantile);
      REQUIRE(previous_quantile <= quantile);
      previous_quantile = quantile;
    }

    //std::cout << sketch.to_string();

    uint32_t count = 0;
    uint64_t total_weight = 0;
    for (auto it: sketch) {
      ++count;
      total_weight += it.second;
    }
    REQUIRE(count == sketch.get_num_retained());
    REQUIRE(total_weight == sketch.get_n());
  }

  SECTION("consistency between get_rank adn get_PMF/CDF") {
    kll_float_sketch sketch(200, 0);
    const int n = 1000;
    float values[n];
    for (int i = 0; i < n; i++) {
      sketch.update(static_cast<float>(i));
      values[i] = static_cast<float>(i);
    }
    { // inclusive=false (default)
      const auto ranks(sketch.get_CDF(values, n));
      const auto pmf(sketch.get_PMF(values, n));

      double subtotal_pmf = 0;
      for (int i = 0; i < n; i++) {
        if (sketch.get_rank(values[i]) != ranks[i]) {
          std::cerr << "checking rank vs CDF for value " << i << std::endl;
          REQUIRE(sketch.get_rank(values[i]) == ranks[i]);
        }
        subtotal_pmf += pmf[i];
        if (abs(ranks[i] - subtotal_pmf) > NUMERIC_NOISE_TOLERANCE) {
          std::cerr << "CDF vs PMF for value " << i << std::endl;
          REQUIRE(ranks[i] == Approx(subtotal_pmf).margin(NUMERIC_NOISE_TOLERANCE));
        }
      }
    }
    {  // inclusive=true
      const auto ranks(sketch.get_CDF<true>(values, n));
      const auto pmf(sketch.get_PMF<true>(values, n));

      double subtotal_pmf = 0;
      for (int i = 0; i < n; i++) {
        if (sketch.get_rank<true>(values[i]) != ranks[i]) {
          std::cerr << "checking rank vs CDF for value " << i << std::endl;
          REQUIRE(sketch.get_rank(values[i]) == ranks[i]);
        }
        subtotal_pmf += pmf[i];
        if (abs(ranks[i] - subtotal_pmf) > NUMERIC_NOISE_TOLERANCE) {
          std::cerr << "CDF vs PMF for value " << i << std::endl;
          REQUIRE(ranks[i] == Approx(subtotal_pmf).margin(NUMERIC_NOISE_TOLERANCE));
        }
      }
    }
  }

  SECTION("deserialize from java") {
    std::ifstream is;
    is.exceptions(std::ios::failbit | std::ios::badbit);
    is.open(testBinaryInputPath + "kll_sketch_from_java.sk", std::ios::binary);
    auto sketch = kll_float_sketch::deserialize(is, test_allocator<float>(0));
    REQUIRE_FALSE(sketch.is_empty());
    REQUIRE(sketch.is_estimation_mode());
    REQUIRE(sketch.get_n() == 1000000);
    REQUIRE(sketch.get_num_retained() == 614);
    REQUIRE(sketch.get_min_value() == 0.0);
    REQUIRE(sketch.get_max_value() == 999999.0);
  }

  SECTION("stream serialize deserialize empty") {
    kll_float_sketch sketch(200, 0);
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    REQUIRE(static_cast<size_t>(s.tellp()) == sketch.get_serialized_size_bytes());
    auto sketch2 = kll_float_sketch::deserialize(s, test_allocator<float>(0));
    REQUIRE(static_cast<size_t>(s.tellp()) == sketch2.get_serialized_size_bytes());
    REQUIRE(s.tellg() == s.tellp());
    REQUIRE(sketch2.is_empty() == sketch.is_empty());
    REQUIRE(sketch2.is_estimation_mode() == sketch.is_estimation_mode());
    REQUIRE(sketch2.get_n() == sketch.get_n());
    REQUIRE(sketch2.get_num_retained() == sketch.get_num_retained());
    REQUIRE(std::isnan(sketch2.get_min_value()));
    REQUIRE(std::isnan(sketch2.get_max_value()));
    REQUIRE(sketch2.get_normalized_rank_error(false) == sketch.get_normalized_rank_error(false));
    REQUIRE(sketch2.get_normalized_rank_error(true) == sketch.get_normalized_rank_error(true));
  }

  SECTION("bytes serialize deserialize empty") {
    kll_float_sketch sketch(200, 0);
    auto bytes = sketch.serialize();
    auto sketch2 = kll_float_sketch::deserialize(bytes.data(), bytes.size(), serde<float>(), 0);
    REQUIRE(bytes.size() == sketch.get_serialized_size_bytes());
    REQUIRE(sketch2.is_empty() == sketch.is_empty());
    REQUIRE(sketch2.is_estimation_mode() == sketch.is_estimation_mode());
    REQUIRE(sketch2.get_n() == sketch.get_n());
    REQUIRE(sketch2.get_num_retained() == sketch.get_num_retained());
    REQUIRE(std::isnan(sketch2.get_min_value()));
    REQUIRE(std::isnan(sketch2.get_max_value()));
    REQUIRE(sketch2.get_normalized_rank_error(false) == sketch.get_normalized_rank_error(false));
    REQUIRE(sketch2.get_normalized_rank_error(true) == sketch.get_normalized_rank_error(true));
  }

  SECTION("stream serialize deserialize one item") {
    kll_float_sketch sketch(200, 0);
    sketch.update(1.0f);
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    REQUIRE(static_cast<size_t>(s.tellp()) == sketch.get_serialized_size_bytes());
    auto sketch2 = kll_float_sketch::deserialize(s, serde<float>(), 0);
    REQUIRE(static_cast<size_t>(s.tellp()) == sketch2.get_serialized_size_bytes());
    REQUIRE(s.tellg() == s.tellp());
    REQUIRE_FALSE(sketch2.is_empty());
    REQUIRE_FALSE(sketch2.is_estimation_mode());
    REQUIRE(sketch2.get_n() == 1);
    REQUIRE(sketch2.get_num_retained() == 1);
    REQUIRE(sketch2.get_min_value() == 1.0);
    REQUIRE(sketch2.get_max_value() == 1.0);
    REQUIRE(sketch2.get_quantile(0.5) == 1.0);
    REQUIRE(sketch2.get_rank(1) == 0.0);
    REQUIRE(sketch2.get_rank(2) == 1.0);
  }

  SECTION("bytes serialize deserialize one item") {
    kll_float_sketch sketch(200, 0);
    sketch.update(1.0f);
    auto bytes = sketch.serialize();
    REQUIRE(bytes.size() == sketch.get_serialized_size_bytes());
    auto sketch2 = kll_float_sketch::deserialize(bytes.data(), bytes.size(), serde<float>(), 0);
    REQUIRE(bytes.size() == sketch2.get_serialized_size_bytes());
    REQUIRE_FALSE(sketch2.is_empty());
    REQUIRE_FALSE(sketch2.is_estimation_mode());
    REQUIRE(sketch2.get_n() == 1);
    REQUIRE(sketch2.get_num_retained() == 1);
    REQUIRE(sketch2.get_min_value() == 1.0);
    REQUIRE(sketch2.get_max_value() == 1.0);
    REQUIRE(sketch2.get_quantile(0.5) == 1.0);
    REQUIRE(sketch2.get_rank(1) == 0.0);
    REQUIRE(sketch2.get_rank(2) == 1.0);
  }

  SECTION("deserialize one item v1") {
    std::ifstream is;
    is.exceptions(std::ios::failbit | std::ios::badbit);
    is.open(testBinaryInputPath + "kll_sketch_float_one_item_v1.sk", std::ios::binary);
    auto sketch = kll_float_sketch::deserialize(is, serde<float>(), 0);
    REQUIRE_FALSE(sketch.is_empty());
    REQUIRE_FALSE(sketch.is_estimation_mode());
    REQUIRE(sketch.get_n() == 1);
    REQUIRE(sketch.get_num_retained() == 1);
    REQUIRE(sketch.get_min_value() == 1.0);
    REQUIRE(sketch.get_max_value() == 1.0);
  }

  SECTION("stream serialize deserialize three items") {
    kll_float_sketch sketch(200, 0);
    sketch.update(1.0f);
    sketch.update(2.0f);
    sketch.update(3.0f);
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    REQUIRE(static_cast<size_t>(s.tellp()) == sketch.get_serialized_size_bytes());
    auto sketch2 = kll_float_sketch::deserialize(s, serde<float>(), 0);
    REQUIRE(static_cast<size_t>(s.tellp()) == sketch2.get_serialized_size_bytes());
    REQUIRE(s.tellg() == s.tellp());
    REQUIRE_FALSE(sketch2.is_empty());
    REQUIRE_FALSE(sketch2.is_estimation_mode());
    REQUIRE(sketch2.get_n() == 3);
    REQUIRE(sketch2.get_num_retained() == 3);
    REQUIRE(sketch2.get_min_value() == 1.0);
    REQUIRE(sketch2.get_max_value() == 3.0);
  }

  SECTION("bytes serialize deserialize three items") {
    kll_float_sketch sketch(200, 0);
    sketch.update(1.0f);
    sketch.update(2.0f);
    sketch.update(3.0f);
    auto bytes = sketch.serialize();
    REQUIRE(bytes.size() == sketch.get_serialized_size_bytes());
    auto sketch2 = kll_float_sketch::deserialize(bytes.data(), bytes.size(), serde<float>(), 0);
    REQUIRE(bytes.size() == sketch2.get_serialized_size_bytes());
    REQUIRE_FALSE(sketch2.is_empty());
    REQUIRE_FALSE(sketch2.is_estimation_mode());
    REQUIRE(sketch2.get_n() == 3);
    REQUIRE(sketch2.get_num_retained() == 3);
    REQUIRE(sketch2.get_min_value() == 1.0);
    REQUIRE(sketch2.get_max_value() == 3.0);
  }

  SECTION("stream serialize deserialize many floats") {
    kll_float_sketch sketch(200, 0);
    const int n = 1000;
    for (int i = 0; i < n; i++) sketch.update(static_cast<float>(i));
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    REQUIRE(static_cast<size_t>(s.tellp()) == sketch.get_serialized_size_bytes());
    auto sketch2 = kll_float_sketch::deserialize(s, serde<float>(), 0);
    REQUIRE(static_cast<size_t>(s.tellp()) == sketch2.get_serialized_size_bytes());
    REQUIRE(s.tellg() == s.tellp());
    REQUIRE(sketch2.is_empty() == sketch.is_empty());
    REQUIRE(sketch2.is_estimation_mode() == sketch.is_estimation_mode());
    REQUIRE(sketch2.get_n() == sketch.get_n());
    REQUIRE(sketch2.get_num_retained() == sketch.get_num_retained());
    REQUIRE(sketch2.get_min_value() == sketch.get_min_value());
    REQUIRE(sketch2.get_max_value() == sketch.get_max_value());
    REQUIRE(sketch2.get_normalized_rank_error(false) == sketch.get_normalized_rank_error(false));
    REQUIRE(sketch2.get_normalized_rank_error(true) == sketch.get_normalized_rank_error(true));
    REQUIRE(sketch2.get_quantile(0.5) == sketch.get_quantile(0.5));
    REQUIRE(sketch2.get_rank(0) == sketch.get_rank(0));
    REQUIRE(sketch2.get_rank(static_cast<float>(n)) == sketch.get_rank(static_cast<float>(n)));
  }

  SECTION("bytes serialize deserialize many floats") {
    kll_float_sketch sketch(200, 0);
    const int n = 1000;
    for (int i = 0; i < n; i++) sketch.update(static_cast<float>(i));
    auto bytes = sketch.serialize();
    REQUIRE(bytes.size() == sketch.get_serialized_size_bytes());
    auto sketch2 = kll_float_sketch::deserialize(bytes.data(), bytes.size(), serde<float>(), 0);
    REQUIRE(bytes.size() == sketch2.get_serialized_size_bytes());
    REQUIRE(sketch2.is_empty() == sketch.is_empty());
    REQUIRE(sketch2.is_estimation_mode() == sketch.is_estimation_mode());
    REQUIRE(sketch2.get_n() == sketch.get_n());
    REQUIRE(sketch2.get_num_retained() == sketch.get_num_retained());
    REQUIRE(sketch2.get_min_value() == sketch.get_min_value());
    REQUIRE(sketch2.get_max_value() == sketch.get_max_value());
    REQUIRE(sketch2.get_normalized_rank_error(false) == sketch.get_normalized_rank_error(false));
    REQUIRE(sketch2.get_normalized_rank_error(true) == sketch.get_normalized_rank_error(true));
    REQUIRE(sketch2.get_quantile(0.5) == sketch.get_quantile(0.5));
    REQUIRE(sketch2.get_rank(0) == sketch.get_rank(0));
    REQUIRE(sketch2.get_rank(static_cast<float>(n)) == sketch.get_rank(static_cast<float>(n)));
    REQUIRE_THROWS_AS(kll_sketch<int>::deserialize(bytes.data(), 7), std::out_of_range);
    REQUIRE_THROWS_AS(kll_sketch<int>::deserialize(bytes.data(), 15), std::out_of_range);
    REQUIRE_THROWS_AS(kll_sketch<int>::deserialize(bytes.data(), bytes.size() - 1), std::out_of_range);
  }

  SECTION("bytes serialize deserialize many ints") {
    kll_sketch<int> sketch;
    const int n = 1000;
    for (int i = 0; i < n; i++) sketch.update(i);
    auto bytes = sketch.serialize();
    REQUIRE(bytes.size() == sketch.get_serialized_size_bytes());
    auto sketch2 = kll_sketch<int>::deserialize(bytes.data(), bytes.size());
    REQUIRE(bytes.size() == sketch2.get_serialized_size_bytes());
    REQUIRE(sketch2.is_empty() == sketch.is_empty());
    REQUIRE(sketch2.is_estimation_mode() == sketch.is_estimation_mode());
    REQUIRE(sketch2.get_n() == sketch.get_n());
    REQUIRE(sketch2.get_num_retained() == sketch.get_num_retained());
    REQUIRE(sketch2.get_min_value() == sketch.get_min_value());
    REQUIRE(sketch2.get_max_value() == sketch.get_max_value());
    REQUIRE(sketch2.get_normalized_rank_error(false) == sketch.get_normalized_rank_error(false));
    REQUIRE(sketch2.get_normalized_rank_error(true) == sketch.get_normalized_rank_error(true));
    REQUIRE(sketch2.get_quantile(0.5) == sketch.get_quantile(0.5));
    REQUIRE(sketch2.get_rank(0) == sketch.get_rank(0));
    REQUIRE(sketch2.get_rank(n) == sketch.get_rank(n));
    REQUIRE_THROWS_AS(kll_sketch<int>::deserialize(bytes.data(), 7), std::out_of_range);
    REQUIRE_THROWS_AS(kll_sketch<int>::deserialize(bytes.data(), 15), std::out_of_range);
    REQUIRE_THROWS_AS(kll_sketch<int>::deserialize(bytes.data(), bytes.size() - 1), std::out_of_range);
  }

  SECTION("floor of log2 of fraction") {
    REQUIRE(kll_helper::floor_of_log2_of_fraction(0, 1) == 0);
    REQUIRE(kll_helper::floor_of_log2_of_fraction(1, 2) == 0);
    REQUIRE(kll_helper::floor_of_log2_of_fraction(2, 2) == 0);
    REQUIRE(kll_helper::floor_of_log2_of_fraction(3, 2) == 0);
    REQUIRE(kll_helper::floor_of_log2_of_fraction(4, 2) == 1);
    REQUIRE(kll_helper::floor_of_log2_of_fraction(5, 2) == 1);
    REQUIRE(kll_helper::floor_of_log2_of_fraction(6, 2) == 1);
    REQUIRE(kll_helper::floor_of_log2_of_fraction(7, 2) == 1);
    REQUIRE(kll_helper::floor_of_log2_of_fraction(8, 2) == 2);
  }

  SECTION("out of order split points, float") {
    kll_float_sketch sketch(200, 0);
    sketch.update(0); // has too be non-empty to reach the check
    float split_points[2] = {1, 0};
    REQUIRE_THROWS_AS(sketch.get_CDF(split_points, 2), std::invalid_argument);
  }

  SECTION("out of order split points, int") {
    kll_sketch<int> sketch;
    sketch.update(0); // has too be non-empty to reach the check
    int split_points[2] = {1, 0};
    REQUIRE_THROWS_AS(sketch.get_CDF(split_points, 2), std::invalid_argument);
  }

  SECTION("NaN split point") {
    kll_float_sketch sketch(200, 0);
    sketch.update(0); // has too be non-empty to reach the check
    float split_points[1] = {std::numeric_limits<float>::quiet_NaN()};
    REQUIRE_THROWS_AS(sketch.get_CDF(split_points, 1), std::invalid_argument);
  }

  SECTION("merge") {
    kll_float_sketch sketch1(200, 0);
    kll_float_sketch sketch2(200, 0);
    const int n = 10000;
    for (int i = 0; i < n; i++) {
      sketch1.update(static_cast<float>(i));
      sketch2.update(static_cast<float>((2 * n) - i - 1));
    }

    REQUIRE(sketch1.get_min_value() == 0.0f);
    REQUIRE(sketch1.get_max_value() == n - 1);
    REQUIRE(sketch2.get_min_value() == n);
    REQUIRE(sketch2.get_max_value() == 2.0f * n - 1);

    sketch1.merge(sketch2);

    REQUIRE_FALSE(sketch1.is_empty());
    REQUIRE(sketch1.get_n() == 2 * n);
    REQUIRE(sketch1.get_min_value() == 0.0f);
    REQUIRE(sketch1.get_max_value() == 2.0f * n - 1);
    REQUIRE(sketch1.get_quantile(0.5) == Approx(n).margin(n * RANK_EPS_FOR_K_200));
  }

  SECTION("merge lower k") {
    kll_float_sketch sketch1(256, 0);
    kll_float_sketch sketch2(128, 0);
    const int n = 10000;
    for (int i = 0; i < n; i++) {
      sketch1.update(static_cast<float>(i));
      sketch2.update(static_cast<float>((2 * n) - i - 1));
    }

    REQUIRE(sketch1.get_min_value() == 0.0f);
    REQUIRE(sketch1.get_max_value() == n - 1);
    REQUIRE(sketch2.get_min_value() == n);
    REQUIRE(sketch2.get_max_value() == 2.0f * n - 1);

    REQUIRE(sketch1.get_k() == 256);
    REQUIRE(sketch2.get_k() == 128);

    REQUIRE(sketch1.get_normalized_rank_error(false) < sketch2.get_normalized_rank_error(false));
    REQUIRE(sketch1.get_normalized_rank_error(true) < sketch2.get_normalized_rank_error(true));

    sketch1.merge(sketch2);

    // sketch1 must get "contaminated" by the lower K in sketch2
    REQUIRE(sketch2.get_normalized_rank_error(false) == sketch1.get_normalized_rank_error(false));
    REQUIRE(sketch2.get_normalized_rank_error(true) == sketch1.get_normalized_rank_error(true));

    REQUIRE_FALSE(sketch1.is_empty());
    REQUIRE(sketch1.get_n() == 2 * n);
    REQUIRE(sketch1.get_min_value() == 0.0f);
    REQUIRE(sketch1.get_max_value() == 2.0f * n - 1);
    REQUIRE(sketch1.get_quantile(0.5) == Approx(n).margin(n * RANK_EPS_FOR_K_200));
  }

  SECTION("merge exact mode, lower k") {
    kll_float_sketch sketch1(256, 0);
    kll_float_sketch sketch2(128, 0);
    const int n = 10000;
    for (int i = 0; i < n; i++) {
      sketch1.update(static_cast<float>(i));
    }

    // rank error should not be affected by a merge with an empty sketch with lower k
    const double rank_error_before_merge = sketch1.get_normalized_rank_error(true);
    sketch1.merge(sketch2);
    REQUIRE(sketch1.get_normalized_rank_error(true) == rank_error_before_merge);

    REQUIRE_FALSE(sketch1.is_empty());
    REQUIRE(sketch1.get_n() == n);
    REQUIRE(sketch1.get_min_value() == 0.0f);
    REQUIRE(sketch1.get_max_value() == n - 1);
    REQUIRE(sketch1.get_quantile(0.5) == Approx(n / 2).margin(n / 2 * RANK_EPS_FOR_K_200));

    sketch2.update(0);
    sketch1.merge(sketch2);
    // rank error should not be affected by a merge with a sketch in exact mode with lower k
    REQUIRE(sketch1.get_normalized_rank_error(true) == rank_error_before_merge);
  }

  SECTION("merge min value from other") {
    kll_float_sketch sketch1(200, 0);
    kll_float_sketch sketch2(200, 0);
    sketch1.update(1.0f);
    sketch2.update(2.0f);
    sketch2.merge(sketch1);
    REQUIRE(sketch2.get_min_value() == 1.0f);
    REQUIRE(sketch2.get_max_value() == 2.0f);
  }

  SECTION("merge min and max values from other") {
    kll_float_sketch sketch1(200, 0);
    for (int i = 0; i < 1000000; i++) sketch1.update(static_cast<float>(i));
    kll_float_sketch sketch2(200, 0);
    sketch2.merge(sketch1);
    REQUIRE(sketch2.get_min_value() == 0.0f);
    REQUIRE(sketch2.get_max_value() == 999999.0f);
  }

  SECTION("sketch of ints") {
    kll_sketch<int> sketch;
    REQUIRE_THROWS_AS(sketch.get_quantile(0), std::runtime_error);
    REQUIRE_THROWS_AS(sketch.get_min_value(), std::runtime_error);
    REQUIRE_THROWS_AS(sketch.get_max_value(), std::runtime_error);

    const int n = 1000;
    for (int i = 0; i < n; i++) sketch.update(i);

    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    REQUIRE(static_cast<size_t>(s.tellp()) == sketch.get_serialized_size_bytes());
    auto sketch2 = kll_sketch<int>::deserialize(s);
    REQUIRE(static_cast<size_t>(s.tellp()) == sketch2.get_serialized_size_bytes());
    REQUIRE(s.tellg() == s.tellp());
    REQUIRE(sketch2.is_empty() == sketch.is_empty());
    REQUIRE(sketch2.is_estimation_mode() == sketch.is_estimation_mode());
    REQUIRE(sketch2.get_n() == sketch.get_n());
    REQUIRE(sketch2.get_num_retained() == sketch.get_num_retained());
    REQUIRE(sketch2.get_min_value() == sketch.get_min_value());
    REQUIRE(sketch2.get_max_value() == sketch.get_max_value());
    REQUIRE(sketch2.get_normalized_rank_error(false) == sketch.get_normalized_rank_error(false));
    REQUIRE(sketch2.get_normalized_rank_error(true) == sketch.get_normalized_rank_error(true));
    REQUIRE(sketch2.get_quantile(0.5) == sketch.get_quantile(0.5));
    REQUIRE(sketch2.get_rank(0) == sketch.get_rank(0));
    REQUIRE(sketch2.get_rank(n) == sketch.get_rank(n));
  }

  SECTION("sketch of strings stream") {
    kll_string_sketch sketch1(200, 0);
    REQUIRE_THROWS_AS(sketch1.get_quantile(0), std::runtime_error);
    REQUIRE_THROWS_AS(sketch1.get_min_value(), std::runtime_error);
    REQUIRE_THROWS_AS(sketch1.get_max_value(), std::runtime_error);
    REQUIRE(sketch1.get_serialized_size_bytes() == 8);

    const int n = 1000;
    for (int i = 0; i < n; i++) sketch1.update(std::to_string(i));

    REQUIRE(sketch1.get_min_value() == std::string("0"));
    REQUIRE(sketch1.get_max_value() == std::string("999"));

    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch1.serialize(s);
    REQUIRE(static_cast<size_t>(s.tellp()) == sketch1.get_serialized_size_bytes());
    auto sketch2 = kll_string_sketch::deserialize(s, test_allocator<std::string>(0));
    REQUIRE(static_cast<size_t>(s.tellp()) == sketch2.get_serialized_size_bytes());
    REQUIRE(s.tellg() == s.tellp());
    REQUIRE(sketch2.is_empty() == sketch1.is_empty());
    REQUIRE(sketch2.is_estimation_mode() == sketch1.is_estimation_mode());
    REQUIRE(sketch2.get_n() == sketch1.get_n());
    REQUIRE(sketch2.get_num_retained() == sketch1.get_num_retained());
    REQUIRE(sketch2.get_min_value() == sketch1.get_min_value());
    REQUIRE(sketch2.get_max_value() == sketch1.get_max_value());
    REQUIRE(sketch2.get_normalized_rank_error(false) == sketch1.get_normalized_rank_error(false));
    REQUIRE(sketch2.get_normalized_rank_error(true) == sketch1.get_normalized_rank_error(true));
    REQUIRE(sketch2.get_quantile(0.5) == sketch1.get_quantile(0.5));
    REQUIRE(sketch2.get_rank(std::to_string(0)) == sketch1.get_rank(std::to_string(0)));
    REQUIRE(sketch2.get_rank(std::to_string(n)) == sketch1.get_rank(std::to_string(n)));

    // to take a look using hexdump
    //std::ofstream os("kll-string.sk");
    //sketch1.serialize(os);

    // debug print
    //sketch1.to_stream(std::cout);
  }

  SECTION("sketch of strings bytes") {
    kll_string_sketch sketch1(200, 0);
    REQUIRE_THROWS_AS(sketch1.get_quantile(0), std::runtime_error);
    REQUIRE_THROWS_AS(sketch1.get_min_value(), std::runtime_error);
    REQUIRE_THROWS_AS(sketch1.get_max_value(), std::runtime_error);
    REQUIRE(sketch1.get_serialized_size_bytes() == 8);

    const int n = 1000;
    for (int i = 0; i < n; i++) sketch1.update(std::to_string(i));

    REQUIRE(sketch1.get_min_value() == std::string("0"));
    REQUIRE(sketch1.get_max_value() == std::string("999"));

    auto bytes = sketch1.serialize();
    REQUIRE(bytes.size() == sketch1.get_serialized_size_bytes());
    auto sketch2 = kll_string_sketch::deserialize(bytes.data(), bytes.size(), serde<std::string>(), 0);
    REQUIRE(bytes.size() == sketch2.get_serialized_size_bytes());
    REQUIRE(sketch2.is_empty() == sketch1.is_empty());
    REQUIRE(sketch2.is_estimation_mode() == sketch1.is_estimation_mode());
    REQUIRE(sketch2.get_n() == sketch1.get_n());
    REQUIRE(sketch2.get_num_retained() == sketch1.get_num_retained());
    REQUIRE(sketch2.get_min_value() == sketch1.get_min_value());
    REQUIRE(sketch2.get_max_value() == sketch1.get_max_value());
    REQUIRE(sketch2.get_normalized_rank_error(false) == sketch1.get_normalized_rank_error(false));
    REQUIRE(sketch2.get_normalized_rank_error(true) == sketch1.get_normalized_rank_error(true));
    REQUIRE(sketch2.get_quantile(0.5) == sketch1.get_quantile(0.5));
    REQUIRE(sketch2.get_rank(std::to_string(0)) == sketch1.get_rank(std::to_string(0)));
    REQUIRE(sketch2.get_rank(std::to_string(n)) == sketch1.get_rank(std::to_string(n)));
  }


  SECTION("sketch of strings, single item, bytes") {
    kll_string_sketch sketch1(200, 0);
    sketch1.update("a");
    auto bytes = sketch1.serialize();
    REQUIRE(bytes.size() == sketch1.get_serialized_size_bytes());
    auto sketch2 = kll_string_sketch::deserialize(bytes.data(), bytes.size(), serde<std::string>(), 0);
    REQUIRE(bytes.size() == sketch2.get_serialized_size_bytes());
  }

  SECTION("copy") {
    kll_sketch<int> sketch1;
    const int n(1000);
    for (int i = 0; i < n; i++) sketch1.update(i);

    // copy constructor
    kll_sketch<int> sketch2(sketch1);
    for (int i = 0; i < n; i++) {
      REQUIRE(sketch2.get_rank(i) == sketch1.get_rank(i));
    }

    // copy assignment
    kll_sketch<int> sketch3;
    sketch3 = sketch1;
    for (int i = 0; i < n; i++) {
      REQUIRE(sketch3.get_rank(i) == sketch1.get_rank(i));
    }
  }

  SECTION("move") {
    kll_sketch<int> sketch1;
    const int n(100);
    for (int i = 0; i < n; i++) sketch1.update(i);

    // move constructor
    kll_sketch<int> sketch2(std::move(sketch1));
    for (int i = 0; i < n; i++) {
      REQUIRE(sketch2.get_rank(i) == (double) i / n);
    }

    // move assignment
    kll_sketch<int> sketch3;
    sketch3 = std::move(sketch2);
    for (int i = 0; i < n; i++) {
      REQUIRE(sketch3.get_rank(i) == (double) i / n);
    }
  }

  SECTION("max serialized size arithmetic type") {
    REQUIRE(kll_sketch<float>::get_max_serialized_size_bytes(200, 10) == 1968);
    REQUIRE(kll_sketch<float>::get_max_serialized_size_bytes(200, 100) == 2316);
    REQUIRE(kll_sketch<float>::get_max_serialized_size_bytes(200, 1000) == 2440);
    REQUIRE(kll_sketch<float>::get_max_serialized_size_bytes(200, 1000000) == 2800);
    REQUIRE(kll_sketch<float>::get_max_serialized_size_bytes(200, 1000000000) == 3160);
  }

  SECTION("max serialized size non-arithmetic type") {
    REQUIRE(kll_sketch<std::string>::get_max_serialized_size_bytes(200, 10, 4) == 1968);
    REQUIRE(kll_sketch<std::string>::get_max_serialized_size_bytes(200, 100, 4) == 2316);
    REQUIRE(kll_sketch<std::string>::get_max_serialized_size_bytes(200, 1000, 4) == 2440);
    REQUIRE(kll_sketch<std::string>::get_max_serialized_size_bytes(200, 1000000, 4) == 2800);
    REQUIRE(kll_sketch<std::string>::get_max_serialized_size_bytes(200, 1000000000, 4) == 3160);
  }

  SECTION("issue #236") {
    kll_sketch<int8_t> kll;
    kll.update(1);
    kll.update(2);
    kll.update(3);
    auto blob = kll.serialize();
    auto kll2 = kll_sketch<int8_t>::deserialize(blob.data(), blob.size());
  }

  SECTION("sorted view") {
    kll_sketch<int> kll;
    kll.update(2);
    kll.update(3);
    kll.update(1);

    { // non-cumulative, using operator->
      auto view = kll.get_sorted_view(false);
      REQUIRE(view.size() == 3);
      auto it = view.begin();
      REQUIRE(it->first == 1);
      REQUIRE(it->second == 1);
      ++it;
      REQUIRE(it->first == 2);
      REQUIRE(it->second == 1);
      ++it;
      REQUIRE(it->first == 3);
      REQUIRE(it->second == 1);
    }
    { // cumulative, non-inclusive, using operator->
      auto view = kll.get_sorted_view(true);
      REQUIRE(view.size() == 3);
      auto it = view.begin();
      REQUIRE(it->first == 1);
      REQUIRE(it->second == 0);
      ++it;
      REQUIRE(it->first == 2);
      REQUIRE(it->second == 1);
      ++it;
      REQUIRE(it->first == 3);
      REQUIRE(it->second == 2);
    }
    { // cumulative, inclusive, using operator*
      auto view = kll.get_sorted_view<true>(true);
      REQUIRE(view.size() == 3);
      auto it = view.begin();
      REQUIRE((*it).first == 1);
      REQUIRE((*it).second == 1);
      ++it;
      REQUIRE((*it).first == 2);
      REQUIRE((*it).second == 2);
      ++it;
      REQUIRE((*it).first == 3);
      REQUIRE((*it).second == 3);
    }
  }

  SECTION("type conversion: empty") {
    kll_sketch<double> kll_double;
    kll_sketch<float> kll_float(kll_double);
    REQUIRE(kll_float.is_empty());
    REQUIRE(kll_float.get_k() == kll_double.get_k());
    REQUIRE(kll_float.get_n() == 0);
    REQUIRE(kll_float.get_num_retained() == 0);
  }

  SECTION("type conversion: over k") {
    kll_sketch<double> kll_double;
    for (int i = 0; i < 1000; ++i) kll_double.update(static_cast<double>(i));
    kll_sketch<float> kll_float(kll_double);
    REQUIRE(!kll_float.is_empty());
    REQUIRE(kll_float.get_k() == kll_double.get_k());
    REQUIRE(kll_float.get_n() == kll_double.get_n());
    REQUIRE(kll_float.get_num_retained() == kll_double.get_num_retained());

    auto sv_float = kll_float.get_sorted_view(false);
    auto sv_double = kll_double.get_sorted_view(false);
    auto sv_float_it = sv_float.begin();
    auto sv_double_it = sv_double.begin();
    while (sv_float_it != sv_float.end()) {
      REQUIRE(sv_double_it != sv_double.end());
      auto float_pair = *sv_float_it;
      auto double_pair = *sv_double_it;
      REQUIRE(float_pair.first == Approx(double_pair.first).margin(0.01));
      REQUIRE(float_pair.second == double_pair.second);
      ++sv_float_it;
      ++sv_double_it;
    }
    REQUIRE(sv_double_it == sv_double.end());
  }

  class A {
    int val;
  public:
    A(int val): val(val) {}
    int get_val() const { return val; }
  };

  struct less_A {
    bool operator()(const A& a1, const A& a2) const { return a1.get_val() < a2.get_val(); }
  };

  class B {
    int val;
  public:
    explicit B(const A& a): val(a.get_val()) {}
    int get_val() const { return val; }
  };

  struct less_B {
    bool operator()(const B& b1, const B& b2) const { return b1.get_val() < b2.get_val(); }
  };

  SECTION("type conversion: custom types") {
    kll_sketch<A, less_A> sa;
    sa.update(1);
    sa.update(2);
    sa.update(3);

    kll_sketch<B, less_B> sb(sa);
    REQUIRE(sb.get_n() == 3);
  }

  // cleanup
  REQUIRE(test_allocator_total_bytes == 0);
}

} /* namespace datasketches */

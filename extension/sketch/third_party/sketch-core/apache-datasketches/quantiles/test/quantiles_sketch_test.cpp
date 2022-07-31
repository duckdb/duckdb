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
#include <sstream>
#include <fstream>

#include <quantiles_sketch.hpp>
#include <test_allocator.hpp>
#include <common_defs.hpp>

namespace datasketches {

static const double RANK_EPS_FOR_K_128 = 0.01725;
static const double NUMERIC_NOISE_TOLERANCE = 1E-6;

#ifdef TEST_BINARY_INPUT_PATH
static std::string testBinaryInputPath = TEST_BINARY_INPUT_PATH;
#else
static std::string testBinaryInputPath = "test/";
#endif

// typical usage would be just quantiles_sketch<float> or quantiles_sketch<std::string>, but here we use test_allocator
using quantiles_float_sketch = quantiles_sketch<float, std::less<float>, test_allocator<float>>;
using quantiles_string_sketch = quantiles_sketch<std::string, std::less<std::string>, test_allocator<std::string>>;

TEST_CASE("quantiles sketch", "[quantiles_sketch]") {

  // setup
  test_allocator_total_bytes = 0;

  SECTION("k limits") {
    quantiles_float_sketch sketch1(quantiles_constants::MIN_K, 0); // this should work
    quantiles_float_sketch sketch2(quantiles_constants::MAX_K, 0); // this should work
    REQUIRE_THROWS_AS(new quantiles_float_sketch(quantiles_constants::MIN_K - 1, 0), std::invalid_argument);
    REQUIRE_THROWS_AS(new quantiles_float_sketch(40, 0), std::invalid_argument); // not power of 2
    // MAX_K + 1 makes no sense because k is uint16_t
  }

  SECTION("empty") {
    quantiles_float_sketch sketch(128, 0);
    REQUIRE(sketch.is_empty());
    REQUIRE_FALSE(sketch.is_estimation_mode());
    REQUIRE(sketch.get_n() == 0);
    REQUIRE(sketch.get_num_retained() == 0);
    REQUIRE(std::isnan(sketch.get_rank(0)));
    REQUIRE(std::isnan(sketch.get_min_value()));
    REQUIRE(std::isnan(sketch.get_max_value()));
    REQUIRE(std::isnan(sketch.get_quantile(0.5)));
    const double fractions[3] {0, 0.5, 1};
    REQUIRE(sketch.get_quantiles(fractions, 3).empty());
    const float split_points[1] {0};
    REQUIRE(sketch.get_PMF(split_points, 1).empty());
    REQUIRE(sketch.get_CDF(split_points, 1).empty());

    for (auto it: sketch) {
      unused(it);
      FAIL("should be no iterations over an empty sketch");
    }
  }

  SECTION("get bad quantile") {
    quantiles_float_sketch sketch(64, 0);
    sketch.update(0.0f); // has to be non-empty to reach the check
    REQUIRE_THROWS_AS(sketch.get_quantile(-1), std::invalid_argument);
  }

  SECTION("one item") {
    quantiles_float_sketch sketch(128, 0);
    sketch.update(1.0f);
    REQUIRE_FALSE(sketch.is_empty());
    REQUIRE_FALSE(sketch.is_estimation_mode());
    REQUIRE(sketch.get_n() == 1);
    REQUIRE(sketch.get_num_retained() == 1);
    REQUIRE(sketch.get_rank(1.0f) == 0.0);
    REQUIRE(sketch.get_rank(2.0f) == 1.0);
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
    quantiles_float_sketch sketch(256, 0);
    sketch.update(std::numeric_limits<float>::quiet_NaN());
    REQUIRE(sketch.is_empty());

    sketch.update(0.0f);
    sketch.update(std::numeric_limits<float>::quiet_NaN());
    REQUIRE(sketch.get_n() == 1);
  }


  SECTION("sampling mode") {
    const uint16_t k = 8;
    const uint32_t n = 16 * (2 * k) + 1;
    quantiles_float_sketch sk(k, 0);
    for (uint32_t i = 0; i < n; ++i) {
      sk.update(static_cast<float>(i));
    }
  }

  SECTION("many items, exact mode") {
    const uint32_t n = 127;
    quantiles_float_sketch sketch(n + 1, 0);
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

    int count = 0;
    for (auto it: sketch) {
      REQUIRE(it.second == 1);
      ++count;
    }
    REQUIRE(count == n);

    const double fractions[3] {0, 0.5, 1};
    auto quantiles = sketch.get_quantiles(fractions, 3);
    REQUIRE(quantiles.size() == 3);
    REQUIRE(quantiles[0] == 0.0);
    REQUIRE(quantiles[1] == static_cast<float>(n / 2));
    REQUIRE(quantiles[2] == n - 1 );

    for (uint32_t i = 0; i < n; i++) {
      const double trueRank = (double) i / n;
      REQUIRE(sketch.get_rank(static_cast<float>(i)) == trueRank);
    }

    // the alternative method must produce the same result
    auto quantiles2 = sketch.get_quantiles(3);
    REQUIRE(quantiles2.size() == 3);
    REQUIRE(quantiles[0] == quantiles2[0]);
    REQUIRE(quantiles[1] == quantiles2[1]);
    REQUIRE(quantiles[2] == quantiles2[2]);
  }

  SECTION("10 items") {
    quantiles_float_sketch sketch(128, 0);
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
    quantiles_float_sketch sketch(128, 0);
    for (int i = 0; i < 100; ++i) sketch.update(static_cast<float>(i));
    REQUIRE(sketch.get_quantile(0) == 0);
    REQUIRE(sketch.get_quantile(0.01) == 1);
    REQUIRE(sketch.get_quantile(0.5) == 50);
    REQUIRE(sketch.get_quantile(0.99) == 99.0);
    REQUIRE(sketch.get_quantile(1) == 99.0);
  }

  SECTION("many items, estimation mode") {
    quantiles_float_sketch sketch(128, 0);
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
      const double trueRank = static_cast<float>(i) / n;
      const double sketchRank = sketch.get_rank(static_cast<float>(i));
      REQUIRE(sketchRank == Approx(trueRank).margin(RANK_EPS_FOR_K_128));
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

  SECTION("consistency between get_rank and get_PMF/CDF") {
    quantiles_float_sketch sketch(64, 0);
    const int n = 1000;
    float values[n];
    for (int i = 0; i < n; i++) {
      sketch.update(static_cast<float>(i));
      values[i] = static_cast<float>(i);
    }

    const auto ranks(sketch.get_CDF(values, n));
    const auto pmf(sketch.get_PMF(values, n));

    double subtotal_pmf(0);
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

  SECTION("inclusive true vs false") {
    quantiles_sketch<int> sketch(32);
    const int n = 100;
    for (int i = 1; i <= n; i++) {
      sketch.update(i);
    }

    // get_rank()
    // using knowledge of internal structure
    // value still in the base buffer to avoid randomness
    REQUIRE(sketch.get_rank<false>(80) == 0.79);
    REQUIRE(sketch.get_rank<true>(80) == 0.80);

    // value pushed into higher level
    REQUIRE(sketch.get_rank<false>(50) == Approx(0.49).margin(0.01));
    REQUIRE(sketch.get_rank<true>(50) == 0.50);
  
    // get_quantile()
    // value still in base buffer
    REQUIRE(sketch.get_quantile<false>(0.70) == 71);
    REQUIRE(sketch.get_quantile<true>(0.70) == 70);
  
    // value pushed into higher levell
    int quantile = sketch.get_quantile<false>(0.30);
    if (quantile != 31 && quantile != 32) { FAIL(); }
    
    quantile = sketch.get_quantile<true>(0.30);
    if (quantile != 29 && quantile != 30) { FAIL(); }
  }

  SECTION("stream serialize deserialize empty") {
    quantiles_float_sketch sketch(128, 0);
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    REQUIRE(static_cast<size_t>(s.tellp()) == sketch.get_serialized_size_bytes());
    auto sketch2 = quantiles_float_sketch::deserialize(s, serde<float>(), test_allocator<float>(0));
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
    quantiles_float_sketch sketch(256, 0);
    auto bytes = sketch.serialize();
    auto sketch2 = quantiles_float_sketch::deserialize(bytes.data(), bytes.size(), serde<float>(), 0);
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
    quantiles_float_sketch sketch(32, 0);
    sketch.update(1.0f);
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    REQUIRE(static_cast<size_t>(s.tellp()) == sketch.get_serialized_size_bytes());
    auto sketch2 = quantiles_float_sketch::deserialize(s, serde<float>(), test_allocator<float>(0));
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
    quantiles_float_sketch sketch(64, 0);
    sketch.update(1.0f);
    auto bytes = sketch.serialize();
    REQUIRE(bytes.size() == sketch.get_serialized_size_bytes());
    auto sketch2 = quantiles_float_sketch::deserialize(bytes.data(), bytes.size(), serde<float>(), 0);
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

  SECTION("stream serialize deserialize three items") {
    quantiles_float_sketch sketch(128, 0);
    sketch.update(1.0f);
    sketch.update(2.0f);
    sketch.update(3.0f);
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    REQUIRE(static_cast<size_t>(s.tellp()) == sketch.get_serialized_size_bytes());
    auto sketch2 = quantiles_float_sketch::deserialize(s, serde<float>(), test_allocator<float>(0));
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
    quantiles_float_sketch sketch(128, 0);
    sketch.update(1.0f);
    sketch.update(2.0f);
    sketch.update(3.0f);
    auto bytes = sketch.serialize();
    REQUIRE(bytes.size() == sketch.get_serialized_size_bytes());
    auto sketch2 = quantiles_float_sketch::deserialize(bytes.data(), bytes.size(), serde<float>(), 0);
    REQUIRE(bytes.size() == sketch2.get_serialized_size_bytes());
    REQUIRE_FALSE(sketch2.is_empty());
    REQUIRE_FALSE(sketch2.is_estimation_mode());
    REQUIRE(sketch2.get_n() == 3);
    REQUIRE(sketch2.get_num_retained() == 3);
    REQUIRE(sketch2.get_min_value() == 1.0);
    REQUIRE(sketch2.get_max_value() == 3.0);
  }

  SECTION("stream serialize deserialize many floats") {
    quantiles_float_sketch sketch(128, 0);
    const int n = 1000;
    for (int i = 0; i < n; i++) sketch.update(static_cast<float>(i));
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    REQUIRE(static_cast<size_t>(s.tellp()) == sketch.get_serialized_size_bytes());
    auto sketch2 = quantiles_float_sketch::deserialize(s, serde<float>(), test_allocator<float>(0));
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
    quantiles_float_sketch sketch(128, 0);
    const int n = 1000;
    for (int i = 0; i < n; i++) sketch.update(static_cast<float>(i));
    auto bytes = sketch.serialize();
    REQUIRE(bytes.size() == sketch.get_serialized_size_bytes());
    auto sketch2 = quantiles_float_sketch::deserialize(bytes.data(), bytes.size(), serde<float>(), 0);
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
    REQUIRE_THROWS_AS(quantiles_sketch<int>::deserialize(bytes.data(), 7), std::out_of_range);
    REQUIRE_THROWS_AS(quantiles_sketch<int>::deserialize(bytes.data(), 15), std::out_of_range);
    REQUIRE_THROWS_AS(quantiles_sketch<int>::deserialize(bytes.data(), bytes.size() - 1), std::out_of_range);
  }

  SECTION("bytes serialize deserialize many ints") {
    quantiles_sketch<int> sketch;
    const int n = 1000;
    for (int i = 0; i < n; i++) sketch.update(i);
    auto bytes = sketch.serialize();
    REQUIRE(bytes.size() == sketch.get_serialized_size_bytes());
    auto sketch2 = quantiles_sketch<int>::deserialize(bytes.data(), bytes.size());
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
    REQUIRE_THROWS_AS(quantiles_sketch<int>::deserialize(bytes.data(), 7), std::out_of_range);
    REQUIRE_THROWS_AS(quantiles_sketch<int>::deserialize(bytes.data(), 15), std::out_of_range);
    REQUIRE_THROWS_AS(quantiles_sketch<int>::deserialize(bytes.data(), bytes.size() - 1), std::out_of_range);
  }

  SECTION("out of order split points, float") {
    quantiles_float_sketch sketch(256, 0);
    sketch.update(0.0f); // has too be non-empty to reach the check
    float split_points[2] = {1, 0};
    REQUIRE_THROWS_AS(sketch.get_CDF(split_points, 2), std::invalid_argument);
  }

  SECTION("out of order split points, int") {
    quantiles_sketch<int> sketch;
    sketch.update(0); // has too be non-empty to reach the check
    int split_points[2] = {1, 0};
    REQUIRE_THROWS_AS(sketch.get_CDF(split_points, 2), std::invalid_argument);
  }

  SECTION("NaN split point") {
    quantiles_float_sketch sketch(512, 0);
    sketch.update(0.0f); // has too be non-empty to reach the check
    float split_points[1] = {std::numeric_limits<float>::quiet_NaN()};
    REQUIRE_THROWS_AS(sketch.get_CDF(split_points, 1), std::invalid_argument);
  }

  SECTION("merge") {
    quantiles_float_sketch sketch1(128, 0);
    quantiles_float_sketch sketch2(128, 0);
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
    REQUIRE(sketch1.get_quantile(0.5) == Approx(n).margin(n * RANK_EPS_FOR_K_128));
  }

  SECTION("merge from const") {
    quantiles_float_sketch sketch1(128, 0);
    quantiles_float_sketch sketch2(128, 0);
    const int n = 10000;
    for (int i = 0; i < n; i++) {
      sketch1.update(static_cast<float>(i));
      sketch2.update(static_cast<float>((2 * n) - i - 1));
    }

    REQUIRE(sketch1.get_min_value() == 0.0f);
    REQUIRE(sketch1.get_max_value() == n - 1);
    REQUIRE(sketch2.get_min_value() == n);
    REQUIRE(sketch2.get_max_value() == 2.0f * n - 1);

    sketch1.merge(const_cast<const quantiles_float_sketch&>(sketch2));

    REQUIRE_FALSE(sketch1.is_empty());
    REQUIRE(sketch1.get_n() == 2 * n);
    REQUIRE(sketch1.get_min_value() == 0.0f);
    REQUIRE(sketch1.get_max_value() == 2.0f * n - 1);
    REQUIRE(sketch1.get_quantile(0.5) == Approx(n).margin(n * RANK_EPS_FOR_K_128));
  }


  SECTION("merge lower k") {
    quantiles_float_sketch sketch1(256, 0);
    quantiles_float_sketch sketch2(128, 0);
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
    REQUIRE(sketch1.get_quantile(0.5) == Approx(n).margin(n * RANK_EPS_FOR_K_128));
  }

  SECTION("merge exact mode, lower k") {
    quantiles_float_sketch sketch1(256, 0);
    quantiles_float_sketch sketch2(128, 0);
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
    REQUIRE(sketch1.get_quantile(0.5) == Approx(n / 2).margin(n / 2 * RANK_EPS_FOR_K_128));

    sketch2.update(static_cast<float>(0));
    sketch1.merge(sketch2);
    // rank error should not be affected by a merge with a sketch in exact mode with lower k
    REQUIRE(sketch1.get_normalized_rank_error(true) == rank_error_before_merge);
  }

  SECTION("merge min value from other") {
    quantiles_float_sketch sketch1(128, 0);
    quantiles_float_sketch sketch2(128, 0);
    sketch1.update(1.0f);
    sketch2.update(2.0f);
    sketch2.merge(sketch1);
    REQUIRE(sketch2.get_min_value() == 1.0f);
    REQUIRE(sketch2.get_max_value() == 2.0f);
  }

  SECTION("merge min and max values from other") {
    quantiles_float_sketch sketch1(128, 0);
    for (int i = 0; i < 1000000; i++) sketch1.update(static_cast<float>(i));
    quantiles_float_sketch sketch2(128, 0);
    sketch2.merge(sketch1);
    REQUIRE(sketch2.get_min_value() == 0.0f);
    REQUIRE(sketch2.get_max_value() == 999999.0f);
  }

  SECTION("merge: two empty") {
    quantiles_float_sketch sk1(128, 0);
    quantiles_float_sketch sk2(64, 0);
    sk1.merge(sk2);
    REQUIRE(sk1.get_n() == 0);
    REQUIRE(sk1.get_k() == 128);

    sk2.merge(const_cast<const quantiles_float_sketch&>(sk1));
    REQUIRE(sk2.get_n() == 0);
    REQUIRE(sk2.get_k() == 64);
  }

  SECTION("merge: exact as input") {
    const uint16_t k = 128;
    quantiles_float_sketch sketch1(2 * k, 0);
    quantiles_float_sketch sketch2(k, 0);

    for (int i = 0; i < k / 2; i++) {
      sketch1.update(static_cast<float>(i));
      sketch2.update(static_cast<float>(i));
    }

    for (int i = 0; i < 100 * k; i++) {
      sketch1.update(static_cast<float>(i));
    }
    
    sketch1.merge(sketch2);
    REQUIRE(sketch1.get_n() == 101 * k);
    REQUIRE(sketch1.get_k() == 2 * k); // no reason to have shrunk
    REQUIRE(sketch1.get_min_value() == 0.0f);
    REQUIRE(sketch1.get_max_value() == static_cast<float>(100 * k - 1));
  }

  SECTION("merge: src estimation, tgt exact, tgt.k > src.k") {
    const uint16_t k = 128;
    quantiles_float_sketch sketch1(2 * k, 0);
    quantiles_float_sketch sketch2(k, 0);

    for (int i = 0; i < k / 2; i++) {
      sketch1.update(static_cast<float>(i));
      sketch2.update(static_cast<float>(i));
    }
    
    for (int i = 0; i < 100 * k; i++) {
      sketch2.update(static_cast<float>(i));
    }

    sketch1.merge(sketch2);
    REQUIRE(sketch1.get_n() == 101 * k);
    REQUIRE(sketch1.get_k() == k); // no reason to have shrunk
    REQUIRE(sketch1.get_min_value() == 0.0f);
    REQUIRE(sketch1.get_max_value() == static_cast<float>(100 * k - 1));
  }

  SECTION("merge: both estimation, tgt.k < src.k") {
    const uint16_t k = 128;
    quantiles_float_sketch sketch1(k, 0);
    quantiles_float_sketch sketch2(2 * k, 0);

    for (int i = 0; i < 100 * k; i++) {
      sketch1.update(static_cast<float>(i));
      sketch2.update(static_cast<float>(-i));
    }
    
    sketch1.merge(sketch2);
    REQUIRE(sketch1.get_n() == 200 * k);
    REQUIRE(sketch1.get_k() == k); // no reason to have shrunk
    REQUIRE(sketch1.get_min_value() == static_cast<float>(-100 * k + 1));
    REQUIRE(sketch1.get_max_value() == static_cast<float>(100 * k - 1));
    REQUIRE(sketch1.get_quantile(0.5) == Approx(0.0).margin(100 * k * RANK_EPS_FOR_K_128));
  }

  SECTION("merge: src estimation, tgt exact, equal k") {
    const uint16_t k = 128;
    quantiles_float_sketch sketch1(k, 0);
    quantiles_float_sketch sketch2(k, 0);

    for (int i = 0; i < k / 2; i++) {
      sketch1.update(static_cast<float>(i));
      sketch2.update(static_cast<float>(k - i - 1));
    }
    
    for (int i = k; i < 100 * k; i++) {
      sketch2.update(static_cast<float>(i));
    }

    sketch1.merge(sketch2);
    REQUIRE(sketch1.get_n() == 100 * k);
    REQUIRE(sketch1.get_k() == k);
    REQUIRE(sketch1.get_min_value() == 0.0f);
    REQUIRE(sketch1.get_max_value() == static_cast<float>(100 * k - 1));
    float n = 100 * k - 1;
    REQUIRE(sketch1.get_quantile(0.5) == Approx(n / 2).margin(n / 2 * RANK_EPS_FOR_K_128));
  }

  SECTION("merge: both estimation, no base buffer, same k") {
    const uint16_t k = 128;
    quantiles_float_sketch sketch1(k, 0);
    quantiles_float_sketch sketch2(k, 0);

    uint64_t n = 2 * k;
    for (uint64_t i = 0; i < n; i++) {
      sketch1.update(static_cast<float>(i));
      sketch2.update(static_cast<float>(2 * n - i - 1));
    }
    
    sketch1.merge(sketch2);
    REQUIRE(sketch1.get_n() == 2 * n);
    REQUIRE(sketch1.get_k() == k);
    REQUIRE(sketch1.get_min_value() == 0.0f);
    REQUIRE(sketch1.get_max_value() == static_cast<float>(2 * n - 1));
    REQUIRE(sketch1.get_quantile(0.5) == Approx(n).margin(n * RANK_EPS_FOR_K_128));
  }

  SECTION("merge: both estimation, no base buffer, tgt.k < src.k") {
    const uint16_t k = 128;
    quantiles_float_sketch sketch1(k, 0);
    quantiles_float_sketch sketch2(2 * k, 0);

    uint64_t n = 4 * k;
    for (uint64_t i = 0; i < n; i++) {
      sketch1.update(static_cast<float>(i));
      sketch2.update(static_cast<float>(2 * n - i - 1));
    }
    
    sketch1.merge(sketch2);
    REQUIRE(sketch1.get_n() == 2 * n);
    REQUIRE(sketch1.get_k() == k);
    REQUIRE(sketch1.get_min_value() == 0.0f);
    REQUIRE(sketch1.get_max_value() == static_cast<float>(2 * n - 1));
    REQUIRE(sketch1.get_quantile(0.5) == Approx(n).margin(n * RANK_EPS_FOR_K_128));
  }

  SECTION("sketch of ints") {
    quantiles_sketch<int> sketch;
    REQUIRE_THROWS_AS(sketch.get_quantile(0), std::runtime_error);
    REQUIRE_THROWS_AS(sketch.get_min_value(), std::runtime_error);
    REQUIRE_THROWS_AS(sketch.get_max_value(), std::runtime_error);

    const int n = 10000;
    for (int i = 0; i < n; i++) sketch.update(i);

    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    REQUIRE(static_cast<size_t>(s.tellp()) == sketch.get_serialized_size_bytes());
    auto sketch2 = quantiles_sketch<int>::deserialize(s);
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
    quantiles_string_sketch sketch1(128, 0);
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
    auto sketch2 = quantiles_string_sketch::deserialize(s, serde<std::string>(), test_allocator<std::string>(0));
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
    //std::ofstream os("quantiles-string.sk");
    //sketch1.serialize(os);
  }

  SECTION("sketch of strings bytes") {
    quantiles_string_sketch sketch1(128, 0);
    REQUIRE_THROWS_AS(sketch1.get_quantile(0), std::runtime_error);
    REQUIRE_THROWS_AS(sketch1.get_min_value(), std::runtime_error);
    REQUIRE_THROWS_AS(sketch1.get_max_value(), std::runtime_error);
    REQUIRE(sketch1.get_serialized_size_bytes() == 8);

    const int n = 10000;
    for (int i = 0; i < n; i++) sketch1.update(std::to_string(i));

    REQUIRE(sketch1.get_min_value() == std::string("0"));
    REQUIRE(sketch1.get_max_value() == std::string("9999"));

    auto bytes = sketch1.serialize();
    REQUIRE(bytes.size() == sketch1.get_serialized_size_bytes());
    auto sketch2 = quantiles_string_sketch::deserialize(bytes.data(), bytes.size(), serde<std::string>(), 0);
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
    quantiles_string_sketch sketch1(64, 0);
    sketch1.update("a");
    auto bytes = sketch1.serialize();
    REQUIRE(bytes.size() == sketch1.get_serialized_size_bytes());
    auto sketch2 = quantiles_string_sketch::deserialize(bytes.data(), bytes.size(), serde<std::string>(), 0);
    REQUIRE(bytes.size() == sketch2.get_serialized_size_bytes());
  }

  SECTION("copy") {
    quantiles_sketch<int> sketch1;
    const int n(1000);
    for (int i = 0; i < n; i++) sketch1.update(i);

    // copy constructor
    quantiles_sketch<int> sketch2(sketch1);
    for (int i = 0; i < n; i++) {
      REQUIRE(sketch2.get_rank(i) == sketch1.get_rank(i));
    }

    // copy assignment
    quantiles_sketch<int> sketch3;
    sketch3 = sketch1;
    for (int i = 0; i < n; i++) {
      REQUIRE(sketch3.get_rank(i) == sketch1.get_rank(i));
    }
  }

  SECTION("move") {
    quantiles_sketch<int> sketch1;
    const int n(100);
    for (int i = 0; i < n; i++) sketch1.update(i);

    // move constructor
    quantiles_sketch<int> sketch2(std::move(sketch1));
    for (int i = 0; i < n; i++) {
      REQUIRE(sketch2.get_rank(i) == (double) i / n);
    }

    // move assignment
    quantiles_sketch<int> sketch3;
    sketch3 = std::move(sketch2);
    for (int i = 0; i < n; i++) {
      REQUIRE(sketch3.get_rank(i) == (double) i / n);
    }
  }

  SECTION("Type converting copy constructor") {
    const uint16_t k = 8;
    const int n = 403;
    quantiles_sketch<double> sk_double(k);
    
    quantiles_sketch<float> sk_float(k, sk_double.get_allocator());
    REQUIRE(sk_float.is_empty());
    
    for (int i = 0; i < n; ++i) sk_double.update(i + .01);

    quantiles_sketch<int> sk_int(sk_double);
    REQUIRE(sk_double.get_n() == sk_int.get_n());
    REQUIRE(sk_double.get_k() == sk_int.get_k());
    REQUIRE(sk_double.get_num_retained() == sk_int.get_num_retained());

    auto sv_double = sk_double.get_sorted_view(false);
    std::vector<std::pair<double, uint64_t>> vec_double(sv_double.begin(), sv_double.end()); 

    auto sv_int = sk_int.get_sorted_view(false);
    std::vector<std::pair<int, uint64_t>> vec_int(sv_int.begin(), sv_int.end()); 

    REQUIRE(vec_double.size() == vec_int.size());
    
    for (size_t i = 0; i < vec_int.size(); ++i) {
      // known truncation with conversion so approximate result
      REQUIRE(vec_double[i].first == Approx(vec_int[i].first).margin(0.1));
      // exact equality for weights
      REQUIRE(vec_double[i].second == vec_int[i].second);
    }
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
    quantiles_sketch<A, less_A> sa;
    sa.update(1);
    sa.update(2);
    sa.update(3);

    quantiles_sketch<B, less_B> sb(sa);
    REQUIRE(sb.get_n() == 3);
  }

  // cleanup
  if (test_allocator_total_bytes != 0) {
    REQUIRE(test_allocator_total_bytes == 0);
  }
}

} /* namespace datasketches */

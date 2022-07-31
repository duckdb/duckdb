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

#include <req_sketch.hpp>

#include <fstream>
#include <sstream>
#include <limits>
#include <stdexcept>

namespace datasketches {

#ifdef TEST_BINARY_INPUT_PATH
const std::string input_path = TEST_BINARY_INPUT_PATH;
#else
const std::string input_path = "test/";
#endif

TEST_CASE("req sketch: empty", "[req_sketch]") {
  //std::cout << "sizeof(req_float_sketch)=" << sizeof(req_sketch<float>) << "\n";
  req_sketch<float> sketch(12);
  REQUIRE(sketch.get_k() == 12);
  REQUIRE(sketch.is_HRA());
  REQUIRE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_n() == 0);
  REQUIRE(sketch.get_num_retained() == 0);
  REQUIRE(std::isnan(sketch.get_rank(0)));
  REQUIRE(std::isnan(sketch.get_rank(std::numeric_limits<float>::infinity())));
  REQUIRE(std::isnan(sketch.get_min_value()));
  REQUIRE(std::isnan(sketch.get_max_value()));
  REQUIRE(std::isnan(sketch.get_quantile(0)));
  REQUIRE(std::isnan(sketch.get_quantile(0.5)));
  REQUIRE(std::isnan(sketch.get_quantile(1)));
  const double ranks[3] {0, 0.5, 1};
  REQUIRE(sketch.get_quantiles(ranks, 3).size() == 0);

  const float split_points[1] {0};
  REQUIRE(sketch.get_CDF(split_points, 1).empty());
  REQUIRE(sketch.get_PMF(split_points, 1).empty());
}

TEST_CASE("req sketch: single value, lra", "[req_sketch]") {
  req_sketch<float> sketch(12, false);
  sketch.update(1.0f);
  REQUIRE_FALSE(sketch.is_HRA());
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_n() == 1);
  REQUIRE(sketch.get_num_retained() == 1);
  REQUIRE(sketch.get_rank(1.0f) == 0);
  REQUIRE(sketch.get_rank<true>(1.0f) == 1);
  REQUIRE(sketch.get_rank(1.1f) == 1);
  REQUIRE(sketch.get_rank(std::numeric_limits<float>::infinity()) == 1);
  REQUIRE(sketch.get_quantile(0) == 1);
  REQUIRE(sketch.get_quantile(0.5) == 1);
  REQUIRE(sketch.get_quantile(1) == 1);

  const double ranks[3] {0, 0.5, 1};
  auto quantiles = sketch.get_quantiles(ranks, 3);
  REQUIRE(quantiles.size() == 3);
  REQUIRE(quantiles[0] == 1);
  REQUIRE(quantiles[1] == 1);
  REQUIRE(quantiles[2] == 1);

  unsigned count = 0;
  for (auto it: sketch) {
    REQUIRE(it.second == 1);
    ++count;
  }
  REQUIRE(count == 1);
}

TEST_CASE("req sketch: repeated values", "[req_sketch]") {
  req_sketch<float> sketch(12);
  sketch.update(1.0f);
  sketch.update(1.0f);
  sketch.update(1.0f);
  sketch.update(2.0f);
  sketch.update(2.0f);
  sketch.update(2.0f);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_n() == 6);
  REQUIRE(sketch.get_num_retained() == 6);
  REQUIRE(sketch.get_rank(1.0f) == 0);
  REQUIRE(sketch.get_rank<true>(1.0f) == 0.5);
  REQUIRE(sketch.get_rank(2.0f) == 0.5);
  REQUIRE(sketch.get_rank<true>(2.0f) == 1);
}

TEST_CASE("req sketch: exact mode", "[req_sketch]") {
  req_sketch<float> sketch(12);
  for (size_t i = 1; i <= 10; ++i) sketch.update(static_cast<float>(i));
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_n() == 10);
  REQUIRE(sketch.get_num_retained() == 10);

  // like KLL
  REQUIRE(sketch.get_rank(1.0f) == 0);
  REQUIRE(sketch.get_rank(2.0f) == 0.1);
  REQUIRE(sketch.get_rank(6.0f) == 0.5);
  REQUIRE(sketch.get_rank(9.0f) == 0.8);
  REQUIRE(sketch.get_rank(10.0f) == 0.9);

  // inclusive
  REQUIRE(sketch.get_rank<true>(1.0f) == 0.1);
  REQUIRE(sketch.get_rank<true>(2.0f) == 0.2);
  REQUIRE(sketch.get_rank<true>(5.0f) == 0.5);
  REQUIRE(sketch.get_rank<true>(9.0f) == 0.9);
  REQUIRE(sketch.get_rank<true>(10.0f) == 1);

  // like KLL
  REQUIRE(sketch.get_quantile(0) == 1);
  REQUIRE(sketch.get_quantile(0.1) == 2);
  REQUIRE(sketch.get_quantile(0.5) == 6);
  REQUIRE(sketch.get_quantile(0.9) == 10);
  REQUIRE(sketch.get_quantile(1) == 10);

  // inclusive
  REQUIRE(sketch.get_quantile<true>(0) == 1);
  REQUIRE(sketch.get_quantile<true>(0.1) == 1);
  REQUIRE(sketch.get_quantile<true>(0.5) == 5);
  REQUIRE(sketch.get_quantile<true>(0.9) == 9);
  REQUIRE(sketch.get_quantile<true>(1) == 10);

  const double ranks[3] {0, 0.5, 1};
  auto quantiles = sketch.get_quantiles(ranks, 3);
  REQUIRE(quantiles.size() == 3);
  REQUIRE(quantiles[0] == 1);
  REQUIRE(quantiles[1] == 6);
  REQUIRE(quantiles[2] == 10);

  const float splits[3] {2, 6, 9};
  auto cdf = sketch.get_CDF(splits, 3);
  REQUIRE(cdf[0] == 0.1);
  REQUIRE(cdf[1] == 0.5);
  REQUIRE(cdf[2] == 0.8);
  REQUIRE(cdf[3] == 1);
  auto pmf = sketch.get_PMF(splits, 3);
  REQUIRE(pmf[0] == Approx(0.1).margin(1e-8));
  REQUIRE(pmf[1] == Approx(0.4).margin(1e-8));
  REQUIRE(pmf[2] == Approx(0.3).margin(1e-8));
  REQUIRE(pmf[3] == Approx(0.2).margin(1e-8));

  REQUIRE(sketch.get_rank_lower_bound(0.5, 1) == 0.5);
  REQUIRE(sketch.get_rank_upper_bound(0.5, 1) == 0.5);
}

TEST_CASE("req sketch: estimation mode", "[req_sketch]") {
  req_sketch<float> sketch(12);
  const size_t n = 100000;
  for (size_t i = 0; i < n; ++i) sketch.update(static_cast<float>(i));
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_n() == n);
//  std::cout << sketch.to_string(true);
  REQUIRE(sketch.get_num_retained() < n);
  REQUIRE(sketch.get_rank(0) == 0);
  REQUIRE(sketch.get_rank(static_cast<float>(n)) == 1);
  REQUIRE(sketch.get_rank(n / 2.0f) == Approx(0.5).margin(0.01));
  REQUIRE(sketch.get_rank(n - 1.0f) == Approx(1).margin(0.01));
  REQUIRE(sketch.get_min_value() == 0);
  REQUIRE(sketch.get_max_value() == n - 1);
  REQUIRE(sketch.get_rank_lower_bound(0.5, 1) < 0.5);
  REQUIRE(sketch.get_rank_upper_bound(0.5, 1) > 0.5);

  unsigned count = 0;
  for (auto it: sketch) {
    REQUIRE(it.second >= 1);
    ++count;
  }
  REQUIRE(count == sketch.get_num_retained());
}

TEST_CASE("req sketch: stream serialize-deserialize empty", "[req_sketch]") {
  req_sketch<float> sketch(12);

  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  sketch.serialize(s);
  auto sketch2 = req_sketch<float>::deserialize(s);
  REQUIRE(s.tellg() == s.tellp());
  REQUIRE(sketch2.is_empty() == sketch.is_empty());
  REQUIRE(sketch2.is_estimation_mode() == sketch.is_estimation_mode());
  REQUIRE(sketch2.get_num_retained() == sketch.get_num_retained());
  REQUIRE(sketch2.get_n() == sketch.get_n());
  REQUIRE(std::isnan(sketch2.get_min_value()));
  REQUIRE(std::isnan(sketch2.get_max_value()));
}

TEST_CASE("req sketch: byte serialize-deserialize empty", "[req_sketch]") {
  req_sketch<float> sketch(12);

  auto bytes = sketch.serialize();
  REQUIRE(bytes.size() == sketch.get_serialized_size_bytes());
  auto sketch2 = req_sketch<float>::deserialize(bytes.data(), bytes.size());
  REQUIRE(bytes.size() == sketch2.get_serialized_size_bytes());
  REQUIRE(sketch2.is_empty() == sketch.is_empty());
  REQUIRE(sketch2.is_estimation_mode() == sketch.is_estimation_mode());
  REQUIRE(sketch2.get_num_retained() == sketch.get_num_retained());
  REQUIRE(sketch2.get_n() == sketch.get_n());
  REQUIRE(std::isnan(sketch2.get_min_value()));
  REQUIRE(std::isnan(sketch2.get_max_value()));
}

TEST_CASE("req sketch: stream serialize-deserialize single item", "[req_sketch]") {
  req_sketch<float> sketch(12);
  sketch.update(1.0f);

  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  sketch.serialize(s);
  auto sketch2 = req_sketch<float>::deserialize(s);
  REQUIRE(s.tellg() == s.tellp());
  REQUIRE(sketch2.is_empty() == sketch.is_empty());
  REQUIRE(sketch2.is_estimation_mode() == sketch.is_estimation_mode());
  REQUIRE(sketch2.get_num_retained() == sketch.get_num_retained());
  REQUIRE(sketch2.get_n() == sketch.get_n());
  REQUIRE(sketch2.get_min_value() == sketch.get_min_value());
  REQUIRE(sketch2.get_max_value() == sketch.get_max_value());
}

TEST_CASE("req sketch: byte serialize-deserialize single item", "[req_sketch]") {
  req_sketch<float> sketch(12);
  sketch.update(1.0f);

  auto bytes = sketch.serialize();
  REQUIRE(bytes.size() == sketch.get_serialized_size_bytes());
  auto sketch2 = req_sketch<float>::deserialize(bytes.data(), bytes.size());
  //std::cout << sketch2.to_string(true);
  REQUIRE(bytes.size() == sketch2.get_serialized_size_bytes());
  REQUIRE(sketch2.is_empty() == sketch.is_empty());
  REQUIRE(sketch2.is_estimation_mode() == sketch.is_estimation_mode());
  REQUIRE(sketch2.get_num_retained() == sketch.get_num_retained());
  REQUIRE(sketch2.get_n() == sketch.get_n());
  REQUIRE(sketch2.get_min_value() == sketch.get_min_value());
  REQUIRE(sketch2.get_max_value() == sketch.get_max_value());
}

TEST_CASE("req sketch: stream serialize-deserialize exact mode", "[req_sketch]") {
  req_sketch<float> sketch(12);
  const size_t n = 50;
  for (size_t i = 0; i < n; ++i) sketch.update(static_cast<float>(i));
  REQUIRE_FALSE(sketch.is_estimation_mode());

  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  sketch.serialize(s);
  auto sketch2 = req_sketch<float>::deserialize(s);
  REQUIRE(s.tellg() == s.tellp());
  REQUIRE(sketch2.is_empty() == sketch.is_empty());
  REQUIRE(sketch2.is_estimation_mode() == sketch.is_estimation_mode());
  REQUIRE(sketch2.get_num_retained() == sketch.get_num_retained());
  REQUIRE(sketch2.get_n() == sketch.get_n());
  REQUIRE(sketch2.get_min_value() == sketch.get_min_value());
  REQUIRE(sketch2.get_max_value() == sketch.get_max_value());
}

TEST_CASE("req sketch: byte serialize-deserialize exact mode", "[req_sketch]") {
  req_sketch<float> sketch(12);
  const size_t n = 50;
  for (size_t i = 0; i < n; ++i) sketch.update(static_cast<float>(i));
  REQUIRE_FALSE(sketch.is_estimation_mode());

  auto bytes = sketch.serialize();
  REQUIRE(bytes.size() == sketch.get_serialized_size_bytes());
  auto sketch2 = req_sketch<float>::deserialize(bytes.data(), bytes.size());
  //std::cout << sketch2.to_string(true);
  REQUIRE(bytes.size() == sketch2.get_serialized_size_bytes());
  REQUIRE(sketch2.is_empty() == sketch.is_empty());
  REQUIRE(sketch2.is_estimation_mode() == sketch.is_estimation_mode());
  REQUIRE(sketch2.get_num_retained() == sketch.get_num_retained());
  REQUIRE(sketch2.get_n() == sketch.get_n());
  REQUIRE(sketch2.get_min_value() == sketch.get_min_value());
  REQUIRE(sketch2.get_max_value() == sketch.get_max_value());
}

TEST_CASE("req sketch: stream serialize-deserialize estimation mode", "[req_sketch]") {
  req_sketch<float> sketch(12);
  const size_t n = 100000;
  for (size_t i = 0; i < n; ++i) sketch.update(static_cast<float>(i));
  REQUIRE(sketch.is_estimation_mode());

  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  sketch.serialize(s);
  auto sketch2 = req_sketch<float>::deserialize(s);
  REQUIRE(s.tellg() == s.tellp());
  REQUIRE(sketch2.is_empty() == sketch.is_empty());
  REQUIRE(sketch2.is_estimation_mode() == sketch.is_estimation_mode());
  REQUIRE(sketch2.get_num_retained() == sketch.get_num_retained());
  REQUIRE(sketch2.get_n() == sketch.get_n());
  REQUIRE(sketch2.get_min_value() == sketch.get_min_value());
  REQUIRE(sketch2.get_max_value() == sketch.get_max_value());
}

TEST_CASE("req sketch: byte serialize-deserialize estimation mode", "[req_sketch]") {
  req_sketch<float> sketch(12);
  const size_t n = 100000;
  for (size_t i = 0; i < n; ++i) sketch.update(static_cast<float>(i));
  REQUIRE(sketch.is_estimation_mode());

  auto bytes = sketch.serialize();
  REQUIRE(bytes.size() == sketch.get_serialized_size_bytes());
  auto sketch2 = req_sketch<float>::deserialize(bytes.data(), bytes.size());
  REQUIRE(bytes.size() == sketch2.get_serialized_size_bytes());
  REQUIRE(sketch2.is_empty() == sketch.is_empty());
  REQUIRE(sketch2.is_estimation_mode() == sketch.is_estimation_mode());
  REQUIRE(sketch2.get_num_retained() == sketch.get_num_retained());
  REQUIRE(sketch2.get_n() == sketch.get_n());
  REQUIRE(sketch2.get_min_value() == sketch.get_min_value());
  REQUIRE(sketch2.get_max_value() == sketch.get_max_value());
}

TEST_CASE("req sketch: serialize deserialize stream and bytes equivalence", "[req_sketch]") {
  req_sketch<float> sketch(12);
  const size_t n = 100000;
  for (size_t i = 0; i < n; ++i) sketch.update(static_cast<float>(i));
  REQUIRE(sketch.is_estimation_mode());

  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  sketch.serialize(s);
  auto bytes = sketch.serialize();
  REQUIRE(bytes.size() == static_cast<size_t>(s.tellp()));
  for (size_t i = 0; i < bytes.size(); ++i) {
    REQUIRE(((char*)bytes.data())[i] == (char)s.get());
  }

  s.seekg(0); // rewind
  auto sketch1 = req_sketch<float>::deserialize(s);
  auto sketch2 = req_sketch<float>::deserialize(bytes.data(), bytes.size());
  REQUIRE(bytes.size() == static_cast<size_t>(s.tellg()));
  REQUIRE(sketch2.is_empty() == sketch1.is_empty());
  REQUIRE(sketch2.is_estimation_mode() == sketch.is_estimation_mode());
  REQUIRE(sketch2.get_num_retained() == sketch.get_num_retained());
  REQUIRE(sketch2.get_n() == sketch.get_n());
  REQUIRE(sketch2.get_min_value() == sketch.get_min_value());
  REQUIRE(sketch2.get_max_value() == sketch.get_max_value());
}

TEST_CASE("req sketch: stream deserialize from Java - empty", "[req_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(input_path + "req_float_empty_from_java.sk", std::ios::binary);
  auto sketch = req_sketch<float>::deserialize(is);
  REQUIRE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_n() == 0);
  REQUIRE(sketch.get_num_retained() == 0);
  REQUIRE(std::isnan(sketch.get_min_value()));
  REQUIRE(std::isnan(sketch.get_max_value()));
}

TEST_CASE("req sketch: stream deserialize from Java - single item", "[req_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(input_path + "req_float_single_item_from_java.sk", std::ios::binary);
  auto sketch = req_sketch<float>::deserialize(is);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_n() == 1);
  REQUIRE(sketch.get_num_retained() == 1);
  REQUIRE(sketch.get_min_value() == 1);
  REQUIRE(sketch.get_max_value() == 1);
  REQUIRE(sketch.get_rank(1.0f) == 0);
  REQUIRE(sketch.get_rank<true>(1.0f) == 1);
}

TEST_CASE("req sketch: stream deserialize from Java - raw items", "[req_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(input_path + "req_float_raw_items_from_java.sk", std::ios::binary);
  auto sketch = req_sketch<float>::deserialize(is);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_n() == 4);
  REQUIRE(sketch.get_num_retained() == 4);
  REQUIRE(sketch.get_min_value() == 0);
  REQUIRE(sketch.get_max_value() == 3);
  REQUIRE(sketch.get_rank(2.0f) == 0.5);
}

TEST_CASE("req sketch: stream deserialize from Java - exact mode", "[req_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(input_path + "req_float_exact_from_java.sk", std::ios::binary);
  auto sketch = req_sketch<float>::deserialize(is);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_n() == 100);
  REQUIRE(sketch.get_num_retained() == 100);
  REQUIRE(sketch.get_min_value() == 0);
  REQUIRE(sketch.get_max_value() == 99);
  REQUIRE(sketch.get_rank(50.0f) == 0.5);
}

TEST_CASE("req sketch: stream deserialize from Java - estimation mode", "[req_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(input_path + "req_float_estimation_from_java.sk", std::ios::binary);
  auto sketch = req_sketch<float>::deserialize(is);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_n() == 10000);
  REQUIRE(sketch.get_num_retained() == 2942);
  REQUIRE(sketch.get_min_value() == 0);
  REQUIRE(sketch.get_max_value() == 9999);
  REQUIRE(sketch.get_rank(5000.0f) == 0.5);
}

TEST_CASE("req sketch: merge into empty", "[req_sketch]") {
  req_sketch<float> sketch1(40);

  req_sketch<float> sketch2(40);
  for (size_t i = 0; i < 1000; ++i) sketch2.update(static_cast<float>(i));

  sketch1.merge(sketch2);
  REQUIRE(sketch1.get_min_value() == 0);
  REQUIRE(sketch1.get_max_value() == 999);
  REQUIRE(sketch1.get_quantile(0.25) == Approx(250).margin(3));
  REQUIRE(sketch1.get_quantile(0.5) == Approx(500).margin(3));
  REQUIRE(sketch1.get_quantile(0.75) == Approx(750).margin(3));
  REQUIRE(sketch1.get_rank(500.0f) == Approx(0.5).margin(0.01));
}

TEST_CASE("req sketch: merge", "[req_sketch]") {
  req_sketch<float> sketch1(100);
  for (size_t i = 0; i < 1000; ++i) sketch1.update(static_cast<float>(i));

  req_sketch<float> sketch2(100);
  for (size_t i = 1000; i < 2000; ++i) sketch2.update(static_cast<float>(i));

  sketch1.merge(sketch2);
  REQUIRE(sketch1.get_min_value() == 0);
  REQUIRE(sketch1.get_max_value() == 1999);
  REQUIRE(sketch1.get_quantile(0.25) == Approx(500).margin(3));
  REQUIRE(sketch1.get_quantile(0.5) == Approx(1000).margin(1));
  REQUIRE(sketch1.get_quantile(0.75) == Approx(1500).margin(1));
  REQUIRE(sketch1.get_rank(1000.0f) == Approx(0.5).margin(0.01));
}

TEST_CASE("req sketch: merge multiple", "[req_sketch]") {
  req_sketch<float> sketch1(12);
  for (size_t i = 0; i < 40; ++i) sketch1.update(static_cast<float>(i));

  req_sketch<float> sketch2(12);
  for (size_t i = 40; i < 80; ++i) sketch2.update(static_cast<float>(i));

  req_sketch<float> sketch3(12);
  for (size_t i = 80; i < 120; ++i) sketch3.update(static_cast<float>(i));

  req_sketch<float> sketch(12);
  sketch.merge(sketch1);
  sketch.merge(sketch2);
  sketch.merge(sketch3);
  REQUIRE(sketch.get_min_value() == 0);
  REQUIRE(sketch.get_max_value() == 119);
  REQUIRE(sketch.get_quantile(0.5) == Approx(60).margin(3));
  REQUIRE(sketch.get_rank(60.0f) == Approx(0.5).margin(0.01));
}

TEST_CASE("req sketch: merge incompatible HRA and LRA", "[req_sketch]") {
  req_sketch<float> sketch1(12);
  sketch1.update(1.0f);

  req_sketch<float> sketch2(12, false);
  sketch2.update(1.0f);

  REQUIRE_THROWS_AS(sketch1.merge(sketch2), std::invalid_argument);
}

TEST_CASE("req sketch: type conversion - empty", "[req_sketch]") {
  req_sketch<double> req_double(12);
  req_sketch<float> req_float(req_double);
  REQUIRE(req_float.is_empty());
  REQUIRE(req_float.get_k() == req_double.get_k());
  REQUIRE(req_float.get_n() == 0);
  REQUIRE(req_float.get_num_retained() == 0);
}

TEST_CASE("req sketch: type conversion - several levels", "[req_sketch]") {
  req_sketch<double> req_double(12);
  for (int i = 0; i < 1000; ++i) req_double.update(static_cast<double>(i));
  req_sketch<float> req_float(req_double);
  REQUIRE(!req_float.is_empty());
  REQUIRE(req_float.get_k() == req_double.get_k());
  REQUIRE(req_float.get_n() == req_double.get_n());
  REQUIRE(req_float.get_num_retained() == req_double.get_num_retained());

  auto sv_float = req_float.get_sorted_view(false);
  auto sv_double = req_double.get_sorted_view(false);
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

TEST_CASE("req sketch: type conversion - custom types") {
  req_sketch<A, less_A> sa(4);
  sa.update(1);
  sa.update(2);
  sa.update(3);

  req_sketch<B, less_B> sb(sa);
  REQUIRE(sb.get_n() == 3);
}

//TEST_CASE("for manual comparison with Java") {
//  req_sketch<float> sketch(12, false);
//  for (size_t i = 0; i < 100000; ++i) sketch.update(i);
//  sketch.merge(sketch);
//  std::ofstream os;
//  os.exceptions(std::ios::failbit | std::ios::badbit);
//  os.open("req_float_lra_12_100000_merged.sk", std::ios::binary);
//  sketch.get_quantile(0.5); // force sorting level 0
//  sketch.serialize(os);
//}

} /* namespace datasketches */

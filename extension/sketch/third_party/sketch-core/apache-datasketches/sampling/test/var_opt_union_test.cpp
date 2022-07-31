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

#include <var_opt_union.hpp>

#include <catch.hpp>

#include <vector>
#include <string>
#include <sstream>
#include <fstream>
#include <cmath>
#include <random>
#include <stdexcept>

#ifdef TEST_BINARY_INPUT_PATH
static std::string testBinaryInputPath = TEST_BINARY_INPUT_PATH;
#else
static std::string testBinaryInputPath = "test/";
#endif

namespace datasketches {
  
static constexpr double EPS = 1e-13;

static var_opt_sketch<int> create_unweighted_sketch(uint32_t k, uint64_t n) {
  var_opt_sketch<int> sk(k);
  for (uint64_t i = 0; i < n; ++i) {
    sk.update(static_cast<int>(i), 1.0);
  }
  return sk;
}

// if exact_compare = false, checks for equivalence -- specific R region values may differ but
// R region weights must match
template<typename T, typename S, typename A>
static void check_if_equal(var_opt_sketch<T,S,A>& sk1, var_opt_sketch<T,S,A>& sk2, bool exact_compare = true) {
  REQUIRE(sk1.get_k() == sk2.get_k());
  REQUIRE(sk1.get_n() == sk2.get_n());
  REQUIRE(sk1.get_num_samples() == sk2.get_num_samples());
      
  auto it1 = sk1.begin();
  auto it2 = sk2.begin();
  size_t i = 0;

  while ((it1 != sk1.end()) && (it2 != sk2.end())) {
    const std::pair<const T&, const double> p1 = *it1;
    const std::pair<const T&, const double> p2 = *it2;
    if (exact_compare) {
      REQUIRE(p1.first == p2.first); // data values
    }
    REQUIRE(p1.second == p2.second); // weight values
    ++i;
    ++it1;
    ++it2;
  }

  REQUIRE((it1 == sk1.end() && it2 == sk2.end())); // iterators must end at the same time
}

// compare serialization and deserialization results, checking string and stream methods to
// ensure that the resulting binary images are compatible.
// if exact_compare = false, checks for equivalence -- specific R region values may differ but
// R region weights must match
template<typename T, typename S, typename A>
static void compare_serialization_deserialization(var_opt_union<T,S,A>& vo_union, bool exact_compare = true) {
  std::vector<uint8_t> bytes = vo_union.serialize();

  var_opt_union<T> u_from_bytes = var_opt_union<T>::deserialize(bytes.data(), bytes.size());
  var_opt_sketch<T> sk1 = vo_union.get_result();
  var_opt_sketch<T> sk2 = u_from_bytes.get_result();
  check_if_equal(sk1, sk2, exact_compare);

  std::string str(bytes.begin(), bytes.end());
  std::stringstream ss;
  ss.str(str);

  var_opt_union<T> u_from_stream = var_opt_union<T>::deserialize(ss);
  sk2 = u_from_stream.get_result();
  check_if_equal(sk1, sk2, exact_compare);

  ss.seekg(0); // didn't put anything so only reset read position
  vo_union.serialize(ss);
  u_from_stream = var_opt_union<T>::deserialize(ss);
  sk2 = u_from_stream.get_result();
  check_if_equal(sk1, sk2, exact_compare);

  std::string str_from_stream = ss.str();
  var_opt_union<T> u_from_str = var_opt_union<T>::deserialize(str_from_stream.c_str(), str_from_stream.size());
  sk2 = u_from_str.get_result();
  check_if_equal(sk1, sk2, exact_compare);

  // check truncated input, too
  REQUIRE_THROWS_AS(var_opt_union<T>::deserialize(bytes.data(), bytes.size() - 5), std::out_of_range);
  std::string str_trunc((char*)&bytes[0], bytes.size() - 5);
  ss.str(str_trunc);
  // next line may throw either std::illegal_argument or std::runtime_exception
  REQUIRE_THROWS_AS(var_opt_union<T>::deserialize(ss), std::exception);
}

TEST_CASE("varopt union: bad prelongs", "[var_opt_union]") {
  var_opt_sketch<int> sk = create_unweighted_sketch(32, 33);
  var_opt_union<int> u(32);
  u.update(sk);
  std::vector<uint8_t> bytes = u.serialize();

  bytes[0] = 0; // corrupt the preamble longs byte to be too small
  REQUIRE_THROWS_AS(var_opt_union<int>::deserialize(bytes.data(), bytes.size()), std::invalid_argument);

  // create a stringstream to check the same
  std::stringstream ss;
  std::string str(bytes.begin(), bytes.end());
  ss.str(str);
  REQUIRE_THROWS_AS(var_opt_union<int>::deserialize(ss), std::invalid_argument);
}

TEST_CASE("varopt union: bad serialization version", "[var_opt_union]") {
  var_opt_sketch<int> sk = create_unweighted_sketch(16, 16);
  var_opt_union<int> u(32);
  u.update(sk);
  std::vector<uint8_t> bytes = u.serialize();
  bytes[1] = 0; // corrupt the serialization version byte

  REQUIRE_THROWS_AS(var_opt_union<int>::deserialize(bytes.data(), bytes.size()), std::invalid_argument);

  // create a stringstream to check the same
  std::stringstream ss;
  std::string str(bytes.begin(), bytes.end());
  ss.str(str);
  REQUIRE_THROWS_AS(var_opt_union<int>::deserialize(ss), std::invalid_argument);
}

TEST_CASE("varopt union: invalid k", "[var_opt_union]") {
  REQUIRE_THROWS_AS(var_opt_union<int>(0), std::invalid_argument);
  REQUIRE_THROWS_AS(var_opt_union<int>(1U << 31), std::invalid_argument);
}

TEST_CASE("varopt union: bad family", "[var_opt_union]") {
  var_opt_sketch<int> sk = create_unweighted_sketch(16, 16);
  var_opt_union<int> u(15);
  u.update(sk);
  std::vector<uint8_t> bytes = u.serialize();
  bytes[2] = 0; // corrupt the family byte

  REQUIRE_THROWS_AS(var_opt_union<int>::deserialize(bytes.data(), bytes.size()), std::invalid_argument);

  std::stringstream ss;
  std::string str(bytes.begin(), bytes.end());
  ss.str(str);
  REQUIRE_THROWS_AS(var_opt_union<int>::deserialize(ss), std::invalid_argument);
}

TEST_CASE("varopt union: empty union", "[var_opt_union]") {
  uint32_t k = 2048;
  var_opt_sketch<std::string> sk(k);
  var_opt_union<std::string> u(k);
  u.update(sk);

  var_opt_sketch<std::string> result = u.get_result();
  REQUIRE(result.is_empty());
  REQUIRE(result.get_n() == 0);
  REQUIRE(result.get_num_samples() == 0);
  REQUIRE(result.get_k() == k);
}

TEST_CASE("varopt union: two exact sketches", "[var_opt_union]") {
  int n = 4; // 2n < k
  uint32_t k = 10;
  var_opt_sketch<int> sk1(k), sk2(k);

  for (int i = 1; i <= n; ++i) {
    sk1.update(i, static_cast<double>(i));
    sk2.update(-i, static_cast<double>(i));
  }

  var_opt_union<int> u(k);
  u.update(sk1);
  u.update(sk2);

  var_opt_sketch<int> result = u.get_result();
  REQUIRE(result.get_n() == 2ULL * n);
  REQUIRE(result.get_k() == k);
}

TEST_CASE("varopt union: heavy sampling sketch", "[var_opt_union]") {
  uint64_t n1 = 20;
  uint32_t k1 = 10;
  uint64_t n2 = 6;
  uint32_t k2 = 5;
  var_opt_sketch<int64_t> sk1(k1), sk2(k2);
  for (uint64_t i = 1; i <= n1; ++i) {
    sk1.update(i, static_cast<double>(i));
  }

  for (uint64_t i = 1; i < n2; ++i) { // we'll add a very heavy one later
    sk2.update(-1 * static_cast<int64_t>(i), i + 1000.0);
  }
  sk2.update(-1 * static_cast<int64_t>(n2), 1000000.0);

  var_opt_union<int64_t> u(k1);
  u.update(sk1);
  u.update(sk2);

  var_opt_sketch<int64_t> result = u.get_result();
  REQUIRE(result.get_n() == n1 + n2);
  REQUIRE(result.get_k() == k2); // heavy enough the result pulls back to k2

  u.reset();
  result = u.get_result();
  REQUIRE(result.get_n() == 0);
  REQUIRE(result.get_k() == k1); // union reset so empty result reflects max_k
}

TEST_CASE("varopt union: identical sampling sketches", "[var_opt_union]") {
  uint32_t k = 20;
  uint64_t n = 50;
  var_opt_sketch<int> sk = create_unweighted_sketch(k, n);

  var_opt_union<int> u(k);
  u.update(sk);
  u.update(sk);

  var_opt_sketch<int> result = u.get_result();
  double expected_wt = 2.0 * n;
  subset_summary ss = result.estimate_subset_sum([](int){return true;});
  REQUIRE(result.get_n() == 2 * n);
  REQUIRE(ss.total_sketch_weight == Approx(expected_wt).margin(EPS));

  // add another sketch, such that sketch_tau < outer_tau
  sk = create_unweighted_sketch(k, k + 1); // tau = (k + 1) / k
  u.update(sk);
  result = u.get_result();
  expected_wt = (2.0 * n) + k + 1;
  ss = result.estimate_subset_sum([](int){return true;});
  REQUIRE(result.get_n() == (2 * n) + k + 1);
  REQUIRE(ss.total_sketch_weight == Approx(expected_wt).margin(EPS));
}

TEST_CASE("varopt union: small sampling sketch", "[var_opt_union]") {
  uint32_t k_small = 16;
  uint32_t k_max = 128;
  uint64_t n1 = 32;
  uint64_t n2 = 64;

  var_opt_sketch<float> sk(k_small);
  for (uint64_t i = 0; i < n1; ++i) { sk.update(static_cast<float>(i)); }
  sk.update(-1.0f, static_cast<double>(n1 * n1)); // add a heavy item

  var_opt_union<float> u(k_max);
  u.update(sk);

  // another one, but different n to get a different per-item weight
  var_opt_sketch<float> sk2(k_small);
  for (uint64_t i = 0; i < n2; ++i) { sk2.update(static_cast<float>(i)); }
  u.update(sk2);

  // should trigger migrate_marked_items_by_decreasing_k()
  var_opt_sketch<float> result = u.get_result();
  REQUIRE(result.get_n() == n1 + n2 + 1);
  
  double expected_wt = 1.0 * (n1 + n2); // n1 + n2 light items, ignore the heavy one
  subset_summary ss = result.estimate_subset_sum([](float x){return x >= 0;});
  REQUIRE(ss.estimate == Approx(expected_wt).margin(EPS));
  REQUIRE(ss.total_sketch_weight == Approx(expected_wt + (n1 * n1)).margin(EPS));
  REQUIRE(result.get_k() < k_max);

  // check that mark information is preserved as expected
  compare_serialization_deserialization(u, false);
}

TEST_CASE("varopt union: serialize empty", "[var_opt_union]") {
  var_opt_union<std::string> u(100);
  compare_serialization_deserialization(u);
}

TEST_CASE("varopt union: serialize exact", "[var_opt_union]") {
  uint32_t k = 100;
  var_opt_union<int> u(k);
  var_opt_sketch<int> sk = create_unweighted_sketch(k, k / 2);
  u.update(sk);

  compare_serialization_deserialization(u);
}

TEST_CASE("varopt union: serialize sampling", "[var_opt_union]") {
  uint32_t k = 100;
  var_opt_union<int> u(k);
  var_opt_sketch<int> sk = create_unweighted_sketch(k, 2 * k);
  u.update(sk);

  compare_serialization_deserialization(u);
}

TEST_CASE("varopt union: deserialize from java", "[var_opt_union]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(testBinaryInputPath + "varopt_union_double_sampling.sk", std::ios::binary);
  var_opt_union<double> u = var_opt_union<double>::deserialize(is);
    
  // must reduce k in the process, like in small_sampling_sketch()
  var_opt_sketch<double> result = u.get_result();
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.get_n() == 97);
  
  double expected_wt = 96.0;// light items -- ignoring the heavy one
  subset_summary ss = result.estimate_subset_sum([](double x){return x >= 0;});
  REQUIRE(ss.estimate == Approx(expected_wt).margin(EPS));
  REQUIRE(ss.total_sketch_weight == Approx(expected_wt + 1024.0).margin(EPS));
  REQUIRE(result.get_k() < 128);
}

}

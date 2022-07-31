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

#include <var_opt_sketch.hpp>

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

template<typename T, typename S, typename A>
static void check_if_equal(var_opt_sketch<T,S,A>& sk1, var_opt_sketch<T,S,A>& sk2) {
  REQUIRE(sk1.get_k() == sk2.get_k());
  REQUIRE(sk1.get_n() == sk2.get_n());
  REQUIRE(sk1.get_num_samples() == sk2.get_num_samples());
      
  auto it1 = sk1.begin();
  auto it2 = sk2.begin();
  size_t i = 0;

  while ((it1 != sk1.end()) && (it2 != sk2.end())) {
    const std::pair<const T&, const double> p1 = *it1;
    const std::pair<const T&, const double> p2 = *it2;
    REQUIRE(p1.first == p2.first);   // data values
    REQUIRE(p1.second == p2.second); // weights
    ++i;
    ++it1;
    ++it2;
  }

  REQUIRE((it1 == sk1.end() && it2 == sk2.end())); // iterators must end at the same time
}

TEST_CASE("varopt sketch: invalid k", "[var_opt_sketch]") {
  REQUIRE_THROWS_AS(var_opt_sketch<int>(0), std::invalid_argument);
  REQUIRE_THROWS_AS(var_opt_sketch<int>(1U << 31), std::invalid_argument); // aka k < 0
}

TEST_CASE("varopt sketch: bad serialization version", "[var_opt_sketch]") {
  var_opt_sketch<int> sk = create_unweighted_sketch(16, 16);
  std::vector<uint8_t> bytes = sk.serialize();
  bytes[1] = 0; // corrupt the serialization version byte

  REQUIRE_THROWS_AS(var_opt_sketch<int>::deserialize(bytes.data(), bytes.size()), std::invalid_argument);

  // create a stringstream to check the same
  std::stringstream ss;
  std::string str(bytes.begin(), bytes.end());
  ss.str(str);
  REQUIRE_THROWS_AS(var_opt_sketch<int>::deserialize(ss), std::invalid_argument);
}

TEST_CASE("varopt sketch: bad family", "[var_opt_sketch]") {
  var_opt_sketch<int> sk = create_unweighted_sketch(16, 16);
  std::vector<uint8_t> bytes = sk.serialize();
  bytes[2] = 0; // corrupt the family byte

  REQUIRE_THROWS_AS(var_opt_sketch<int>::deserialize(bytes.data(), bytes.size()), std::invalid_argument);

  // create a stringstream to check the same
  std::stringstream ss;
  std::string str(bytes.begin(), bytes.end());
  ss.str(str);
  REQUIRE_THROWS_AS(var_opt_sketch<int>::deserialize(ss), std::invalid_argument);
}

TEST_CASE("varopt sketch: bad prelongs", "[var_opt_sketch]") {
  // The nubmer of preamble longs shares bits with resize_factor, but the latter
  // has no invalid values as it gets 2 bites for 4 enum values.
  var_opt_sketch<int> sk = create_unweighted_sketch(32, 33);
  std::vector<uint8_t> bytes = sk.serialize();

  bytes[0] = 0; // corrupt the preamble longs byte to be too small
  REQUIRE_THROWS_AS(var_opt_sketch<int>::deserialize(bytes.data(), bytes.size()), std::invalid_argument);

  bytes[0] = 2; // corrupt the preamble longs byte to 2
  REQUIRE_THROWS_AS(var_opt_sketch<int>::deserialize(bytes.data(), bytes.size()), std::invalid_argument);

  bytes[0] = 5; // corrupt the preamble longs byte to be too large
  REQUIRE_THROWS_AS(var_opt_sketch<int>::deserialize(bytes.data(), bytes.size()), std::invalid_argument);
}

TEST_CASE("varopt sketch: malformed preamble", "[var_opt_sketch]") {
  uint32_t k = 50;
  var_opt_sketch<int> sk = create_unweighted_sketch(k, k);
  const std::vector<uint8_t> src_bytes = sk.serialize();

  // we'll re-use the same bytes several times so we'll use copies
  std::vector<uint8_t> bytes(src_bytes);

  // no items in R, but preamble longs indicates full
  bytes[0] = 4; // PREAMBLE_LONGS_FULL
  REQUIRE_THROWS_AS(var_opt_sketch<int>::deserialize(bytes.data(), bytes.size()), std::invalid_argument);

  // k = 0
  bytes = src_bytes; 
  *reinterpret_cast<int32_t*>(&bytes[4]) = 0;
  REQUIRE_THROWS_AS(var_opt_sketch<int>::deserialize(bytes.data(), bytes.size()), std::invalid_argument);

  // negative H region count in Java (signed ints)
  // throws due to H count != n in exact mode
  bytes = src_bytes; 
  *reinterpret_cast<int32_t*>(&bytes[16]) = -1;
  REQUIRE_THROWS_AS(var_opt_sketch<int>::deserialize(bytes.data(), bytes.size()), std::invalid_argument);

  // negative R region count in Java (signed ints)
  // throws due to non-zero R in sampling mode
  bytes = src_bytes; 
  *reinterpret_cast<int32_t*>(&bytes[20]) = -128;
  REQUIRE_THROWS_AS(var_opt_sketch<int>::deserialize(bytes.data(), bytes.size()), std::invalid_argument);
}

TEST_CASE("varopt sketch: empty sketch", "[var_opt_sketch]") {
  var_opt_sketch<std::string> sk(5);
  REQUIRE(sk.get_n() == 0);
  REQUIRE(sk.get_num_samples() == 0);

  std::vector<uint8_t> bytes = sk.serialize();
  REQUIRE(bytes.size() == (1 << 3)); // num bytes in PREAMBLE_LONGS_EMPTY

  var_opt_sketch<std::string> loaded_sk = var_opt_sketch<std::string>::deserialize(bytes.data(), bytes.size());
  REQUIRE(loaded_sk.get_n() == 0);
  REQUIRE(loaded_sk.get_num_samples() == 0);
}

TEST_CASE("varopt sketch: non-empty degenerate sketch", "[var_opt_sketch]") {
  // Make an empty serialized sketch, then extend it to a
  // PREAMBLE_LONGS_WARMUP-sized byte array, with no items.
  // Then clear the empty flag so it will try to load the rest.
  var_opt_sketch<std::string> sk(12, resize_factor::X2);
  std::vector<uint8_t> bytes = sk.serialize();
  while (bytes.size() < 24) { // PREAMBLE_LONGS_WARMUP * 8
    bytes.push_back((uint8_t) 0);
  }
  
  // ensure non-empty -- H and R region sizes already set to 0
  bytes[3] = 0; // set flags bit to not-empty (other bits should already be 0)

  REQUIRE_THROWS_AS(var_opt_sketch<std::string>::deserialize(bytes.data(), bytes.size()), std::invalid_argument);
}

TEST_CASE("varopt sketch: invalid weight", "[var_opt_sketch]") {
  var_opt_sketch<std::string> sk(100, resize_factor::X2);
  REQUIRE_THROWS_AS(sk.update("invalid_weight", -1.0), std::invalid_argument);

  // should not throw but sketch shoulds till be empty
  sk.update("zero weight", 0.0);
  REQUIRE(sk.is_empty());
}

TEST_CASE("varopt sketch: corrupt serialized weight", "[var_opt_sketch]") {
  var_opt_sketch<int> sk = create_unweighted_sketch(100, 20);
  auto bytes = sk.serialize();
    
  // weights are in the first double after the preamble
  size_t preamble_bytes = (bytes[0] & 0x3f) << 3;
  *reinterpret_cast<double*>(&bytes[preamble_bytes]) = -1.5;

  REQUIRE_THROWS_AS(var_opt_sketch<int>::deserialize(bytes.data(), bytes.size()), std::invalid_argument);

  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  for (auto& b : bytes) { ss >> b; }
  REQUIRE_THROWS_AS(var_opt_sketch<int>::deserialize(ss), std::invalid_argument);
}

TEST_CASE("varopt sketch: cumulative weight", "[var_opt_sketch]") {
  uint32_t k = 256;
  uint64_t n = 10 * k;
  var_opt_sketch<int> sk(k);

  std::random_device rd; // possibly unsafe in MinGW with GCC < 9.2
  std::mt19937_64 rand(rd());
  std::normal_distribution<double> N(0.0, 1.0);

  double input_sum = 0.0;
  for (size_t i = 0; i < n; ++i) {
    // generate weights aboev and below 1.0 using w ~ exp(5*N(0,1))
    // which covers about 10 orders of magnitude
    double w = std::exp(5 * N(rand));
    input_sum += w;
    sk.update(static_cast<int>(i), w);
  }

  double output_sum = 0.0;
  for (auto it : sk) { // std::pair<int, weight>
    output_sum += it.second;
  }
    
  double weight_ratio = output_sum / input_sum;
  REQUIRE(std::abs(weight_ratio - 1.0) == Approx(0).margin(EPS));
}

TEST_CASE("varopt sketch: under-full sketch serialization", "[var_opt_sketch]") {
  var_opt_sketch<int> sk = create_unweighted_sketch(100, 10); // need n < k

  auto bytes = sk.serialize();
  var_opt_sketch<int> sk_from_bytes = var_opt_sketch<int>::deserialize(bytes.data(), bytes.size());
  check_if_equal(sk, sk_from_bytes);

  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  sk.serialize(ss);
  var_opt_sketch<int> sk_from_stream = var_opt_sketch<int>::deserialize(ss);
  check_if_equal(sk, sk_from_stream);

  // ensure we unroll properly
  REQUIRE_THROWS_AS(var_opt_sketch<int>::deserialize(bytes.data(), bytes.size() - 1), std::out_of_range);
  std::string str_trunc((char*)&bytes[0], bytes.size() - 1);
  ss.str(str_trunc);
  REQUIRE_THROWS_AS(var_opt_sketch<int>::deserialize(ss), std::runtime_error);
}

TEST_CASE("varopt sketch: end-of-warmup sketch serialization", "[var_opt_sketch]") {
  var_opt_sketch<int> sk = create_unweighted_sketch(2843, 2843); // need n == k
  auto bytes = sk.serialize();

  // ensure still only 3 preamble longs
  REQUIRE((bytes.data()[0] & 0x3f) == 3); // PREAMBLE_LONGS_WARMUP

  var_opt_sketch<int> sk_from_bytes = var_opt_sketch<int>::deserialize(bytes.data(), bytes.size());
  check_if_equal(sk, sk_from_bytes);

  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  sk.serialize(ss);
  var_opt_sketch<int> sk_from_stream = var_opt_sketch<int>::deserialize(ss);
  check_if_equal(sk, sk_from_stream);

  // ensure we unroll properly
  REQUIRE_THROWS_AS(var_opt_sketch<int>::deserialize(bytes.data(), bytes.size() - 1000), std::out_of_range);
  std::string str_trunc((char*)&bytes[0], bytes.size() - 100);
  ss.str(str_trunc);
  REQUIRE_THROWS_AS(var_opt_sketch<int>::deserialize(ss), std::runtime_error);
}

TEST_CASE("varopt sketch: full sketch serialization", "[var_opt_sketch]") {
  var_opt_sketch<int> sk = create_unweighted_sketch(32, 32);
  sk.update(100, 100.0);
  sk.update(101, 101.0);

  // first 2 entries should be heavy and in heap order (smallest at root)
  auto it = sk.begin();
  const std::pair<const int, const double> p1 = *it;
  ++it;
  const std::pair<const int, const double> p2 = *it;
  REQUIRE(p1.second == Approx(100.0).margin(EPS));
  REQUIRE(p2.second == Approx(101.0).margin(EPS));
  REQUIRE(p1.first == 100);
  REQUIRE(p2.first == 101);

  // check for 4 preamble longs
  auto bytes = sk.serialize();
  REQUIRE((bytes.data()[0] & 0x3f) == 4);; // PREAMBLE_LONGS_WARMUP

  var_opt_sketch<int> sk_from_bytes = var_opt_sketch<int>::deserialize(bytes.data(), bytes.size());
  check_if_equal(sk, sk_from_bytes);

  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  sk.serialize(ss);
  var_opt_sketch<int> sk_from_stream = var_opt_sketch<int>::deserialize(ss);
  check_if_equal(sk, sk_from_stream);

  // ensure we unroll properly
  REQUIRE_THROWS_AS(var_opt_sketch<int>::deserialize(bytes.data(), bytes.size() - 100), std::out_of_range);
  std::string str_trunc((char*)&bytes[0], bytes.size() - 100);
  ss.str(str_trunc);
  REQUIRE_THROWS_AS(var_opt_sketch<int>::deserialize(ss), std::runtime_error);
}

TEST_CASE("varopt sketch: string serialization", "[var_opt_sketch]") {
  var_opt_sketch<std::string> sk(5);
  sk.update("a", 1.0);
  sk.update("bc", 1.0);
  sk.update("def", 1.0);
  sk.update("ghij", 1.0);
  sk.update("klmno", 1.0);
  sk.update("heavy item", 100.0);

  auto bytes = sk.serialize();
  var_opt_sketch<std::string> sk_from_bytes = var_opt_sketch<std::string>::deserialize(bytes.data(), bytes.size());
  check_if_equal(sk, sk_from_bytes);

  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  sk.serialize(ss);
  var_opt_sketch<std::string> sk_from_stream = var_opt_sketch<std::string>::deserialize(ss);
  check_if_equal(sk, sk_from_stream);

  // ensure we unroll properly
  REQUIRE_THROWS_AS(var_opt_sketch<std::string>::deserialize(bytes.data(), bytes.size() - 12), std::out_of_range);
  std::string str_trunc((char*)&bytes[0], bytes.size() - 12);
  ss.str(str_trunc);
  REQUIRE_THROWS_AS(var_opt_sketch<std::string>::deserialize(ss), std::runtime_error);
}

TEST_CASE("varopt sketch: pseudo-light update", "[var_opt_sketch]") {
  uint32_t k = 1024;
  var_opt_sketch<int> sk = create_unweighted_sketch(k, k + 1);
  sk.update(0, 1.0); // k+2nd update

  // check the first weight, assuming all k items are unweighted
  // (and consequently in R).
  // Expected: (k + 2) / |R| = (k + 2) / k
  auto it = sk.begin();
  double wt = (*it).second;
  REQUIRE(wt == Approx((k + 2.0) / k).margin(EPS));
}

TEST_CASE("varopt sketch: pseudo-heavy update", "[var_opt_sketch]") {
  uint32_t k = 1024;
  double wt_scale = 10.0 * k;
  var_opt_sketch<int> sk = create_unweighted_sketch(k, k + 1);

  // Next k-1 updates should be update_pseudo_heavy_general()
  // Last one should call update_pseudo_heavy_r_eq_1(), since we'll have
  // added k-1 heavy items, leaving only 1 item left in R
  for (uint32_t i = 1; i <= k; ++i) {
    sk.update(-1 * static_cast<int>(i), k + (i * wt_scale));
  }

  auto it = sk.begin();

  // Expected: lightest "heavy" item (first one out): k + 2*wt_scale
  double wt = (*it).second;
  REQUIRE(wt == Approx(1.0 * (k + (2 * wt_scale))).margin(EPS));

  // we don't know which R item is left, but there should be only one, at the end
  // of the sample set.
  // Expected: k+1 + (min "heavy" item) / |R| = ((k+1) + (k*wt_scale)) / 1 = wt_scale + 2k + 1
  while (it != sk.end()) {
    wt = (*it).second;
    ++it;
  }
  REQUIRE(wt == Approx(1.0 + wt_scale + (2 * k)).margin(EPS));
}

TEST_CASE("varopt sketch: reset", "[var_opt_sketch]") {
  uint32_t k = 1024;
  uint64_t n1 = 20;
  uint64_t n2 = 2 * k;
  var_opt_sketch<std::string> sk(k);

  // reset from sampling mode
  for (uint64_t i = 0; i < n2; ++i) {
    sk.update(std::to_string(i), 100.0 + i);
  }
  REQUIRE(sk.get_n() == n2);
  REQUIRE(sk.get_k() == k);

  sk.reset();
  REQUIRE(sk.get_n() == 0);
  REQUIRE(sk.get_k() == k);

  // reset from exact mode
  for (uint64_t i = 0; i < n1; ++i)
    sk.update(std::to_string(i));
  REQUIRE(sk.get_n() == n1);
  REQUIRE(sk.get_k() == k);

  sk.reset();
  REQUIRE(sk.get_n() == 0);
  REQUIRE(sk.get_k() == k);
}

TEST_CASE("varopt sketch: estimate subset sum", "[var_opt_sketch]") {
  uint32_t k = 10;
  var_opt_sketch<int> sk(k);

  // empty sketch -- all zeros
  subset_summary summary = sk.estimate_subset_sum([](int){ return true; });
  REQUIRE(summary.estimate == 0.0);
  REQUIRE(summary.total_sketch_weight == 0.0);

  // add items, keeping in exact mode
  double total_weight = 0.0;
  for (uint32_t i = 1; i <= (k - 1); ++i) {
    sk.update(i, 1.0 * i);
    total_weight += 1.0 * i;
  }

  summary = sk.estimate_subset_sum([](int){ return true; });
  REQUIRE(summary.estimate == total_weight);
  REQUIRE(summary.lower_bound == total_weight);
  REQUIRE(summary.upper_bound == total_weight);
  REQUIRE(summary.total_sketch_weight == total_weight);

  // add a few more items, pushing to sampling mode
  for (uint32_t i = k; i <= (k + 1); ++i) {
    sk.update(i, 1.0 * i);
    total_weight += 1.0 * i;
  }

  // predicate always true so estimate == upper bound
  summary = sk.estimate_subset_sum([](int){ return true; });
  REQUIRE(summary.estimate == Approx(total_weight).margin(EPS));
  REQUIRE(summary.upper_bound == Approx(total_weight).margin(EPS));
  REQUIRE(summary.lower_bound < total_weight);
  REQUIRE(summary.total_sketch_weight == Approx(total_weight).margin(EPS));
  
  // predicate always false so estimate == lower bound == 0.0
  summary = sk.estimate_subset_sum([](int){ return false; });
  REQUIRE(summary.estimate == 0.0);
  REQUIRE(summary.lower_bound == 0.0);
  REQUIRE(summary.upper_bound > 0.0);
  REQUIRE(summary.total_sketch_weight == Approx(total_weight).margin(EPS));

  // finally, a non-degenerate predicate
  // insert negative items with identical weights, filter for negative weights only
  for (uint32_t i = 1; i <= (k + 1); ++i) {
    sk.update(-1 * static_cast<int32_t>(i), static_cast<double>(i));
    total_weight += 1.0 * i;
  }

  summary = sk.estimate_subset_sum([](int x) { return x < 0; });
  REQUIRE(summary.estimate >= summary.lower_bound);
  REQUIRE(summary.estimate <= summary.upper_bound);

  // allow pretty generous bounds when testing
  REQUIRE(summary.lower_bound < (total_weight / 1.4));
  REQUIRE(summary.upper_bound > (total_weight / 2.6));
  REQUIRE(summary.total_sketch_weight == Approx(total_weight).margin(EPS));

  // and another data type, keeping it in exact mode for simplicity
  var_opt_sketch<bool> sk2(k);
  total_weight = 0.0;
  for (uint32_t i = 1; i <= (k - 1); ++i) {
    sk2.update((i % 2) == 0, 1.0 * i);
    total_weight += i;
  }

  summary = sk2.estimate_subset_sum([](bool b){ return !b; });
  REQUIRE(summary.estimate == summary.lower_bound);
  REQUIRE(summary.estimate == summary.upper_bound);
  REQUIRE(summary.estimate < total_weight); // exact mode, so know it must be strictly less
}

TEST_CASE("varopt sketch: deserialize exact from java", "[var_opt_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(testBinaryInputPath + "varopt_sketch_string_exact.sk", std::ios::binary);
  var_opt_sketch<std::string> sketch = var_opt_sketch<std::string>::deserialize(is);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.get_k() == 1024);
  REQUIRE(sketch.get_n() == 200);
  REQUIRE(sketch.get_num_samples() == 200);
  subset_summary ss = sketch.estimate_subset_sum([](std::string){ return true; });

  double tgt_wt = 0.0;
  for (int i = 1; i <= 200; ++i) { tgt_wt += 1000.0 / i; }
  REQUIRE(ss.total_sketch_weight == Approx(tgt_wt).margin(EPS));
}


TEST_CASE("varopt sketch: deserialize sampling from java", "[var_opt_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(testBinaryInputPath + "varopt_sketch_long_sampling.sk", std::ios::binary);
  var_opt_sketch<int64_t> sketch = var_opt_sketch<int64_t>::deserialize(is);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.get_k() == 1024);
  REQUIRE(sketch.get_n() == 2003);
  REQUIRE(sketch.get_num_samples() == sketch.get_k());
  subset_summary ss = sketch.estimate_subset_sum([](int64_t){ return true; });
  REQUIRE(ss.estimate == Approx(332000.0).margin(EPS));
  REQUIRE(ss.total_sketch_weight == Approx(332000.0).margin(EPS));

  ss = sketch.estimate_subset_sum([](int64_t x){ return x < 0; });
  REQUIRE(ss.estimate == 330000.0); // heavy item, weight is exact

  ss = sketch.estimate_subset_sum([](int64_t x){ return x >= 0; });
  REQUIRE(ss.estimate == Approx(2000.0).margin(EPS));
}

}

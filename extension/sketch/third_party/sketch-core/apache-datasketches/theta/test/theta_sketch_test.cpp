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

#include <istream>
#include <fstream>
#include <sstream>
#include <vector>
#include <stdexcept>

#include <catch.hpp>
#include <theta_sketch.hpp>

namespace datasketches {

#ifdef TEST_BINARY_INPUT_PATH
const std::string inputPath = TEST_BINARY_INPUT_PATH;
#else
const std::string inputPath = "test/";
#endif

TEST_CASE("theta sketch: empty", "[theta_sketch]") {
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  REQUIRE(update_sketch.is_empty());
  REQUIRE_FALSE(update_sketch.is_estimation_mode());
  REQUIRE(update_sketch.get_theta() == 1.0);
  REQUIRE(update_sketch.get_estimate() == 0.0);
  REQUIRE(update_sketch.get_lower_bound(1) == 0.0);
  REQUIRE(update_sketch.get_upper_bound(1) == 0.0);
  REQUIRE(update_sketch.is_ordered());

  compact_theta_sketch compact_sketch = update_sketch.compact();
  REQUIRE(compact_sketch.is_empty());
  REQUIRE_FALSE(compact_sketch.is_estimation_mode());
  REQUIRE(compact_sketch.get_theta() == 1.0);
  REQUIRE(compact_sketch.get_estimate() == 0.0);
  REQUIRE(compact_sketch.get_lower_bound(1) == 0.0);
  REQUIRE(compact_sketch.get_upper_bound(1) == 0.0);
  REQUIRE(compact_sketch.is_ordered());

  // empty is forced to be ordered
  REQUIRE(update_sketch.compact(false).is_ordered());
}

TEST_CASE("theta sketch: non empty no retained keys", "[theta_sketch]") {
  update_theta_sketch update_sketch = update_theta_sketch::builder().set_p(0.001f).build();
  update_sketch.update(1);
  //std::cerr << update_sketch.to_string();
  REQUIRE(update_sketch.get_num_retained() == 0);
  REQUIRE_FALSE(update_sketch.is_empty());
  REQUIRE(update_sketch.is_estimation_mode());
  REQUIRE(update_sketch.get_estimate() == 0.0);
  REQUIRE(update_sketch.get_lower_bound(1) == 0.0);
  REQUIRE(update_sketch.get_upper_bound(1) > 0);

  compact_theta_sketch compact_sketch = update_sketch.compact();
  REQUIRE(compact_sketch.get_num_retained() == 0);
  REQUIRE_FALSE(compact_sketch.is_empty());
  REQUIRE(compact_sketch.is_estimation_mode());
  REQUIRE(compact_sketch.get_estimate() == 0.0);
  REQUIRE(compact_sketch.get_lower_bound(1) == 0.0);
  REQUIRE(compact_sketch.get_upper_bound(1) > 0);

  update_sketch.reset();
  REQUIRE(update_sketch.is_empty());
  REQUIRE_FALSE(update_sketch.is_estimation_mode());
  REQUIRE(update_sketch.get_theta() == 1.0);
  REQUIRE(update_sketch.get_estimate() == 0.0);
  REQUIRE(update_sketch.get_lower_bound(1) == 0.0);
  REQUIRE(update_sketch.get_upper_bound(1) == 0.0);
}

TEST_CASE("theta sketch: single item", "[theta_sketch]") {
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  update_sketch.update(1);
  REQUIRE_FALSE(update_sketch.is_empty());
  REQUIRE_FALSE(update_sketch.is_estimation_mode());
  REQUIRE(update_sketch.get_theta() == 1.0);
  REQUIRE(update_sketch.get_estimate() == 1.0);
  REQUIRE(update_sketch.get_lower_bound(1) == 1.0);
  REQUIRE(update_sketch.get_upper_bound(1) == 1.0);
  REQUIRE(update_sketch.is_ordered()); // one item is ordered

  compact_theta_sketch compact_sketch = update_sketch.compact();
  REQUIRE_FALSE(compact_sketch.is_empty());
  REQUIRE_FALSE(compact_sketch.is_estimation_mode());
  REQUIRE(compact_sketch.get_theta() == 1.0);
  REQUIRE(compact_sketch.get_estimate() == 1.0);
  REQUIRE(compact_sketch.get_lower_bound(1) == 1.0);
  REQUIRE(compact_sketch.get_upper_bound(1) == 1.0);
  REQUIRE(compact_sketch.is_ordered());

  // single item is forced to be ordered
  REQUIRE(update_sketch.compact(false).is_ordered());
}

TEST_CASE("theta sketch: resize exact", "[theta_sketch]") {
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  for (int i = 0; i < 2000; i++) update_sketch.update(i);
  REQUIRE_FALSE(update_sketch.is_empty());
  REQUIRE_FALSE(update_sketch.is_estimation_mode());
  REQUIRE(update_sketch.get_theta() == 1.0);
  REQUIRE(update_sketch.get_estimate() == 2000.0);
  REQUIRE(update_sketch.get_lower_bound(1) == 2000.0);
  REQUIRE(update_sketch.get_upper_bound(1) == 2000.0);
  REQUIRE_FALSE(update_sketch.is_ordered());

  compact_theta_sketch compact_sketch = update_sketch.compact();
  REQUIRE_FALSE(compact_sketch.is_empty());
  REQUIRE_FALSE(compact_sketch.is_estimation_mode());
  REQUIRE(compact_sketch.get_theta() == 1.0);
  REQUIRE(compact_sketch.get_estimate() == 2000.0);
  REQUIRE(compact_sketch.get_lower_bound(1) == 2000.0);
  REQUIRE(compact_sketch.get_upper_bound(1) == 2000.0);
  REQUIRE(compact_sketch.is_ordered());

  update_sketch.reset();
  REQUIRE(update_sketch.is_empty());
  REQUIRE_FALSE(update_sketch.is_estimation_mode());
  REQUIRE(update_sketch.get_theta() == 1.0);
  REQUIRE(update_sketch.get_estimate() == 0.0);
  REQUIRE(update_sketch.get_lower_bound(1) == 0.0);
  REQUIRE(update_sketch.get_upper_bound(1) == 0.0);
  REQUIRE(update_sketch.is_ordered());

}

TEST_CASE("theta sketch: estimation", "[theta_sketch]") {
  update_theta_sketch update_sketch = update_theta_sketch::builder().set_resize_factor(update_theta_sketch::resize_factor::X1).build();
  const int n = 8000;
  for (int i = 0; i < n; i++) update_sketch.update(i);
  //std::cerr << update_sketch.to_string();
  REQUIRE_FALSE(update_sketch.is_empty());
  REQUIRE(update_sketch.is_estimation_mode());
  REQUIRE(update_sketch.get_theta() < 1.0);
  REQUIRE(update_sketch.get_estimate() == Approx((double) n).margin(n * 0.01));
  REQUIRE(update_sketch.get_lower_bound(1) < n);
  REQUIRE(update_sketch.get_upper_bound(1) > n);

  const uint32_t k = 1 << update_theta_sketch::builder::DEFAULT_LG_K;
  REQUIRE(update_sketch.get_num_retained() >= k);
  update_sketch.trim();
  REQUIRE(update_sketch.get_num_retained() == k);

  compact_theta_sketch compact_sketch = update_sketch.compact();
  REQUIRE_FALSE(compact_sketch.is_empty());
  REQUIRE(compact_sketch.is_ordered());
  REQUIRE(compact_sketch.is_estimation_mode());
  REQUIRE(compact_sketch.get_theta() < 1.0);
  REQUIRE(compact_sketch.get_estimate() == Approx((double) n).margin(n * 0.01));
  REQUIRE(compact_sketch.get_lower_bound(1) < n);
  REQUIRE(compact_sketch.get_upper_bound(1) > n);
}

TEST_CASE("theta sketch: deserialize compact empty from java", "[theta_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "theta_compact_empty_from_java.sk", std::ios::binary);
  auto sketch = compact_theta_sketch::deserialize(is);
  REQUIRE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_num_retained() == 0);
  REQUIRE(sketch.get_theta() == 1.0);
  REQUIRE(sketch.get_estimate() == 0.0);
  REQUIRE(sketch.get_lower_bound(1) == 0.0);
  REQUIRE(sketch.get_upper_bound(1) == 0.0);
}

TEST_CASE("theta sketch: deserialize compact v1 empty from java", "[theta_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "theta_compact_empty_from_java_v1.sk", std::ios::binary);
  auto sketch = compact_theta_sketch::deserialize(is);
  REQUIRE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_num_retained() == 0);
  REQUIRE(sketch.get_theta() == 1.0);
  REQUIRE(sketch.get_estimate() == 0.0);
  REQUIRE(sketch.get_lower_bound(1) == 0.0);
  REQUIRE(sketch.get_upper_bound(1) == 0.0);
}

TEST_CASE("theta sketch: deserialize compact v2 empty from java", "[theta_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "theta_compact_empty_from_java_v2.sk", std::ios::binary);
  auto sketch = compact_theta_sketch::deserialize(is);
  REQUIRE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_num_retained() == 0);
  REQUIRE(sketch.get_theta() == 1.0);
  REQUIRE(sketch.get_estimate() == 0.0);
  REQUIRE(sketch.get_lower_bound(1) == 0.0);
  REQUIRE(sketch.get_upper_bound(1) == 0.0);
}

TEST_CASE("theta sketch: deserialize single item from java", "[theta_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "theta_compact_single_item_from_java.sk", std::ios::binary);
  auto sketch = compact_theta_sketch::deserialize(is);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_num_retained() == 1);
  REQUIRE(sketch.get_theta() == 1.0);
  REQUIRE(sketch.get_estimate() == 1.0);
  REQUIRE(sketch.get_lower_bound(1) == 1.0);
  REQUIRE(sketch.get_upper_bound(1) == 1.0);
}

TEST_CASE("theta sketch: deserialize compact exact from java", "[theta_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "theta_compact_exact_from_java.sk", std::ios::binary);
  auto sketch = compact_theta_sketch::deserialize(is);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.is_ordered());
  REQUIRE(sketch.get_num_retained() == 100);

  // the same construction process in Java must have produced exactly the same sketch
  auto update_sketch = update_theta_sketch::builder().build();
  const int n = 100;
  for (int i = 0; i < n; i++) update_sketch.update(i);
  REQUIRE(sketch.get_num_retained() == update_sketch.get_num_retained());
  REQUIRE(sketch.get_theta() == Approx(update_sketch.get_theta()).margin(1e-10));
  REQUIRE(sketch.get_estimate() == Approx(update_sketch.get_estimate()).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(1) == Approx(update_sketch.get_lower_bound(1)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(1) == Approx(update_sketch.get_upper_bound(1)).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(2) == Approx(update_sketch.get_lower_bound(2)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(2) == Approx(update_sketch.get_upper_bound(2)).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(3) == Approx(update_sketch.get_lower_bound(3)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(3) == Approx(update_sketch.get_upper_bound(3)).margin(1e-10));
  compact_theta_sketch compact_sketch = update_sketch.compact();
  // the sketches are ordered, so the iteration sequence must match exactly
  auto iter = sketch.begin();
  for (const auto& key: compact_sketch) {
    REQUIRE(*iter == key);
    ++iter;
  }
}

TEST_CASE("theta sketch: deserialize compact estimation from java", "[theta_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "theta_compact_estimation_from_java.sk", std::ios::binary);
  auto sketch = compact_theta_sketch::deserialize(is);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.is_estimation_mode());
  REQUIRE(sketch.is_ordered());
  REQUIRE(sketch.get_num_retained() == 4342);
  REQUIRE(sketch.get_theta() == Approx(0.531700444213199).margin(1e-10));
  REQUIRE(sketch.get_estimate() == Approx(8166.25234614053).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(2) == Approx(7996.956955317471).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(2) == Approx(8339.090301078124).margin(1e-10));

  // the same construction process in Java must have produced exactly the same sketch
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  const int n = 8192;
  for (int i = 0; i < n; i++) update_sketch.update(i);
  REQUIRE(sketch.get_num_retained() == update_sketch.get_num_retained());
  REQUIRE(sketch.get_theta() == Approx(update_sketch.get_theta()).margin(1e-10));
  REQUIRE(sketch.get_estimate() == Approx(update_sketch.get_estimate()).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(1) == Approx(update_sketch.get_lower_bound(1)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(1) == Approx(update_sketch.get_upper_bound(1)).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(2) == Approx(update_sketch.get_lower_bound(2)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(2) == Approx(update_sketch.get_upper_bound(2)).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(3) == Approx(update_sketch.get_lower_bound(3)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(3) == Approx(update_sketch.get_upper_bound(3)).margin(1e-10));
  compact_theta_sketch compact_sketch = update_sketch.compact();
  // the sketches are ordered, so the iteration sequence must match exactly
  auto iter = sketch.begin();
  for (const auto& key: compact_sketch) {
    REQUIRE(*iter == key);
    ++iter;
  }
}

TEST_CASE("theta sketch: deserialize compact v1 estimation from java", "[theta_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "theta_compact_estimation_from_java_v1.sk", std::ios::binary);
  auto sketch = compact_theta_sketch::deserialize(is);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.is_estimation_mode());
  REQUIRE(sketch.is_ordered());
  REQUIRE(sketch.get_num_retained() == 4342);
  REQUIRE(sketch.get_theta() == Approx(0.531700444213199).margin(1e-10));
  REQUIRE(sketch.get_estimate() == Approx(8166.25234614053).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(2) == Approx(7996.956955317471).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(2) == Approx(8339.090301078124).margin(1e-10));

  // the same construction process in Java must have produced exactly the same sketch
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  const int n = 8192;
  for (int i = 0; i < n; i++) update_sketch.update(i);
  REQUIRE(sketch.get_num_retained() == update_sketch.get_num_retained());
  REQUIRE(sketch.get_theta() == Approx(update_sketch.get_theta()).margin(1e-10));
  REQUIRE(sketch.get_estimate() == Approx(update_sketch.get_estimate()).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(1) == Approx(update_sketch.get_lower_bound(1)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(1) == Approx(update_sketch.get_upper_bound(1)).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(2) == Approx(update_sketch.get_lower_bound(2)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(2) == Approx(update_sketch.get_upper_bound(2)).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(3) == Approx(update_sketch.get_lower_bound(3)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(3) == Approx(update_sketch.get_upper_bound(3)).margin(1e-10));
  compact_theta_sketch compact_sketch = update_sketch.compact();
  // the sketches are ordered, so the iteration sequence must match exactly
  auto iter = sketch.begin();
  for (const auto& key: compact_sketch) {
    REQUIRE(*iter == key);
    ++iter;
  }
}

TEST_CASE("theta sketch: deserialize compact v2 estimation from java", "[theta_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "theta_compact_estimation_from_java_v2.sk", std::ios::binary);
  auto sketch = compact_theta_sketch::deserialize(is);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.is_estimation_mode());
  REQUIRE(sketch.is_ordered());
  REQUIRE(sketch.get_num_retained() == 4342);
  REQUIRE(sketch.get_theta() == Approx(0.531700444213199).margin(1e-10));
  REQUIRE(sketch.get_estimate() == Approx(8166.25234614053).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(2) == Approx(7996.956955317471).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(2) == Approx(8339.090301078124).margin(1e-10));

  // the same construction process in Java must have produced exactly the same sketch
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  const int n = 8192;
  for (int i = 0; i < n; i++) update_sketch.update(i);
  REQUIRE(sketch.get_num_retained() == update_sketch.get_num_retained());
  REQUIRE(sketch.get_theta() == Approx(update_sketch.get_theta()).margin(1e-10));
  REQUIRE(sketch.get_estimate() == Approx(update_sketch.get_estimate()).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(1) == Approx(update_sketch.get_lower_bound(1)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(1) == Approx(update_sketch.get_upper_bound(1)).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(2) == Approx(update_sketch.get_lower_bound(2)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(2) == Approx(update_sketch.get_upper_bound(2)).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(3) == Approx(update_sketch.get_lower_bound(3)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(3) == Approx(update_sketch.get_upper_bound(3)).margin(1e-10));
  compact_theta_sketch compact_sketch = update_sketch.compact();
  // the sketches are ordered, so the iteration sequence must match exactly
  auto iter = sketch.begin();
  for (const auto& key: compact_sketch) {
    REQUIRE(*iter == key);
    ++iter;
  }
}

TEST_CASE("theta sketch: serialize deserialize stream and bytes equivalence", "[theta_sketch]") {
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  const int n = 8192;
  for (int i = 0; i < n; i++) update_sketch.update(i);

  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  update_sketch.compact().serialize(s);
  auto bytes = update_sketch.compact().serialize();
  REQUIRE(bytes.size() == static_cast<size_t>(s.tellp()));
  for (size_t i = 0; i < bytes.size(); ++i) {
    REQUIRE(((char*)bytes.data())[i] == (char)s.get());
  }

  s.seekg(0); // rewind
  compact_theta_sketch deserialized_sketch1 = compact_theta_sketch::deserialize(s);
  compact_theta_sketch deserialized_sketch2 = compact_theta_sketch::deserialize(bytes.data(), bytes.size());
  REQUIRE(bytes.size() == static_cast<size_t>(s.tellg()));
  REQUIRE(deserialized_sketch2.is_empty() == deserialized_sketch1.is_empty());
  REQUIRE(deserialized_sketch2.is_ordered() == deserialized_sketch1.is_ordered());
  REQUIRE(deserialized_sketch2.get_num_retained() == deserialized_sketch1.get_num_retained());
  REQUIRE(deserialized_sketch2.get_theta() == deserialized_sketch1.get_theta());
  REQUIRE(deserialized_sketch2.get_estimate() == deserialized_sketch1.get_estimate());
  REQUIRE(deserialized_sketch2.get_lower_bound(1) == deserialized_sketch1.get_lower_bound(1));
  REQUIRE(deserialized_sketch2.get_upper_bound(1) == deserialized_sketch1.get_upper_bound(1));
  // the sketches are ordered, so the iteration sequence must match exactly
  auto iter = deserialized_sketch1.begin();
  for (auto key: deserialized_sketch2) {
    REQUIRE(*iter == key);
    ++iter;
  }
}

TEST_CASE("theta sketch: deserialize empty buffer overrun", "[theta_sketch]") {
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  auto bytes = update_sketch.compact().serialize();
  REQUIRE_THROWS_AS(compact_theta_sketch::deserialize(bytes.data(), bytes.size() - 1), std::out_of_range);
}

TEST_CASE("theta sketch: deserialize single item buffer overrun", "[theta_sketch]") {
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  update_sketch.update(1);
  auto bytes = update_sketch.compact().serialize();
  REQUIRE_THROWS_AS(compact_theta_sketch::deserialize(bytes.data(), 7), std::out_of_range);
  REQUIRE_THROWS_AS(compact_theta_sketch::deserialize(bytes.data(), bytes.size() - 1), std::out_of_range);
}

TEST_CASE("theta sketch: deserialize exact mode buffer overrun", "[theta_sketch]") {
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  for (int i = 0; i < 1000; ++i) update_sketch.update(i);
  auto bytes = update_sketch.compact().serialize();
  REQUIRE_THROWS_AS(compact_theta_sketch::deserialize(bytes.data(), 7), std::out_of_range);
  REQUIRE_THROWS_AS(compact_theta_sketch::deserialize(bytes.data(), 8), std::out_of_range);
  REQUIRE_THROWS_AS(compact_theta_sketch::deserialize(bytes.data(), 16), std::out_of_range);
  REQUIRE_THROWS_AS(compact_theta_sketch::deserialize(bytes.data(), bytes.size() - 1), std::out_of_range);
}

TEST_CASE("theta sketch: deserialize estimation mode buffer overrun", "[theta_sketch]") {
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  for (int i = 0; i < 10000; ++i) update_sketch.update(i);
  auto bytes = update_sketch.compact().serialize();
  REQUIRE_THROWS_AS(compact_theta_sketch::deserialize(bytes.data(), 7), std::out_of_range);
  REQUIRE_THROWS_AS(compact_theta_sketch::deserialize(bytes.data(), 8), std::out_of_range);
  REQUIRE_THROWS_AS(compact_theta_sketch::deserialize(bytes.data(), 16), std::out_of_range);
  REQUIRE_THROWS_AS(compact_theta_sketch::deserialize(bytes.data(), 24), std::out_of_range);
  REQUIRE_THROWS_AS(compact_theta_sketch::deserialize(bytes.data(), bytes.size() - 1), std::out_of_range);
}

TEST_CASE("theta sketch: conversion constructor and wrapped compact", "[theta_sketch]") {
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  const int n = 8192;
  for (int i = 0; i < n; i++) update_sketch.update(i);

  // unordered
  auto unordered_compact1 = update_sketch.compact(false);
  compact_theta_sketch unordered_compact2(update_sketch, false);
  auto it = unordered_compact1.begin();
  for (auto entry: unordered_compact2) {
    REQUIRE(*it == entry);
    ++it;
  }

  // ordered
  auto ordered_compact1 = update_sketch.compact();
  compact_theta_sketch ordered_compact2(update_sketch, true);
  it = ordered_compact1.begin();
  for (auto entry: ordered_compact2) {
    REQUIRE(*it == entry);
    ++it;
  }

  // wrapped compact
  auto bytes = ordered_compact1.serialize();
  auto ordered_compact3 = wrapped_compact_theta_sketch::wrap(bytes.data(), bytes.size());
  it = ordered_compact1.begin();
  for (auto entry: ordered_compact3) {
    REQUIRE(*it == entry);
    ++it;
  }
  REQUIRE(ordered_compact3.get_estimate() == ordered_compact1.get_estimate());
  REQUIRE(ordered_compact3.get_lower_bound(1) == ordered_compact1.get_lower_bound(1));
  REQUIRE(ordered_compact3.get_upper_bound(1) == ordered_compact1.get_upper_bound(1));
  REQUIRE(ordered_compact3.is_estimation_mode() == ordered_compact1.is_estimation_mode());
  REQUIRE(ordered_compact3.get_theta() == ordered_compact1.get_theta());


  // seed mismatch
  REQUIRE_THROWS_AS(wrapped_compact_theta_sketch::wrap(bytes.data(), bytes.size(), 0), std::invalid_argument);
}

TEST_CASE("theta sketch: wrap compact empty from java", "[theta_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "theta_compact_empty_from_java.sk", std::ios::binary | std::ios::ate);

  std::vector<uint8_t> buf;
  if(is) {
      auto size = is.tellg();
      buf.reserve(size);
      buf.assign(size, 0);
      is.seekg(0, std::ios_base::beg);
      is.read((char*)(buf.data()), buf.size());
  }

  auto sketch = wrapped_compact_theta_sketch::wrap(buf.data(), buf.size());
  REQUIRE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_num_retained() == 0);
  REQUIRE(sketch.get_theta() == 1.0);
  REQUIRE(sketch.get_estimate() == 0.0);
  REQUIRE(sketch.get_lower_bound(1) == 0.0);
  REQUIRE(sketch.get_upper_bound(1) == 0.0);
}

TEST_CASE("theta sketch: wrap compact v1 empty from java", "[theta_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "theta_compact_empty_from_java_v1.sk", std::ios::binary | std::ios::ate);

  std::vector<uint8_t> buf;
  if(is) {
      auto size = is.tellg();
      buf.reserve(size);
      buf.assign(size, 0);
      is.seekg(0, std::ios_base::beg);
      is.read((char*)(buf.data()), buf.size());
  }

  auto sketch = wrapped_compact_theta_sketch::wrap(buf.data(), buf.size());
  REQUIRE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_num_retained() == 0);
  REQUIRE(sketch.get_theta() == 1.0);
  REQUIRE(sketch.get_estimate() == 0.0);
  REQUIRE(sketch.get_lower_bound(1) == 0.0);
  REQUIRE(sketch.get_upper_bound(1) == 0.0);
}

TEST_CASE("theta sketch: wrap compact v2 empty from java", "[theta_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "theta_compact_empty_from_java_v2.sk", std::ios::binary | std::ios::ate);

  std::vector<uint8_t> buf;
  if(is) {
      auto size = is.tellg();
      buf.reserve(size);
      buf.assign(size, 0);
      is.seekg(0, std::ios_base::beg);
      is.read((char*)(buf.data()), buf.size());
  }

  auto sketch = wrapped_compact_theta_sketch::wrap(buf.data(), buf.size());
  REQUIRE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_num_retained() == 0);
  REQUIRE(sketch.get_theta() == 1.0);
  REQUIRE(sketch.get_estimate() == 0.0);
  REQUIRE(sketch.get_lower_bound(1) == 0.0);
  REQUIRE(sketch.get_upper_bound(1) == 0.0);
}

TEST_CASE("theta sketch: wrap single item from java", "[theta_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "theta_compact_single_item_from_java.sk", std::ios::binary | std::ios::ate);
  std::vector<uint8_t> buf;
  if(is) {
      auto size = is.tellg();
      buf.reserve(size);
      buf.assign(size, 0);
      is.seekg(0, std::ios_base::beg);
      is.read((char*)(buf.data()), buf.size());
  }

  auto sketch = wrapped_compact_theta_sketch::wrap(buf.data(), buf.size());
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_num_retained() == 1);
  REQUIRE(sketch.get_theta() == 1.0);
  REQUIRE(sketch.get_estimate() == 1.0);
  REQUIRE(sketch.get_lower_bound(1) == 1.0);
  REQUIRE(sketch.get_upper_bound(1) == 1.0);
}

TEST_CASE("theta sketch: wrap compact estimation from java", "[theta_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "theta_compact_estimation_from_java.sk", std::ios::binary | std::ios::ate);
  std::vector<uint8_t> buf;
  if(is) {
      auto size = is.tellg();
      buf.reserve(size);
      buf.assign(size, 0);
      is.seekg(0, std::ios_base::beg);
      is.read((char*)(buf.data()), buf.size());
  }

  auto sketch = wrapped_compact_theta_sketch::wrap(buf.data(), buf.size());
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.is_estimation_mode());
  REQUIRE(sketch.is_ordered());
  REQUIRE(sketch.get_num_retained() == 4342);
  REQUIRE(sketch.get_theta() == Approx(0.531700444213199).margin(1e-10));
  REQUIRE(sketch.get_estimate() == Approx(8166.25234614053).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(2) == Approx(7996.956955317471).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(2) == Approx(8339.090301078124).margin(1e-10));

  // the same construction process in Java must have produced exactly the same sketch
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  const int n = 8192;
  for (int i = 0; i < n; i++) update_sketch.update(i);
  REQUIRE(sketch.get_num_retained() == update_sketch.get_num_retained());
  REQUIRE(sketch.get_theta() == Approx(update_sketch.get_theta()).margin(1e-10));
  REQUIRE(sketch.get_estimate() == Approx(update_sketch.get_estimate()).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(1) == Approx(update_sketch.get_lower_bound(1)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(1) == Approx(update_sketch.get_upper_bound(1)).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(2) == Approx(update_sketch.get_lower_bound(2)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(2) == Approx(update_sketch.get_upper_bound(2)).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(3) == Approx(update_sketch.get_lower_bound(3)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(3) == Approx(update_sketch.get_upper_bound(3)).margin(1e-10));
  compact_theta_sketch compact_sketch = update_sketch.compact();
  // the sketches are ordered, so the iteration sequence must match exactly
  auto iter = sketch.begin();
  for (const auto& key: compact_sketch) {
    REQUIRE(*iter == key);
    ++iter;
  }
}

TEST_CASE("theta sketch: wrap compact v1 estimation from java", "[theta_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "theta_compact_estimation_from_java_v1.sk", std::ios::binary | std::ios::ate);
  std::vector<uint8_t> buf;
  if(is) {
      auto size = is.tellg();
      buf.reserve(size);
      buf.assign(size, 0);
      is.seekg(0, std::ios_base::beg);
      is.read((char*)(buf.data()), buf.size());
  }

  auto sketch = wrapped_compact_theta_sketch::wrap(buf.data(), buf.size());
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.is_estimation_mode());
//  REQUIRE(sketch.is_ordered());       // v1 may not be ordered
  REQUIRE(sketch.get_num_retained() == 4342);
  REQUIRE(sketch.get_theta() == Approx(0.531700444213199).margin(1e-10));
  REQUIRE(sketch.get_estimate() == Approx(8166.25234614053).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(2) == Approx(7996.956955317471).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(2) == Approx(8339.090301078124).margin(1e-10));

  // the same construction process in Java must have produced exactly the same sketch
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  const int n = 8192;
  for (int i = 0; i < n; i++) update_sketch.update(i);
  REQUIRE(sketch.get_num_retained() == update_sketch.get_num_retained());
  REQUIRE(sketch.get_theta() == Approx(update_sketch.get_theta()).margin(1e-10));
  REQUIRE(sketch.get_estimate() == Approx(update_sketch.get_estimate()).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(1) == Approx(update_sketch.get_lower_bound(1)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(1) == Approx(update_sketch.get_upper_bound(1)).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(2) == Approx(update_sketch.get_lower_bound(2)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(2) == Approx(update_sketch.get_upper_bound(2)).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(3) == Approx(update_sketch.get_lower_bound(3)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(3) == Approx(update_sketch.get_upper_bound(3)).margin(1e-10));
  compact_theta_sketch compact_sketch = update_sketch.compact();
  // the sketches are ordered, so the iteration sequence must match exactly
  auto iter = sketch.begin();
  for (const auto& key: compact_sketch) {
    REQUIRE(*iter == key);
    ++iter;
  }
}

TEST_CASE("theta sketch: wrap compact v2 estimation from java", "[theta_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "theta_compact_estimation_from_java_v2.sk", std::ios::binary | std::ios::ate);
  std::vector<uint8_t> buf;
  if(is) {
      auto size = is.tellg();
      buf.reserve(size);
      buf.assign(size, 0);
      is.seekg(0, std::ios_base::beg);
      is.read((char*)(buf.data()), buf.size());
  }

  auto sketch = wrapped_compact_theta_sketch::wrap(buf.data(), buf.size());
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.is_estimation_mode());
//  REQUIRE(sketch.is_ordered());       // v1 may not be ordered
  REQUIRE(sketch.get_num_retained() == 4342);
  REQUIRE(sketch.get_theta() == Approx(0.531700444213199).margin(1e-10));
  REQUIRE(sketch.get_estimate() == Approx(8166.25234614053).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(2) == Approx(7996.956955317471).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(2) == Approx(8339.090301078124).margin(1e-10));

  // the same construction process in Java must have produced exactly the same sketch
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  const int n = 8192;
  for (int i = 0; i < n; i++) update_sketch.update(i);
  REQUIRE(sketch.get_num_retained() == update_sketch.get_num_retained());
  REQUIRE(sketch.get_theta() == Approx(update_sketch.get_theta()).margin(1e-10));
  REQUIRE(sketch.get_estimate() == Approx(update_sketch.get_estimate()).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(1) == Approx(update_sketch.get_lower_bound(1)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(1) == Approx(update_sketch.get_upper_bound(1)).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(2) == Approx(update_sketch.get_lower_bound(2)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(2) == Approx(update_sketch.get_upper_bound(2)).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(3) == Approx(update_sketch.get_lower_bound(3)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(3) == Approx(update_sketch.get_upper_bound(3)).margin(1e-10));
  compact_theta_sketch compact_sketch = update_sketch.compact();
  // the sketches are ordered, so the iteration sequence must match exactly
  auto iter = sketch.begin();
  for (const auto& key: compact_sketch) {
    REQUIRE(*iter == key);
    ++iter;
  }
}

} /* namespace datasketches */

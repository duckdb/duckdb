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

#include <iostream>
#include <fstream>
#include <sstream>
#include <array>

#include <catch.hpp>
#include <array_of_doubles_sketch.hpp>
#include <array_of_doubles_union.hpp>
#include <array_of_doubles_intersection.hpp>
#include <array_of_doubles_a_not_b.hpp>

namespace datasketches {

#ifdef TEST_BINARY_INPUT_PATH
const std::string inputPath = TEST_BINARY_INPUT_PATH;
#else
const std::string inputPath = "test/";
#endif

TEST_CASE("aod sketch: reset", "[tuple_sketch]") {
  auto update_sketch = update_array_of_doubles_sketch::builder().build();
  std::vector<double> a = {1};
  update_sketch.update(1, a);
  REQUIRE(!update_sketch.is_empty());
  REQUIRE(update_sketch.get_num_retained() == 1);
  update_sketch.reset();
  REQUIRE(update_sketch.is_empty());
  REQUIRE(update_sketch.get_num_retained() == 0);
}

TEST_CASE("aod sketch: serialization compatibility with java - empty", "[tuple_sketch]") {
  auto update_sketch = update_array_of_doubles_sketch::builder().build();
  REQUIRE(update_sketch.is_empty());
  REQUIRE(update_sketch.get_num_retained() == 0);
  auto compact_sketch = update_sketch.compact();

  // read binary sketch from Java
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "aod_1_compact_empty_from_java.sk", std::ios::binary);
  auto compact_sketch_from_java = compact_array_of_doubles_sketch::deserialize(is);
  REQUIRE(compact_sketch.get_num_retained() == compact_sketch_from_java.get_num_retained());
  REQUIRE(compact_sketch.get_theta() == Approx(compact_sketch_from_java.get_theta()).margin(1e-10));
  REQUIRE(compact_sketch.get_estimate() == Approx(compact_sketch_from_java.get_estimate()).margin(1e-10));
  REQUIRE(compact_sketch.get_lower_bound(1) == Approx(compact_sketch_from_java.get_lower_bound(1)).margin(1e-10));
  REQUIRE(compact_sketch.get_upper_bound(1) == Approx(compact_sketch_from_java.get_upper_bound(1)).margin(1e-10));
}

TEST_CASE("aod sketch: serialization compatibility with java - empty configured for three values", "[tuple_sketch]") {
  auto update_sketch = update_array_of_doubles_sketch::builder(3).build();
  REQUIRE(update_sketch.is_empty());
  REQUIRE(update_sketch.get_num_retained() == 0);
  REQUIRE(update_sketch.get_num_values() == 3);
  auto compact_sketch = update_sketch.compact();

  // read binary sketch from Java
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "aod_3_compact_empty_from_java.sk", std::ios::binary);
  auto compact_sketch_from_java = compact_array_of_doubles_sketch::deserialize(is);
  REQUIRE(compact_sketch.get_num_values() == compact_sketch_from_java.get_num_values());
  REQUIRE(compact_sketch.get_num_retained() == compact_sketch_from_java.get_num_retained());
  REQUIRE(compact_sketch.get_theta() == Approx(compact_sketch_from_java.get_theta()).margin(1e-10));
  REQUIRE(compact_sketch.get_estimate() == Approx(compact_sketch_from_java.get_estimate()).margin(1e-10));
  REQUIRE(compact_sketch.get_lower_bound(1) == Approx(compact_sketch_from_java.get_lower_bound(1)).margin(1e-10));
  REQUIRE(compact_sketch.get_upper_bound(1) == Approx(compact_sketch_from_java.get_upper_bound(1)).margin(1e-10));
}

TEST_CASE("aod sketch: serialization compatibility with java - non-empty no entries", "[tuple_sketch]") {
  auto update_sketch = update_array_of_doubles_sketch::builder().set_p(0.01f).build();
  std::vector<double> a = {1};
  update_sketch.update(1, a);
  REQUIRE_FALSE(update_sketch.is_empty());
  REQUIRE(update_sketch.get_num_retained() == 0);
  auto compact_sketch = update_sketch.compact();

  // read binary sketch from Java
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "aod_1_compact_non_empty_no_entries_from_java.sk", std::ios::binary);
  auto compact_sketch_from_java = compact_array_of_doubles_sketch::deserialize(is);
  REQUIRE(compact_sketch.get_num_retained() == compact_sketch_from_java.get_num_retained());
  REQUIRE(compact_sketch.get_theta() == Approx(compact_sketch_from_java.get_theta()).margin(1e-10));
  REQUIRE(compact_sketch.get_estimate() == Approx(compact_sketch_from_java.get_estimate()).margin(1e-10));
  REQUIRE(compact_sketch.get_lower_bound(1) == Approx(compact_sketch_from_java.get_lower_bound(1)).margin(1e-10));
  REQUIRE(compact_sketch.get_upper_bound(1) == Approx(compact_sketch_from_java.get_upper_bound(1)).margin(1e-10));
}

TEST_CASE("aod sketch: serialization compatibility with java - estimation mode", "[tuple_sketch]") {
  auto update_sketch = update_array_of_doubles_sketch::builder().build();
  std::vector<double> a = {1};
  for (int i = 0; i < 8192; ++i) update_sketch.update(i, a);
  auto compact_sketch = update_sketch.compact();

  // read binary sketch from Java
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "aod_1_compact_estimation_from_java.sk", std::ios::binary);
  auto compact_sketch_from_java = compact_array_of_doubles_sketch::deserialize(is);
  REQUIRE(compact_sketch.get_num_retained() == compact_sketch_from_java.get_num_retained());
  REQUIRE(compact_sketch.get_theta() == Approx(compact_sketch_from_java.get_theta()).margin(1e-10));
  REQUIRE(compact_sketch.get_estimate() == Approx(compact_sketch_from_java.get_estimate()).margin(1e-10));
  REQUIRE(compact_sketch.get_lower_bound(1) == Approx(compact_sketch_from_java.get_lower_bound(1)).margin(1e-10));
  REQUIRE(compact_sketch.get_upper_bound(1) == Approx(compact_sketch_from_java.get_upper_bound(1)).margin(1e-10));
  REQUIRE(compact_sketch.get_lower_bound(2) == Approx(compact_sketch_from_java.get_lower_bound(2)).margin(1e-10));
  REQUIRE(compact_sketch.get_upper_bound(2) == Approx(compact_sketch_from_java.get_upper_bound(2)).margin(1e-10));
  REQUIRE(compact_sketch.get_lower_bound(3) == Approx(compact_sketch_from_java.get_lower_bound(3)).margin(1e-10));
  REQUIRE(compact_sketch.get_upper_bound(3) == Approx(compact_sketch_from_java.get_upper_bound(3)).margin(1e-10));

  // sketch from Java is not ordered
  // transform it to ordered so that iteration sequence would match exactly
  compact_array_of_doubles_sketch ordered_sketch_from_java(compact_sketch_from_java, true);
  auto it = ordered_sketch_from_java.begin();
  for (const auto& entry: compact_sketch) {
    REQUIRE(entry == *it);
    ++it;
  }
}

TEST_CASE("aod sketch: serialization compatibility with java - exact mode with two values", "[tuple_sketch]") {
  auto update_sketch = update_array_of_doubles_sketch::builder(2).build();
  std::vector<double> a = {1, 2};
  for (int i = 0; i < 1000; ++i) update_sketch.update(i, a.data()); // pass vector as pointer
  auto compact_sketch = update_sketch.compact();
  REQUIRE_FALSE(compact_sketch.is_estimation_mode());

  // read binary sketch from Java
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "aod_2_compact_exact_from_java.sk", std::ios::binary);
  auto compact_sketch_from_java = compact_array_of_doubles_sketch::deserialize(is);
  REQUIRE(compact_sketch.get_num_retained() == compact_sketch_from_java.get_num_retained());
  REQUIRE(compact_sketch.get_theta() == Approx(compact_sketch_from_java.get_theta()).margin(1e-10));
  REQUIRE(compact_sketch.get_estimate() == Approx(compact_sketch_from_java.get_estimate()).margin(1e-10));
  REQUIRE(compact_sketch.get_lower_bound(1) == Approx(compact_sketch_from_java.get_lower_bound(1)).margin(1e-10));
  REQUIRE(compact_sketch.get_upper_bound(1) == Approx(compact_sketch_from_java.get_upper_bound(1)).margin(1e-10));
  REQUIRE(compact_sketch.get_lower_bound(2) == Approx(compact_sketch_from_java.get_lower_bound(2)).margin(1e-10));
  REQUIRE(compact_sketch.get_upper_bound(2) == Approx(compact_sketch_from_java.get_upper_bound(2)).margin(1e-10));
  REQUIRE(compact_sketch.get_lower_bound(3) == Approx(compact_sketch_from_java.get_lower_bound(3)).margin(1e-10));
  REQUIRE(compact_sketch.get_upper_bound(3) == Approx(compact_sketch_from_java.get_upper_bound(3)).margin(1e-10));

  // sketch from Java is not ordered
  // transform it to ordered so that iteration sequence would match exactly
  compact_array_of_doubles_sketch ordered_sketch_from_java(compact_sketch_from_java, true);
  auto it = ordered_sketch_from_java.begin();
  for (const auto& entry: compact_sketch) {
    REQUIRE(entry.first == (*it).first);
    REQUIRE(entry.second.size() == 2);
    REQUIRE(entry.second[0] == (*it).second[0]);
    REQUIRE(entry.second[1] == (*it).second[1]);
    ++it;
  }
}

TEST_CASE("aod sketch: stream serialize deserialize - estimation mode", "[tuple_sketch]") {
  auto update_sketch = update_array_of_doubles_sketch::builder(2).build();
  std::vector<double> a = {1, 2};
  for (int i = 0; i < 8192; ++i) update_sketch.update(i, a);
  auto compact_sketch = update_sketch.compact();

  std::stringstream ss;
  ss.exceptions(std::ios::failbit | std::ios::badbit);
  compact_sketch.serialize(ss);
  auto deserialized_sketch = compact_array_of_doubles_sketch::deserialize(ss);
  REQUIRE(compact_sketch.get_num_retained() == deserialized_sketch.get_num_retained());
  REQUIRE(compact_sketch.get_theta() == Approx(deserialized_sketch.get_theta()).margin(1e-10));
  REQUIRE(compact_sketch.get_estimate() == Approx(deserialized_sketch.get_estimate()).margin(1e-10));
  REQUIRE(compact_sketch.get_lower_bound(1) == Approx(deserialized_sketch.get_lower_bound(1)).margin(1e-10));
  REQUIRE(compact_sketch.get_upper_bound(1) == Approx(deserialized_sketch.get_upper_bound(1)).margin(1e-10));
  REQUIRE(compact_sketch.get_lower_bound(2) == Approx(deserialized_sketch.get_lower_bound(2)).margin(1e-10));
  REQUIRE(compact_sketch.get_upper_bound(2) == Approx(deserialized_sketch.get_upper_bound(2)).margin(1e-10));
  REQUIRE(compact_sketch.get_lower_bound(3) == Approx(deserialized_sketch.get_lower_bound(3)).margin(1e-10));
  REQUIRE(compact_sketch.get_upper_bound(3) == Approx(deserialized_sketch.get_upper_bound(3)).margin(1e-10));
  // sketches must be ordered and the iteration sequence must match exactly
  auto it = deserialized_sketch.begin();
  for (const auto& entry: compact_sketch) {
    REQUIRE(entry.first == (*it).first);
    REQUIRE(entry.second.size() == 2);
    REQUIRE(entry.second[0] == (*it).second[0]);
    REQUIRE(entry.second[1] == (*it).second[1]);
    ++it;
  }
}

TEST_CASE("aod sketch: bytes to stream serialize deserialize - estimation mode", "[tuple_sketch]") {
  auto update_sketch = update_array_of_doubles_sketch::builder(2).build();
  std::vector<double> a = {1, 2};
  for (int i = 0; i < 8192; ++i) update_sketch.update(i, a);
  auto compact_sketch = update_sketch.compact();

  auto bytes = compact_sketch.serialize();
  std::stringstream ss;
  ss.exceptions(std::ios::failbit | std::ios::badbit);
  ss.write(reinterpret_cast<const char*>(bytes.data()), bytes.size());
  auto deserialized_sketch = compact_array_of_doubles_sketch::deserialize(ss);
  REQUIRE(compact_sketch.get_num_retained() == deserialized_sketch.get_num_retained());
  REQUIRE(compact_sketch.get_theta() == Approx(deserialized_sketch.get_theta()).margin(1e-10));
  REQUIRE(compact_sketch.get_estimate() == Approx(deserialized_sketch.get_estimate()).margin(1e-10));
  REQUIRE(compact_sketch.get_lower_bound(1) == Approx(deserialized_sketch.get_lower_bound(1)).margin(1e-10));
  REQUIRE(compact_sketch.get_upper_bound(1) == Approx(deserialized_sketch.get_upper_bound(1)).margin(1e-10));
  REQUIRE(compact_sketch.get_lower_bound(2) == Approx(deserialized_sketch.get_lower_bound(2)).margin(1e-10));
  REQUIRE(compact_sketch.get_upper_bound(2) == Approx(deserialized_sketch.get_upper_bound(2)).margin(1e-10));
  REQUIRE(compact_sketch.get_lower_bound(3) == Approx(deserialized_sketch.get_lower_bound(3)).margin(1e-10));
  REQUIRE(compact_sketch.get_upper_bound(3) == Approx(deserialized_sketch.get_upper_bound(3)).margin(1e-10));
  // sketches must be ordered and the iteration sequence must match exactly
  auto it = deserialized_sketch.begin();
  for (const auto& entry: compact_sketch) {
    REQUIRE(entry.first == (*it).first);
    REQUIRE(entry.second.size() == 2);
    REQUIRE(entry.second[0] == (*it).second[0]);
    REQUIRE(entry.second[1] == (*it).second[1]);
    ++it;
  }
}

TEST_CASE("aod sketch: bytes serialize deserialize - estimation mode", "[tuple_sketch]") {
  auto update_sketch = update_array_of_doubles_sketch::builder(2).build();
  std::vector<double> a = {1, 2};
  for (int i = 0; i < 8192; ++i) update_sketch.update(i, a);
  auto compact_sketch = update_sketch.compact();

  auto bytes = compact_sketch.serialize();
  auto deserialized_sketch = compact_array_of_doubles_sketch::deserialize(bytes.data(), bytes.size());
  REQUIRE(compact_sketch.get_num_retained() == deserialized_sketch.get_num_retained());
  REQUIRE(compact_sketch.get_theta() == Approx(deserialized_sketch.get_theta()).margin(1e-10));
  REQUIRE(compact_sketch.get_estimate() == Approx(deserialized_sketch.get_estimate()).margin(1e-10));
  REQUIRE(compact_sketch.get_lower_bound(1) == Approx(deserialized_sketch.get_lower_bound(1)).margin(1e-10));
  REQUIRE(compact_sketch.get_upper_bound(1) == Approx(deserialized_sketch.get_upper_bound(1)).margin(1e-10));
  REQUIRE(compact_sketch.get_lower_bound(2) == Approx(deserialized_sketch.get_lower_bound(2)).margin(1e-10));
  REQUIRE(compact_sketch.get_upper_bound(2) == Approx(deserialized_sketch.get_upper_bound(2)).margin(1e-10));
  REQUIRE(compact_sketch.get_lower_bound(3) == Approx(deserialized_sketch.get_lower_bound(3)).margin(1e-10));
  REQUIRE(compact_sketch.get_upper_bound(3) == Approx(deserialized_sketch.get_upper_bound(3)).margin(1e-10));
  // sketches must be ordered and the iteration sequence must match exactly
  auto it = deserialized_sketch.begin();
  for (const auto& entry: compact_sketch) {
    REQUIRE(entry.first == (*it).first);
    REQUIRE(entry.second.size() == 2);
    REQUIRE(entry.second[0] == (*it).second[0]);
    REQUIRE(entry.second[1] == (*it).second[1]);
    ++it;
  }
}

TEST_CASE("aod union: half overlap", "[tuple_sketch]") {
  std::vector<double> a = {1};

  auto update_sketch1 = update_array_of_doubles_sketch::builder().build();
  for (int i = 0; i < 1000; ++i) update_sketch1.update(i, a);

  auto update_sketch2 = update_array_of_doubles_sketch::builder().build();
  for (int i = 500; i < 1500; ++i) update_sketch2.update(i, a);

  auto u = array_of_doubles_union::builder().build();
  u.update(update_sketch1);
  u.update(update_sketch2);
  auto result = u.get_result();
  REQUIRE(result.get_estimate() == Approx(1500).margin(0.01));

  u.reset();
  result = u.get_result();
  REQUIRE(result.is_empty());
  REQUIRE(result.get_num_retained() == 0);
}

TEST_CASE("aod intersection: half overlap", "[tuple_sketch]") {
  std::vector<double> a = {1};

  auto update_sketch1 = update_array_of_doubles_sketch::builder().build();
  for (int i = 0; i < 1000; ++i) update_sketch1.update(i, a);

  auto update_sketch2 = update_array_of_doubles_sketch::builder().build();
  for (int i = 500; i < 1500; ++i) update_sketch2.update(i, a);

  array_of_doubles_intersection<array_of_doubles_union_policy> intersection;
  intersection.update(update_sketch1);
  intersection.update(update_sketch2);
  auto result = intersection.get_result();
  REQUIRE(result.get_estimate() == Approx(500).margin(0.01));
}

TEST_CASE("aod a-not-b: half overlap", "[tuple_sketch]") {
  double a[1] = {1};

  auto update_sketch1 = update_array_of_doubles_sketch::builder().build();
  for (int i = 0; i < 1000; ++i) update_sketch1.update(i, a);

  auto update_sketch2 = update_array_of_doubles_sketch::builder().build();
  for (int i = 500; i < 1500; ++i) update_sketch2.update(i, a);

  array_of_doubles_a_not_b a_not_b;
  auto result = a_not_b.compute(update_sketch1, update_sketch2);
  REQUIRE(result.get_estimate() == Approx(500).margin(0.01));
}

} /* namespace datasketches */

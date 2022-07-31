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
#include <stdexcept>

#include <catch.hpp>
#include <tuple_union.hpp>
#include <theta_sketch.hpp>

namespace datasketches {

TEST_CASE("tuple_union float: empty", "[tuple union]") {
  auto update_sketch = update_tuple_sketch<float>::builder().build();
  auto u = tuple_union<float>::builder().build();
  u.update(update_sketch);
  auto result = u.get_result();
//  std::cout << result.to_string(true);
  REQUIRE(result.is_empty());
  REQUIRE(result.get_num_retained() == 0);
  REQUIRE(!result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 0);
}

TEST_CASE("tupe_union float: empty theta sketch", "[tuple union]") {
  auto update_sketch = update_theta_sketch::builder().build();

  auto u = tuple_union<float>::builder().build();
  u.update(compact_tuple_sketch<float>(update_sketch, 0));
  auto result = u.get_result();
//  std::cout << result.to_string(true);
  REQUIRE(result.is_empty());
  REQUIRE(result.get_num_retained() == 0);
  REQUIRE(!result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 0);
}

TEST_CASE("tuple_union float: non-empty no retained entries", "[tuple union]") {
  auto update_sketch = update_tuple_sketch<float>::builder().set_p(0.001f).build();
//  std::cout << update_sketch.to_string();
  update_sketch.update(1, 1.0f);
  REQUIRE(!update_sketch.is_empty());
  REQUIRE(update_sketch.get_num_retained() == 0);
  auto u = tuple_union<float>::builder().build();
  u.update(update_sketch);
  auto result = u.get_result();
//  std::cout << result.to_string(true);
  REQUIRE(!result.is_empty());
  REQUIRE(result.get_num_retained() == 0);
  REQUIRE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 0);
  REQUIRE(result.get_theta() == Approx(0.001).margin(1e-10));
}

TEST_CASE("tuple_union float: simple case", "[tuple union]") {
  auto update_sketch1 = update_tuple_sketch<float>::builder().build();
  update_sketch1.update(1, 1.0f);
  update_sketch1.update(2, 1.0f);

  auto update_sketch2 = update_tuple_sketch<float>::builder().build();
  update_sketch2.update(1, 1.0f);
  update_sketch2.update(3, 1.0f);

  auto u = tuple_union<float>::builder().build();
  u.update(update_sketch1);
  u.update(update_sketch2);
  auto result = u.get_result();
  REQUIRE(result.get_num_retained() == 3);

  u.reset();
  result = u.get_result();
  REQUIRE(result.is_empty());
  REQUIRE(result.get_num_retained() == 0);
  REQUIRE(!result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 0);
}

TEST_CASE("tuple_union float: exact mode half overlap", "[tuple union]") {
  auto update_sketch1 = update_tuple_sketch<float>::builder().build();
  int value = 0;
  for (int i = 0; i < 1000; ++i) update_sketch1.update(value++, 1.0f);

  auto update_sketch2 = update_tuple_sketch<float>::builder().build();
  value = 500;
  for (int i = 0; i < 1000; ++i) update_sketch2.update(value++, 1.0f);

  { // unordered
    auto u = tuple_union<float>::builder().build();
    u.update(update_sketch1);
    u.update(update_sketch2);
    auto result = u.get_result();
    REQUIRE(!result.is_empty());
    REQUIRE(!result.is_estimation_mode());
    REQUIRE(result.get_estimate() == Approx(1500).margin(1500 * 0.01));
  }
  { // ordered
    auto u = tuple_union<float>::builder().build();
    u.update(update_sketch1.compact());
    u.update(update_sketch2.compact());
    auto result = u.get_result();
    REQUIRE(!result.is_empty());
    REQUIRE(!result.is_estimation_mode());
    REQUIRE(result.get_estimate() == Approx(1500).margin(1500 * 0.01));
  }
}

TEST_CASE("tuple_union float: estimation mode half overlap", "[tuple union]") {
  auto update_sketch1 = update_tuple_sketch<float>::builder().build();
  int value = 0;
  for (int i = 0; i < 10000; ++i) update_sketch1.update(value++, 1.0f);

  auto update_sketch2 = update_tuple_sketch<float>::builder().build();
  value = 5000;
  for (int i = 0; i < 10000; ++i) update_sketch2.update(value++, 1.0f);

  { // unordered
    auto u = tuple_union<float>::builder().build();
    u.update(update_sketch1);
    u.update(update_sketch2);
    auto result = u.get_result();
    REQUIRE(!result.is_empty());
    REQUIRE(result.is_estimation_mode());
    REQUIRE(result.get_estimate() == Approx(15000).margin(15000 * 0.01));
  }
  { // ordered
    auto u = tuple_union<float>::builder().build();
    u.update(update_sketch1.compact());
    u.update(update_sketch2.compact());
    auto result = u.get_result();
    REQUIRE(!result.is_empty());
    REQUIRE(result.is_estimation_mode());
    REQUIRE(result.get_estimate() == Approx(15000).margin(15000 * 0.01));
  }
}

TEST_CASE("tuple_union float: seed mismatch", "[tuple union]") {
  auto update_sketch = update_tuple_sketch<float>::builder().build();
  update_sketch.update(1, 1.0f); // non-empty should not be ignored

  auto u = tuple_union<float>::builder().set_seed(123).build();
  REQUIRE_THROWS_AS(u.update(update_sketch), std::invalid_argument);
}

TEST_CASE("tuple_union float: full overlap with theta sketch", "[tuple union]") {
  auto u = tuple_union<float>::builder().build();

  // tuple update
  auto update_tuple = update_tuple_sketch<float>::builder().build();
  for (unsigned i = 0; i < 10; ++i) update_tuple.update(i, 1.0f);
  u.update(update_tuple);

  // tuple compact
  auto compact_tuple = update_tuple.compact();
  u.update(compact_tuple);

  // theta update
  auto update_theta = update_theta_sketch::builder().build();
  for (unsigned i = 0; i < 10; ++i) update_theta.update(i);
  u.update(compact_tuple_sketch<float>(update_theta, 1));

  // theta compact
  auto compact_theta = update_theta.compact();
  u.update(compact_tuple_sketch<float>(compact_theta, 1));

  auto result = u.get_result();
//  std::cout << result.to_string(true);
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.get_num_retained() == 10);
  REQUIRE(!result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 10);
  for (const auto& entry: result) {
    REQUIRE(entry.second == 4);
  }
}

} /* namespace datasketches */

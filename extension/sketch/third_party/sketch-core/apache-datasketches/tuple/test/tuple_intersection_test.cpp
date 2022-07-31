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

#include <catch.hpp>
#include <tuple_intersection.hpp>
#include <theta_sketch.hpp>
#include <stdexcept>

namespace datasketches {

template<typename Summary>
struct subtracting_intersection_policy {
  void operator()(Summary& summary, const Summary& other) const {
    summary -= other;
  }
};

using tuple_intersection_float = tuple_intersection<float, subtracting_intersection_policy<float>>;

TEST_CASE("tuple intersection: invalid", "[tuple_intersection]") {
  tuple_intersection_float intersection;
  REQUIRE_FALSE(intersection.has_result());
  REQUIRE_THROWS_AS(intersection.get_result(), std::invalid_argument);
}

TEST_CASE("tuple intersection: empty", "[tuple_intersection]") {
  auto sketch = update_tuple_sketch<float>::builder().build();
  tuple_intersection_float intersection;
  intersection.update(sketch);
  auto result = intersection.get_result();
  REQUIRE(result.get_num_retained() == 0);
  REQUIRE(result.is_empty());
  REQUIRE_FALSE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 0.0);

  intersection.update(sketch);
  result = intersection.get_result();
  REQUIRE(result.get_num_retained() == 0);
  REQUIRE(result.is_empty());
  REQUIRE_FALSE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 0.0);
}

TEST_CASE("tuple intersection: non empty no retained keys", "[tuple_intersection]") {
  auto sketch = update_tuple_sketch<float>::builder().set_p(0.001f).build();
  sketch.update(1, 1.0f);
  tuple_intersection_float intersection;
  intersection.update(sketch);
  auto result = intersection.get_result();
  REQUIRE(result.get_num_retained() == 0);
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.is_estimation_mode());
  REQUIRE(result.get_theta() == Approx(0.001).margin(1e-10));
  REQUIRE(result.get_estimate() == 0.0);

  intersection.update(sketch);
  result = intersection.get_result();
  REQUIRE(result.get_num_retained() == 0);
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.is_estimation_mode());
  REQUIRE(result.get_theta() == Approx(0.001).margin(1e-10));
  REQUIRE(result.get_estimate() == 0.0);
}

TEST_CASE("tuple intersection: exact mode half overlap", "[tuple_intersection]") {
  auto sketch1 = update_tuple_sketch<float>::builder().build();
  int value = 0;
  for (int i = 0; i < 1000; i++) sketch1.update(value++, 1.0f);

  auto sketch2 = update_tuple_sketch<float>::builder().build();
  value = 500;
  for (int i = 0; i < 1000; i++) sketch2.update(value++, 1.0f);

  { // unordered
    tuple_intersection_float intersection;
    intersection.update(sketch1);
    intersection.update(sketch2);
    auto result = intersection.get_result();
    REQUIRE_FALSE(result.is_empty());
    REQUIRE_FALSE(result.is_estimation_mode());
    REQUIRE(result.get_estimate() == 500.0);
  }
  { // ordered
    tuple_intersection_float intersection;
    intersection.update(sketch1.compact());
    intersection.update(sketch2.compact());
    auto result = intersection.get_result();
    REQUIRE_FALSE(result.is_empty());
    REQUIRE_FALSE(result.is_estimation_mode());
    REQUIRE(result.get_estimate() == 500.0);
  }
}

TEST_CASE("tuple intersection: exact mode disjoint", "[tuple_intersection]") {
  auto sketch1 = update_tuple_sketch<float>::builder().build();
  int value = 0;
  for (int i = 0; i < 1000; i++) sketch1.update(value++, 1.0f);

  auto sketch2 = update_tuple_sketch<float>::builder().build();
  for (int i = 0; i < 1000; i++) sketch2.update(value++, 1.0f);

  { // unordered
    tuple_intersection_float intersection;
    intersection.update(sketch1);
    intersection.update(sketch2);
    auto result = intersection.get_result();
    REQUIRE(result.is_empty());
    REQUIRE_FALSE(result.is_estimation_mode());
    REQUIRE(result.get_estimate() == 0.0);
  }
  { // ordered
    tuple_intersection_float intersection;
    intersection.update(sketch1.compact());
    intersection.update(sketch2.compact());
    auto result = intersection.get_result();
    REQUIRE(result.is_empty());
    REQUIRE_FALSE(result.is_estimation_mode());
    REQUIRE(result.get_estimate() == 0.0);
  }
}

TEST_CASE("mixed intersection: exact mode half overlap", "[tuple_intersection]") {
  auto sketch1 = update_tuple_sketch<float>::builder().build();
  int value = 0;
  for (int i = 0; i < 1000; i++) sketch1.update(value++, 1.0f);

  auto sketch2 = update_theta_sketch::builder().build();
  value = 500;
  for (int i = 0; i < 1000; i++) sketch2.update(value++);

  { // unordered
    tuple_intersection_float intersection;
    intersection.update(sketch1);
    intersection.update(compact_tuple_sketch<float>(sketch2, 1, false));
    auto result = intersection.get_result();
    REQUIRE_FALSE(result.is_empty());
    REQUIRE_FALSE(result.is_estimation_mode());
    REQUIRE(result.get_estimate() == 500.0);
  }
  { // ordered
    tuple_intersection_float intersection;
    intersection.update(sketch1.compact());
    intersection.update(compact_tuple_sketch<float>(sketch2.compact(), 1));
    auto result = intersection.get_result();
    REQUIRE_FALSE(result.is_empty());
    REQUIRE_FALSE(result.is_estimation_mode());
    REQUIRE(result.get_estimate() == 500.0);
  }
}

TEST_CASE("tuple intersection: estimation mode half overlap", "[tuple_intersection]") {
  auto sketch1 = update_tuple_sketch<float>::builder().build();
  int value = 0;
  for (int i = 0; i < 10000; i++) sketch1.update(value++, 1.0f);

  auto sketch2 = update_tuple_sketch<float>::builder().build();
  value = 5000;
  for (int i = 0; i < 10000; i++) sketch2.update(value++, 1.0f);

  { // unordered
    tuple_intersection_float intersection;
    intersection.update(sketch1);
    intersection.update(sketch2);
    auto result = intersection.get_result();
    REQUIRE_FALSE(result.is_empty());
    REQUIRE(result.is_estimation_mode());
    REQUIRE(result.get_estimate() == Approx(5000).margin(5000 * 0.02));
  }
  { // ordered
    tuple_intersection_float intersection;
    intersection.update(sketch1.compact());
    intersection.update(sketch2.compact());
    auto result = intersection.get_result();
    REQUIRE_FALSE(result.is_empty());
    REQUIRE(result.is_estimation_mode());
    REQUIRE(result.get_estimate() == Approx(5000).margin(5000 * 0.02));
  }
}

TEST_CASE("tuple intersection: estimation mode disjoint", "[tuple_intersection]") {
  auto sketch1 = update_tuple_sketch<float>::builder().build();
  int value = 0;
  for (int i = 0; i < 10000; i++) sketch1.update(value++, 1.0f);

  auto sketch2 = update_tuple_sketch<float>::builder().build();
  for (int i = 0; i < 10000; i++) sketch2.update(value++, 1.0f);

  { // unordered
    tuple_intersection_float intersection;
    intersection.update(sketch1);
    intersection.update(sketch2);
    auto result = intersection.get_result();
    REQUIRE_FALSE(result.is_empty());
    REQUIRE(result.is_estimation_mode());
    REQUIRE(result.get_estimate() == 0.0);
  }
  { // ordered
    tuple_intersection_float intersection;
    intersection.update(sketch1.compact());
    intersection.update(sketch2.compact());
    auto result = intersection.get_result();
    REQUIRE_FALSE(result.is_empty());
    REQUIRE(result.is_estimation_mode());
    REQUIRE(result.get_estimate() == 0.0);
  }
}

TEST_CASE("tuple intersection: seed mismatch", "[tuple_intersection]") {
  auto sketch = update_tuple_sketch<float>::builder().build();
  sketch.update(1, 1.0f); // non-empty should not be ignored
  tuple_intersection_float intersection(123);
  REQUIRE_THROWS_AS(intersection.update(sketch), std::invalid_argument);
}

} /* namespace datasketches */

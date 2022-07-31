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
#include <tuple_a_not_b.hpp>
#include <theta_sketch.hpp>
#include <stdexcept>

namespace datasketches {

TEST_CASE("tuple a-not-b: empty", "[tuple_a_not_b]") {
  auto a = update_tuple_sketch<float>::builder().build();
  auto b = update_tuple_sketch<float>::builder().build();
  tuple_a_not_b<float> a_not_b;
  auto result = a_not_b.compute(a, b);
  REQUIRE(result.get_num_retained() == 0);
  REQUIRE(result.is_empty());
  REQUIRE_FALSE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 0.0);
}

TEST_CASE("tuple a-not-b: non empty no retained keys", "[tuple_a_not_b]") {
  auto a = update_tuple_sketch<float>::builder().build();
  a.update(1, 1.0f);
  auto b = update_tuple_sketch<float>::builder().set_p(0.001f).build();
  tuple_a_not_b<float> a_not_b;

  // B is still empty
  auto result = a_not_b.compute(a, b);
  REQUIRE_FALSE(result.is_empty());
  REQUIRE_FALSE(result.is_estimation_mode());
  REQUIRE(result.get_num_retained() == 1);
  REQUIRE(result.get_theta() == Approx(1).margin(1e-10));
  REQUIRE(result.get_estimate() == 1.0);

  // B is not empty in estimation mode and no entries
  b.update(1, 1.0f);
  REQUIRE(b.get_num_retained() == 0);

  result = a_not_b.compute(a, b);
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.is_estimation_mode());
  REQUIRE(result.get_num_retained() == 0);
  REQUIRE(result.get_theta() == Approx(0.001).margin(1e-10));
  REQUIRE(result.get_estimate() == 0.0);
}

TEST_CASE("tuple a-not-b: exact mode half overlap", "[tuple_a_not_b]") {
  auto a = update_tuple_sketch<float>::builder().build();
  int value = 0;
  for (int i = 0; i < 1000; i++) a.update(value++, 1.0f);

  auto b = update_tuple_sketch<float>::builder().build();
  value = 500;
  for (int i = 0; i < 1000; i++) b.update(value++, 1.0f);

  tuple_a_not_b<float> a_not_b;

  // unordered inputs, ordered result
  auto result = a_not_b.compute(a, b);
  REQUIRE_FALSE(result.is_empty());
  REQUIRE_FALSE(result.is_estimation_mode());
  REQUIRE(result.is_ordered());
  REQUIRE(result.get_estimate() == 500.0);

  // unordered inputs, unordered result
  result = a_not_b.compute(a, b, false);
  REQUIRE_FALSE(result.is_empty());
  REQUIRE_FALSE(result.is_estimation_mode());
  REQUIRE_FALSE(result.is_ordered());
  REQUIRE(result.get_estimate() == 500.0);

  // ordered inputs
  result = a_not_b.compute(a.compact(), b.compact());
  REQUIRE_FALSE(result.is_empty());
  REQUIRE_FALSE(result.is_estimation_mode());
  REQUIRE(result.is_ordered());
  REQUIRE(result.get_estimate() == 500.0);

  // A is ordered, so the result is ordered regardless
  result = a_not_b.compute(a.compact(), b, false);
  REQUIRE_FALSE(result.is_empty());
  REQUIRE_FALSE(result.is_estimation_mode());
  REQUIRE(result.is_ordered());
  REQUIRE(result.get_estimate() == 500.0);
}

TEST_CASE("mixed a-not-b: exact mode half overlap", "[tuple_a_not_b]") {
  auto a = update_tuple_sketch<float>::builder().build();
  int value = 0;
  for (int i = 0; i < 1000; i++) a.update(value++, 1.0f);

  auto b = update_theta_sketch::builder().build();
  value = 500;
  for (int i = 0; i < 1000; i++) b.update(value++);

  tuple_a_not_b<float> a_not_b;

  // unordered inputs, ordered result
  auto result = a_not_b.compute(a, compact_tuple_sketch<float>(b, 1, false));
  REQUIRE_FALSE(result.is_empty());
  REQUIRE_FALSE(result.is_estimation_mode());
  REQUIRE(result.is_ordered());
  REQUIRE(result.get_estimate() == 500.0);

  // unordered inputs, unordered result
  result = a_not_b.compute(a, compact_tuple_sketch<float>(b, 1, false), false);
  REQUIRE_FALSE(result.is_empty());
  REQUIRE_FALSE(result.is_estimation_mode());
  REQUIRE_FALSE(result.is_ordered());
  REQUIRE(result.get_estimate() == 500.0);

  // ordered inputs
  result = a_not_b.compute(a.compact(), compact_tuple_sketch<float>(b.compact(), 1));
  REQUIRE_FALSE(result.is_empty());
  REQUIRE_FALSE(result.is_estimation_mode());
  REQUIRE(result.is_ordered());
  REQUIRE(result.get_estimate() == 500.0);

  // A is ordered, so the result is ordered regardless
  result = a_not_b.compute(a.compact(), compact_tuple_sketch<float>(b, 1, false), false);
  REQUIRE_FALSE(result.is_empty());
  REQUIRE_FALSE(result.is_estimation_mode());
  REQUIRE(result.is_ordered());
  REQUIRE(result.get_estimate() == 500.0);
}

TEST_CASE("tuple a-not-b: exact mode disjoint", "[tuple_a_not_b]") {
  auto a = update_tuple_sketch<float>::builder().build();
  int value = 0;
  for (int i = 0; i < 1000; i++) a.update(value++, 1.0f);

  auto b = update_tuple_sketch<float>::builder().build();
  for (int i = 0; i < 1000; i++) b.update(value++, 1.0f);

  tuple_a_not_b<float> a_not_b;

  // unordered inputs
  auto result = a_not_b.compute(a, b);
  REQUIRE_FALSE(result.is_empty());
  REQUIRE_FALSE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 1000.0);

  // ordered inputs
  result = a_not_b.compute(a.compact(), b.compact());
  REQUIRE_FALSE(result.is_empty());
  REQUIRE_FALSE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 1000.0);
}

TEST_CASE("tuple a-not-b: exact mode full overlap", "[tuple_a_not_b]") {
  auto sketch = update_tuple_sketch<float>::builder().build();
  int value = 0;
  for (int i = 0; i < 1000; i++) sketch.update(value++, 1.0f);

  tuple_a_not_b<float> a_not_b;

  // unordered inputs
  auto result = a_not_b.compute(sketch, sketch);
  REQUIRE(result.is_empty());
  REQUIRE_FALSE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 0.0);

  // ordered inputs
  result = a_not_b.compute(sketch.compact(), sketch.compact());
  REQUIRE(result.is_empty());
  REQUIRE_FALSE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 0.0);
}

TEST_CASE("tuple a-not-b: estimation mode half overlap", "[tuple_a_not_b]") {
  auto a = update_tuple_sketch<float>::builder().build();
  int value = 0;
  for (int i = 0; i < 10000; i++) a.update(value++, 1.0f);

  auto b = update_tuple_sketch<float>::builder().build();
  value = 5000;
  for (int i = 0; i < 10000; i++) b.update(value++, 1.0f);

  tuple_a_not_b<float> a_not_b;

  // unordered inputs
  auto result = a_not_b.compute(a, b);
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == Approx(5000).margin(5000 * 0.02));

  // ordered inputs
  result = a_not_b.compute(a.compact(), b.compact());
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == Approx(5000).margin(5000 * 0.02));
}

TEST_CASE("tuple a-not-b: estimation mode disjoint", "[tuple_a_not_b]") {
  auto a = update_tuple_sketch<float>::builder().build();
  int value = 0;
  for (int i = 0; i < 10000; i++) a.update(value++, 1.0f);

  auto b = update_tuple_sketch<float>::builder().build();
  for (int i = 0; i < 10000; i++) b.update(value++, 1.0f);

  tuple_a_not_b<float> a_not_b;

  // unordered inputs
  auto result = a_not_b.compute(a, b);
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == Approx(10000).margin(10000 * 0.02));

  // ordered inputs
  result = a_not_b.compute(a.compact(), b.compact());
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == Approx(10000).margin(10000 * 0.02));
}

TEST_CASE("tuple a-not-b: estimation mode full overlap", "[tuple_a_not_b]") {
  auto sketch = update_tuple_sketch<float>::builder().build();
  int value = 0;
  for (int i = 0; i < 10000; i++) sketch.update(value++, 1.0f);

  tuple_a_not_b<float> a_not_b;

  // unordered inputs
  auto result = a_not_b.compute(sketch, sketch);
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 0.0);

  // ordered inputs
  result = a_not_b.compute(sketch.compact(), sketch.compact());
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 0.0);
}

TEST_CASE("tuple a-not-b: seed mismatch", "[tuple_a_not_b]") {
  auto sketch = update_tuple_sketch<float>::builder().build();
  sketch.update(1, 1.0f); // non-empty should not be ignored
  tuple_a_not_b<float> a_not_b(123);
  REQUIRE_THROWS_AS(a_not_b.compute(sketch, sketch), std::invalid_argument);
}

TEST_CASE("tuple a-not-b: issue #152", "[tuple_a_not_b]") {
  auto a = update_tuple_sketch<float>::builder().build();
  int value = 0;
  for (int i = 0; i < 10000; i++) a.update(value++, 1.0f);

  auto b = update_tuple_sketch<float>::builder().build();
  value = 5000;
  for (int i = 0; i < 25000; i++) b.update(value++, 1.0f);

  tuple_a_not_b<float> a_not_b;

  // unordered inputs
  auto result = a_not_b.compute(a, b);
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == Approx(5000).margin(5000 * 0.03));

  // ordered inputs
  result = a_not_b.compute(a.compact(), b.compact());
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == Approx(5000).margin(5000 * 0.03));
}

} /* namespace datasketches */

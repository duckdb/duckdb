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

#include <theta_intersection.hpp>

#include <stdexcept>

namespace datasketches {

TEST_CASE("theta intersection: invalid", "[theta_intersection]") {
  theta_intersection intersection;
  REQUIRE_FALSE(intersection.has_result());
  REQUIRE_THROWS_AS(intersection.get_result(), std::invalid_argument);
}

TEST_CASE("theta intersection: empty", "[theta_intersection]") {
  theta_intersection intersection;
  update_theta_sketch sketch = update_theta_sketch::builder().build();
  intersection.update(sketch);
  compact_theta_sketch result = intersection.get_result();
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

TEST_CASE("theta intersection: non empty no retained keys", "[theta_intersection]") {
  update_theta_sketch sketch = update_theta_sketch::builder().set_p(0.001f).build();
  sketch.update(1);
  theta_intersection intersection;
  intersection.update(sketch);
  compact_theta_sketch result = intersection.get_result();
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

TEST_CASE("theta intersection: exact mode half overlap unordered", "[theta_intersection]") {
  update_theta_sketch sketch1 = update_theta_sketch::builder().build();
  int value = 0;
  for (int i = 0; i < 1000; i++) sketch1.update(value++);

  update_theta_sketch sketch2 = update_theta_sketch::builder().build();
  value = 500;
  for (int i = 0; i < 1000; i++) sketch2.update(value++);

  theta_intersection intersection;
  intersection.update(sketch1);
  intersection.update(sketch2);
  compact_theta_sketch result = intersection.get_result();
  REQUIRE_FALSE(result.is_empty());
  REQUIRE_FALSE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 500.0);
}

TEST_CASE("theta intersection: exact mode half overlap ordered", "[theta_intersection]") {
  update_theta_sketch sketch1 = update_theta_sketch::builder().build();
  int value = 0;
  for (int i = 0; i < 1000; i++) sketch1.update(value++);

  update_theta_sketch sketch2 = update_theta_sketch::builder().build();
  value = 500;
  for (int i = 0; i < 1000; i++) sketch2.update(value++);

  theta_intersection intersection;
  intersection.update(sketch1.compact());
  intersection.update(sketch2.compact());
  compact_theta_sketch result = intersection.get_result();
  REQUIRE_FALSE(result.is_empty());
  REQUIRE_FALSE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 500.0);
}

TEST_CASE("theta intersection: exact mode disjoint unordered", "[theta_intersection]") {
  update_theta_sketch sketch1 = update_theta_sketch::builder().build();
  int value = 0;
  for (int i = 0; i < 1000; i++) sketch1.update(value++);

  update_theta_sketch sketch2 = update_theta_sketch::builder().build();
  for (int i = 0; i < 1000; i++) sketch2.update(value++);

  theta_intersection intersection;
  intersection.update(sketch1);
  intersection.update(sketch2);
  compact_theta_sketch result = intersection.get_result();
  REQUIRE(result.is_empty());
  REQUIRE_FALSE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 0.0);
}

TEST_CASE("theta intersection: exact mode disjoint ordered", "[theta_intersection]") {
  update_theta_sketch sketch1 = update_theta_sketch::builder().build();
  int value = 0;
  for (int i = 0; i < 1000; i++) sketch1.update(value++);

  update_theta_sketch sketch2 = update_theta_sketch::builder().build();
  for (int i = 0; i < 1000; i++) sketch2.update(value++);

  theta_intersection intersection;
  intersection.update(sketch1.compact());
  intersection.update(sketch2.compact());
  compact_theta_sketch result = intersection.get_result();
  REQUIRE(result.is_empty());
  REQUIRE_FALSE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 0.0);
}

TEST_CASE("theta intersection: estimation mode half overlap unordered", "[theta_intersection]") {
  update_theta_sketch sketch1 = update_theta_sketch::builder().build();
  int value = 0;
  for (int i = 0; i < 10000; i++) sketch1.update(value++);

  update_theta_sketch sketch2 = update_theta_sketch::builder().build();
  value = 5000;
  for (int i = 0; i < 10000; i++) sketch2.update(value++);

  theta_intersection intersection;
  intersection.update(sketch1);
  intersection.update(sketch2);
  compact_theta_sketch result = intersection.get_result();
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == Approx(5000).margin(5000 * 0.02));
}

TEST_CASE("theta intersection: estimation mode half overlap ordered", "[theta_intersection]") {
  update_theta_sketch sketch1 = update_theta_sketch::builder().build();
  int value = 0;
  for (int i = 0; i < 10000; i++) sketch1.update(value++);

  update_theta_sketch sketch2 = update_theta_sketch::builder().build();
  value = 5000;
  for (int i = 0; i < 10000; i++) sketch2.update(value++);

  theta_intersection intersection;
  intersection.update(sketch1.compact());
  intersection.update(sketch2.compact());
  compact_theta_sketch result = intersection.get_result();
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == Approx(5000).margin(5000 * 0.02));
}

TEST_CASE("theta intersection: estimation mode half overlap ordered wrapped compact", "[theta_intersection]") {
  update_theta_sketch sketch1 = update_theta_sketch::builder().build();
  int value = 0;
  for (int i = 0; i < 10000; i++) sketch1.update(value++);
  auto bytes1 = sketch1.compact().serialize();

  update_theta_sketch sketch2 = update_theta_sketch::builder().build();
  value = 5000;
  for (int i = 0; i < 10000; i++) sketch2.update(value++);
  auto bytes2 = sketch2.compact().serialize();

  theta_intersection intersection;
  intersection.update(wrapped_compact_theta_sketch::wrap(bytes1.data(), bytes1.size()));
  intersection.update(wrapped_compact_theta_sketch::wrap(bytes2.data(), bytes2.size()));
  compact_theta_sketch result = intersection.get_result();
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == Approx(5000).margin(5000 * 0.02));
}

TEST_CASE("theta intersection: estimation mode disjoint unordered", "[theta_intersection]") {
  update_theta_sketch sketch1 = update_theta_sketch::builder().build();
  int value = 0;
  for (int i = 0; i < 10000; i++) sketch1.update(value++);

  update_theta_sketch sketch2 = update_theta_sketch::builder().build();
  for (int i = 0; i < 10000; i++) sketch2.update(value++);

  theta_intersection intersection;
  intersection.update(sketch1);
  intersection.update(sketch2);
  compact_theta_sketch result = intersection.get_result();
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 0.0);
}

TEST_CASE("theta intersection: estimation mode disjoint ordered", "[theta_intersection]") {
  update_theta_sketch sketch1 = update_theta_sketch::builder().build();
  int value = 0;
  for (int i = 0; i < 10000; i++) sketch1.update(value++);

  update_theta_sketch sketch2 = update_theta_sketch::builder().build();
  for (int i = 0; i < 10000; i++) sketch2.update(value++);

  theta_intersection intersection;
  intersection.update(sketch1.compact());
  intersection.update(sketch2.compact());
  compact_theta_sketch result = intersection.get_result();
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 0.0);
}

TEST_CASE("theta intersection: seed mismatch", "[theta_intersection]") {
  update_theta_sketch sketch = update_theta_sketch::builder().build();
  sketch.update(1); // non-empty should not be ignored
  theta_intersection intersection(123);
  REQUIRE_THROWS_AS(intersection.update(sketch), std::invalid_argument);
}

} /* namespace datasketches */

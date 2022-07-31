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

#include <theta_union.hpp>

#include <stdexcept>

namespace datasketches {

TEST_CASE("theta union: empty", "[theta_union]") {
  update_theta_sketch sketch1 = update_theta_sketch::builder().build();
  theta_union u = theta_union::builder().build();
  compact_theta_sketch sketch2 = u.get_result();
  REQUIRE(sketch2.get_num_retained() == 0);
  REQUIRE(sketch2.is_empty());
  REQUIRE_FALSE(sketch2.is_estimation_mode());

  u.update(sketch1);
  sketch2 = u.get_result();
  REQUIRE(sketch2.get_num_retained() == 0);
  REQUIRE(sketch2.is_empty());
  REQUIRE_FALSE(sketch2.is_estimation_mode());
}

TEST_CASE("theta union: non empty no retained keys", "[theta_union]") {
  update_theta_sketch update_sketch = update_theta_sketch::builder().set_p(0.001f).build();
  update_sketch.update(1);
  theta_union u = theta_union::builder().build();
  u.update(update_sketch);
  compact_theta_sketch sketch = u.get_result();
  REQUIRE(sketch.get_num_retained() == 0);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_theta() == Approx(0.001).margin(1e-10));
}

TEST_CASE("theta union: exact mode half overlap", "[theta_union]") {
  auto sketch1 = update_theta_sketch::builder().build();
  int value = 0;
  for (int i = 0; i < 1000; i++) sketch1.update(value++);

  auto sketch2 = update_theta_sketch::builder().build();
  value = 500;
  for (int i = 0; i < 1000; i++) sketch2.update(value++);

  auto u = theta_union::builder().build();
  u.update(sketch1);
  u.update(sketch2);
  auto sketch3 = u.get_result();
  REQUIRE_FALSE(sketch3.is_empty());
  REQUIRE_FALSE(sketch3.is_estimation_mode());
  REQUIRE(sketch3.get_estimate() == 1500.0);

  u.reset();
  sketch3 = u.get_result();
  REQUIRE(sketch3.get_num_retained() == 0);
  REQUIRE(sketch3.is_empty());
  REQUIRE_FALSE(sketch3.is_estimation_mode());
}

TEST_CASE("theta union: exact mode half overlap wrapped compact", "[theta_union]") {
  auto sketch1 = update_theta_sketch::builder().build();
  int value = 0;
  for (int i = 0; i < 1000; i++) sketch1.update(value++);
  auto bytes1 = sketch1.compact().serialize();

  auto sketch2 = update_theta_sketch::builder().build();
  value = 500;
  for (int i = 0; i < 1000; i++) sketch2.update(value++);
  auto bytes2 = sketch2.compact().serialize();

  auto u = theta_union::builder().build();
  u.update(wrapped_compact_theta_sketch::wrap(bytes1.data(), bytes1.size()));
  u.update(wrapped_compact_theta_sketch::wrap(bytes2.data(), bytes2.size()));
  compact_theta_sketch sketch3 = u.get_result();
  REQUIRE_FALSE(sketch3.is_empty());
  REQUIRE_FALSE(sketch3.is_estimation_mode());
  REQUIRE(sketch3.get_estimate() == 1500.0);
}

TEST_CASE("theta union: estimation mode half overlap", "[theta_union]") {
  auto sketch1 = update_theta_sketch::builder().build();
  int value = 0;
  for (int i = 0; i < 10000; i++) sketch1.update(value++);

  auto sketch2 = update_theta_sketch::builder().build();
  value = 5000;
  for (int i = 0; i < 10000; i++) sketch2.update(value++);

  auto u = theta_union::builder().build();
  u.update(sketch1);
  u.update(sketch2);
  auto sketch3 = u.get_result();
  REQUIRE_FALSE(sketch3.is_empty());
  REQUIRE(sketch3.is_estimation_mode());
  REQUIRE(sketch3.get_estimate() == Approx(15000).margin(15000 * 0.01));
  //std::cerr << sketch3.to_string(true);

  u.reset();
  sketch3 = u.get_result();
  REQUIRE(sketch3.get_num_retained() == 0);
  REQUIRE(sketch3.is_empty());
  REQUIRE_FALSE(sketch3.is_estimation_mode());
}

TEST_CASE("theta union: seed mismatch", "[theta_union]") {
  update_theta_sketch sketch = update_theta_sketch::builder().build();
  sketch.update(1); // non-empty should not be ignored
  theta_union u = theta_union::builder().set_seed(123).build();
  REQUIRE_THROWS_AS(u.update(sketch), std::invalid_argument);
}

} /* namespace datasketches */

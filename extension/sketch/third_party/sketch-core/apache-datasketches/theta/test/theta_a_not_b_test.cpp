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

#include <theta_a_not_b.hpp>

#include <stdexcept>

namespace datasketches {

TEST_CASE("theta a-not-b: empty", "[theta_a_not_b]") {
  theta_a_not_b a_not_b;
  update_theta_sketch a = update_theta_sketch::builder().build();
  update_theta_sketch b = update_theta_sketch::builder().build();
  compact_theta_sketch result = a_not_b.compute(a, b);
  REQUIRE(result.get_num_retained() == 0);
  REQUIRE(result.is_empty());
  REQUIRE_FALSE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 0.0);
}

TEST_CASE("theta a-not-b: non empty no retained keys", "[theta_a_not_b]") {
  update_theta_sketch a = update_theta_sketch::builder().build();
  a.update(1);
  update_theta_sketch b = update_theta_sketch::builder().set_p(0.001f).build();
  theta_a_not_b a_not_b;

  // B is still empty
  compact_theta_sketch result = a_not_b.compute(a, b);
  REQUIRE_FALSE(result.is_empty());
  REQUIRE_FALSE(result.is_estimation_mode());
  REQUIRE(result.get_num_retained() == 1);
  REQUIRE(result.get_theta() == Approx(1).margin(1e-10));
  REQUIRE(result.get_estimate() == 1.0);

  // B is not empty in estimation mode and no entries
  b.update(1);
  REQUIRE(b.get_num_retained() == 0U);

  result = a_not_b.compute(a, b);
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.is_estimation_mode());
  REQUIRE(result.get_num_retained() == 0);
  REQUIRE(result.get_theta() == Approx(0.001).margin(1e-10));
  REQUIRE(result.get_estimate() == 0.0);
}

TEST_CASE("theta a-not-b: exact mode half overlap", "[theta_a_not_b]") {
  update_theta_sketch a = update_theta_sketch::builder().build();
  int value = 0;
  for (int i = 0; i < 1000; i++) a.update(value++);

  update_theta_sketch b = update_theta_sketch::builder().build();
  value = 500;
  for (int i = 0; i < 1000; i++) b.update(value++);

  theta_a_not_b a_not_b;

  // unordered inputs, ordered result
  compact_theta_sketch result = a_not_b.compute(a, b);
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

TEST_CASE("theta a-not-b: exact mode disjoint", "[theta_a_not_b]") {
  update_theta_sketch a = update_theta_sketch::builder().build();
  int value = 0;
  for (int i = 0; i < 1000; i++) a.update(value++);

  update_theta_sketch b = update_theta_sketch::builder().build();
  for (int i = 0; i < 1000; i++) b.update(value++);

  theta_a_not_b a_not_b;

  // unordered inputs
  compact_theta_sketch result = a_not_b.compute(a, b);
  REQUIRE_FALSE(result.is_empty());
  REQUIRE_FALSE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 1000.0);

  // ordered inputs
  result = a_not_b.compute(a.compact(), b.compact());
  REQUIRE_FALSE(result.is_empty());
  REQUIRE_FALSE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 1000.0);
}

TEST_CASE("theta a-not-b: exact mode full overlap", "[theta_a_not_b]") {
  update_theta_sketch sketch = update_theta_sketch::builder().build();
  int value = 0;
  for (int i = 0; i < 1000; i++) sketch.update(value++);

  theta_a_not_b a_not_b;

  // unordered inputs
  compact_theta_sketch result = a_not_b.compute(sketch, sketch);
  REQUIRE(result.is_empty());
  REQUIRE_FALSE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 0.0);

  // ordered inputs
  result = a_not_b.compute(sketch.compact(), sketch.compact());
  REQUIRE(result.is_empty());
  REQUIRE_FALSE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 0.0);
}

TEST_CASE("theta a-not-b: estimation mode half overlap", "[theta_a_not_b]") {
  update_theta_sketch a = update_theta_sketch::builder().build();
  int value = 0;
  for (int i = 0; i < 10000; i++) a.update(value++);

  update_theta_sketch b = update_theta_sketch::builder().build();
  value = 5000;
  for (int i = 0; i < 10000; i++) b.update(value++);

  theta_a_not_b a_not_b;

  // unordered inputs
  compact_theta_sketch result = a_not_b.compute(a, b);
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == Approx(5000).margin(5000 * 0.02));

  // ordered inputs
  result = a_not_b.compute(a.compact(), b.compact());
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == Approx(5000).margin(5000 * 0.02));
}

TEST_CASE("theta a-not-b: estimation mode half overlap wrapped compact", "[theta_a_not_b]") {
  update_theta_sketch a = update_theta_sketch::builder().build();
  int value = 0;
  for (int i = 0; i < 10000; i++) a.update(value++);
  auto bytes_a = a.compact().serialize();

  update_theta_sketch b = update_theta_sketch::builder().build();
  value = 5000;
  for (int i = 0; i < 10000; i++) b.update(value++);
  auto bytes_b = b.compact().serialize();

  theta_a_not_b a_not_b;

  auto result = a_not_b.compute(
    wrapped_compact_theta_sketch::wrap(bytes_a.data(), bytes_a.size()),
    wrapped_compact_theta_sketch::wrap(bytes_b.data(), bytes_b.size())
  );
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == Approx(5000).margin(5000 * 0.02));
}

TEST_CASE("theta a-not-b: estimation mode disjoint", "[theta_a_not_b]") {
  update_theta_sketch a = update_theta_sketch::builder().build();
  int value = 0;
  for (int i = 0; i < 10000; i++) a.update(value++);

  update_theta_sketch b = update_theta_sketch::builder().build();
  for (int i = 0; i < 10000; i++) b.update(value++);

  theta_a_not_b a_not_b;

  // unordered inputs
  compact_theta_sketch result = a_not_b.compute(a, b);
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == Approx(10000).margin(10000 * 0.02));

  // ordered inputs
  result = a_not_b.compute(a.compact(), b.compact());
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == Approx(10000).margin(10000 * 0.02));
}

TEST_CASE("theta a-not-b: estimation mode full overlap", "[theta_a_not_b]") {
  update_theta_sketch sketch = update_theta_sketch::builder().build();
  int value = 0;
  for (int i = 0; i < 10000; i++) sketch.update(value++);

  theta_a_not_b a_not_b;

  // unordered inputs
  compact_theta_sketch result = a_not_b.compute(sketch, sketch);
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 0.0);

  // ordered inputs
  result = a_not_b.compute(sketch.compact(), sketch.compact());
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 0.0);
}

TEST_CASE("theta a-not-b: seed mismatch", "[theta_a_not_b]") {
  update_theta_sketch sketch = update_theta_sketch::builder().build();
  sketch.update(1); // non-empty should not be ignored
  theta_a_not_b a_not_b(123);
  REQUIRE_THROWS_AS(a_not_b.compute(sketch, sketch), std::invalid_argument);
}

TEST_CASE("theta a-not-b: issue #152", "[theta_a_not_b]") {
  update_theta_sketch a = update_theta_sketch::builder().build();
  int value = 0;
  for (int i = 0; i < 10000; i++) a.update(value++);

  update_theta_sketch b = update_theta_sketch::builder().build();
  value = 5000;
  for (int i = 0; i < 25000; i++) b.update(value++);

  theta_a_not_b a_not_b;

  // unordered inputs
  compact_theta_sketch result = a_not_b.compute(a, b);
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

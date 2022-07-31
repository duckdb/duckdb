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

#include "theta_jaccard_similarity.hpp"

namespace datasketches {

TEST_CASE("theta jaccard: empty", "[theta_sketch]") {
  auto sk_a = update_theta_sketch::builder().build();
  auto sk_b = update_theta_sketch::builder().build();
  auto jc = theta_jaccard_similarity::jaccard(sk_a, sk_b);
  REQUIRE(jc == std::array<double, 3>{1, 1, 1});
}

TEST_CASE("theta jaccard: same sketch exact mode", "[theta_sketch]") {
  auto sk = update_theta_sketch::builder().build();
  for (int i = 0; i < 1000; ++i) sk.update(i);

  // update sketch
  auto jc = theta_jaccard_similarity::jaccard(sk, sk);
  REQUIRE(jc == std::array<double, 3>{1, 1, 1});

  // compact sketch
  jc = theta_jaccard_similarity::jaccard(sk.compact(), sk.compact());
  REQUIRE(jc == std::array<double, 3>{1, 1, 1});
}

TEST_CASE("theta jaccard: full overlap exact mode", "[theta_sketch]") {
  auto sk_a = update_theta_sketch::builder().build();
  auto sk_b = update_theta_sketch::builder().build();
  for (int i = 0; i < 1000; ++i) {
    sk_a.update(i);
    sk_b.update(i);
  }

  // update sketches
  auto jc = theta_jaccard_similarity::jaccard(sk_a, sk_b);
  REQUIRE(jc == std::array<double, 3>{1, 1, 1});

  // compact sketches
  jc = theta_jaccard_similarity::jaccard(sk_a.compact(), sk_b.compact());
  REQUIRE(jc == std::array<double, 3>{1, 1, 1});
}

TEST_CASE("theta jaccard: disjoint exact mode", "[theta_sketch]") {
  auto sk_a = update_theta_sketch::builder().build();
  auto sk_b = update_theta_sketch::builder().build();
  for (int i = 0; i < 1000; ++i) {
    sk_a.update(i);
    sk_b.update(i + 1000);
  }

  // update sketches
  auto jc = theta_jaccard_similarity::jaccard(sk_a, sk_b);
  REQUIRE(jc == std::array<double, 3>{0, 0, 0});

  // compact sketches
  jc = theta_jaccard_similarity::jaccard(sk_a.compact(), sk_b.compact());
  REQUIRE(jc == std::array<double, 3>{0, 0, 0});
}

TEST_CASE("theta jaccard: half overlap estimation mode", "[theta_sketch]") {
  auto sk_a = update_theta_sketch::builder().build();
  auto sk_b = update_theta_sketch::builder().build();
  for (int i = 0; i < 10000; ++i) {
    sk_a.update(i);
    sk_b.update(i + 5000);
  }

  // update sketches
  auto jc = theta_jaccard_similarity::jaccard(sk_a, sk_b);
  REQUIRE(jc[0] == Approx(0.33).margin(0.01));
  REQUIRE(jc[1] == Approx(0.33).margin(0.01));
  REQUIRE(jc[2] == Approx(0.33).margin(0.01));

  // compact sketches
  jc = theta_jaccard_similarity::jaccard(sk_a.compact(), sk_b.compact());
  REQUIRE(jc[0] == Approx(0.33).margin(0.01));
  REQUIRE(jc[1] == Approx(0.33).margin(0.01));
  REQUIRE(jc[2] == Approx(0.33).margin(0.01));
}

TEST_CASE("theta jaccard: half overlap estimation mode custom seed", "[theta_sketch]") {
  const uint64_t seed = 123;
  auto sk_a = update_theta_sketch::builder().set_seed(seed).build();
  auto sk_b = update_theta_sketch::builder().set_seed(seed).build();
  for (int i = 0; i < 10000; ++i) {
    sk_a.update(i);
    sk_b.update(i + 5000);
  }

  // update sketches
  auto jc = theta_jaccard_similarity::jaccard(sk_a, sk_b, seed);
  REQUIRE(jc[0] == Approx(0.33).margin(0.01));
  REQUIRE(jc[1] == Approx(0.33).margin(0.01));
  REQUIRE(jc[2] == Approx(0.33).margin(0.01));

  // compact sketches
  jc = theta_jaccard_similarity::jaccard(sk_a.compact(), sk_b.compact(), seed);
  REQUIRE(jc[0] == Approx(0.33).margin(0.01));
  REQUIRE(jc[1] == Approx(0.33).margin(0.01));
  REQUIRE(jc[2] == Approx(0.33).margin(0.01));
}

/**
 * The distribution is quite tight, about +/- 0.7%, which is pretty good since the accuracy of the
 * underlying sketch is about +/- 1.56%.
 */
TEST_CASE("theta jaccard: similarity test", "[theta_sketch]") {
  const int8_t min_lg_k = 12;
  const int u1 = 1 << 20;
  const int u2 = static_cast<int>(u1 * 0.95);
  const double threshold = 0.943;

  auto expected = update_theta_sketch::builder().set_lg_k(min_lg_k).build();
  for (int i = 0; i < u1; ++i) expected.update(i);

  auto actual = update_theta_sketch::builder().set_lg_k(min_lg_k).build();
  for (int i = 0; i < u2; ++i) actual.update(i);

  REQUIRE(theta_jaccard_similarity::similarity_test(actual, expected, threshold));
  REQUIRE(theta_jaccard_similarity::similarity_test(actual, actual, threshold));
}

TEST_CASE("theta jaccard: similarity test custom seed", "[theta_sketch]") {
  const int8_t min_lg_k = 12;
  const int u1 = 1 << 20;
  const int u2 = static_cast<int>(u1 * 0.95);
  const double threshold = 0.943;
  const uint64_t seed = 1234;

  auto expected = update_theta_sketch::builder().set_lg_k(min_lg_k).set_seed(seed).build();
  for (int i = 0; i < u1; ++i) expected.update(i);

  auto actual = update_theta_sketch::builder().set_lg_k(min_lg_k).set_seed(seed).build();
  for (int i = 0; i < u2; ++i) actual.update(i);

  REQUIRE(theta_jaccard_similarity::similarity_test(actual, expected, threshold, seed));
  REQUIRE(theta_jaccard_similarity::similarity_test(actual, actual, threshold, seed));
}

/**
 * The distribution is much looser here, about +/- 14%. This is due to the fact that intersections loose accuracy
 * as the ratio of intersection to the union becomes a small number.
 */
TEST_CASE("theta jaccard: dissimilarity test", "[theta_sketch]") {
  const int8_t min_lg_k = 12;
  const int u1 = 1 << 20;
  const int u2 = static_cast<int>(u1 * 0.05);
  const double threshold = 0.061;

  auto expected = update_theta_sketch::builder().set_lg_k(min_lg_k).build();
  for (int i = 0; i < u1; ++i) expected.update(i);

  auto actual = update_theta_sketch::builder().set_lg_k(min_lg_k).build();
  for (int i = 0; i < u2; ++i) actual.update(i);

  REQUIRE(theta_jaccard_similarity::dissimilarity_test(actual, expected, threshold));
  REQUIRE_FALSE(theta_jaccard_similarity::dissimilarity_test(actual, actual, threshold));
}

TEST_CASE("theta jaccard: dissimilarity test custom seed", "[theta_sketch]") {
  const int8_t min_lg_k = 12;
  const int u1 = 1 << 20;
  const int u2 = static_cast<int>(u1 * 0.05);
  const double threshold = 0.061;
  const uint64_t seed = 1234;

  auto expected = update_theta_sketch::builder().set_lg_k(min_lg_k).set_seed(seed).build();
  for (int i = 0; i < u1; ++i) expected.update(i);

  auto actual = update_theta_sketch::builder().set_lg_k(min_lg_k).set_seed(seed).build();
  for (int i = 0; i < u2; ++i) actual.update(i);

  REQUIRE(theta_jaccard_similarity::dissimilarity_test(actual, expected, threshold, seed));
  REQUIRE_FALSE(theta_jaccard_similarity::dissimilarity_test(actual, actual, threshold, seed));
}

} /* namespace datasketches */

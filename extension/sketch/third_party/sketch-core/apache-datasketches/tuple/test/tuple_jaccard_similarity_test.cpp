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

#include "tuple_jaccard_similarity.hpp"

namespace datasketches {

using tuple_jaccard_similarity_float = tuple_jaccard_similarity<float, default_union_policy<float>>;

TEST_CASE("tuple jaccard: empty", "[tuple_sketch]") {
  auto sk_a = update_tuple_sketch<float>::builder().build();
  auto sk_b = update_tuple_sketch<float>::builder().build();

  // update sketches
  auto jc = tuple_jaccard_similarity_float::jaccard(sk_a, sk_b);
  REQUIRE(jc == std::array<double, 3>{1, 1, 1});

  // compact sketches
  jc = tuple_jaccard_similarity_float::jaccard(sk_a.compact(), sk_b.compact());
  REQUIRE(jc == std::array<double, 3>{1, 1, 1});

  REQUIRE(tuple_jaccard_similarity_float::exactly_equal(sk_a, sk_b));
}

TEST_CASE("tuple jaccard: same sketch exact mode", "[tuple_sketch]") {
  auto sk = update_tuple_sketch<float>::builder().build();
  for (int i = 0; i < 1000; ++i) sk.update(i, 1.0f);

  // update sketch
  auto jc = tuple_jaccard_similarity_float::jaccard(sk, sk);
  REQUIRE(jc == std::array<double, 3>{1, 1, 1});

  // compact sketch
  jc = tuple_jaccard_similarity_float::jaccard(sk.compact(), sk.compact());
  REQUIRE(jc == std::array<double, 3>{1, 1, 1});

  REQUIRE(tuple_jaccard_similarity_float::exactly_equal(sk, sk));
}

TEST_CASE("tuple jaccard: full overlap exact mode", "[tuple_sketch]") {
  auto sk_a = update_tuple_sketch<float>::builder().build();
  auto sk_b = update_tuple_sketch<float>::builder().build();
  for (int i = 0; i < 1000; ++i) {
    sk_a.update(i, 1.0f);
    sk_b.update(i, 1.0f);
  }

  // update sketches
  auto jc = tuple_jaccard_similarity_float::jaccard(sk_a, sk_b);
  REQUIRE(jc == std::array<double, 3>{1, 1, 1});

  // compact sketches
  jc = tuple_jaccard_similarity_float::jaccard(sk_a.compact(), sk_b.compact());
  REQUIRE(jc == std::array<double, 3>{1, 1, 1});

  REQUIRE(tuple_jaccard_similarity_float::exactly_equal(sk_a, sk_b));
  REQUIRE(tuple_jaccard_similarity_float::exactly_equal(sk_a.compact(), sk_b));
  REQUIRE(tuple_jaccard_similarity_float::exactly_equal(sk_a, sk_b.compact()));
  REQUIRE(tuple_jaccard_similarity_float::exactly_equal(sk_a.compact(), sk_b.compact()));
}

TEST_CASE("tuple jaccard: disjoint exact mode", "[tuple_sketch]") {
  auto sk_a = update_tuple_sketch<float>::builder().build();
  auto sk_b = update_tuple_sketch<float>::builder().build();
  for (int i = 0; i < 1000; ++i) {
    sk_a.update(i, 1.0f);
    sk_b.update(i + 1000, 1.0f);
  }

  // update sketches
  auto jc = tuple_jaccard_similarity_float::jaccard(sk_a, sk_b);
  REQUIRE(jc == std::array<double, 3>{0, 0, 0});

  // compact sketches
  jc = tuple_jaccard_similarity_float::jaccard(sk_a.compact(), sk_b.compact());
  REQUIRE(jc == std::array<double, 3>{0, 0, 0});
}

} /* namespace datasketches */

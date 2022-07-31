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
#include <sstream>

#include <kll_sketch.hpp>
#include <test_allocator.hpp>
#include <test_type.hpp>

namespace datasketches {

using kll_test_type_sketch = kll_sketch<test_type, test_type_less, test_type_serde, test_allocator<test_type>>;
using alloc = test_allocator<test_type>;

TEST_CASE("kll sketch custom type", "[kll_sketch]") {

  // setup section
  test_allocator_total_bytes = 0;

  SECTION("compact level zero") {
    kll_test_type_sketch sketch(8, 0);
    REQUIRE_THROWS_AS(sketch.get_quantile(0), std::runtime_error);
    REQUIRE_THROWS_AS(sketch.get_min_value(), std::runtime_error);
    REQUIRE_THROWS_AS(sketch.get_max_value(), std::runtime_error);
    REQUIRE(sketch.get_serialized_size_bytes() == 8);

    sketch.update(1);
    sketch.update(2);
    sketch.update(3);
    sketch.update(4);
    sketch.update(5);
    sketch.update(6);
    sketch.update(7);
    sketch.update(8);
    sketch.update(9);

    //sketch.to_stream(std::cout);

    REQUIRE(sketch.is_estimation_mode());
    REQUIRE(sketch.get_n() > sketch.get_num_retained());
    REQUIRE(sketch.get_min_value().get_value() == 1);
    REQUIRE(sketch.get_max_value().get_value() == 9);
  }

  SECTION("merge small") {
    kll_test_type_sketch sketch1(8, 0);
    sketch1.update(1);

    kll_test_type_sketch sketch2(8, 0);
    sketch2.update(2);

    sketch2.merge(sketch1);

    //sketch2.to_stream(std::cout);

    REQUIRE_FALSE(sketch2.is_estimation_mode());
    REQUIRE(sketch2.get_num_retained() == sketch2.get_n());
    REQUIRE(sketch2.get_min_value().get_value() == 1);
    REQUIRE(sketch2.get_max_value().get_value() == 2);
  }

  SECTION("merge higher levels") {
    kll_test_type_sketch sketch1(8, 0);
    sketch1.update(1);
    sketch1.update(2);
    sketch1.update(3);
    sketch1.update(4);
    sketch1.update(5);
    sketch1.update(6);
    sketch1.update(7);
    sketch1.update(8);
    sketch1.update(9);

    kll_test_type_sketch sketch2(8, 0);
    sketch2.update(10);
    sketch2.update(11);
    sketch2.update(12);
    sketch2.update(13);
    sketch2.update(14);
    sketch2.update(15);
    sketch2.update(16);
    sketch2.update(17);
    sketch2.update(18);

    sketch2.merge(sketch1);

    //sketch2.to_stream(std::cout);

    REQUIRE(sketch2.is_estimation_mode());
    REQUIRE(sketch2.get_n() > sketch2.get_num_retained());
    REQUIRE(sketch2.get_min_value().get_value() == 1);
    REQUIRE(sketch2.get_max_value().get_value() == 18);
  }

  SECTION("serialize deserialize") {
    kll_test_type_sketch sketch1(200, 0);

    const int n = 1000;
    for (int i = 0; i < n; i++) sketch1.update(i);

    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch1.serialize(s);
    REQUIRE((size_t) s.tellp() == sketch1.get_serialized_size_bytes());
    auto sketch2 = kll_test_type_sketch::deserialize(s, alloc(0));
    REQUIRE((size_t) s.tellg() == sketch2.get_serialized_size_bytes());
    REQUIRE(s.tellg() == s.tellp());
    REQUIRE(sketch2.is_empty() == sketch1.is_empty());
    REQUIRE(sketch2.is_estimation_mode() == sketch1.is_estimation_mode());
    REQUIRE(sketch2.get_n() == sketch1.get_n());
    REQUIRE(sketch2.get_num_retained() == sketch1.get_num_retained());
    REQUIRE(sketch2.get_min_value().get_value() == sketch1.get_min_value().get_value());
    REQUIRE(sketch2.get_max_value().get_value() == sketch1.get_max_value().get_value());
    REQUIRE(sketch2.get_normalized_rank_error(false) == sketch1.get_normalized_rank_error(false));
    REQUIRE(sketch2.get_normalized_rank_error(true) == sketch1.get_normalized_rank_error(true));
    REQUIRE(sketch2.get_quantile(0.5).get_value() == sketch1.get_quantile(0.5).get_value());
    REQUIRE(sketch2.get_rank(0) == sketch1.get_rank(0));
    REQUIRE(sketch2.get_rank(n) == sketch1.get_rank(n));
    REQUIRE(sketch2.get_rank(n / 2) == sketch1.get_rank(n / 2));
  }

  SECTION("moving merge") {
    kll_test_type_sketch sketch1(8, 0);
    for (int i = 0; i < 10; i++) sketch1.update(i);
    kll_test_type_sketch sketch2(8, 0);
    sketch2.update(10);
    sketch2.merge(std::move(sketch1));
    REQUIRE(sketch2.get_min_value().get_value() == 0);
    REQUIRE(sketch2.get_max_value().get_value() == 10);
    REQUIRE(sketch2.get_n() == 11);
  }

  // cleanup
  if (test_allocator_total_bytes != 0) {
    REQUIRE(test_allocator_total_bytes == 0);
  }
}

} /* namespace datasketches */

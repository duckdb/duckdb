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

#include <req_sketch.hpp>
#include <test_allocator.hpp>
#include <test_type.hpp>

namespace datasketches {

using req_test_type_sketch = req_sketch<test_type, test_type_less, test_type_serde, test_allocator<test_type>>;
using alloc = test_allocator<test_type>;

TEST_CASE("req sketch custom type", "[req_sketch]") {

  // setup section
  test_allocator_total_bytes = 0;

  SECTION("compact level zero") {
    req_test_type_sketch sketch(4, true, 0);
    REQUIRE_THROWS_AS(sketch.get_quantile(0), std::runtime_error);
    REQUIRE_THROWS_AS(sketch.get_min_value(), std::runtime_error);
    REQUIRE_THROWS_AS(sketch.get_max_value(), std::runtime_error);
    REQUIRE(sketch.get_serialized_size_bytes() == 8);

    for (int i = 0; i < 24; ++i) sketch.update(i);
    //std::cout << sketch.to_string(true);

    REQUIRE(sketch.is_estimation_mode());
    REQUIRE(sketch.get_n() > sketch.get_num_retained());
    REQUIRE(sketch.get_min_value().get_value() == 0);
    REQUIRE(sketch.get_max_value().get_value() == 23);
  }

  SECTION("merge small") {
    req_test_type_sketch sketch1(4, true, 0);
    sketch1.update(1);

    req_test_type_sketch sketch2(4, true, 0);
    sketch2.update(2);

    sketch2.merge(sketch1);

    //std::cout << sketch2.to_string(true);

    REQUIRE_FALSE(sketch2.is_estimation_mode());
    REQUIRE(sketch2.get_num_retained() == sketch2.get_n());
    REQUIRE(sketch2.get_min_value().get_value() == 1);
    REQUIRE(sketch2.get_max_value().get_value() == 2);
  }

  SECTION("merge higher levels") {
    req_test_type_sketch sketch1(4, true, 0);
    for (int i = 0; i < 24; ++i) sketch1.update(i);

    req_test_type_sketch sketch2(4, true, 0);
    for (int i = 0; i < 24; ++i) sketch2.update(i);

    sketch2.merge(sketch1);

    //std::cout << sketch2.to_string(true);

    REQUIRE(sketch2.is_estimation_mode());
    REQUIRE(sketch2.get_n() > sketch2.get_num_retained());
    REQUIRE(sketch2.get_min_value().get_value() == 0);
    REQUIRE(sketch2.get_max_value().get_value() == 23);
  }

  SECTION("serialize deserialize") {
    req_test_type_sketch sketch1(12, true, 0);

    const int n = 1000;
    for (int i = 0; i < n; i++) sketch1.update(i);

    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch1.serialize(s);
    REQUIRE((size_t) s.tellp() == sketch1.get_serialized_size_bytes());
    auto sketch2 = req_test_type_sketch::deserialize(s, alloc(0));
    REQUIRE((size_t) s.tellg() == sketch2.get_serialized_size_bytes());
    REQUIRE(s.tellg() == s.tellp());
    REQUIRE(sketch2.is_empty() == sketch1.is_empty());
    REQUIRE(sketch2.is_estimation_mode() == sketch1.is_estimation_mode());
    REQUIRE(sketch2.get_n() == sketch1.get_n());
    REQUIRE(sketch2.get_num_retained() == sketch1.get_num_retained());
    REQUIRE(sketch2.get_min_value().get_value() == sketch1.get_min_value().get_value());
    REQUIRE(sketch2.get_max_value().get_value() == sketch1.get_max_value().get_value());
    REQUIRE(sketch2.get_quantile(0.5).get_value() == sketch1.get_quantile(0.5).get_value());
    REQUIRE(sketch2.get_rank(0) == sketch1.get_rank(0));
    REQUIRE(sketch2.get_rank(n) == sketch1.get_rank(n));
    REQUIRE(sketch2.get_rank(n / 2) == sketch1.get_rank(n / 2));
  }

  SECTION("moving merge") {
    req_test_type_sketch sketch1(4, true, 0);
    for (int i = 0; i < 10; i++) sketch1.update(i);
    req_test_type_sketch sketch2(4, true, 0);
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

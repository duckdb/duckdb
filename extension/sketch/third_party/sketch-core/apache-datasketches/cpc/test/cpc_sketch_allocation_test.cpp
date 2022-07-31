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


#include <cstring>
#include <sstream>
#include <fstream>

#include <catch.hpp>

#include "cpc_sketch.hpp"
#include "cpc_union.hpp"
#include "test_allocator.hpp"

namespace datasketches {

using cpc_sketch_test_alloc = cpc_sketch_alloc<test_allocator<uint8_t>>;
using alloc = test_allocator<uint8_t>;

TEST_CASE("cpc sketch allocation: serialize deserialize empty", "[cpc_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    cpc_sketch_test_alloc sketch(11, DEFAULT_SEED, 0);
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    auto deserialized = cpc_sketch_test_alloc::deserialize(s, DEFAULT_SEED, alloc(0));
    REQUIRE(deserialized.is_empty() == sketch.is_empty());
    REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
    REQUIRE(deserialized.validate());
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("cpc sketch allocation: serialize deserialize sparse", "[cpc_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    cpc_sketch_test_alloc sketch(11, DEFAULT_SEED, 0);
    const int n(100);
    for (int i = 0; i < n; i++) sketch.update(i);
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    auto deserialized = cpc_sketch_test_alloc::deserialize(s, DEFAULT_SEED, alloc(0));
    REQUIRE(deserialized.is_empty() == sketch.is_empty());
    REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
    REQUIRE(deserialized.validate());
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("cpc sketch allocation: serialize deserialize hybrid", "[cpc_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    cpc_sketch_test_alloc sketch(11, DEFAULT_SEED, 0);
    const int n(200);
    for (int i = 0; i < n; i++) sketch.update(i);
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    auto deserialized = cpc_sketch_test_alloc::deserialize(s, DEFAULT_SEED, alloc(0));
    REQUIRE(deserialized.is_empty() == sketch.is_empty());
    REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
    REQUIRE(deserialized.validate());
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("cpc sketch allocation: serialize deserialize pinned", "[cpc_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    cpc_sketch_test_alloc sketch(11, DEFAULT_SEED, 0);
    const int n(2000);
    for (int i = 0; i < n; i++) sketch.update(i);
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    auto deserialized = cpc_sketch_test_alloc::deserialize(s, DEFAULT_SEED, alloc(0));
    REQUIRE(deserialized.is_empty() == sketch.is_empty());
    REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
    REQUIRE(deserialized.validate());
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("cpc sketch allocation: serialize deserialize sliding", "[cpc_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    cpc_sketch_test_alloc sketch(11, DEFAULT_SEED, 0);
    const int n(20000);
    for (int i = 0; i < n; i++) sketch.update(i);
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    auto deserialized = cpc_sketch_test_alloc::deserialize(s, DEFAULT_SEED, alloc(0));
    REQUIRE(deserialized.is_empty() == sketch.is_empty());
    REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
    REQUIRE(deserialized.validate());
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("cpc sketch allocation: serializing deserialize sliding large", "[cpc_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    cpc_sketch_test_alloc sketch(11, DEFAULT_SEED, 0);
    const int n(3000000);
    for (int i = 0; i < n; i++) sketch.update(i);
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    auto deserialized = cpc_sketch_test_alloc::deserialize(s, DEFAULT_SEED, alloc(0));
    REQUIRE(deserialized.is_empty() == sketch.is_empty());
    REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
    REQUIRE(deserialized.validate());
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("cpc sketch allocation: serialize deserialize empty, bytes", "[cpc_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    cpc_sketch_test_alloc sketch(11, DEFAULT_SEED, 0);
    auto bytes = sketch.serialize();
    auto deserialized = cpc_sketch_test_alloc::deserialize(bytes.data(), bytes.size(), DEFAULT_SEED, 0);
    REQUIRE(deserialized.is_empty() == sketch.is_empty());
    REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
    REQUIRE(deserialized.validate());
    REQUIRE_THROWS_AS(cpc_sketch_test_alloc::deserialize(bytes.data(), bytes.size() - 1, DEFAULT_SEED, 0), std::out_of_range);
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("cpc sketch allocation: serialize deserialize sparse, bytes", "[cpc_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    cpc_sketch_test_alloc sketch(11, DEFAULT_SEED, 0);
    const int n(100);
    for (int i = 0; i < n; i++) sketch.update(i);
    auto bytes = sketch.serialize();
    auto deserialized = cpc_sketch_test_alloc::deserialize(bytes.data(), bytes.size(), DEFAULT_SEED, 0);
    REQUIRE(deserialized.is_empty() == sketch.is_empty());
    REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
    REQUIRE(deserialized.validate());
    REQUIRE_THROWS_AS(cpc_sketch_test_alloc::deserialize(bytes.data(), 7, DEFAULT_SEED, 0), std::out_of_range);
    REQUIRE_THROWS_AS(cpc_sketch_test_alloc::deserialize(bytes.data(), 15, DEFAULT_SEED, 0), std::out_of_range);
    REQUIRE_THROWS_AS(cpc_sketch_test_alloc::deserialize(bytes.data(), bytes.size() - 1, DEFAULT_SEED, 0), std::out_of_range);
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("cpc sketch allocation: serialize deserialize hybrid, bytes", "[cpc_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    cpc_sketch_test_alloc sketch(11, DEFAULT_SEED, 0);
    const int n(200);
    for (int i = 0; i < n; i++) sketch.update(i);
    auto bytes = sketch.serialize();
    auto deserialized = cpc_sketch_test_alloc::deserialize(bytes.data(), bytes.size(), DEFAULT_SEED, 0);
    REQUIRE(deserialized.is_empty() == sketch.is_empty());
    REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
    REQUIRE(deserialized.validate());
    REQUIRE_THROWS_AS(cpc_sketch_test_alloc::deserialize(bytes.data(), 7, DEFAULT_SEED, 0), std::out_of_range);
    REQUIRE_THROWS_AS(cpc_sketch_test_alloc::deserialize(bytes.data(), 15, DEFAULT_SEED, 0), std::out_of_range);
    REQUIRE_THROWS_AS(cpc_sketch_test_alloc::deserialize(bytes.data(), bytes.size() - 1, DEFAULT_SEED, 0), std::out_of_range);
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("cpc sketch allocation: serialize deserialize pinned, bytes", "[cpc_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    cpc_sketch_test_alloc sketch(11, DEFAULT_SEED, 0);
    const int n(2000);
    for (int i = 0; i < n; i++) sketch.update(i);
    auto bytes = sketch.serialize();
    auto deserialized = cpc_sketch_test_alloc::deserialize(bytes.data(), bytes.size(), DEFAULT_SEED, 0);
    REQUIRE(deserialized.is_empty() == sketch.is_empty());
    REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
    REQUIRE(deserialized.validate());
    REQUIRE_THROWS_AS(cpc_sketch_test_alloc::deserialize(bytes.data(), 7, DEFAULT_SEED, 0), std::out_of_range);
    REQUIRE_THROWS_AS(cpc_sketch_test_alloc::deserialize(bytes.data(), 15, DEFAULT_SEED, 0), std::out_of_range);
    REQUIRE_THROWS_AS(cpc_sketch_test_alloc::deserialize(bytes.data(), bytes.size() - 1, DEFAULT_SEED, 0), std::out_of_range);
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("cpc sketch allocation: serialize deserialize sliding, bytes", "[cpc_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    cpc_sketch_test_alloc sketch(11, DEFAULT_SEED, 0);
    const int n(20000);
    for (int i = 0; i < n; i++) sketch.update(i);
    auto bytes = sketch.serialize();
    auto deserialized = cpc_sketch_test_alloc::deserialize(bytes.data(), bytes.size(), DEFAULT_SEED, 0);
    REQUIRE(deserialized.is_empty() == sketch.is_empty());
    REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
    REQUIRE(deserialized.validate());
    REQUIRE_THROWS_AS(cpc_sketch_test_alloc::deserialize(bytes.data(), 7, DEFAULT_SEED, 0), std::out_of_range);
    REQUIRE_THROWS_AS(cpc_sketch_test_alloc::deserialize(bytes.data(), 15, DEFAULT_SEED, 0), std::out_of_range);
    REQUIRE_THROWS_AS(cpc_sketch_test_alloc::deserialize(bytes.data(), bytes.size() - 1, DEFAULT_SEED, 0), std::out_of_range);
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

using cpc_union_test_alloc = cpc_union_alloc<test_allocator<uint8_t>>;

TEST_CASE("cpc sketch allocation: union") {
  cpc_sketch_test_alloc s1(11, DEFAULT_SEED, 0);
  s1.update(1);

  cpc_sketch_test_alloc s2(11, DEFAULT_SEED, 0);
  s2.update(2);

  cpc_union_test_alloc u(11, DEFAULT_SEED, 0);
  u.update(s1);
  u.update(s2);
  auto s3 = u.get_result();
  REQUIRE_FALSE(s3.is_empty());
}

} /* namespace datasketches */

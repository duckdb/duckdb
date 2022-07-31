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
#include <tuple_sketch.hpp>
#include <test_allocator.hpp>
#include <test_type.hpp>

namespace datasketches {

static const bool ALLOCATOR_TEST_DEBUG = false;

struct test_type_replace_policy {
  test_type create() const { return test_type(0); }
  void update(test_type& summary, const test_type& update) const {
    if (ALLOCATOR_TEST_DEBUG) std::cerr << "policy::update lvalue begin" << std::endl;
    summary = update;
    if (ALLOCATOR_TEST_DEBUG) std::cerr << "policy::update lvalue end" << std::endl;
  }
  void update(test_type& summary, test_type&& update) const {
    if (ALLOCATOR_TEST_DEBUG) std::cerr << "policy::update rvalue begin" << std::endl;
    summary = std::move(update);
    if (ALLOCATOR_TEST_DEBUG) std::cerr << "policy::update rvalue end" << std::endl;
  }
};

using update_tuple_sketch_test =
    update_tuple_sketch<test_type, test_type, test_type_replace_policy, test_allocator<test_type>>;
using compact_tuple_sketch_test =
    compact_tuple_sketch<test_type, test_allocator<test_type>>;

TEST_CASE("tuple sketch with test allocator: estimation mode", "[tuple_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    auto update_sketch = update_tuple_sketch_test::builder(test_type_replace_policy(), test_allocator<test_type>(0)).build();
    for (int i = 0; i < 10000; ++i) update_sketch.update(i, 1);
    for (int i = 0; i < 10000; ++i) update_sketch.update(i, 2);
    REQUIRE(!update_sketch.is_empty());
    REQUIRE(update_sketch.is_estimation_mode());
    unsigned count = 0;
    for (const auto& entry: update_sketch) {
      REQUIRE(entry.second.get_value() == 2);
      ++count;
    }
    REQUIRE(count == update_sketch.get_num_retained());

    update_sketch.trim();
    REQUIRE(update_sketch.get_num_retained() == (1U << update_sketch.get_lg_k()));

    auto compact_sketch = update_sketch.compact();
    REQUIRE(!compact_sketch.is_empty());
    REQUIRE(compact_sketch.is_estimation_mode());
    count = 0;
    for (const auto& entry: compact_sketch) {
      REQUIRE(entry.second.get_value() == 2);
      ++count;
    }
    REQUIRE(count == update_sketch.get_num_retained());

    auto bytes = compact_sketch.serialize(0, test_type_serde());
    auto deserialized_sketch = compact_tuple_sketch_test::deserialize(bytes.data(), bytes.size(), DEFAULT_SEED, test_type_serde(), test_allocator<test_type>(0));
    REQUIRE(deserialized_sketch.get_estimate() == compact_sketch.get_estimate());

    // update sketch copy
    if (ALLOCATOR_TEST_DEBUG) std::cout << update_sketch.to_string();
    update_tuple_sketch_test update_sketch_copy(update_sketch);
    update_sketch_copy = update_sketch;
    // update sketch move
    update_tuple_sketch_test update_sketch_moved(std::move(update_sketch_copy));
    update_sketch_moved = std::move(update_sketch);

    // compact sketch copy
    compact_tuple_sketch_test compact_sketch_copy(compact_sketch);
    compact_sketch_copy = compact_sketch;
    // compact sketch move
    compact_tuple_sketch_test compact_sketch_moved(std::move(compact_sketch_copy));
    compact_sketch_moved = std::move(compact_sketch);
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

} /* namespace datasketches */

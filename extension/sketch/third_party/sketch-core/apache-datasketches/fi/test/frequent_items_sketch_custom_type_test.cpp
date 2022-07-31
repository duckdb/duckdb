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
#include <stdexcept>

#include "frequent_items_sketch.hpp"
#include "test_type.hpp"
#include "test_allocator.hpp"

namespace datasketches {

using frequent_test_type_sketch = frequent_items_sketch<test_type, float, test_type_hash, test_type_equal, test_type_serde, test_allocator<test_type>>;
using alloc = test_allocator<test_type>;

TEST_CASE("frequent items: custom type", "[frequent_items_sketch]") {
  frequent_test_type_sketch sketch(3, frequent_test_type_sketch::LG_MIN_MAP_SIZE, 0);
  sketch.update(1, 10); // should survive the purge
  sketch.update(2);
  sketch.update(3);
  sketch.update(4);
  sketch.update(5);
  sketch.update(6);
  sketch.update(7);
  test_type a8(8);
  sketch.update(a8);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.get_total_weight() == 17);
  REQUIRE(sketch.get_estimate(1) == 10);

  auto items = sketch.get_frequent_items(frequent_items_error_type::NO_FALSE_POSITIVES);
  REQUIRE(items.size() == 1); // only 1 item should be above threshold
  REQUIRE(items[0].get_item().get_value() == 1);
  REQUIRE(items[0].get_estimate() == 10);

  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  sketch.serialize(s);
  auto sketch2 = frequent_test_type_sketch::deserialize(s, alloc(0));
  REQUIRE_FALSE(sketch2.is_empty());
  REQUIRE(sketch2.get_total_weight() == 17);
  REQUIRE(sketch2.get_estimate(1) == 10);
  REQUIRE(sketch.get_num_active_items() == sketch2.get_num_active_items());
  REQUIRE(sketch.get_maximum_error() == sketch2.get_maximum_error());

  auto bytes = sketch.serialize();
  auto sketch3 = frequent_test_type_sketch::deserialize(bytes.data(), bytes.size(), alloc(0));
  REQUIRE_FALSE(sketch3.is_empty());
  REQUIRE(sketch3.get_total_weight() == 17);
  REQUIRE(sketch3.get_estimate(1) == 10);
  REQUIRE(sketch.get_num_active_items() == sketch3.get_num_active_items());
  REQUIRE(sketch.get_maximum_error() == sketch3.get_maximum_error());
}

// this is to see the debug print from test_type if enabled there to make sure items are moved
TEST_CASE("frequent items: moving merge", "[frequent_items_sketch]") {
  frequent_test_type_sketch sketch1(3, frequent_test_type_sketch::LG_MIN_MAP_SIZE, 0);
  sketch1.update(1);

  frequent_test_type_sketch sketch2(3, frequent_test_type_sketch::LG_MIN_MAP_SIZE, 0);
  sketch2.update(2);

  sketch2.merge(std::move(sketch1));
  REQUIRE(sketch2.get_total_weight() == 2);
}

TEST_CASE("frequent items: negative weight", "[frequent_items_sketch]") {
  frequent_test_type_sketch sketch(3, frequent_test_type_sketch::LG_MIN_MAP_SIZE, 0);
  REQUIRE_THROWS_AS(sketch.update(1, -1), std::invalid_argument);
}

} /* namespace datasketches */

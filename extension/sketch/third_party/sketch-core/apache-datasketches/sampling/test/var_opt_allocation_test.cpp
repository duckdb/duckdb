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

#include <var_opt_sketch.hpp>
#include <var_opt_union.hpp>
#include <test_type.hpp>
#include <test_allocator.hpp>

#include <catch.hpp>

#include <sstream>

namespace datasketches {

using var_opt_test_sketch = var_opt_sketch<test_type, test_type_serde, test_allocator<test_type>>;
using var_opt_test_union = var_opt_union<test_type, test_type_serde, test_allocator<test_type>>;
using alloc = test_allocator<test_type>;

TEST_CASE("varopt allocation test", "[var_opt_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    var_opt_test_sketch sk1(10, var_opt_test_sketch::DEFAULT_RESIZE_FACTOR, 0);
    for (int i = 0; i < 100; ++i) sk1.update(i);
    auto bytes1 = sk1.serialize();
    auto sk2 = var_opt_test_sketch::deserialize(bytes1.data(), bytes1.size(), test_type_serde(), 0);

    std::stringstream ss;
    sk1.serialize(ss);
    auto sk3 = var_opt_test_sketch::deserialize(ss, alloc(0));

    var_opt_test_union u1(10, 0);
    u1.update(sk1);
    u1.update(sk2);
    u1.update(sk3);

    auto bytes2 = u1.serialize();
    auto u2 = var_opt_test_union::deserialize(bytes2.data(), bytes2.size(), test_type_serde(), 0);
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE( "varopt union: move", "[var_opt_union][test_type]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    uint32_t n = 20;
    uint32_t k = 5;
    var_opt_test_union u(k, 0);
    var_opt_test_sketch sk1(k, var_opt_test_sketch::DEFAULT_RESIZE_FACTOR, 0);
    var_opt_test_sketch sk2(k, var_opt_test_sketch::DEFAULT_RESIZE_FACTOR, 0);

    // move udpates
    for (int i = 0; i < (int) n; ++i) {
      sk1.update(i);
      sk2.update(-i);
    }
    REQUIRE(sk1.get_n() == n);
    REQUIRE(sk2.get_n() == n);

    // move unions
    u.update(std::move(sk2));
    u.update(std::move(sk1));
    REQUIRE(u.get_result().get_n() == 2 * n);

    // move constructor
    var_opt_test_union u2(std::move(u));
    REQUIRE(u2.get_result().get_n() == 2 * n);

    // move assignment
    var_opt_test_union u3(k, 0);
    u3 = std::move(u2);
    REQUIRE(u3.get_result().get_n() == 2 * n);
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

}

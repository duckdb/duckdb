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
#include <tuple>

namespace datasketches {

using three_doubles = std::tuple<double, double, double>;

// this is needed for a test below, but should be defined here
std::ostream& operator<<(std::ostream& os, const three_doubles& tuple) {
  os << std::get<0>(tuple) << ", " << std::get<1>(tuple) << ", " << std::get<2>(tuple);
  return os;
}

}

#include <catch.hpp>
#include <tuple_sketch.hpp>

namespace datasketches {

TEST_CASE("tuple sketch float: builder", "[tuple_sketch]") {
  auto builder = update_tuple_sketch<float>::builder();
  builder.set_lg_k(10).set_p(0.5f).set_resize_factor(theta_constants::resize_factor::X2).set_seed(123);
  auto sketch = builder.build();
  REQUIRE(sketch.get_lg_k() == 10);
  REQUIRE(sketch.get_theta() == 1.0); // empty sketch should have theta 1.0
  REQUIRE(sketch.get_rf() == theta_constants::resize_factor::X2);
  REQUIRE(sketch.get_seed_hash() == compute_seed_hash(123));
  sketch.update(1, 0);
  REQUIRE(sketch.get_theta() == 0.5); // theta = p
}

TEST_CASE("tuple sketch float: empty", "[tuple_sketch]") {
  auto update_sketch = update_tuple_sketch<float>::builder().build();
  std::cout << "sizeof(update_tuple_sketch<float>)=" << sizeof(update_sketch) << std::endl;
  REQUIRE(update_sketch.is_empty());
  REQUIRE(!update_sketch.is_estimation_mode());
  REQUIRE(update_sketch.get_estimate() == 0);
  REQUIRE(update_sketch.get_lower_bound(1) == 0);
  REQUIRE(update_sketch.get_upper_bound(1) == 0);
  REQUIRE(update_sketch.get_theta() == 1);
  REQUIRE(update_sketch.get_num_retained() == 0);
  REQUIRE(update_sketch.is_ordered());

  auto compact_sketch = update_sketch.compact();
  std::cout << "sizeof(compact_tuple_sketch<float>)=" << sizeof(compact_sketch) << std::endl;
  REQUIRE(compact_sketch.is_empty());
  REQUIRE(!compact_sketch.is_estimation_mode());
  REQUIRE(compact_sketch.get_estimate() == 0);
  REQUIRE(compact_sketch.get_lower_bound(1) == 0);
  REQUIRE(compact_sketch.get_upper_bound(1) == 0);
  REQUIRE(compact_sketch.get_theta() == 1);
  REQUIRE(compact_sketch.get_num_retained() == 0);
  REQUIRE(compact_sketch.is_ordered());

  // empty is forced to be ordered
  REQUIRE(update_sketch.compact(false).is_ordered());
}

TEST_CASE("tuple sketch: single item", "[theta_sketch]") {
  auto update_sketch = update_tuple_sketch<float>::builder().build();
  update_sketch.update(1, 1.0f);
  REQUIRE_FALSE(update_sketch.is_empty());
  REQUIRE_FALSE(update_sketch.is_estimation_mode());
  REQUIRE(update_sketch.get_theta() == 1.0);
  REQUIRE(update_sketch.get_estimate() == 1.0);
  REQUIRE(update_sketch.get_lower_bound(1) == 1.0);
  REQUIRE(update_sketch.get_upper_bound(1) == 1.0);
  REQUIRE(update_sketch.is_ordered()); // one item is ordered

  auto compact_sketch = update_sketch.compact();
  REQUIRE_FALSE(compact_sketch.is_empty());
  REQUIRE_FALSE(compact_sketch.is_estimation_mode());
  REQUIRE(compact_sketch.get_theta() == 1.0);
  REQUIRE(compact_sketch.get_estimate() == 1.0);
  REQUIRE(compact_sketch.get_lower_bound(1) == 1.0);
  REQUIRE(compact_sketch.get_upper_bound(1) == 1.0);
  REQUIRE(compact_sketch.is_ordered());

  // single item is forced to be ordered
  REQUIRE(update_sketch.compact(false).is_ordered());
}

TEST_CASE("tuple sketch float: exact mode", "[tuple_sketch]") {
  auto update_sketch = update_tuple_sketch<float>::builder().build();
  update_sketch.update(1, 1.0f);
  update_sketch.update(2, 2.0f);
  update_sketch.update(1, 1.0f);
//  std::cout << update_sketch.to_string(true);
  REQUIRE_FALSE(update_sketch.is_empty());
  REQUIRE_FALSE(update_sketch.is_estimation_mode());
  REQUIRE(update_sketch.get_estimate() == 2);
  REQUIRE(update_sketch.get_lower_bound(1) == 2);
  REQUIRE(update_sketch.get_upper_bound(1) == 2);
  REQUIRE(update_sketch.get_theta() == 1);
  REQUIRE(update_sketch.get_num_retained() == 2);
  REQUIRE_FALSE(update_sketch.is_ordered());
  int count = 0;
  for (const auto& entry: update_sketch) {
    REQUIRE(entry.second == 2);
    ++count;
  }
  REQUIRE(count == 2);

  auto compact_sketch = update_sketch.compact();
//  std::cout << compact_sketch.to_string(true);
  REQUIRE_FALSE(compact_sketch.is_empty());
  REQUIRE_FALSE(compact_sketch.is_estimation_mode());
  REQUIRE(compact_sketch.get_estimate() == 2);
  REQUIRE(compact_sketch.get_lower_bound(1) == 2);
  REQUIRE(compact_sketch.get_upper_bound(1) == 2);
  REQUIRE(compact_sketch.get_theta() == 1);
  REQUIRE(compact_sketch.get_num_retained() == 2);
  REQUIRE(compact_sketch.is_ordered());
  count = 0;
  for (const auto& entry: compact_sketch) {
    REQUIRE(entry.second == 2);
    ++count;
  }
  REQUIRE(count == 2);

  { // stream
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    compact_sketch.serialize(s);
    auto deserialized_sketch = compact_tuple_sketch<float>::deserialize(s);
    REQUIRE(!deserialized_sketch.is_empty());
    REQUIRE(!deserialized_sketch.is_estimation_mode());
    REQUIRE(deserialized_sketch.get_estimate() == 2);
    REQUIRE(deserialized_sketch.get_lower_bound(1) == 2);
    REQUIRE(deserialized_sketch.get_upper_bound(1) == 2);
    REQUIRE(deserialized_sketch.get_theta() == 1);
    REQUIRE(deserialized_sketch.get_num_retained() == 2);
    REQUIRE(deserialized_sketch.is_ordered());
//    std::cout << "deserialized sketch:" << std::endl;
//    std::cout << deserialized_sketch.to_string(true);
  }
  { // bytes
    auto bytes = compact_sketch.serialize();
    auto deserialized_sketch = compact_tuple_sketch<float>::deserialize(bytes.data(), bytes.size());
    REQUIRE(!deserialized_sketch.is_empty());
    REQUIRE(!deserialized_sketch.is_estimation_mode());
    REQUIRE(deserialized_sketch.get_estimate() == 2);
    REQUIRE(deserialized_sketch.get_lower_bound(1) == 2);
    REQUIRE(deserialized_sketch.get_upper_bound(1) == 2);
    REQUIRE(deserialized_sketch.get_theta() == 1);
    REQUIRE(deserialized_sketch.get_num_retained() == 2);
    REQUIRE(deserialized_sketch.is_ordered());
//    std::cout << deserialized_sketch.to_string(true);
  }
  // mixed
  {
    auto bytes = compact_sketch.serialize();
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    s.write(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    auto deserialized_sketch = compact_tuple_sketch<float>::deserialize(s);
    auto it = deserialized_sketch.begin();
    for (const auto& entry: compact_sketch) {
      REQUIRE(entry.first == (*it).first);
      REQUIRE(entry.second == (*it).second);
      ++it;
    }
  }

  update_sketch.reset();
  REQUIRE(update_sketch.is_empty());
  REQUIRE_FALSE(update_sketch.is_estimation_mode());
  REQUIRE(update_sketch.get_estimate() == 0);
  REQUIRE(update_sketch.get_lower_bound(1) == 0);
  REQUIRE(update_sketch.get_upper_bound(1) == 0);
  REQUIRE(update_sketch.get_theta() == 1);
  REQUIRE(update_sketch.get_num_retained() == 0);
  REQUIRE(update_sketch.is_ordered());
}

template<typename T>
class max_value_policy {
public:
  max_value_policy(const T& initial_value): initial_value(initial_value) {}
  T create() const { return initial_value; }
  void update(T& summary, const T& update) const { summary = std::max(summary, update); }
private:
  T initial_value;
};

using max_float_update_tuple_sketch = update_tuple_sketch<float, float, max_value_policy<float>>;

TEST_CASE("tuple sketch: float, custom policy", "[tuple_sketch]") {
  auto update_sketch = max_float_update_tuple_sketch::builder(max_value_policy<float>(5)).build();
  update_sketch.update(1, 1.0f);
  update_sketch.update(1, 2.0f);
  update_sketch.update(2, 10.0f);
  update_sketch.update(3, 3.0f);
  update_sketch.update(3, 7.0f);
//  std::cout << update_sketch.to_string(true);
  int count = 0;
  float sum = 0;
  for (const auto& entry: update_sketch) {
    sum += entry.second;
    ++count;
  }
  REQUIRE(count == 3);
  REQUIRE(sum == 22); // 5 + 10 + 7
}

struct three_doubles_update_policy {
  std::tuple<double, double, double> create() const {
    return std::tuple<double, double, double>(0, 0, 0);
  }
  void update(std::tuple<double, double, double>& summary, const std::tuple<double, double, double>& update) const {
    std::get<0>(summary) += std::get<0>(update);
    std::get<1>(summary) += std::get<1>(update);
    std::get<2>(summary) += std::get<2>(update);
  }
};

TEST_CASE("tuple sketch: tuple of doubles", "[tuple_sketch]") {
  using three_doubles_update_tuple_sketch = update_tuple_sketch<three_doubles, three_doubles, three_doubles_update_policy>;
  auto update_sketch = three_doubles_update_tuple_sketch::builder().build();
  update_sketch.update(1, three_doubles(1, 2, 3));
//  std::cout << update_sketch.to_string(true);
  const auto& entry = *update_sketch.begin();
  REQUIRE(std::get<0>(entry.second) == 1.0);
  REQUIRE(std::get<1>(entry.second) == 2.0);
  REQUIRE(std::get<2>(entry.second) == 3.0);

  auto compact_sketch = update_sketch.compact();
//  std::cout << compact_sketch.to_string(true);
  REQUIRE(compact_sketch.get_num_retained() == 1);
}

TEST_CASE("tuple sketch: float, update with different types of keys", "[tuple_sketch]") {
  auto sketch = update_tuple_sketch<float>::builder().build();

  sketch.update(static_cast<uint64_t>(1), 1.0f);
  REQUIRE(sketch.get_num_retained() == 1);

  sketch.update(static_cast<int64_t>(1), 1.0f);
  REQUIRE(sketch.get_num_retained() == 1);

  sketch.update(static_cast<uint32_t>(1), 1.0f);
  REQUIRE(sketch.get_num_retained() == 1);

  sketch.update(static_cast<int32_t>(1), 1.0f);
  REQUIRE(sketch.get_num_retained() == 1);

  sketch.update(static_cast<uint16_t>(1), 1.0f);
  REQUIRE(sketch.get_num_retained() == 1);

  sketch.update(static_cast<int16_t>(1), 1.0f);
  REQUIRE(sketch.get_num_retained() == 1);

  sketch.update(static_cast<uint8_t>(1), 1.0f);
  REQUIRE(sketch.get_num_retained() == 1);

  sketch.update(static_cast<int8_t>(1), 1.0f);
  REQUIRE(sketch.get_num_retained() == 1);

  sketch.update(1.0, 1.0f);
  REQUIRE(sketch.get_num_retained() == 2);

  sketch.update(static_cast<float>(1), 1.0f);
  REQUIRE(sketch.get_num_retained() == 2);

  sketch.update("a", 1.0f);
  REQUIRE(sketch.get_num_retained() == 3);
}

} /* namespace datasketches */

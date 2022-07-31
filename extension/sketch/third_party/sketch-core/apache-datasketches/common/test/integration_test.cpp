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

#include "cpc_sketch.hpp"
#include "cpc_union.hpp"
#include "frequent_items_sketch.hpp"
#include "hll.hpp"
#include "kll_sketch.hpp"
#include "req_sketch.hpp"
#include "var_opt_sketch.hpp"
#include "var_opt_union.hpp"
#include "theta_sketch.hpp"
#include "theta_union.hpp"
#include "theta_intersection.hpp"
#include "theta_a_not_b.hpp"
#include "tuple_sketch.hpp"
#include "tuple_union.hpp"
#include "tuple_intersection.hpp"
#include "tuple_a_not_b.hpp"

namespace datasketches {

template<typename Summary>
struct subtracting_intersection_policy {
  void operator()(Summary& summary, const Summary& other) const {
    summary -= other;
  }
};

using tuple_intersection_float = tuple_intersection<float, subtracting_intersection_policy<float>>;

TEST_CASE("integration: declare all sketches", "[integration]") {
  cpc_sketch cpc(12);
  cpc_union cpc_u(12);

  frequent_items_sketch<std::string> fi(100);

  hll_sketch hll(13);
  hll_union hll_u(13);

  kll_sketch<double> kll(200);

  req_sketch<double> req(12);

  var_opt_sketch<std::string> vo(100);
  var_opt_union<std::string> vo_u(100);

  update_theta_sketch theta = update_theta_sketch::builder().build();
  theta_union theta_u = theta_union::builder().build();
  theta_intersection theta_i;
  theta_a_not_b theta_anb;

  auto tuple = update_tuple_sketch<float>::builder().build();
  auto tuple_u = tuple_union<float>::builder().build();
  tuple_intersection_float tuple_i;
  tuple_a_not_b<float> tuple_anb;
}

} /* namespace datasketches */

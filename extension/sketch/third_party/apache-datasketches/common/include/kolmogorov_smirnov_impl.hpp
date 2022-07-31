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

#ifndef KOLMOGOROV_SMIRNOV_IMPL_HPP_
#define KOLMOGOROV_SMIRNOV_IMPL_HPP_

#include <cmath>
#include <algorithm>

namespace datasketches {

template<typename Sketch>
double kolmogorov_smirnov::delta(const Sketch& sketch1, const Sketch& sketch2) {
  auto comparator = sketch1.get_comparator(); // assuming the same comparator in sketch2
  auto view1 = sketch1.get_sorted_view(true);
  auto view2 = sketch2.get_sorted_view(true);
  auto it1 = view1.begin();
  auto it2 = view2.begin();
  const auto n1 = sketch1.get_n();
  const auto n2 = sketch2.get_n();
  double delta = 0;
  while (it1 != view1.end() && it2 != view2.end()) {
    const double norm_cum_wt1 = static_cast<double>((*it1).second) / n1;
    const double norm_cum_wt2 = static_cast<double>((*it2).second) / n2;
    delta = std::max(delta, std::abs(norm_cum_wt1 - norm_cum_wt2));
    if (comparator((*it1).first, (*it2).first)) {
      ++it1;
    } else if (comparator((*it2).first, (*it1).first)) {
      ++it2;
    } else {
      ++it1;
      ++it2;
    }
  }
  const double norm_cum_wt1 = it1 == view1.end() ? 1 : static_cast<double>((*it1).second) / n1;
  const double norm_cum_wt2 = it2 == view2.end() ? 1 : static_cast<double>((*it2).second) / n2;
  delta = std::max(delta, std::abs(norm_cum_wt1 - norm_cum_wt2));
  return delta;
}

template<typename Sketch>
double kolmogorov_smirnov::threshold(const Sketch& sketch1, const Sketch& sketch2, double p) {
  const double r1 = sketch1.get_num_retained();
  const double r2 = sketch2.get_num_retained();
  const double alpha_factor = sqrt(-0.5 * log(0.5 * p));
  const double delta_area_threshold = alpha_factor * sqrt((r1 + r2) / (r1 * r2));
  const double eps1 = sketch1.get_normalized_rank_error(false);
  const double eps2 = sketch2.get_normalized_rank_error(false);
  return delta_area_threshold + eps1 + eps2;
}

template<typename Sketch>
bool kolmogorov_smirnov::test(const Sketch& sketch1, const Sketch& sketch2, double p) {
  return delta(sketch1, sketch2) > threshold(sketch1, sketch2, p);
}

} /* namespace datasketches */

#endif

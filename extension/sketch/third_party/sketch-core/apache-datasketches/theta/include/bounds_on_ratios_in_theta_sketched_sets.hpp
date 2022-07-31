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

#ifndef BOUNDS_ON_RATIOS_IN_THETA_SKETCHED_SETS_HPP_
#define BOUNDS_ON_RATIOS_IN_THETA_SKETCHED_SETS_HPP_

#include <cstdint>
#include <stdexcept>

#include "bounds_on_ratios_in_sampled_sets.hpp"

namespace datasketches {

/**
 * This is to compute the bounds on the estimate of the ratio <i>B / A</i>, where:
 * <ul>
 * <li><i>A</i> is a Theta Sketch of population <i>PopA</i>.</li>
 * <li><i>B</i> is a Theta Sketch of population <i>PopB</i> that is a subset of <i>A</i>,
 * obtained by an intersection of <i>A</i> with some other Theta Sketch <i>C</i>,
 * which acts like a predicate or selection clause.</li>
 * <li>The estimate of the ratio <i>PopB/PopA</i> is
 * estimate_of_b_over_a(<i>A, B</i>).</li>
 * <li>The Upper Bound estimate on the ratio PopB/PopA is
 * upper_bound_for_b_over_a(<i>A, B</i>).</li>
 * <li>The Lower Bound estimate on the ratio PopB/PopA is
 * lower_bound_for_b_over_a(<i>A, B</i>).</li>
 * </ul>
 * Note: The theta of <i>A</i> cannot be greater than the theta of <i>B</i>.
 * If <i>B</i> is formed as an intersection of <i>A</i> and some other set <i>C</i>,
 * then the theta of <i>B</i> is guaranteed to be less than or equal to the theta of <i>B</i>.
 */
template<typename ExtractKey>
class bounds_on_ratios_in_theta_sketched_sets {
public:
  /**
   * Gets the approximate lower bound for B over A based on a 95% confidence interval
   * @param sketchA the sketch A
   * @param sketchB the sketch B
   * @return the approximate lower bound for B over A
   */
  template<typename SketchA, typename SketchB>
  static double lower_bound_for_b_over_a(const SketchA& sketch_a, const SketchB& sketch_b) {
    const uint64_t theta64_a = sketch_a.get_theta64();
    const uint64_t theta64_b = sketch_b.get_theta64();
    check_thetas(theta64_a, theta64_b);

    const uint64_t count_b = sketch_b.get_num_retained();
    const uint64_t count_a = theta64_a == theta64_b
        ? sketch_a.get_num_retained()
        : count_less_than_theta64(sketch_a, theta64_b);

    if (count_a == 0) return 0;
    const double f = sketch_b.get_theta();
    return bounds_on_ratios_in_sampled_sets::lower_bound_for_b_over_a(count_a, count_b, f);
  }

  /**
   * Gets the approximate upper bound for B over A based on a 95% confidence interval
   * @param sketchA the sketch A
   * @param sketchB the sketch B
   * @return the approximate upper bound for B over A
   */
  template<typename SketchA, typename SketchB>
  static double upper_bound_for_b_over_a(const SketchA& sketch_a, const SketchB& sketch_b) {
    const uint64_t theta64_a = sketch_a.get_theta64();
    const uint64_t theta64_b = sketch_b.get_theta64();
    check_thetas(theta64_a, theta64_b);

    const uint64_t count_b = sketch_b.get_num_retained();
    const uint64_t count_a = (theta64_a == theta64_b)
        ? sketch_a.get_num_retained()
        : count_less_than_theta64(sketch_a, theta64_b);

    if (count_a == 0) return 1;
    const double f = sketch_b.get_theta();
    return bounds_on_ratios_in_sampled_sets::upper_bound_for_b_over_a(count_a, count_b, f);
  }

  /**
   * Gets the estimate for B over A
   * @param sketchA the sketch A
   * @param sketchB the sketch B
   * @return the estimate for B over A
   */
  template<typename SketchA, typename SketchB>
  static double estimate_of_b_over_a(const SketchA& sketch_a, const SketchB& sketch_b) {
    const uint64_t theta64_a = sketch_a.get_theta64();
    const uint64_t theta64_b = sketch_b.get_theta64();
    check_thetas(theta64_a, theta64_b);

    const uint64_t count_b = sketch_b.get_num_retained();
    const uint64_t count_a = (theta64_a == theta64_b)
        ? sketch_a.get_num_retained()
        : count_less_than_theta64(sketch_a, theta64_b);

    if (count_a == 0) return 0.5;
    return static_cast<double>(count_b) / static_cast<double>(count_a);
  }

private:

  static inline void check_thetas(uint64_t theta_a, uint64_t theta_b) {
    if (theta_b > theta_a) {
      throw std::invalid_argument("theta_a must be <= theta_b");
    }
  }

  template<typename Sketch>
  static uint64_t count_less_than_theta64(const Sketch& sketch, uint64_t theta) {
    uint64_t count = 0;
    for (const auto& entry: sketch) if (ExtractKey()(entry) < theta) ++count;
    return count;
  }

};

} /* namespace datasketches */

# endif

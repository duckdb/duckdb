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

#ifndef BOUNDS_ON_RATIOS_IN_SAMPLED_SETS_HPP_
#define BOUNDS_ON_RATIOS_IN_SAMPLED_SETS_HPP_

#include <cstdint>
#include <string>
#include <stdexcept>

#include "bounds_binomial_proportions.hpp"

namespace datasketches {

/**
 * This class is used to compute the bounds on the estimate of the ratio <i>|B| / |A|</i>, where:
 * <ul>
 * <li><i>|A|</i> is the unknown size of a set <i>A</i> of unique identifiers.</li>
 * <li><i>|B|</i> is the unknown size of a subset <i>B</i> of <i>A</i>.</li>
 * <li><i>a</i> = <i>|S<sub>A</sub>|</i> is the observed size of a sample of <i>A</i>
 * that was obtained by Bernoulli sampling with a known inclusion probability <i>f</i>.</li>
 * <li><i>b</i> = <i>|S<sub>A</sub> &cap; B|</i> is the observed size of a subset
 * of <i>S<sub>A</sub></i>.</li>
 * </ul>
 */
class bounds_on_ratios_in_sampled_sets {
public:
  static constexpr double NUM_STD_DEVS = 2.0;

  /**
   * Return the approximate lower bound based on a 95% confidence interval
   * @param a See class javadoc
   * @param b See class javadoc
   * @param f the inclusion probability used to produce the set with size <i>a</i> and should
   * generally be less than 0.5. Above this value, the results not be reliable.
   * When <i>f</i> = 1.0 this returns the estimate.
   * @return the approximate upper bound
   */
  static double lower_bound_for_b_over_a(uint64_t a, uint64_t b, double f) {
    check_inputs(a, b, f);
    if (a == 0) return 0.0;
    if (f == 1.0) return static_cast<double>(b) / static_cast<double>(a);
    return bounds_binomial_proportions::approximate_lower_bound_on_p(a, b, NUM_STD_DEVS * hacky_adjuster(f));
  }

  /**
   * Return the approximate upper bound based on a 95% confidence interval
   * @param a See class javadoc
   * @param b See class javadoc
   * @param f the inclusion probability used to produce the set with size <i>a</i>.
   * @return the approximate lower bound
   */
  static double upper_bound_for_b_over_a(uint64_t a, uint64_t b, double f) {
    check_inputs(a, b, f);
    if (a == 0) return 1.0;
    if (f == 1.0) return static_cast<double>(b) / static_cast<double>(a);
    return bounds_binomial_proportions::approximate_upper_bound_on_p(a, b, NUM_STD_DEVS * hacky_adjuster(f));
  }

  /**
   * Return the estimate of b over a
   * @param a See class javadoc
   * @param b See class javadoc
   * @return the estimate of b over a
   */
  static double get_estimate_of_b_over_a(uint64_t a, uint64_t b) {
    check_inputs(a, b, 0.3);
    if (a == 0) return 0.5;
    return static_cast<double>(b) / static_cast<double>(a);
  }

  /**
   * Return the estimate of A. See class javadoc.
   * @param a See class javadoc
   * @param f the inclusion probability used to produce the set with size <i>a</i>.
   * @return the approximate lower bound
   */
  static double estimate_of_a(uint64_t a, double f) {
    check_inputs(a, 1, f);
    return a / f;
  }

  /**
   * Return the estimate of B. See class javadoc.
   * @param b See class javadoc
   * @param f the inclusion probability used to produce the set with size <i>b</i>.
   * @return the approximate lower bound
   */
  static double estimate_of_b(uint64_t b, double f) {
    check_inputs(b + 1, b, f);
    return b / f;
  }

private:
  /**
   * This hackyAdjuster is tightly coupled with the width of the confidence interval normally
   * specified with number of standard deviations. To simplify this interface the number of
   * standard deviations has been fixed to 2.0, which corresponds to a confidence interval of
   * 95%.
   * @param f the inclusion probability used to produce the set with size <i>a</i>.
   * @return the hacky Adjuster
   */
  static double hacky_adjuster(double f) {
    const double tmp = sqrt(1.0 - f);
    return (f <= 0.5) ? tmp : tmp + (0.01 * (f - 0.5));
  }

  static void check_inputs(uint64_t a, uint64_t b, double f) {
    if (a < b) {
      throw std::invalid_argument("a must be >= b: a = " + std::to_string(a) + ", b = " + std::to_string(b));
    }
    if ((f > 1.0) || (f <= 0.0)) {
      throw std::invalid_argument("Required: ((f <= 1.0) && (f > 0.0)): " + std::to_string(f));
    }
  }

};

} /* namespace datasketches */

# endif

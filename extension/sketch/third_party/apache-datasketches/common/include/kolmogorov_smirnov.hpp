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

#ifndef KOLMOGOROV_SMIRNOV_HPP_
#define KOLMOGOROV_SMIRNOV_HPP_

namespace datasketches {

class kolmogorov_smirnov {
public:
  /**
   * Computes the raw delta area between two quantile sketches for the Kolmogorov-Smirnov Test.
   * Will work for a type-matched pair of KLL or Quantiles sketches of the same parameterized type T.
   * @param sketch1 KLL sketch 1
   * @param sketch2 KLL sketch 2
   * @return the raw delta between two KLL quantile sketches
   */
  template<typename Sketch>
  static double delta(const Sketch& sketch1, const Sketch& sketch2);

  /**
   * Computes the adjusted delta area threshold for the Kolmogorov-Smirnov Test.
   * Adjusts the computed threshold by the error epsilons of the two given sketches.
   * See <a href="https://en.wikipedia.org/wiki/Kolmogorov-Smirnov_test">Kolmogorov–Smirnov Test</a>
   * Will work for a type-matched pair of KLL or Quantiles sketches of the same parameterized type T.
   * @param sketch1 KLL sketch 1
   * @param sketch2 KLL sketch 2
   * @param p Target p-value. Typically .001 to .1, e.g., .05.
   * @return the adjusted threshold to be compared with the raw delta
   */
  template<typename Sketch>
  static double threshold(const Sketch& sketch1, const Sketch& sketch2, double p);

  /**
   * Performs the Kolmogorov-Smirnov Test between two quantile sketches.
   * Will work for a type-matched pair of KLL or Quantiles sketches of the same parameterized type T.
   * Note: if the given sketches have insufficient data or if the sketch sizes are too small,
   * this will return false.
   * @param sketch1 KLL sketch 1
   * @param sketch2 KLL sketch 2
   * @param p Target p-value. Typically .001 to .1, e.g., .05.
   * @return Boolean indicating whether we can reject the null hypothesis (that the sketches
   * reflect the same underlying distribution) using the provided p-value.
   */
  template<typename Sketch>
  static bool test(const Sketch& sketch1, const Sketch& sketch2, double p);
};

} /* namespace datasketches */

#include "kolmogorov_smirnov_impl.hpp"

#endif

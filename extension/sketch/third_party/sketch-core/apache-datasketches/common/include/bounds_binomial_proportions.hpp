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

#ifndef _BOUNDS_BINOMIAL_PROPORTIONS_HPP_
#define _BOUNDS_BINOMIAL_PROPORTIONS_HPP_

#include <cmath>
#include <stdexcept>

namespace datasketches {

/**
 * Confidence intervals for binomial proportions.
 *
 * <p>This class computes an approximation to the Clopper-Pearson confidence interval
 * for a binomial proportion. Exact Clopper-Pearson intervals are strictly
 * conservative, but these approximations are not.</p>
 *
 * <p>The main inputs are numbers <i>n</i> and <i>k</i>, which are not the same as other things
 * that are called <i>n</i> and <i>k</i> in our sketching library. There is also a third
 * parameter, numStdDev, that specifies the desired confidence level.</p>
 * <ul>
 * <li><i>n</i> is the number of independent randomized trials. It is given and therefore known.
 * </li>
 * <li><i>p</i> is the probability of a trial being a success. It is unknown.</li>
 * <li><i>k</i> is the number of trials (out of <i>n</i>) that turn out to be successes. It is
 * a random variable governed by a binomial distribution. After any given
 * batch of <i>n</i> independent trials, the random variable <i>k</i> has a specific
 * value which is observed and is therefore known.</li>
 * <li><i>pHat</i> = <i>k</i> / <i>n</i> is an unbiased estimate of the unknown success
 * probability <i>p</i>.</li>
 * </ul>
 *
 * <p>Alternatively, consider a coin with unknown heads probability <i>p</i>. Where
 * <i>n</i> is the number of independent flips of that coin, and <i>k</i> is the number
 * of times that the coin comes up heads during a given batch of <i>n</i> flips.
 * This class computes a frequentist confidence interval [lowerBoundOnP, upperBoundOnP] for the
 * unknown <i>p</i>.</p>
 *
 * <p>Conceptually, the desired confidence level is specified by a tail probability delta.</p>
 *
 * <p>Ideally, over a large ensemble of independent batches of trials,
 * the fraction of batches in which the true <i>p</i> lies below lowerBoundOnP would be at most
 * delta, and the fraction of batches in which the true <i>p</i> lies above upperBoundOnP
 * would also be at most delta.
 *
 * <p>Setting aside the philosophical difficulties attaching to that statement, it isn't quite
 * true because we are approximating the Clopper-Pearson interval.</p>
 *
 * <p>Finally, we point out that in this class's interface, the confidence parameter delta is
 * not specified directly, but rather through a "number of standard deviations" numStdDev.
 * The library effectively converts that to a delta via delta = normalCDF (-1.0 * numStdDev).</p>
 *
 * <p>It is perhaps worth emphasizing that the library is NOT merely adding and subtracting
 * numStdDev standard deviations to the estimate. It is doing something better, that to some
 * extent accounts for the fact that the binomial distribution has a non-gaussian shape.</p>
 *
 * <p>In particular, it is using an approximation to the inverse of the incomplete beta function
 * that appears as formula 26.5.22 on page 945 of the "Handbook of Mathematical Functions"
 * by Abramowitz and Stegun.</p>
 *
 * @author Kevin Lang
 * @author Jon Malkin
 */
class bounds_binomial_proportions { // confidence intervals for binomial proportions

public:
  /**
   * Computes lower bound of approximate Clopper-Pearson confidence interval for a binomial
   * proportion.
   *
   * <p>Implementation Notes:<br>
   * The approximateLowerBoundOnP is defined with respect to the right tail of the binomial
   * distribution.</p>
   * <ul>
   * <li>We want to solve for the <i>p</i> for which sum<sub><i>j,k,n</i></sub>bino(<i>j;n,p</i>)
   * = delta.</li>
   * <li>We now restate that in terms of the left tail.</li>
   * <li>We want to solve for the p for which sum<sub><i>j,0,(k-1)</i></sub>bino(<i>j;n,p</i>)
   * = 1 - delta.</li>
   * <li>Define <i>x</i> = 1-<i>p</i>.</li>
   * <li>We want to solve for the <i>x</i> for which I<sub><i>x(n-k+1,k)</i></sub> = 1 - delta.</li>
   * <li>We specify 1-delta via numStdDevs through the right tail of the standard normal
   * distribution.</li>
   * <li>Smaller values of numStdDevs correspond to bigger values of 1-delta and hence to smaller
   * values of delta. In fact, usefully small values of delta correspond to negative values of
   * numStdDevs.</li>
   * <li>return <i>p</i> = 1-<i>x</i>.</li>
   * </ul>
   *
   * @param n is the number of trials. Must be non-negative.
   * @param k is the number of successes. Must be non-negative, and cannot exceed n.
   * @param num_std_devs the number of standard deviations defining the confidence interval
   * @return the lower bound of the approximate Clopper-Pearson confidence interval for the
   * unknown success probability.
   */
  static inline double approximate_lower_bound_on_p(uint64_t n, uint64_t k, double num_std_devs) {
    check_inputs(n, k);
    if (n == 0) { return 0.0; } // the coin was never flipped, so we know nothing
    else if (k == 0) { return 0.0; }
    else if (k == 1) { return (exact_lower_bound_on_p_k_eq_1(n, delta_of_num_stdevs(num_std_devs))); }
    else if (k == n) { return (exact_lower_bound_on_p_k_eq_n(n, delta_of_num_stdevs(num_std_devs))); }
    else {
      double x = abramowitz_stegun_formula_26p5p22((n - k) + 1.0, static_cast<double>(k), (-1.0 * num_std_devs));
      return (1.0 - x); // which is p
    }
  }

  /**
   * Computes upper bound of approximate Clopper-Pearson confidence interval for a binomial
   * proportion.
   *
   * <p>Implementation Notes:<br>
   * The approximateUpperBoundOnP is defined with respect to the left tail of the binomial
   * distribution.</p>
   * <ul>
   * <li>We want to solve for the <i>p</i> for which sum<sub><i>j,0,k</i></sub>bino(<i>j;n,p</i>)
   * = delta.</li>
   * <li>Define <i>x</i> = 1-<i>p</i>.</li>
   * <li>We want to solve for the <i>x</i> for which I<sub><i>x(n-k,k+1)</i></sub> = delta.</li>
   * <li>We specify delta via numStdDevs through the right tail of the standard normal
   * distribution.</li>
   * <li>Bigger values of numStdDevs correspond to smaller values of delta.</li>
   * <li>return <i>p</i> = 1-<i>x</i>.</li>
   * </ul>
   * @param n is the number of trials. Must be non-negative.
   * @param k is the number of successes. Must be non-negative, and cannot exceed <i>n</i>.
   * @param num_std_devs the number of standard deviations defining the confidence interval
   * @return the upper bound of the approximate Clopper-Pearson confidence interval for the
   * unknown success probability.
   */
  static inline double approximate_upper_bound_on_p(uint64_t n, uint64_t k, double num_std_devs) {
    check_inputs(n, k);
    if (n == 0) { return 1.0; } // the coin was never flipped, so we know nothing
    else if (k == n) { return 1.0; }
    else if (k == (n - 1)) {
      return (exact_upper_bound_on_p_k_eq_minusone(n, delta_of_num_stdevs(num_std_devs)));
    }
    else if (k == 0) {
      return (exact_upper_bound_on_p_k_eq_zero(n, delta_of_num_stdevs(num_std_devs)));
    }
    else {
      double x = abramowitz_stegun_formula_26p5p22(static_cast<double>(n - k), k + 1.0, num_std_devs);
      return (1.0 - x); // which is p
    }
  }

  /**
   * Computes an estimate of an unknown binomial proportion.
   * @param n is the number of trials. Must be non-negative.
   * @param k is the number of successes. Must be non-negative, and cannot exceed n.
   * @return the estimate of the unknown binomial proportion.
   */
  static inline double estimate_unknown_p(uint64_t n, uint64_t k) {
    check_inputs(n, k);
    if (n == 0) { return 0.5; } // the coin was never flipped, so we know nothing
    else { return ((double) k / (double) n); }
  }

  /**
   * Computes an approximation to the erf() function.
   * @param x is the input to the erf function
   * @return returns erf(x), accurate to roughly 7 decimal digits.
   */
  static inline double erf(double x) {
    if (x < 0.0) { return (-1.0 * (erf_of_nonneg(-1.0 * x))); }
    else { return (erf_of_nonneg(x)); }
  }

  /**
   * Computes an approximation to normal_cdf(x).
   * @param x is the input to the normal_cdf function
   * @return returns the approximation to normalCDF(x).
   */
   static inline double normal_cdf(double x) {
    return (0.5 * (1.0 + (erf(x / (sqrt(2.0))))));
  }

private:
  static inline void check_inputs(uint64_t n, uint64_t k) {
    if (k > n) { throw std::invalid_argument("K cannot exceed N"); }
  }

  //@formatter:off
  // Abramowitz and Stegun formula 7.1.28, p. 88; Claims accuracy of about 7 decimal digits */
  static inline double erf_of_nonneg(double x) {
    // The constants that appear below, formatted for easy checking against the book.
    //    a1 = 0.07052 30784
    //    a3 = 0.00927 05272
    //    a5 = 0.00027 65672
    //    a2 = 0.04228 20123
    //    a4 = 0.00015 20143
    //    a6 = 0.00004 30638
    static const double a1 = 0.0705230784;
    static const double a3 = 0.0092705272;
    static const double a5 = 0.0002765672;
    static const double a2 = 0.0422820123;
    static const double a4 = 0.0001520143;
    static const double a6 = 0.0000430638;
    const double x2 = x * x; // x squared, x cubed, etc.
    const double x3 = x2 * x;
    const double x4 = x2 * x2;
    const double x5 = x2 * x3;
    const double x6 = x3 * x3;
    const double sum = ( 1.0
                 + (a1 * x)
                 + (a2 * x2)
                 + (a3 * x3)
                 + (a4 * x4)
                 + (a5 * x5)
                 + (a6 * x6) );
    const double sum2 = sum * sum; // raise the sum to the 16th power
    const double sum4 = sum2 * sum2;
    const double sum8 = sum4 * sum4;
    const double sum16 = sum8 * sum8;
    return (1.0 - (1.0 / sum16));
  }

  static inline double delta_of_num_stdevs(double kappa) {
    return (normal_cdf(-1.0 * kappa));
  }

  //@formatter:on
  // Formula 26.5.22 on page 945 of Abramowitz & Stegun, which is an approximation
  // of the inverse of the incomplete beta function I_x(a,b) = delta
  // viewed as a scalar function of x.
  // In other words, we specify delta, and it gives us x (with a and b held constant).
  // However, delta is specified in an indirect way through yp which
  // is the number of stdDevs that leaves delta probability in the right
  // tail of a standard gaussian distribution.

  // We point out that the variable names correspond to those in the book,
  // and it is worth keeping it that way so that it will always be easy to verify
  // that the formula was typed in correctly.

  static inline double abramowitz_stegun_formula_26p5p22(double a, double b, double yp) {
    const double b2m1 = (2.0 * b) - 1.0;
    const double a2m1 = (2.0 * a) - 1.0;
    const double lambda = ((yp * yp) - 3.0) / 6.0;
    const double htmp = (1.0 / a2m1) + (1.0 / b2m1);
    const double h = 2.0 / htmp;
    const double term1 = (yp * (sqrt(h + lambda))) / h;
    const double term2 = (1.0 / b2m1) - (1.0 / a2m1);
    const double term3 = (lambda + (5.0 / 6.0)) - (2.0 / (3.0 * h));
    const double w = term1 - (term2 * term3);
    const double xp = a / (a + (b * (exp(2.0 * w))));
    return xp;
  }

  // Formulas for some special cases.

  static inline double exact_upper_bound_on_p_k_eq_zero(uint64_t n, double delta) {
    return (1.0 - pow(delta, (1.0 / n)));
  }

  static inline double exact_lower_bound_on_p_k_eq_n(uint64_t n, double delta) {
    return (pow(delta, (1.0 / n)));
  }

  static inline double exact_lower_bound_on_p_k_eq_1(uint64_t n, double delta) {
    return (1.0 - pow((1.0 - delta), (1.0 / n)));
  }

  static inline double exact_upper_bound_on_p_k_eq_minusone(uint64_t n, double delta) {
    return (pow((1.0 - delta), (1.0 / n)));
  }

};

}

#endif // _BOUNDS_BINOMIAL_PROPORTIONS_HPP_

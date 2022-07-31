# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import unittest
from datasketches import kll_ints_sketch, kll_floats_sketch, kll_doubles_sketch, ks_test
import numpy as np

class KllTest(unittest.TestCase):
    def test_kll_example(self):
      k = 160
      n = 2 ** 20

      # create a sketch and inject ~1 million N(0,1) points as an array and as a single item
      kll = kll_floats_sketch(k)
      kll.update(np.random.normal(size=n-1))
      kll.update(0.0)

      # 0 should be near the median
      self.assertAlmostEqual(0.5, kll.get_rank(0.0), delta=0.035)
      
      # the median should be near 0
      self.assertAlmostEqual(0.0, kll.get_quantile(0.5), delta=0.035)

      # we also track the min/max independently from the rest of the data
      # which lets us know the full observed data range
      self.assertLessEqual(kll.get_min_value(), kll.get_quantile(0.01))
      self.assertLessEqual(0.0, kll.get_rank(kll.get_min_value()))
      self.assertGreaterEqual(kll.get_max_value(), kll.get_quantile(0.99))
      self.assertGreaterEqual(1.0, kll.get_rank(kll.get_max_value()))

      # we can also extract a list of values at a time,
      # here the values should give us something close to [-2, -1, 0, 1, 2].
      # then get the CDF, which will return something close to
      # the original values used in get_quantiles()
      # finally, can check the normalized rank error bound
      pts = kll.get_quantiles([0.0228, 0.1587, 0.5, 0.8413, 0.9772])
      cdf = kll.get_cdf(pts)  # include 1.0 at end to account for all probability mass
      self.assertEqual(len(cdf), len(pts)+1)
      err = kll.normalized_rank_error(False)
      self.assertEqual(err, kll_floats_sketch.get_normalized_rank_error(k, False))

      # and a few basic queries about the sketch
      self.assertFalse(kll.is_empty())
      self.assertTrue(kll.is_estimation_mode())
      self.assertEqual(kll.get_n(), n)
      self.assertEqual(kll.get_k(), k)
      self.assertLess(kll.get_num_retained(), n)

      # merging itself will double the number of items the sketch has seen
      kll.merge(kll)
      self.assertEqual(kll.get_n(), 2*n)

      # we can then serialize and reconstruct the sketch
      kll_bytes = kll.serialize()
      new_kll = kll.deserialize(kll_bytes)
      self.assertEqual(kll.get_num_retained(), new_kll.get_num_retained())
      self.assertEqual(kll.get_min_value(), new_kll.get_min_value())
      self.assertEqual(kll.get_max_value(), new_kll.get_max_value())
      self.assertEqual(kll.get_quantile(0.7), new_kll.get_quantile(0.7))
      self.assertEqual(kll.get_rank(0.0), new_kll.get_rank(0.0))

      # A Kolmogorov-Smirnov Test of kll and new_kll should match, even for
      # a fairly small p-value -- cannot reject the null hypothesis that
      # they come from the same distribution (since they do)
      self.assertFalse(ks_test(kll, new_kll, 0.001))


    def test_kll_ints_sketch(self):
        k = 100
        n = 10
        kll = kll_ints_sketch(k)
        for i in range(0, n):
          kll.update(i)

        self.assertEqual(kll.get_min_value(), 0)
        self.assertEqual(kll.get_max_value(), n-1)
        self.assertEqual(kll.get_n(), n)
        self.assertFalse(kll.is_empty())
        self.assertFalse(kll.is_estimation_mode()) # n < k
        self.assertEqual(kll.get_k(), k)

        pmf = kll.get_pmf([round(n/2)])
        self.assertIsNotNone(pmf)
        self.assertEqual(len(pmf), 2)

        cdf = kll.get_cdf([round(n/2)])
        self.assertIsNotNone(cdf)
        self.assertEqual(len(cdf), 2)

        self.assertEqual(kll.get_quantile(0.5), round(n/2))
        quants = kll.get_quantiles([0.25, 0.5, 0.75])
        self.assertIsNotNone(quants)
        self.assertEqual(len(quants), 3)

        self.assertEqual(kll.get_rank(round(n/2)), 0.5)

        # merge self
        kll.merge(kll)
        self.assertEqual(kll.get_n(), 2 * n)

        sk_bytes = kll.serialize()
        self.assertTrue(isinstance(kll_ints_sketch.deserialize(sk_bytes), kll_ints_sketch))

    def test_kll_doubles_sketch(self):
      # already tested float and ints and it's templatized, so just make sure it instantiates properly
      k = 75
      kll = kll_doubles_sketch(k)
      self.assertTrue(kll.is_empty())

if __name__ == '__main__':
    unittest.main()

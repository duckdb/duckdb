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
from datasketches import quantiles_ints_sketch, quantiles_floats_sketch, quantiles_doubles_sketch, ks_test
import numpy as np

class QuantilesTest(unittest.TestCase):
    def test_quantiles_example(self):
      k = 128
      n = 2 ** 20

      # create a sketch and inject ~1 million N(0,1) points as an array and as a single item
      quantiles = quantiles_floats_sketch(k)
      quantiles.update(np.random.normal(size=n-1))
      quantiles.update(0.0)

      # 0 should be near the median
      self.assertAlmostEqual(0.5, quantiles.get_rank(0.0), delta=0.035)
      
      # the median should be near 0
      self.assertAlmostEqual(0.0, quantiles.get_quantile(0.5), delta=0.035)

      # we also track the min/max independently from the rest of the data
      # which lets us know the full observed data range
      self.assertLessEqual(quantiles.get_min_value(), quantiles.get_quantile(0.01))
      self.assertLessEqual(0.0, quantiles.get_rank(quantiles.get_min_value()))
      self.assertGreaterEqual(quantiles.get_max_value(), quantiles.get_quantile(0.99))
      self.assertGreaterEqual(1.0, quantiles.get_rank(quantiles.get_max_value()))

      # we can also extract a list of values at a time,
      # here the values should give us something close to [-2, -1, 0, 1, 2].
      # then get the CDF, which will return something close to
      # the original values used in get_quantiles()
      # finally, can check the normalized rank error bound
      pts = quantiles.get_quantiles([0.0228, 0.1587, 0.5, 0.8413, 0.9772])
      cdf = quantiles.get_cdf(pts)  # include 1.0 at end to account for all probability mass
      self.assertEqual(len(cdf), len(pts)+1)
      err = quantiles.normalized_rank_error(False)
      self.assertEqual(err, quantiles_floats_sketch.get_normalized_rank_error(k, False))

      # and a few basic queries about the sketch
      self.assertFalse(quantiles.is_empty())
      self.assertTrue(quantiles.is_estimation_mode())
      self.assertEqual(quantiles.get_n(), n)
      self.assertEqual(quantiles.get_k(), k)
      self.assertLess(quantiles.get_num_retained(), n)

      # merging itself will double the number of items the sketch has seen
      quantiles.merge(quantiles)
      self.assertEqual(quantiles.get_n(), 2*n)

      # we can then serialize and reconstruct the sketch
      quantiles_bytes = quantiles.serialize()
      new_quantiles = quantiles.deserialize(quantiles_bytes)
      self.assertEqual(quantiles.get_num_retained(), new_quantiles.get_num_retained())
      self.assertEqual(quantiles.get_min_value(), new_quantiles.get_min_value())
      self.assertEqual(quantiles.get_max_value(), new_quantiles.get_max_value())
      self.assertEqual(quantiles.get_quantile(0.7), new_quantiles.get_quantile(0.7))
      self.assertEqual(quantiles.get_rank(0.0), new_quantiles.get_rank(0.0))

      # If we create a new sketch with a very different distribution, a Kolmogorov-Smirnov Test
      # of the two should return True: we can reject the null hypothesis that the sketches
      # come from the same distributions.
      unif_quantiles = quantiles_floats_sketch(k)
      unif_quantiles.update(np.random.uniform(10, 20, size=n-1))
      self.assertTrue(ks_test(quantiles, unif_quantiles, 0.001))

    def test_quantiles_ints_sketch(self):
        k = 128
        n = 10
        quantiles = quantiles_ints_sketch(k)
        for i in range(0, n):
          quantiles.update(i)

        self.assertEqual(quantiles.get_min_value(), 0)
        self.assertEqual(quantiles.get_max_value(), n-1)
        self.assertEqual(quantiles.get_n(), n)
        self.assertFalse(quantiles.is_empty())
        self.assertFalse(quantiles.is_estimation_mode()) # n < k
        self.assertEqual(quantiles.get_k(), k)

        pmf = quantiles.get_pmf([round(n/2)])
        self.assertIsNotNone(pmf)
        self.assertEqual(len(pmf), 2)

        cdf = quantiles.get_cdf([round(n/2)])
        self.assertIsNotNone(cdf)
        self.assertEqual(len(cdf), 2)

        self.assertEqual(quantiles.get_quantile(0.5), round(n/2))
        quants = quantiles.get_quantiles([0.25, 0.5, 0.75])
        self.assertIsNotNone(quants)
        self.assertEqual(len(quants), 3)

        self.assertEqual(quantiles.get_rank(round(n/2)), 0.5)

        # merge self
        quantiles.merge(quantiles)
        self.assertEqual(quantiles.get_n(), 2 * n)

        sk_bytes = quantiles.serialize()
        self.assertTrue(isinstance(quantiles_ints_sketch.deserialize(sk_bytes), quantiles_ints_sketch))

    def test_quantiles_doubles_sketch(self):
      # already tested floats and ints and it's templatized, so just make sure it instantiates properly
      k = 128
      quantiles = quantiles_doubles_sketch(k)
      self.assertTrue(quantiles.is_empty())

if __name__ == '__main__':
    unittest.main()

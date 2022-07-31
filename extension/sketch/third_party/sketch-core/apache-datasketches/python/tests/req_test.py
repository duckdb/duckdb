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
from datasketches import req_ints_sketch, req_floats_sketch
import numpy as np

class reqTest(unittest.TestCase):
    def test_req_example(self):
      k = 12
      n = 2 ** 20

      # create a sketch and inject ~1 million N(0,1) points as an array and as a single item
      req = req_floats_sketch(k, True) # high rank accuracy
      req.update(np.random.normal(size=n-1))
      req.update(0.0)

      # 0 should be near the median
      self.assertAlmostEqual(0.5, req.get_rank(0.0), delta=0.045)
      
      # the median should be near 0
      self.assertAlmostEqual(0.0, req.get_quantile(0.5), delta=0.045)

      # we also track the min/max independently from the rest of the data
      # which lets us know the full observed data range
      self.assertLessEqual(req.get_min_value(), req.get_quantile(0.01))
      self.assertLessEqual(0.0, req.get_rank(req.get_min_value()))
      self.assertGreaterEqual(req.get_max_value(), req.get_quantile(0.99))
      self.assertGreaterEqual(1.0, req.get_rank(req.get_max_value()))

      # we can also extract a list of values at a time,
      # here the values should give us something close to [-2, -1, 0, 1, 2].
      # then get the CDF, which will return something close to
      # the original values used in get_quantiles()
      # finally, can check the normalized rank error bound
      pts = req.get_quantiles([0.0228, 0.1587, 0.5, 0.8413, 0.9772])
      cdf = req.get_cdf(pts)  # include 1.0 at end to account for all probability mass
      self.assertEqual(len(cdf), len(pts)+1)
      
      # For relative error quantiles, the error depends on the actual rank
      # so we need to use that to detemrine the bounds
      est = req.get_rank(0.999, True)
      lb = req.get_rank_lower_bound(est, 1)
      ub = req.get_rank_upper_bound(est, 1)
      self.assertLessEqual(lb, est)
      self.assertLessEqual(est, ub)

      # and a few basic queries about the sketch
      self.assertFalse(req.is_empty())
      self.assertTrue(req.is_estimation_mode())
      self.assertEqual(req.get_n(), n)
      self.assertLess(req.get_num_retained(), n)
      self.assertEqual(req.get_k(), k)

      # merging itself will double the number of items the sketch has seen
      req.merge(req)
      self.assertEqual(req.get_n(), 2*n)

      # we can then serialize and reconstruct the sketch
      req_bytes = req.serialize()
      new_req = req.deserialize(req_bytes)
      self.assertEqual(req.get_num_retained(), new_req.get_num_retained())
      self.assertEqual(req.get_min_value(), new_req.get_min_value())
      self.assertEqual(req.get_max_value(), new_req.get_max_value())
      self.assertEqual(req.get_quantile(0.7), new_req.get_quantile(0.7))
      self.assertEqual(req.get_rank(0.0), new_req.get_rank(0.0))

    def test_req_ints_sketch(self):
        k = 100
        n = 10
        req = req_ints_sketch(k)
        for i in range(0, n):
          req.update(i)

        self.assertEqual(req.get_min_value(), 0)
        self.assertEqual(req.get_max_value(), n-1)
        self.assertEqual(req.get_n(), n)
        self.assertFalse(req.is_empty())
        self.assertFalse(req.is_estimation_mode()) # n < k
        self.assertEqual(req.get_k(), k)

        pmf = req.get_pmf([round(n/2)])
        self.assertIsNotNone(pmf)
        self.assertEqual(len(pmf), 2)

        cdf = req.get_cdf([round(n/2)])
        self.assertIsNotNone(cdf)
        self.assertEqual(len(cdf), 2)

        self.assertEqual(req.get_quantile(0.5), round(n/2))
        quants = req.get_quantiles([0.25, 0.5, 0.75])
        self.assertIsNotNone(quants)
        self.assertEqual(len(quants), 3)

        self.assertEqual(req.get_rank(round(n/2)), 0.5)

        # merge self
        req.merge(req)
        self.assertEqual(req.get_n(), 2 * n)

        sk_bytes = req.serialize()
        self.assertTrue(isinstance(req_ints_sketch.deserialize(sk_bytes), req_ints_sketch))

    def test_req_floats_sketch(self):
      # already tested ints and it's templatized, so just make sure it instantiates properly
      k = 75
      req = req_floats_sketch(k, False) # low rank accuracy
      self.assertTrue(req.is_empty())
      self.assertFalse(req.is_hra())

if __name__ == '__main__':
    unittest.main()

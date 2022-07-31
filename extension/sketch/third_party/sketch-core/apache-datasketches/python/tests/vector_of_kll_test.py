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
from datasketches import (vector_of_kll_ints_sketches,
                          vector_of_kll_floats_sketches)
import numpy as np

class VectorOfKllSketchesTest(unittest.TestCase):
    def test_vector_of_kll_floats_sketches_example(self):
      k = 200
      d = 3
      n = 2 ** 20

      # create a sketch and inject ~1 million N(0,1) points
      kll = vector_of_kll_floats_sketches(k, d)
      # Track the min/max for each sketch to test later
      smin = np.zeros(d) + np.inf
      smax = np.zeros(d) - np.inf

      for i in range(0, n):
        dat  = np.random.randn(d)
        smin = np.amin([smin, dat], axis=0)
        smax = np.amax([smax, dat], axis=0)
        kll.update(dat)

      # 0 should be near the median
      np.testing.assert_allclose(0.5, kll.get_ranks(0.0), atol=0.035)
      # the median should be near 0
      np.testing.assert_allclose(0.0, kll.get_quantiles(0.5), atol=0.035)
      # we also track the min/max independently from the rest of the data
      # which lets us know the full observed data range
      np.testing.assert_allclose(kll.get_min_values(), smin)
      np.testing.assert_allclose(kll.get_max_values(), smax)
      np.testing.assert_array_less(kll.get_min_values(), kll.get_quantiles(0.01)[:,0])
      np.testing.assert_array_less(kll.get_quantiles(0.99)[:,0], kll.get_max_values())

      # we can also extract a list of values at a time,
      # here the values should give us something close to [-2, -1, 0, 1, 2].
      # then get the CDF, which will return something close to
      # the original values used in get_quantiles()
      # finally, can check the normalized rank error bound
      pts = kll.get_quantiles([0.0228, 0.1587, 0.5, 0.8413, 0.9772])
      # use the mean pts for the CDF, include 1.0 at end to account for all probability mass
      meanpts = np.mean(pts, axis=0)
      cdf = kll.get_cdf(meanpts)
      self.assertEqual(cdf.shape[0], pts.shape[0])
      self.assertEqual(cdf.shape[1], pts.shape[1]+1)

      # and a few basic queries about the sketch
      self.assertFalse(np.all(kll.is_empty()))
      self.assertTrue(np.all(kll.is_estimation_mode()))
      self.assertTrue(np.all(kll.get_n() == n))
      self.assertTrue(np.all(kll.get_num_retained() < n))

      # we can combine sketches across all dimensions and get the reuslt
      result = kll.collapse()
      self.assertEqual(result.get_n(), d * n)

      # merging a copy of itself will double the number of items the sketch has seen
      kll_copy = vector_of_kll_floats_sketches(kll)
      kll.merge(kll_copy)
      np.testing.assert_equal(kll.get_n(), 2*n)

      # we can then serialize and reconstruct the sketch
      kll_bytes = kll.serialize() # serializes each sketch as a list
      new_kll = vector_of_kll_floats_sketches(k, d)
      for s in range(len(kll_bytes)):
        new_kll.deserialize(kll_bytes[s], s)

      # everything should be exactly equal
      np.testing.assert_equal(kll.get_num_retained(), new_kll.get_num_retained())
      np.testing.assert_equal;(kll.get_min_values(), new_kll.get_min_values())
      np.testing.assert_equal(kll.get_max_values(), new_kll.get_max_values())
      np.testing.assert_equal(kll.get_quantiles(0.7), new_kll.get_quantiles(0.7))
      np.testing.assert_equal(kll.get_ranks(0.0), new_kll.get_ranks(0.0))

    def test_kll_ints_sketches(self):
      # already tested floats and it's templatized, so just make sure it instantiates properly
      k = 100
      d = 5
      kll = vector_of_kll_ints_sketches(k, d)
      self.assertTrue(np.all(kll.is_empty()))

    def test_kll_2Dupdates(self):
      # 1D case tested in the first example
      # 2D case will follow same idea, but focusing on update()
      k = 200
      d = 3
      # we'll do ~250k updates of 4 values each (total ~1mil updates, as above)
      n = 2 ** 18
      nbatch = 4

      # create a sketch and inject ~1 million N(0,1) points
      kll = vector_of_kll_floats_sketches(k, d)
      # Track the min/max for each sketch to test later
      smin = np.zeros(d) + np.inf
      smax = np.zeros(d) - np.inf

      for i in range(0, n):
        dat  = np.random.randn(nbatch, d)
        smin = np.amin(np.row_stack((smin, dat)), axis=0)
        smax = np.amax(np.row_stack((smax, dat)), axis=0)
        kll.update(dat)

      # 0 should be near the median
      np.testing.assert_allclose(0.5, kll.get_ranks(0.0), atol=0.035)
      # the median should be near 0
      np.testing.assert_allclose(0.0, kll.get_quantiles(0.5), atol=0.035)
      # we also track the min/max independently from the rest of the data
      # which lets us know the full observed data range
      np.testing.assert_allclose(kll.get_min_values(), smin)
      np.testing.assert_allclose(kll.get_max_values(), smax)

    def test_kll_3Dupdates(self):
      # now test 3D update, which should fail
      k = 200
      d = 3

      # create a sketch
      kll = vector_of_kll_floats_sketches(k, d)

      # we'll try 1 3D update
      dat = np.random.randn(10, 7, d)
      try:
        kll.update(dat)
      except:
        # this is what we expect
        pass
      # the sketches should still be empty
      self.assertTrue(np.all(kll.is_empty()))

if __name__ == '__main__':
    unittest.main()

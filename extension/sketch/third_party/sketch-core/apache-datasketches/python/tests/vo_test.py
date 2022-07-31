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
from datasketches import var_opt_sketch, var_opt_union

class VoTest(unittest.TestCase):
  def test_vo_example(self):
    k = 50  # a small value so we can easily fill the sketch
    vo = var_opt_sketch(k)

    # varopt sampling reduces to standard reservoir sampling
    # if the items are all equally weighted, although the
    # algorithm will be significantly slower than an optimized
    # reservoir sampler
    n = 5 * k
    for i in range(0, n):
      vo.update(i)

    # we can also add a heavy item, using a negative weight for
    # easy filtering later.  keep in mind that "heavy" is a
    # relative concept, so using a fixed multiple of n may not
    # be considered a heavy item for larger values of n
    vo.update(-1, 1000 * n)
    self.assertEqual(k, vo.k)
    self.assertEqual(k, vo.num_samples)
    self.assertEqual(n + 1, vo.n)
    self.assertFalse(vo.is_empty())

    # we can easily get the list of items in the sample
    items = vo.get_samples()
    self.assertEqual(len(items), k)

    # we can also apply a predicate to the sketch to get an estimate
    # (with optimally minimal variance) of the subset sum of items
    # matching that predicate among the entire population

    # we'll use a lambda here, but any function operating on a single
    # item which returns a boolean value should work
    summary = vo.estimate_subset_sum(lambda x: x < 0)
    self.assertEqual(summary['estimate'], 1000 * n)
    self.assertEqual(summary['total_sketch_weight'], 1001 * n)

    # a regular function is similarly handled
    def geq_zero(x):
      return x >= 0
    summary = vo.estimate_subset_sum(geq_zero)
    self.assertEqual(summary['estimate'], n)
    self.assertEqual(summary['total_sketch_weight'], 1001 * n)

    # next we'll create a second, smaller sketch with
    # only heavier items relative to the previous sketch,
    # but with the sketch in sampling mode
    k2 = 5
    vo2 = var_opt_sketch(k2)
    # for weight, use the estimate of all items >=0 from before
    wt = summary['estimate']
    for i in range(0, k2 + 1):
      vo2.update((2 * n) + i, wt)

    # now union the sketches, demonstrating how the
    # union's k may not be equal to that of either
    # input value
    union = var_opt_union(k)
    union.update(vo)
    union.update(vo2)

    result = union.get_result()
    self.assertEqual(n + k2 + 2, result.n)
    self.assertFalse(result.is_empty())
    self.assertGreater(result.k, k2)
    self.assertLess(result.k, k)

    # we can compare what information is available from both
    # the union and a sketch.
    print(union)

    # if we want to print the list of items, there must be a
    # __str__() method for each item (which need not be the same
    # type; they're all generic python objects when used from
    # python), otherwise you may trigger an exception.
    # to_string() is provided as a convenience to avoid direct
    # calls to __str__() with parameters.
    print(result.to_string(True))

if __name__ == '__main__':
  unittest.main()

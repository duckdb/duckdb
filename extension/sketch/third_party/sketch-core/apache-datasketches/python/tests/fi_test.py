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
from datasketches import frequent_strings_sketch, frequent_items_error_type

class FiTest(unittest.TestCase):
  def test_fi_example(self):
    k = 3  # a small value so we can easily fill the sketch
    fi = frequent_strings_sketch(k)

    # we'll use a small number of distinct items so we
    # can use exponentially increasing weights and have
    # some frequent items, decreasing so we have some
    # small items inserted after a purge
    n = 8
    for i in range(0, n):
      fi.update(str(i), 2 ** (n - i))

    # there are two ways to extract items :
    # * NO_FALSE_POSITIVES includes all items with a lower bound
    #   above the a posteriori error
    # * NO_FALSE_NEGATIVES includes all items with an uper bound
    #   above the a posteriori error
    # a more complete discussion may be found at
    # https://datasketches.github.io/docs/Frequency/FrequentItemsOverview.html
    items_no_fp = fi.get_frequent_items(frequent_items_error_type.NO_FALSE_POSITIVES)
    items_no_fn = fi.get_frequent_items(frequent_items_error_type.NO_FALSE_NEGATIVES)
    self.assertLessEqual(len(items_no_fp), len(items_no_fn))

    # the items list returns a decreasing weight-sorted list, and
    # for each item we have (item, estimate, lower_bound, upper_bound)
    item = items_no_fp[1]
    self.assertLessEqual(item[2], item[1]) # lower bound vs estimate 
    self.assertLessEqual(item[1], item[3]) # estimate vs upper bound

    # we can also query directly for a specific item
    id = items_no_fn[0][0]
    est = fi.get_estimate(id)
    lb = fi.get_lower_bound(id)
    ub = fi.get_upper_bound(id)
    self.assertLessEqual(lb, est)
    self.assertLessEqual(est, ub)

    # the values are zero if the item isn't in our list
    self.assertEqual(fi.get_estimate("NaN"), 0)

    # now create a second sketch with a lot of unique
    # values but all with equal weight (of 1) such that
    # the total weight is much larger than the first sketch
    fi2 = frequent_strings_sketch(k)
    wt = fi.get_total_weight()
    for i in range(0, 4*wt):
      fi2.update(str(i))

    # merge the second sketch into the first
    fi.merge(fi2)

    # we can see that the weight is much larger
    self.assertEqual(5 * wt, fi.get_total_weight())

    # querying with NO_FALSE_POSITIVES means we don't find anything
    # heavy enough to return
    items_no_fp = fi.get_frequent_items(frequent_items_error_type.NO_FALSE_POSITIVES)
    self.assertEqual(len(items_no_fp), 0)

    # we do, however, find a few potential heavy items
    # if querying with NO_FALSE_NEGATIVES
    items_no_fn = fi.get_frequent_items(frequent_items_error_type.NO_FALSE_NEGATIVES)
    self.assertGreater(len(items_no_fn), 0)

    # finally, serialize and reconstruct
    fi_bytes = fi.serialize()
    self.assertEqual(len(fi_bytes), fi.get_serialized_size_bytes())
    new_fi = frequent_strings_sketch.deserialize(fi_bytes)

    # and now interrogate the sketch
    self.assertFalse(new_fi.is_empty())
    self.assertGreater(new_fi.get_num_active_items(), 0)
    self.assertEqual(5 * wt, new_fi.get_total_weight())


  def test_fi_sketch(self):
    # only testing a few things not used in the above example
    k = 12
    wt = 10000
    fi = frequent_strings_sketch(k)

    self.assertAlmostEqual(fi.get_sketch_epsilon(), 0.0008545, delta=1e-6)

    sk_apriori_error = fi.get_sketch_epsilon() * wt
    reference_apriori_error = frequent_strings_sketch.get_apriori_error(k, wt)
    self.assertAlmostEqual(sk_apriori_error, reference_apriori_error, delta=1e-6)

if __name__ == '__main__':
  unittest.main()

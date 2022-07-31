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

from datasketches import theta_sketch, update_theta_sketch
from datasketches import compact_theta_sketch, theta_union
from datasketches import theta_intersection, theta_a_not_b
from datasketches import theta_jaccard_similarity

class ThetaTest(unittest.TestCase):
    def test_theta_basic_example(self):
        k = 12      # 2^k = 4096 rows in the table
        n = 1 << 18 # ~256k unique values

        # create a sketch and inject some values
        sk = self.generate_theta_sketch(n, k)

        # we can check that the upper and lower bounds bracket the
        # estimate, without needing to know the exact value.
        self.assertLessEqual(sk.get_lower_bound(1), sk.get_estimate())
        self.assertGreaterEqual(sk.get_upper_bound(1), sk.get_estimate())

        # because this sketch is deterministically generated, we can
        # also compare against the exact value
        self.assertLessEqual(sk.get_lower_bound(1), n)
        self.assertGreaterEqual(sk.get_upper_bound(1), n)

        # compact and serialize for storage, then reconstruct
        sk_bytes = sk.compact().serialize()
        new_sk = compact_theta_sketch.deserialize(sk_bytes)

        # estimate remains unchanged
        self.assertFalse(sk.is_empty())
        self.assertEqual(sk.get_estimate(), new_sk.get_estimate())

    def test_theta_set_operations(self):
        k = 12      # 2^k = 4096 rows in the table
        n = 1 << 18 # ~256k unique values

        # we'll have 1/4 of the values overlap
        offset = int(3 * n / 4) # it's a float w/o cast

        # create a couple sketches and inject some values
        sk1 = self.generate_theta_sketch(n, k)
        sk2 = self.generate_theta_sketch(n, k, offset)

        # UNIONS
        # create a union object
        union = theta_union(k)
        union.update(sk1)
        union.update(sk2)

        # getting result from union returns a compact_theta_sketch
        # compact theta sketches can be used in additional unions
        # or set operations but cannot accept further item updates
        result = union.get_result()
        self.assertTrue(isinstance(result, compact_theta_sketch))

        # since our process here is deterministic, we have
        # checked and know the exact answer is within one
        # standard deviation of the estimate
        self.assertLessEqual(result.get_lower_bound(1), 7 * n / 4)
        self.assertGreaterEqual(result.get_upper_bound(1), 7 * n / 4)


        # INTERSECTIONS
        # create an intersection object
        intersect = theta_intersection() # no lg_k
        intersect.update(sk1)
        intersect.update(sk2)

        # has_result() indicates the intersection has been used,
        # although the result may be the empty set
        self.assertTrue(intersect.has_result())

        # as with unions, the result is a compact sketch
        result = intersect.get_result()
        self.assertTrue(isinstance(result, compact_theta_sketch))

        # we know the sets overlap by 1/4
        self.assertLessEqual(result.get_lower_bound(1), n / 4)
        self.assertGreaterEqual(result.get_upper_bound(1), n / 4)


        # A NOT B
        # create an a_not_b object
        anb = theta_a_not_b() # no lg_k
        result = anb.compute(sk1, sk2)

        # as with unions, the result is a compact sketch
        self.assertTrue(isinstance(result, compact_theta_sketch))

        # we know the sets overlap by 1/4, so the remainder is 3/4
        self.assertLessEqual(result.get_lower_bound(1), 3 * n / 4)
        self.assertGreaterEqual(result.get_upper_bound(1), 3 * n / 4)


        # JACCARD SIMILARITY
        # Jaccard Similarity measure returns (lower_bound, estimate, upper_bound)
        jac = theta_jaccard_similarity.jaccard(sk1, sk2)

        # we can check that results are in the expected order
        self.assertLess(jac[0], jac[1])
        self.assertLess(jac[1], jac[2])

        # checks for sketch equivalency
        self.assertTrue(theta_jaccard_similarity.exactly_equal(sk1, sk1))
        self.assertFalse(theta_jaccard_similarity.exactly_equal(sk1, sk2))

        # we can apply a check for similarity or dissimilarity at a
        # given threshhold, at 97.7% confidence.

        # check that the Jaccard Index is at most (upper bound) 0.2.
        # exact result would be 1/7
        self.assertTrue(theta_jaccard_similarity.dissimilarity_test(sk1, sk2, 0.2))

        # check that the Jaccard Index is at least (lower bound) 0.7
        # exact result would be 3/4, using result from A NOT B test
        self.assertTrue(theta_jaccard_similarity.similarity_test(sk1, result, 0.7))


    def generate_theta_sketch(self, n, k, offset=0):
      sk = update_theta_sketch(k)
      for i in range(0, n):
        sk.update(i + offset)
      return sk
        
if __name__ == '__main__':
    unittest.main()

  
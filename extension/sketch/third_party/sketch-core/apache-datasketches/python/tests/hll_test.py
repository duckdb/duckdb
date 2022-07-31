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
from datasketches import hll_sketch, hll_union, tgt_hll_type

class HllTest(unittest.TestCase):
    def test_hll_example(self):
        k = 12      # 2^k = 4096 rows in the table
        n = 1 << 18 # ~256k unique values

        # create a couple sketches and inject some values
        # we'll have 1/4 of the values overlap
        hll  = hll_sketch(k, tgt_hll_type.HLL_8)
        hll2 = hll_sketch(k, tgt_hll_type.HLL_6)
        offset = int(3 * n / 4) # it's a float w/o cast
        # because we hash on the bits, not an abstract numeric value,
        # hll.update(1) and hll.update(1.0) give different results.
        for i in range(0, n):
            hll.update(i)
            hll2.update(i + offset)
        
        # we can check that the upper and lower bounds bracket the
        # estimate, without needing to know the exact value.
        self.assertLessEqual(hll.get_lower_bound(1), hll.get_estimate())
        self.assertGreaterEqual(hll.get_upper_bound(1), hll.get_estimate())

        # unioning uses a separate class, and we can either get a result
        # sketch or query the union object directly
        union = hll_union(k)
        union.update(hll)
        union.update(hll2)
        result = union.get_result()
        self.assertEqual(result.get_estimate(), union.get_estimate())

        # since our process here (including post-union HLL) is
        # deterministic, we have checked and know the exact
        # answer is within one standard deviation of the estimate
        self.assertLessEqual(union.get_lower_bound(1), 7 * n / 4)
        self.assertGreaterEqual(union.get_upper_bound(1), 7 * n / 4)

        # serialize for storage and reconstruct
        sk_bytes = result.serialize_compact()
        self.assertEqual(len(sk_bytes), result.get_compact_serialization_bytes())
        new_hll = hll_sketch.deserialize(sk_bytes)

        # the sketch can self-report its configuration and status
        self.assertEqual(new_hll.lg_config_k, k)
        self.assertEqual(new_hll.tgt_type, tgt_hll_type.HLL_4)
        self.assertFalse(new_hll.is_empty())

        # if we want to reduce some object overhead, we can also reset
        new_hll.reset()
        self.assertTrue(new_hll.is_empty())

    def test_hll_sketch(self):
        k = 8
        n = 117
        hll = self.generate_sketch(n, k, tgt_hll_type.HLL_6)
        hll.update('string data')
        hll.update(3.14159) # double data

        self.assertLessEqual(hll.get_lower_bound(1), hll.get_estimate())
        self.assertGreaterEqual(hll.get_upper_bound(1), hll.get_estimate())

        self.assertEqual(hll.lg_config_k, k)
        self.assertEqual(hll.tgt_type, tgt_hll_type.HLL_6)

        bytes_compact = hll.serialize_compact()
        bytes_update = hll.serialize_updatable()
        self.assertEqual(len(bytes_compact), hll.get_compact_serialization_bytes())
        self.assertEqual(len(bytes_update), hll.get_updatable_serialization_bytes())

        self.assertFalse(hll.is_compact())
        self.assertFalse(hll.is_empty())

        self.assertTrue(isinstance(hll_sketch.deserialize(bytes_compact), hll_sketch))
        self.assertTrue(isinstance(hll_sketch.deserialize(bytes_update), hll_sketch))

        self.assertIsNotNone(hll_sketch.get_rel_err(True, False, 12, 1))
        self.assertIsNotNone(hll_sketch.get_max_updatable_serialization_bytes(20, tgt_hll_type.HLL_6))

        hll.reset()
        self.assertTrue(hll.is_empty())

    def test_hll_union(self):
        k = 7
        n = 53
        union = hll_union(k)

        sk = self.generate_sketch(n, k, tgt_hll_type.HLL_4, 0)
        union.update(sk)
        sk = self.generate_sketch(3 * n, k, tgt_hll_type.HLL_4, n)
        union.update(sk)
        union.update('string data')
        union.update(1.4142136)

        self.assertLessEqual(union.get_lower_bound(1), union.get_estimate())
        self.assertGreaterEqual(union.get_upper_bound(1), union.get_estimate())

        self.assertEqual(union.lg_config_k, k)
        self.assertFalse(union.is_empty())

        sk = union.get_result()
        self.assertTrue(isinstance(sk, hll_sketch))
        self.assertEqual(sk.tgt_type, tgt_hll_type.HLL_4)
        
    def generate_sketch(self, n, k, sk_type=tgt_hll_type.HLL_4, st_idx=0):
        sk = hll_sketch(k, sk_type)
        for i in range(st_idx, st_idx + n):
            sk.update(i)
        return sk
        
        
if __name__ == '__main__':
    unittest.main()

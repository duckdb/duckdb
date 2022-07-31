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
from datasketches import cpc_sketch, cpc_union

class CpcTest(unittest.TestCase):
  def test_cpc_example(self):
    k = 12      # 2^k = 4096 rows in the table
    n = 1 << 18 # ~256k unique values

    # create a couple sketches and inject some values
    # we'll have 1/4 of the values overlap
    cpc  = cpc_sketch(k)
    cpc2 = cpc_sketch(k)
    offset = int(3 * n / 4) # it's a float w/o cast
    # because we hash on the bits, not an abstract numeric value,
    # cpc.update(1) and cpc.update(1.0) give different results.
    for i in range(0, n):
        cpc.update(i)
        cpc2.update(i + offset)
        
    # although we provide get_composite_estimate() and get_estimate(),
    # the latter will always give the best available estimate.  we
    # recommend using get_estimate().
    # we can check that the upper and lower bounds bracket the
    # estimate, without needing to know the exact value.
    self.assertLessEqual(cpc.get_lower_bound(1), cpc.get_estimate())
    self.assertGreaterEqual(cpc.get_upper_bound(1), cpc.get_estimate())

    # unioning uses a separate class, but we need to get_result()
    # tp query the unioned sketches
    union = cpc_union(k)
    union.update(cpc)
    union.update(cpc2)
    result = union.get_result()

    # since our process here (including post-union CPC) is
    # deterministic, we have checked and know the exact
    # answer is within one standard deviation of the estimate
    self.assertLessEqual(result.get_lower_bound(1), 7 * n / 4)
    self.assertGreaterEqual(result.get_upper_bound(1), 7 * n / 4)
     
    # serialize for storage and reconstruct
    sk_bytes = result.serialize()
    new_cpc = cpc_sketch.deserialize(sk_bytes)
    self.assertFalse(new_cpc.is_empty())

if __name__ == '__main__':
    unittest.main()

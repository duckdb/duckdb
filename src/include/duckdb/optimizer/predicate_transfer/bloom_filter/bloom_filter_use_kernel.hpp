#pragma once

#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {
/**
 * Caller needs to check if table is empty and bloom filter is valid
 */
class BloomFilterUseKernel {

public:
  // use vanilla bloom filter (reuse column indices made by the caller)
  static void
  filter(const vector<Vector> &result,
         shared_ptr<BlockedBloomFilter> bloom_filter,
         SelectionVector &sel,
         idx_t &approved_tuple_count,
         idx_t row_num);
};
}
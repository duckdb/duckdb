#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter_use_kernel.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

void BloomFilterUseKernel::filter(const vector<Vector> &result,
            shared_ptr<BlockedBloomFilter> bloom_filter,
            SelectionVector &sel,
            idx_t &approved_tuple_count,
            idx_t row_num) {
    if (bloom_filter->isEmpty()) {
        approved_tuple_count = 0;
        return;
    }
    idx_t result_count = 0;
    Vector hashes(LogicalType::HASH);
    VectorOperations::Hash(const_cast<Vector &>(result[bloom_filter->BoundColsApplied[0]]), hashes, row_num);
    for(int i = 1; i < bloom_filter->BoundColsApplied.size(); i++) {
	    VectorOperations::CombineHash(hashes, const_cast<Vector &>(result[bloom_filter->BoundColsApplied[i]]), row_num);
    }
    if(hashes.GetVectorType() == VectorType::CONSTANT_VECTOR) {
        hashes.Flatten(row_num);
    }
    bloom_filter->Find(arrow::internal::CpuInfo::AVX2, row_num, (hash_t*)hashes.GetData(), sel, result_count, false);

    approved_tuple_count = result_count;
    return;
}
}
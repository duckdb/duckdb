//===--------------------------------------------------------------------===//
// copy.cpp
// Description: This file contains the implementation of the different copy
// functions
//===--------------------------------------------------------------------===//

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

void VectorOperations::Copy(const Vector &source_p, Vector &target, const SelectionVector &sel_p, idx_t source_count,
                            idx_t source_offset, idx_t target_offset, idx_t copy_count) {
	target.Copy(source_p, sel_p, source_count, source_offset, target_offset, copy_count);
}

void VectorOperations::Copy(const Vector &source_p, Vector &target, const SelectionVector &sel_p, idx_t source_count,
                            idx_t source_offset, idx_t target_offset) {
	D_ASSERT(source_offset <= source_count);
	D_ASSERT(source_p.GetType() == target.GetType());
	idx_t copy_count = source_count - source_offset;
	VectorOperations::Copy(source_p, target, sel_p, source_count, source_offset, target_offset, copy_count);
}

void VectorOperations::Copy(const Vector &source, Vector &target, idx_t source_count, idx_t source_offset,
                            idx_t target_offset) {
	VectorOperations::Copy(source, target, *FlatVector::IncrementalSelectionVector(), source_count, source_offset,
	                       target_offset);
}

} // namespace duckdb

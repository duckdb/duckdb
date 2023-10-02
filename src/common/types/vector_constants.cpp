#include "duckdb/common/types/vector.hpp"

namespace duckdb {

const SelectionVector *ConstantVector::ZeroSelectionVector() {
	static const SelectionVector ZERO_SELECTION_VECTOR =
	    SelectionVector(const_cast<sel_t *>(ConstantVector::ZERO_VECTOR)); // NOLINT
	return &ZERO_SELECTION_VECTOR;
}

const SelectionVector *FlatVector::IncrementalSelectionVector() {
	static const SelectionVector INCREMENTAL_SELECTION_VECTOR;
	return &INCREMENTAL_SELECTION_VECTOR;
}

const sel_t ConstantVector::ZERO_VECTOR[STANDARD_VECTOR_SIZE] = {0};

} // namespace duckdb

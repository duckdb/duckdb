#include "duckdb/common/types/vector.hpp"

namespace duckdb {

// We disable Wexit-time-destructors here
// Otherwise we get a warning about the two selection vectors (ZERO/INCREMENTAL_SELECTION_VECTOR)
// While the SelectionVector does have a non-trivial destructor
// This only does a memory de-allocation if the selection vectors own their data (i.e. selection_data is not null)
// In the case of the FlatVector/ConstantVector, they point towards static regions of memory
// Hence in this case these cause no problems, as the destructors are non-trivial but effectively nops
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wexit-time-destructors"
#endif

const SelectionVector *ConstantVector::ZeroSelectionVector() {
	static const SelectionVector ZERO_SELECTION_VECTOR = SelectionVector((sel_t *)ConstantVector::ZERO_VECTOR);
	return &ZERO_SELECTION_VECTOR;
}

const SelectionVector *FlatVector::IncrementalSelectionVector() {
	static const SelectionVector INCREMENTAL_SELECTION_VECTOR;
	return &INCREMENTAL_SELECTION_VECTOR;
}

const sel_t ConstantVector::ZERO_VECTOR[STANDARD_VECTOR_SIZE] = {0};

#ifdef __clang__
#pragma clang diagnostic pop
#endif

} // namespace duckdb

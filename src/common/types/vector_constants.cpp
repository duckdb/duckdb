#include "duckdb/common/types/vector.hpp"

namespace duckdb {

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wexit-time-destructors"
#endif

const SelectionVector ConstantVector::ZERO_SELECTION_VECTOR = SelectionVector((sel_t *)ConstantVector::ZERO_VECTOR);
const SelectionVector FlatVector::INCREMENTAL_SELECTION_VECTOR;
const sel_t ConstantVector::ZERO_VECTOR[STANDARD_VECTOR_SIZE] = {0};

#ifdef __clang__
#pragma clang diagnostic pop
#endif

} // namespace duckdb

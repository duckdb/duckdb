//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/general_cast.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

struct HandleVectorCastError {
	template <class RESULT_TYPE>
	static RESULT_TYPE Operation(string error_message, ValidityMask &mask, idx_t idx, string *error_message_ptr,
	                             bool &all_converted) {
		HandleCastError::AssignError(error_message, error_message_ptr);
		all_converted = false;
		mask.SetInvalid(idx);
		return NullValue<RESULT_TYPE>();
	}
};

static string UnimplementedCastMessage(const LogicalType &source_type, const LogicalType &target_type) {
	return StringUtil::Format("Unimplemented type for cast (%s -> %s)", source_type.ToString(), target_type.ToString());
}

// NULL cast only works if all values in source are NULL, otherwise an unimplemented cast exception is thrown
static bool TryVectorNullCast(Vector &source, Vector &result, idx_t count, string *error_message) {
	bool success = true;
	if (VectorOperations::HasNotNull(source, count)) {
		HandleCastError::AssignError(UnimplementedCastMessage(source.GetType(), result.GetType()), error_message);
		success = false;
	}
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::SetNull(result, true);
	return success;
}

} // namespace duckdb

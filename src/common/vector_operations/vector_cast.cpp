#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"

namespace duckdb {

bool VectorOperations::TryCast(Vector &source, Vector &result, idx_t count, string *error_message, bool strict) {
	CastFunctionSet set;
	auto cast_function = set.GetCastFunction(source.GetType(), result.GetType());
	CastParameters parameters(cast_function.cast_data.get(), strict, error_message);
	return cast_function.function(source, result, count, parameters);
}

void VectorOperations::Cast(Vector &source, Vector &result, idx_t count, bool strict) {
	VectorOperations::TryCast(source, result, count, nullptr, strict);
}

} // namespace duckdb

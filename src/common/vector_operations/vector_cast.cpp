#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

bool VectorOperations::TryCast(CastFunctionSet &set, GetCastFunctionInput &input, Vector &source, Vector &result,
                               idx_t count, string *error_message, bool strict) {
	auto cast_function = set.GetCastFunction(source.GetType(), result.GetType(), input);
	CastParameters parameters(cast_function.cast_data.get(), strict, error_message);
	return cast_function.function(source, result, count, parameters);
}

bool VectorOperations::DefaultTryCast(Vector &source, Vector &result, idx_t count, string *error_message, bool strict) {
	CastFunctionSet set;
	GetCastFunctionInput input;
	return VectorOperations::TryCast(set, input, source, result, count, error_message, strict);
}

void VectorOperations::DefaultCast(Vector &source, Vector &result, idx_t count, bool strict) {
	VectorOperations::DefaultTryCast(source, result, count, nullptr, strict);
}

bool VectorOperations::TryCast(ClientContext &context, Vector &source, Vector &result, idx_t count,
                               string *error_message, bool strict) {
	auto &config = DBConfig::GetConfig(context);
	auto &set = config.GetCastFunctions();
	GetCastFunctionInput get_input(context);
	return VectorOperations::TryCast(set, get_input, source, result, count, error_message, strict);
}

void VectorOperations::Cast(ClientContext &context, Vector &source, Vector &result, idx_t count, bool strict) {
	VectorOperations::TryCast(context, source, result, count, nullptr, strict);
}

} // namespace duckdb

#include "duckdb/core_functions/scalar/debug_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

static void VectorTypeFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	auto data = ConstantVector::GetData<string_t>(result);
	data[0] = StringVector::AddString(result, EnumUtil::ToString(input.data[0].GetVectorType()));
}

ScalarFunction VectorTypeFun::GetFunction() {
	return ScalarFunction("vector_type",        // name of the function
	                      {LogicalType::ANY},   // argument list
	                      LogicalType::VARCHAR, // return type
	                      VectorTypeFunction);
}

} // namespace duckdb

#include "core_functions/scalar/debug_functions.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/enums/vector_type.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/string_heap.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {
struct ExpressionState;

static void VectorTypeFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	auto data = ConstantVector::GetData<string_t>(result);
	auto &heap = StringVector::GetStringHeap(result);
	data[0] = heap.AddString(EnumUtil::ToString(input.data[0].GetVectorType()));
}

ScalarFunction VectorTypeFun::GetFunction() {
	auto vector_type_fun = ScalarFunction("vector_type",        // name of the function
	                                      {LogicalType::ANY},   // argument list
	                                      LogicalType::VARCHAR, // return type
	                                      VectorTypeFunction);
	vector_type_fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return vector_type_fun;
}

} // namespace duckdb

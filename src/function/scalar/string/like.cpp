#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

static void like_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                          Vector &result) {
	result.Initialize(TypeId::BOOLEAN);
	VectorOperations::Like(inputs[0], inputs[1], result);
}

static void not_like_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                              BoundFunctionExpression &expr, Vector &result) {
	result.Initialize(TypeId::BOOLEAN);
	VectorOperations::NotLike(inputs[0], inputs[1], result);
}

void LikeFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("~~", {SQLType::VARCHAR, SQLType::VARCHAR}, SQLType::BOOLEAN, like_function));
	set.AddFunction(ScalarFunction("!~~", {SQLType::VARCHAR, SQLType::VARCHAR}, SQLType::BOOLEAN, not_like_function));
}

} // namespace duckdb

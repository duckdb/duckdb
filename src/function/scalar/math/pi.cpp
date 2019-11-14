#include "duckdb/function/scalar/math_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

Value pi_value = Value::DOUBLE(PI);

static void pi_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                        Vector &result) {
	assert(input_count == 0);
	result.Reference(pi_value);
}

void PiFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("pi", {}, SQLType::DOUBLE, pi_function));
}

} // namespace duckdb

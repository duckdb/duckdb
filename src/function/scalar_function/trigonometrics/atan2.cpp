#include "function/scalar_function/trigonometric_functions.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "common/exception.hpp"

using namespace std;

namespace duckdb {

void atan2_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                    Vector &result) {
	assert(input_count == 2);

	inputs[0].Cast(TypeId::DOUBLE);
	inputs[1].Cast(TypeId::DOUBLE);

	result.Initialize(TypeId::DOUBLE);
	VectorOperations::ATan2(inputs[0], inputs[1], result);
}

bool atan2_matches_arguments(vector<SQLType> &arguments) {
	if (arguments.size() != 2) {
		return false;
	}
	if ((!IsNumericType(arguments[0].id) && arguments[0].id != SQLTypeId::SQLNULL) ||
	    (!IsNumericType(arguments[1].id) && arguments[1].id != SQLTypeId::SQLNULL)) {
		return false;
	}
	return true;
}

}

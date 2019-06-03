#include "function/scalar_function/ceilfloor.hpp"

#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "common/types/constant_vector.hpp"
#include "common/types/static_vector.hpp"

using namespace std;

namespace duckdb {

void ceil_function(ExpressionExecutor &exec, Vector inputs[], count_t input_count, BoundFunctionExpression &expr,
                   Vector &result) {
	assert(input_count == 1);
	result.Initialize(inputs[0].type);
	VectorOperations::Ceil(inputs[0], result);
}

void floor_function(ExpressionExecutor &exec, Vector inputs[], count_t input_count, BoundFunctionExpression &expr,
                    Vector &result) {
	assert(input_count == 1);
	result.Initialize(inputs[0].type);
	VectorOperations::Floor(inputs[0], result);
}

bool ceilfloor_matches_arguments(vector<SQLType> &arguments) {
	if (arguments.size() != 1) {
		return false;
	}
	switch (arguments[0].id) {
	case SQLTypeId::TINYINT:
	case SQLTypeId::SMALLINT:
	case SQLTypeId::INTEGER:
	case SQLTypeId::BIGINT:
	case SQLTypeId::DECIMAL:
	case SQLTypeId::FLOAT:
	case SQLTypeId::DOUBLE:
	case SQLTypeId::SQLNULL:
		return true;
	default:
		return false;
	}
}

SQLType ceilfloor_get_return_type(vector<SQLType> &arguments) {
	assert(arguments.size() == 1);
	return arguments[0];
}

} // namespace duckdb

#include "function/scalar_function/abs.hpp"

#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

void abs_function(ExpressionExecutor &exec, Vector inputs[], uint64_t input_count, BoundFunctionExpression &expr,
                  Vector &result) {
	result.Initialize(inputs[0].type);
	VectorOperations::Abs(inputs[0], result);
}

bool abs_matches_arguments(vector<SQLType> &arguments) {
	if (arguments.size() != 1) {
		return false;
	}
	switch (arguments[0].id) {
	case SQLTypeId::TINYINT:
	case SQLTypeId::SMALLINT:
	case SQLTypeId::INTEGER:
	case SQLTypeId::BIGINT:
	case SQLTypeId::DECIMAL:
	case SQLTypeId::DOUBLE:
	case SQLTypeId::SQLNULL:
		return true;
	default:
		return false;
	}
}

SQLType abs_get_return_type(vector<SQLType> &arguments) {
	return arguments[0];
}

} // namespace duckdb

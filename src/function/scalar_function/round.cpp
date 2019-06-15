#include "function/scalar_function/round.hpp"

#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

void round_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                    Vector &result) {
	result.Initialize(inputs[0].type);
	VectorOperations::Round(inputs[0], inputs[1], result);
}

bool round_matches_arguments(vector<SQLType> &arguments) {
	if (arguments.size() != 2) {
		return false;
	}
	switch (arguments[0].id) {
	case SQLTypeId::TINYINT:
	case SQLTypeId::SMALLINT:
	case SQLTypeId::INTEGER:
	case SQLTypeId::BIGINT:
	case SQLTypeId::DECIMAL:
	case SQLTypeId::DOUBLE:
		break;
	default:
		return false;
	}
	switch (arguments[1].id) {
	case SQLTypeId::TINYINT:
	case SQLTypeId::SMALLINT:
	case SQLTypeId::INTEGER:
	case SQLTypeId::BIGINT:
		return true;
	default:
		return false;
	}
}

SQLType round_get_return_type(vector<SQLType> &arguments) {
	return arguments[0];
}

} // namespace duckdb

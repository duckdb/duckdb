#include "function/scalar_function/mod.hpp"

#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

void mod_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result) {
	result.Initialize(expr.return_type);
	VectorOperations::Mod(inputs[0], inputs[1], result);
}

bool mod_matches_arguments(vector<SQLType> &arguments) {
	if (arguments.size() != 2) {
		return false;
	}

	for (int i = 0; i < 2 ; i++)
	{
		switch (arguments[i].id) {
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
	}
	return true;
}

SQLType mod_get_return_type(vector<SQLType> &arguments) {
	for (int i = 0; i < 2 ; i++)
	{
		switch (arguments[i].id) {
		case SQLTypeId::FLOAT:
		case SQLTypeId::DECIMAL:
		case SQLTypeId::DOUBLE:
			return SQLTypeId::DOUBLE;
		default:
			break;
		}
	}
	return SQLTypeId::BIGINT;
}

} // namespace duckdb

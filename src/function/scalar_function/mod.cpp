#include "function/scalar_function/mod.hpp"

#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

void mod_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result) {
	result.Initialize(expr.return_type);
	inputs[0].Cast(expr.return_type);
	inputs[1].Cast(expr.return_type);
	VectorOperations::Modulo(inputs[0], inputs[1], result);
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
  return MaxSQLType(arguments[0].id, arguments[1].id);
}

} // namespace duckdb

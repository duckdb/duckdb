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
	case SQLTypeId::DECIMAL:
	case SQLTypeId::DOUBLE:
		return true;
	default:
		return false;
	}
}

SQLType mod_get_return_type(vector<SQLType> &arguments) {
        //return arguments[0];
        if (arguments[0].id == SQLTypeId::DECIMAL ||
            arguments[0].id == SQLTypeId::FLOAT ||
            arguments[0].id == SQLTypeId::DOUBLE ||
            arguments[1].id == SQLTypeId::DECIMAL ||
            arguments[1].id == SQLTypeId::FLOAT ||
            arguments[1].id == SQLTypeId::DOUBLE)
          return SQLTypeId::DOUBLE;
        else
          return SQLTypeId::BIGINT;
}

} // namespace duckdb

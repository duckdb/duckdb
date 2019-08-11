#include "function/scalar/math_functions.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "planner/expression/bound_function_expression.hpp"

using namespace std;

namespace duckdb {

void pow_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result) {
        result.Initialize(TypeId::DOUBLE);
        inputs[0].Cast(TypeId::DOUBLE);
        inputs[1].Cast(TypeId::DOUBLE);
        VectorOperations::Pow(inputs[0], inputs[1], result);
}

bool pow_matches_arguments(vector<SQLType> &arguments) {
        if (arguments.size() != 2) {
                return false;
        }

        for (int i = 0; i < 2; i++) {
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

SQLType pow_get_return_type(vector<SQLType> &arguments) {
        return SQLTypeId::DOUBLE;
}

}

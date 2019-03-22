#include "planner/expression.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

Expression::Expression(ExpressionType type, ExpressionClass expression_class, TypeId return_type, SQLType sql_type)
    : BaseExpression(type, expression_class), return_type(return_type), sql_type(sql_type) {
}

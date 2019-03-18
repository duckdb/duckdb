#include "planner/expression.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

Expression::Expression(ExpressionType type, ExpressionClass expression_class, TypeId return_type) :
	ParsedExpression(type, expression_class), return_type(return_type) {
}

void Expression::Serialize(Serializer &serializer) {
	assert(0);
	throw Exception("Bound expressions cannot be serialized!");
}

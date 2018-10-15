
#include "parser/expression/conjunction_expression.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression>
ConjunctionExpression::Deserialize(ExpressionDeserializeInformation *info,
                                   Deserializer &source) {
	if (info->children.size() < 2) {
		return nullptr;
	}
	return make_unique_base<Expression, ConjunctionExpression>(
	    info->type, move(info->children[0]), move(info->children[1]));
}

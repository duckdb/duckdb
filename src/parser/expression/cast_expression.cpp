
#include "parser/expression/cast_expression.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression>
CastExpression::Deserialize(ExpressionDeserializeInformation *info,
                            Deserializer &source) {
	if (info->children.size() != 1) {
		return nullptr;
	}
	return make_unique_base<Expression, CastExpression>(
	    info->return_type, move(info->children[0]));
}

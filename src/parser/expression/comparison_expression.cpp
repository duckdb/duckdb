
#include "parser/expression/comparison_expression.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression>
ComparisonExpression::Deserialize(ExpressionDeserializeInformation *info,
                                  Deserializer &source) {
	if (info->children.size() != 2) {
		return nullptr;
	}
	return make_unique<ComparisonExpression>(
	    info->type, move(info->children[0]), move(info->children[1]));
}

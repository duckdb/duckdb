
#include "parser/expression/case_expression.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

void CaseExpression::ResolveType() {
	Expression::ResolveType();
	return_type = std::max(children[1]->return_type, children[2]->return_type);
}

unique_ptr<Expression>
CaseExpression::Deserialize(ExpressionDeserializeInformation *info,
                            Deserializer &source) {
	if (info->children.size() != 3) {
		// CASE requires three children
		return nullptr;
	}
	auto expression = make_unique<CaseExpression>();
	expression->return_type = info->return_type;
	expression->children = move(info->children);
	return expression;
}

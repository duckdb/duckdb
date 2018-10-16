
#include "parser/expression/subquery_expression.hpp"

#include "common/exception.hpp"
#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> SubqueryExpression::Copy() {
	assert(children.size() == 0);
	assert(!op);
	assert(!context);
	assert(!plan);
	auto copy = make_unique<SubqueryExpression>();
	copy->CopyProperties(*this);
	copy->subquery = subquery->Copy();
	copy->subquery_type = subquery_type;
	return copy;
}

void SubqueryExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
	serializer.Write<SubqueryType>(subquery_type);
	subquery->Serialize(serializer);
}

unique_ptr<Expression>
SubqueryExpression::Deserialize(ExpressionDeserializeInformation *info,
                                Deserializer &source) {
	bool failed = false;
	auto subquery_type = source.Read<SubqueryType>(failed);
	auto subquery = SelectStatement::Deserialize(source);
	if (failed || !subquery || info->children.size() > 0) {
		return nullptr;
	}
	auto expression = make_unique<SubqueryExpression>();
	expression->subquery_type = subquery_type;
	expression->subquery = move(subquery);
	return expression;
}

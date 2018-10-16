
#include "parser/expression/conjunction_expression.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> ConjunctionExpression::Copy() {
	assert(children.size() == 2);
	auto copy = make_unique<ConjunctionExpression>(type, children[0]->Copy(),
	                                               children[1]->Copy());
	copy->CopyProperties(*this);
	return copy;
}

unique_ptr<Expression>
ConjunctionExpression::Deserialize(ExpressionDeserializeInformation *info,
                                   Deserializer &source) {
	if (info->children.size() != 2) {
		throw SerializationException("Conjunction needs two children!");
	}

	return make_unique_base<Expression, ConjunctionExpression>(
	    info->type, move(info->children[0]), move(info->children[1]));
}


#include "parser/expression/comparison_expression.hpp"

#include "common/exception.hpp"
#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> ComparisonExpression::Copy() {
	assert(children.size() == 2);
	auto copy = make_unique<ComparisonExpression>(type, children[0]->Copy(),
	                                              children[1]->Copy());
	copy->CopyProperties(*this);
	return copy;
}

unique_ptr<Expression>
ComparisonExpression::Deserialize(ExpressionDeserializeInformation *info,
                                  Deserializer &source) {
	if (info->children.size() != 2) {
		throw SerializationException("Comparison needs two children!");
	}

	return make_unique<ComparisonExpression>(
	    info->type, move(info->children[0]), move(info->children[1]));
}

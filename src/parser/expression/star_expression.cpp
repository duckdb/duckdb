
#include "parser/expression/star_expression.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> StarExpression::Copy() {
	assert(children.size() == 0);
	auto copy = make_unique<StarExpression>();
	copy->CopyProperties(*this);
	return copy;
}

unique_ptr<Expression>
StarExpression::Deserialize(ExpressionDeserializeInformation *info,
                            Deserializer &source) {
	if (info->children.size() > 0) {
		throw SerializationException("Star cannot have children!");
	}
	return make_unique<StarExpression>();
}

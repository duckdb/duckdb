#include "parser/expression/default_expression.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> DefaultExpression::Copy() {
	assert(children.size() == 0);
	auto copy = make_unique<DefaultExpression>();
	copy->CopyProperties(*this);
	return copy;
}

unique_ptr<Expression> DefaultExpression::Deserialize(ExpressionDeserializeInfo *info, Deserializer &source) {
	if (info->children.size() > 0) {
		throw SerializationException("Default cannot have children!");
	}
	return make_unique<DefaultExpression>();
}

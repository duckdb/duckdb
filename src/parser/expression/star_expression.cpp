#include "parser/expression/star_expression.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> StarExpression::Copy() {
	auto copy = make_unique<StarExpression>();
	copy->CopyProperties(*this);
	return move(copy);
}

unique_ptr<Expression> StarExpression::Deserialize(ExpressionType type, TypeId return_type, Deserializer &source) {
	return make_unique<StarExpression>();
}

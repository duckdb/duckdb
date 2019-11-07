#include "duckdb/parser/expression/default_expression.hpp"

#include "duckdb/common/exception.hpp"

using namespace duckdb;
using namespace std;

DefaultExpression::DefaultExpression() : ParsedExpression(ExpressionType::VALUE_DEFAULT, ExpressionClass::DEFAULT) {
}

string DefaultExpression::ToString() const {
	return "DEFAULT";
}

unique_ptr<ParsedExpression> DefaultExpression::Copy() const {
	auto copy = make_unique<DefaultExpression>();
	copy->CopyProperties(*this);
	return move(copy);
}

unique_ptr<ParsedExpression> DefaultExpression::Deserialize(ExpressionType type, Deserializer &source) {
	return make_unique<DefaultExpression>();
}

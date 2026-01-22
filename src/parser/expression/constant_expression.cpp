#include "duckdb/parser/expression/constant_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"

namespace duckdb {

ConstantExpression::ConstantExpression() : ParsedExpression(ExpressionType::VALUE_CONSTANT, ExpressionClass::CONSTANT) {
}

ConstantExpression::ConstantExpression(Value val)
    : ParsedExpression(ExpressionType::VALUE_CONSTANT, ExpressionClass::CONSTANT), value(std::move(val)) {
}

string ConstantExpression::ToString() const {
	return value.ToSQLString();
}

bool ConstantExpression::Equal(const ConstantExpression &a, const ConstantExpression &b) {
	return a.value.type() == b.value.type() && !ValueOperations::DistinctFrom(a.value, b.value);
}

hash_t ConstantExpression::Hash() const {
	return value.Hash();
}

unique_ptr<ParsedExpression> ConstantExpression::Copy() const {
	auto copy = make_uniq<ConstantExpression>(value);
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace duckdb

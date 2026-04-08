#include "duckdb/parser/expression/constant_expression.hpp"

#include <utility>

#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types.hpp"

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

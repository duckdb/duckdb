#include "duckdb/parser/expression/parameter_expression.hpp"

#include <utility>

#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

ParameterExpression::ParameterExpression()
    : ParsedExpression(ExpressionType::VALUE_PARAMETER, ExpressionClass::PARAMETER) {
}

string ParameterExpression::ToString() const {
	return "$" + identifier;
}

unique_ptr<ParsedExpression> ParameterExpression::Copy() const {
	auto copy = make_uniq<ParameterExpression>();
	copy->identifier = identifier;
	copy->CopyProperties(*this);
	return std::move(copy);
}

bool ParameterExpression::Equal(const ParameterExpression &a, const ParameterExpression &b) {
	return StringUtil::CIEquals(a.identifier, b.identifier);
}

hash_t ParameterExpression::Hash() const {
	hash_t result = ParsedExpression::Hash();
	return CombineHash(duckdb::Hash(identifier.c_str(), identifier.size()), result);
}

} // namespace duckdb

#include "duckdb/parser/expression/lambdaref_expression.hpp"

#include "duckdb/common/types/hash.hpp"

namespace duckdb {

LambdaRefExpression::LambdaRefExpression(idx_t lambda_idx, string column_name_p)
    : ParsedExpression(ExpressionType::LAMBDA_REF, ExpressionClass::LAMBDA_REF), lambda_idx(lambda_idx),
      column_name(std::move(column_name_p)) {
	alias = column_name;
}

bool LambdaRefExpression::IsScalar() const {
	throw InternalException("lambda reference expressions are transient, IsScalar should never be called");
}

string LambdaRefExpression::GetName() const {
	return column_name;
}

string LambdaRefExpression::ToString() const {
	throw InternalException("lambda reference expressions are transient, ToString should never be called");
}

hash_t LambdaRefExpression::Hash() const {
	hash_t result = ParsedExpression::Hash();
	result = CombineHash(result, lambda_idx);
	result = CombineHash(result, StringUtil::CIHash(column_name));
	return result;
}

unique_ptr<ParsedExpression> LambdaRefExpression::Copy() const {
	throw InternalException("lambda reference expressions are transient, Copy should never be called");
}

} // namespace duckdb

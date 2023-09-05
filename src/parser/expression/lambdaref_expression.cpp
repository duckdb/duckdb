#include "duckdb/parser/expression/lambdaref_expression.hpp"

#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

LambdaRefExpression::LambdaRefExpression(const idx_t lambda_idx, const string &column_name)
    : ParsedExpression(ExpressionType::LAMBDA_REF, ExpressionClass::LAMBDA_REF), lambda_index(lambda_idx),
      column_name(column_name) {
	alias = column_name;
}

bool LambdaRefExpression::IsQualified() const {
	return false;
}

const string &LambdaRefExpression::GetColumnName() const {
	return column_name;
}

string LambdaRefExpression::GetName() const {
	return column_name;
}

string LambdaRefExpression::ToString() const {
	return KeywordHelper::WriteOptionallyQuoted(column_name);
}

bool LambdaRefExpression::Equal(const LambdaRefExpression &a, const LambdaRefExpression &b) {
	return a.lambda_index == b.lambda_index && a.column_name == b.column_name;
}

hash_t LambdaRefExpression::Hash() const {
	hash_t result = ParsedExpression::Hash();
	result = CombineHash(result, lambda_index);
	result = CombineHash(result, StringUtil::CIHash(column_name));
	return result;
}

unique_ptr<ParsedExpression> LambdaRefExpression::Copy() const {
	auto copy = make_uniq<LambdaRefExpression>(lambda_index, column_name);
	copy->CopyProperties(*this);
	return std::move(copy);
}

void LambdaRefExpression::Serialize(FieldWriter &writer) const {
	writer.WriteField<idx_t>(lambda_index);
	writer.WriteString(column_name);
}

unique_ptr<ParsedExpression> LambdaRefExpression::Deserialize(ExpressionType type, FieldReader &reader) {
	auto lambda_idx_p = reader.ReadRequired<idx_t>();
	auto column_name_p = reader.ReadRequired<string>();
	auto expression = make_uniq<LambdaRefExpression>(lambda_idx_p, column_name_p);
	return std::move(expression);
}

} // namespace duckdb

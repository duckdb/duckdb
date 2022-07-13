#include "duckdb/parser/expression/parameter_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

ParameterExpression::ParameterExpression()
    : ParsedExpression(ExpressionType::VALUE_PARAMETER, ExpressionClass::PARAMETER), parameter_nr(0) {
}

string ParameterExpression::ToString() const {
	return "$" + to_string(parameter_nr);
}

unique_ptr<ParsedExpression> ParameterExpression::Copy() const {
	auto copy = make_unique<ParameterExpression>();
	copy->parameter_nr = parameter_nr;
	copy->CopyProperties(*this);
	return move(copy);
}

bool ParameterExpression::Equals(const ParameterExpression *a, const ParameterExpression *b) {
	return a->parameter_nr == b->parameter_nr;
}

hash_t ParameterExpression::Hash() const {
	hash_t result = ParsedExpression::Hash();
	return CombineHash(duckdb::Hash(parameter_nr), result);
}

void ParameterExpression::Serialize(FieldWriter &writer) const {
	writer.WriteField<idx_t>(parameter_nr);
}

unique_ptr<ParsedExpression> ParameterExpression::Deserialize(ExpressionType type, FieldReader &reader) {
	auto expression = make_unique<ParameterExpression>();
	expression->parameter_nr = reader.ReadRequired<idx_t>();
	return move(expression);
}

} // namespace duckdb

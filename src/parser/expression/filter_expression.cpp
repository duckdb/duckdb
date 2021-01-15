#include "duckdb/parser/expression/filter_expression.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

FilterExpression::FilterExpression() : ParsedExpression(ExpressionType::FILTER_EXPR, ExpressionClass::FILTER) {
}

string FilterExpression::ToString() const {
	return filter->GetName();
}

bool FilterExpression::Equals(const FilterExpression *a, const FilterExpression *b) {
	if (!a->filter->Equals(b->filter.get())) {
		return false;
	}
	return true;
}

unique_ptr<ParsedExpression> FilterExpression::Copy() const {
	auto filter_exp = make_unique<FilterExpression>();
	filter_exp->CopyProperties(*this);
	filter_exp->filter = filter->Copy();
	return move(filter_exp);
}

void FilterExpression::Serialize(Serializer &serializer) {
	ParsedExpression::Serialize(serializer);
	filter->Serialize(serializer);
}

unique_ptr<ParsedExpression> FilterExpression::Deserialize(ExpressionType type, Deserializer &source) {
	auto expression = make_unique<FilterExpression>();
	expression->filter = ParsedExpression::Deserialize(source);
	return move(expression);
}

} // namespace duckdb

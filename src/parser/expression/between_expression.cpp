#include "duckdb/parser/expression/between_expression.hpp"

namespace duckdb {

BetweenExpression::BetweenExpression(unique_ptr<ParsedExpression> input_p, unique_ptr<ParsedExpression> lower_p,
                                     unique_ptr<ParsedExpression> upper_p)
    : ParsedExpression(ExpressionType::COMPARE_BETWEEN, ExpressionClass::BETWEEN), input(move(input_p)),
      lower(move(lower_p)), upper(move(upper_p)) {
}

string BetweenExpression::ToString() const {
	return input->ToString() + " BETWEEN " + lower->ToString() + " AND " + upper->ToString();
}

bool BetweenExpression::Equals(const BetweenExpression *a, const BetweenExpression *b) {
	if (!a->input->Equals(b->input.get())) {
		return false;
	}
	if (!a->lower->Equals(b->lower.get())) {
		return false;
	}
	if (!a->upper->Equals(b->upper.get())) {
		return false;
	}
	return true;
}

unique_ptr<ParsedExpression> BetweenExpression::Copy() const {
	auto copy = make_unique<BetweenExpression>(input->Copy(), lower->Copy(), upper->Copy());
	copy->CopyProperties(*this);
	return copy;
}

void BetweenExpression::Serialize(Serializer &serializer) {
	ParsedExpression::Serialize(serializer);
	input->Serialize(serializer);
	lower->Serialize(serializer);
	upper->Serialize(serializer);
}

unique_ptr<ParsedExpression> BetweenExpression::Deserialize(ExpressionType type, Deserializer &source) {
	auto input = ParsedExpression::Deserialize(source);
	auto lower = ParsedExpression::Deserialize(source);
	auto upper = ParsedExpression::Deserialize(source);
	return make_unique<BetweenExpression>(move(input), move(lower), move(upper));
}

} // namespace duckdb
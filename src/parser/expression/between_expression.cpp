#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

BetweenExpression::BetweenExpression(unique_ptr<ParsedExpression> input_p, unique_ptr<ParsedExpression> lower_p,
                                     unique_ptr<ParsedExpression> upper_p)
    : ParsedExpression(ExpressionType::COMPARE_BETWEEN, ExpressionClass::BETWEEN), input(move(input_p)),
      lower(move(lower_p)), upper(move(upper_p)) {
}

string BetweenExpression::ToString() const {
	return ToString<BetweenExpression, ParsedExpression>(*this);
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
	return move(copy);
}

void BetweenExpression::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(*input);
	writer.WriteSerializable(*lower);
	writer.WriteSerializable(*upper);
}

unique_ptr<ParsedExpression> BetweenExpression::Deserialize(ExpressionType type, FieldReader &source) {
	auto input = source.ReadRequiredSerializable<ParsedExpression>();
	auto lower = source.ReadRequiredSerializable<ParsedExpression>();
	auto upper = source.ReadRequiredSerializable<ParsedExpression>();
	return make_unique<BetweenExpression>(move(input), move(lower), move(upper));
}

} // namespace duckdb

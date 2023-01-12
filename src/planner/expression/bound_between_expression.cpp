#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

BoundBetweenExpression::BoundBetweenExpression(unique_ptr<Expression> input, unique_ptr<Expression> lower,
                                               unique_ptr<Expression> upper, bool lower_inclusive, bool upper_inclusive)
    : Expression(ExpressionType::COMPARE_BETWEEN, ExpressionClass::BOUND_BETWEEN, LogicalType::BOOLEAN),
      input(std::move(input)), lower(std::move(lower)), upper(std::move(upper)), lower_inclusive(lower_inclusive),
      upper_inclusive(upper_inclusive) {
}

string BoundBetweenExpression::ToString() const {
	return BetweenExpression::ToString<BoundBetweenExpression, Expression>(*this);
}

bool BoundBetweenExpression::Equals(const BaseExpression *other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto other = (BoundBetweenExpression *)other_p;
	if (!Expression::Equals(input.get(), other->input.get())) {
		return false;
	}
	if (!Expression::Equals(lower.get(), other->lower.get())) {
		return false;
	}
	if (!Expression::Equals(upper.get(), other->upper.get())) {
		return false;
	}
	return lower_inclusive == other->lower_inclusive && upper_inclusive == other->upper_inclusive;
}

unique_ptr<Expression> BoundBetweenExpression::Copy() {
	auto copy = make_unique<BoundBetweenExpression>(input->Copy(), lower->Copy(), upper->Copy(), lower_inclusive,
	                                                upper_inclusive);
	copy->CopyProperties(*this);
	return std::move(copy);
}

void BoundBetweenExpression::Serialize(FieldWriter &writer) const {
	writer.WriteOptional(input);
	writer.WriteOptional(lower);
	writer.WriteOptional(upper);
	writer.WriteField(lower_inclusive);
	writer.WriteField(upper_inclusive);
}

unique_ptr<Expression> BoundBetweenExpression::Deserialize(ExpressionDeserializationState &state, FieldReader &reader) {
	auto input = reader.ReadOptional<Expression>(nullptr, state.gstate);
	auto lower = reader.ReadOptional<Expression>(nullptr, state.gstate);
	auto upper = reader.ReadOptional<Expression>(nullptr, state.gstate);
	auto lower_inclusive = reader.ReadRequired<bool>();
	auto upper_inclusive = reader.ReadRequired<bool>();
	return make_unique<BoundBetweenExpression>(std::move(input), std::move(lower), std::move(upper), lower_inclusive,
	                                           upper_inclusive);
}

} // namespace duckdb

#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

BetweenExpression::BetweenExpression(unique_ptr<ParsedExpression> input_p, unique_ptr<ParsedExpression> lower_p,
                                     unique_ptr<ParsedExpression> upper_p)
    : ParsedExpression(ExpressionType::COMPARE_BETWEEN, ExpressionClass::BETWEEN), input(std::move(input_p)),
      lower(std::move(lower_p)), upper(std::move(upper_p)) {
}

BetweenExpression::BetweenExpression() : BetweenExpression(nullptr, nullptr, nullptr) {
}

string BetweenExpression::ToString() const {
	return ToString<ParsedExpression>(Input(), LowerBound(), UpperBound());
}

unique_ptr<ParsedExpression> BetweenExpression::Copy() const {
	auto copy = make_uniq<BetweenExpression>(input->Copy(), lower->Copy(), upper->Copy());
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace duckdb

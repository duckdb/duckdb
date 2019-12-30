#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/common/serializer.hpp"

using namespace duckdb;
using namespace std;

BetweenExpression::BetweenExpression(unique_ptr<ParsedExpression> input, unique_ptr<ParsedExpression> lower, unique_ptr<ParsedExpression> upper, bool lower_inclusive, bool upper_inclusive) :
	ParsedExpression(ExpressionType::COMPARE_BETWEEN, ExpressionClass::BETWEEN), input(move(input)), lower(move(lower)), upper(move(upper)), lower_inclusive(lower_inclusive), upper_inclusive(upper_inclusive) {
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
	return a->lower_inclusive == b->lower_inclusive && a->upper_inclusive == b->upper_inclusive;
}

unique_ptr<ParsedExpression> BetweenExpression::Copy() const {
	auto copy = make_unique<BetweenExpression>(input->Copy(), lower->Copy(), upper->Copy(), lower_inclusive, upper_inclusive);
	copy->CopyProperties(*this);
	return move(copy);
}

void BetweenExpression::Serialize(Serializer &serializer) {
	ParsedExpression::Serialize(serializer);
	input->Serialize(serializer);
	lower->Serialize(serializer);
	upper->Serialize(serializer);
	serializer.Write<bool>(lower_inclusive);
	serializer.Write<bool>(upper_inclusive);
}

unique_ptr<ParsedExpression> BetweenExpression::Deserialize(ExpressionType type, Deserializer &source) {
	auto input = ParsedExpression::Deserialize(source);
	auto lower = ParsedExpression::Deserialize(source);
	auto upper = ParsedExpression::Deserialize(source);
	bool lower_inclusive = source.Read<bool>();
	bool upper_inclusive = source.Read<bool>();
	return make_unique<BetweenExpression>(move(input), move(lower), move(upper), lower_inclusive, upper_inclusive);
}

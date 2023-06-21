#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"

namespace duckdb {

BetweenExpression::BetweenExpression(unique_ptr<ParsedExpression> input_p, unique_ptr<ParsedExpression> lower_p,
                                     unique_ptr<ParsedExpression> upper_p)
    : ParsedExpression(ExpressionType::COMPARE_BETWEEN, ExpressionClass::BETWEEN), input(std::move(input_p)),
      lower(std::move(lower_p)), upper(std::move(upper_p)) {
}

string BetweenExpression::ToString() const {
	return ToString<BetweenExpression, ParsedExpression>(*this);
}

bool BetweenExpression::Equal(const BetweenExpression &a, const BetweenExpression &b) {
	if (!a.input->Equals(*b.input)) {
		return false;
	}
	if (!a.lower->Equals(*b.lower)) {
		return false;
	}
	if (!a.upper->Equals(*b.upper)) {
		return false;
	}
	return true;
}

unique_ptr<ParsedExpression> BetweenExpression::Copy() const {
	auto copy = make_uniq<BetweenExpression>(input->Copy(), lower->Copy(), upper->Copy());
	copy->CopyProperties(*this);
	return std::move(copy);
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
	return make_uniq<BetweenExpression>(std::move(input), std::move(lower), std::move(upper));
}

void BetweenExpression::FormatSerialize(FormatSerializer &serializer) const {
	ParsedExpression::FormatSerialize(serializer);
	serializer.WriteProperty("input", *input);
	serializer.WriteProperty("lower", *lower);
	serializer.WriteProperty("upper", *upper);
}

unique_ptr<ParsedExpression> BetweenExpression::FormatDeserialize(ExpressionType type,
                                                                  FormatDeserializer &deserializer) {
	auto input = deserializer.ReadProperty<unique_ptr<ParsedExpression>>("input");
	auto lower = deserializer.ReadProperty<unique_ptr<ParsedExpression>>("lower");
	auto upper = deserializer.ReadProperty<unique_ptr<ParsedExpression>>("upper");
	return make_uniq<BetweenExpression>(std::move(input), std::move(lower), std::move(upper));
}

} // namespace duckdb

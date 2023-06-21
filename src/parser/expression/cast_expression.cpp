#include "duckdb/parser/expression/cast_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"

#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"

namespace duckdb {

CastExpression::CastExpression(LogicalType target, unique_ptr<ParsedExpression> child, bool try_cast_p)
    : ParsedExpression(ExpressionType::OPERATOR_CAST, ExpressionClass::CAST), cast_type(std::move(target)),
      try_cast(try_cast_p) {
	D_ASSERT(child);
	this->child = std::move(child);
}

string CastExpression::ToString() const {
	return ToString<CastExpression, ParsedExpression>(*this);
}

bool CastExpression::Equal(const CastExpression &a, const CastExpression &b) {
	if (!a.child->Equals(*b.child)) {
		return false;
	}
	if (a.cast_type != b.cast_type) {
		return false;
	}
	if (a.try_cast != b.try_cast) {
		return false;
	}
	return true;
}

unique_ptr<ParsedExpression> CastExpression::Copy() const {
	auto copy = make_uniq<CastExpression>(cast_type, child->Copy(), try_cast);
	copy->CopyProperties(*this);
	return std::move(copy);
}

void CastExpression::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(*child);
	writer.WriteSerializable(cast_type);
	writer.WriteField<bool>(try_cast);
}

unique_ptr<ParsedExpression> CastExpression::Deserialize(ExpressionType type, FieldReader &reader) {
	auto child = reader.ReadRequiredSerializable<ParsedExpression>();
	auto cast_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	auto try_cast = reader.ReadRequired<bool>();
	return make_uniq_base<ParsedExpression, CastExpression>(cast_type, std::move(child), try_cast);
}

void CastExpression::FormatSerialize(FormatSerializer &serializer) const {
	ParsedExpression::FormatSerialize(serializer);
	serializer.WriteProperty("child", *child);
	serializer.WriteProperty("cast_type", cast_type);
	serializer.WriteProperty("try_cast", try_cast);
}

unique_ptr<ParsedExpression> CastExpression::FormatDeserialize(ExpressionType type, FormatDeserializer &deserializer) {
	auto child = deserializer.ReadProperty<unique_ptr<ParsedExpression>>("child");
	auto cast_type = deserializer.ReadProperty<LogicalType>("cast_type");
	auto try_cast = deserializer.ReadProperty<bool>("try_cast");
	return make_uniq_base<ParsedExpression, CastExpression>(cast_type, std::move(child), try_cast);
}

} // namespace duckdb

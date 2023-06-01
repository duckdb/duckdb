#include "duckdb/parser/expression/operator_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"

#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"

namespace duckdb {

OperatorExpression::OperatorExpression(ExpressionType type, unique_ptr<ParsedExpression> left,
                                       unique_ptr<ParsedExpression> right)
    : ParsedExpression(type, ExpressionClass::OPERATOR) {
	if (left) {
		children.push_back(std::move(left));
	}
	if (right) {
		children.push_back(std::move(right));
	}
}

OperatorExpression::OperatorExpression(ExpressionType type, vector<unique_ptr<ParsedExpression>> children)
    : ParsedExpression(type, ExpressionClass::OPERATOR), children(std::move(children)) {
}

string OperatorExpression::ToString() const {
	return ToString<OperatorExpression, ParsedExpression>(*this);
}

bool OperatorExpression::Equal(const OperatorExpression &a, const OperatorExpression &b) {
	if (a.children.size() != b.children.size()) {
		return false;
	}
	for (idx_t i = 0; i < a.children.size(); i++) {
		if (!a.children[i]->Equals(*b.children[i])) {
			return false;
		}
	}
	return true;
}

unique_ptr<ParsedExpression> OperatorExpression::Copy() const {
	auto copy = make_uniq<OperatorExpression>(type);
	copy->CopyProperties(*this);
	for (auto &it : children) {
		copy->children.push_back(it->Copy());
	}
	return std::move(copy);
}

void OperatorExpression::Serialize(FieldWriter &writer) const {
	writer.WriteSerializableList(children);
}

unique_ptr<ParsedExpression> OperatorExpression::Deserialize(ExpressionType type, FieldReader &reader) {
	auto expression = make_uniq<OperatorExpression>(type);
	expression->children = reader.ReadRequiredSerializableList<ParsedExpression>();
	return std::move(expression);
}

void OperatorExpression::FormatSerialize(FormatSerializer &serializer) const {
	ParsedExpression::FormatSerialize(serializer);
	serializer.WriteProperty("children", children);
}

unique_ptr<ParsedExpression> OperatorExpression::FormatDeserialize(ExpressionType type,
                                                                   FormatDeserializer &deserializer) {
	auto expression = make_uniq<OperatorExpression>(type);
	expression->children = deserializer.ReadProperty<vector<unique_ptr<ParsedExpression>>>("children");
	return std::move(expression);
}

} // namespace duckdb

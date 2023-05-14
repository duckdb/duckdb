#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/expression_util.hpp"

#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"

namespace duckdb {

ConjunctionExpression::ConjunctionExpression(ExpressionType type)
    : ParsedExpression(type, ExpressionClass::CONJUNCTION) {
}

ConjunctionExpression::ConjunctionExpression(ExpressionType type, vector<unique_ptr<ParsedExpression>> children)
    : ParsedExpression(type, ExpressionClass::CONJUNCTION) {
	for (auto &child : children) {
		AddExpression(std::move(child));
	}
}

ConjunctionExpression::ConjunctionExpression(ExpressionType type, unique_ptr<ParsedExpression> left,
                                             unique_ptr<ParsedExpression> right)
    : ParsedExpression(type, ExpressionClass::CONJUNCTION) {
	AddExpression(std::move(left));
	AddExpression(std::move(right));
}

void ConjunctionExpression::AddExpression(unique_ptr<ParsedExpression> expr) {
	if (expr->type == type) {
		// expr is a conjunction of the same type: merge the expression lists together
		auto &other = expr->Cast<ConjunctionExpression>();
		for (auto &child : other.children) {
			children.push_back(std::move(child));
		}
	} else {
		children.push_back(std::move(expr));
	}
}

string ConjunctionExpression::ToString() const {
	return ToString<ConjunctionExpression, ParsedExpression>(*this);
}

bool ConjunctionExpression::Equal(const ConjunctionExpression *a, const ConjunctionExpression *b) {
	return ExpressionUtil::SetEquals(a->children, b->children);
}

unique_ptr<ParsedExpression> ConjunctionExpression::Copy() const {
	vector<unique_ptr<ParsedExpression>> copy_children;
	copy_children.reserve(children.size());
	for (auto &expr : children) {
		copy_children.push_back(expr->Copy());
	}

	auto copy = make_uniq<ConjunctionExpression>(type, std::move(copy_children));
	copy->CopyProperties(*this);
	return std::move(copy);
}

void ConjunctionExpression::Serialize(FieldWriter &writer) const {
	writer.WriteSerializableList(children);
}

unique_ptr<ParsedExpression> ConjunctionExpression::Deserialize(ExpressionType type, FieldReader &reader) {
	auto result = make_uniq<ConjunctionExpression>(type);
	result->children = reader.ReadRequiredSerializableList<ParsedExpression>();
	return std::move(result);
}

void ConjunctionExpression::FormatSerialize(FormatSerializer &serializer) const {
	ParsedExpression::FormatSerialize(serializer);
	serializer.WriteProperty("children", children);
}

unique_ptr<ParsedExpression> ConjunctionExpression::FormatDeserialize(ExpressionType type,
                                                                      FormatDeserializer &deserializer) {
	auto result = make_uniq<ConjunctionExpression>(type);
	result->children = deserializer.ReadProperty<vector<unique_ptr<ParsedExpression>>>("children");
	return std::move(result);
}

} // namespace duckdb

#include "parser/parsed_expression.hpp"

#include "common/printer.hpp"
#include "common/serializer.hpp"
#include "common/types/hash.hpp"

#include "parser/expression/list.hpp"

using namespace duckdb;
using namespace std;

bool ParsedExpression::IsAggregate() {
	bool is_aggregate = false;
	EnumerateChildren([&](ParsedExpression *child) { is_aggregate |= child->IsAggregate(); });
	return is_aggregate;
}

bool ParsedExpression::IsWindow() {
	bool is_window = false;
	EnumerateChildren([&](ParsedExpression *child) { is_window |= child->IsWindow(); });
	return is_window;
}

bool ParsedExpression::IsScalar() {
	bool is_scalar = true;
	EnumerateChildren([&](ParsedExpression *child) {
		if (!child->IsScalar()) {
			is_scalar = false;
		}
	});
	return is_scalar;
}

bool ParsedExpression::HasParameter() {
	bool has_parameter = false;
	EnumerateChildren([&](ParsedExpression *child) { has_parameter |= child->HasParameter(); });
	return has_parameter;
}

bool ParsedExpression::HasSubquery() {
	bool has_subquery = false;
	EnumerateChildren([&](ParsedExpression *child) { has_subquery |= child->HasSubquery(); });
	return has_subquery;
}

void ParsedExpression::Print() {
	Printer::Print(ToString());
}

bool ParsedExpression::Equals(const ParsedExpression *other) const {
	if (!other) {
		return false;
	}
	if (this->type != other->type) {
		return false;
	}
	return true;
}

bool ParsedExpression::Equals(ParsedExpression *left, ParsedExpression *right) {
	if (left == right) {
		// if pointers are equivalent, they are equivalent
		return true;
	}
	if (!left || !right) {
		// otherwise if one of them is nullptr, they are not equivalent
		// because the other one cannot be nullptr then
		return false;
	}
	// otherwise we use the normal equality
	return left->Equals(right);
}

uint64_t ParsedExpression::Hash() const {
	uint64_t hash = duckdb::Hash<uint32_t>((uint32_t)type);
	EnumerateChildren([&](ParsedExpression *child) { hash = CombineHash(child->Hash(), hash); });
	return hash;
}

void ParsedExpression::Serialize(Serializer &serializer) {
	serializer.Write<ExpressionClass>(GetExpressionClass());
	serializer.Write<ExpressionType>(type);
	serializer.WriteString(alias);
}

unique_ptr<ParsedExpression> ParsedExpression::Deserialize(Deserializer &source) {
	auto expression_class = source.Read<ExpressionClass>();
	auto type = source.Read<ExpressionType>();
	auto alias = source.Read<string>();
	unique_ptr<ParsedExpression> result;
	switch (expression_class) {
	case ExpressionClass::AGGREGATE:
		result = AggregateExpression::Deserialize(type, source);
		break;
	case ExpressionClass::CASE:
		result = CaseExpression::Deserialize(type, source);
		break;
	case ExpressionClass::CAST:
		result = CastExpression::Deserialize(type, source);
		break;
	case ExpressionClass::COLUMN_REF:
		result = ColumnRefExpression::Deserialize(type, source);
		break;
	case ExpressionClass::COMPARISON:
		result = ComparisonExpression::Deserialize(type, source);
		break;
	case ExpressionClass::CONJUNCTION:
		result = ConjunctionExpression::Deserialize(type, source);
		break;
	case ExpressionClass::CONSTANT:
		result = ConstantExpression::Deserialize(type, source);
		break;
	case ExpressionClass::DEFAULT:
		result = DefaultExpression::Deserialize(type, source);
		break;
	case ExpressionClass::FUNCTION:
		result = FunctionExpression::Deserialize(type, source);
		break;
	case ExpressionClass::OPERATOR:
		result = OperatorExpression::Deserialize(type, source);
		break;
	case ExpressionClass::STAR:
		result = StarExpression::Deserialize(type, source);
		break;
	case ExpressionClass::SUBQUERY:
		result = SubqueryExpression::Deserialize(type, source);
		break;
	case ExpressionClass::WINDOW:
		result = WindowExpression::Deserialize(type, source);
		break;
	default:
		throw SerializationException("Unsupported type for expression deserialization!");
	}
	result->alias = alias;
	return result;
}

size_t ParsedExpression::ChildCount() const {
	return 0;
}

ParsedExpression *ParsedExpression::GetChild(size_t index) const {
	throw OutOfRangeException("Expression child index out of range!");
}

void ParsedExpression::ReplaceChild(std::function<unique_ptr<ParsedExpression>(unique_ptr<ParsedExpression> expression)> callback,
                              size_t index) {
	throw OutOfRangeException("Expression child index out of range!");
}

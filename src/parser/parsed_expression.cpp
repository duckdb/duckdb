#include "parser/parsed_expression.hpp"

#include "common/serializer.hpp"
#include "common/types/hash.hpp"
#include "parser/expression/list.hpp"
#include "parser/parsed_expression_iterator.hpp"

using namespace duckdb;
using namespace std;

bool ParsedExpression::IsAggregate() const {
	bool is_aggregate = false;
	ParsedExpressionIterator::EnumerateChildren(
	    *this, [&](const ParsedExpression &child) { is_aggregate |= child.IsAggregate(); });
	return is_aggregate;
}

bool ParsedExpression::IsWindow() const {
	bool is_window = false;
	ParsedExpressionIterator::EnumerateChildren(*this,
	                                            [&](const ParsedExpression &child) { is_window |= child.IsWindow(); });
	return is_window;
}

bool ParsedExpression::IsScalar() const {
	bool is_scalar = true;
	ParsedExpressionIterator::EnumerateChildren(*this, [&](const ParsedExpression &child) {
		if (!child.IsScalar()) {
			is_scalar = false;
		}
	});
	return is_scalar;
}

bool ParsedExpression::HasParameter() const {
	bool has_parameter = false;
	ParsedExpressionIterator::EnumerateChildren(
	    *this, [&](const ParsedExpression &child) { has_parameter |= child.HasParameter(); });
	return has_parameter;
}

bool ParsedExpression::HasSubquery() const {
	bool has_subquery = false;
	ParsedExpressionIterator::EnumerateChildren(
	    *this, [&](const ParsedExpression &child) { has_subquery |= child.HasSubquery(); });
	return has_subquery;
}

uint64_t ParsedExpression::Hash() const {
	uint64_t hash = duckdb::Hash<uint32_t>((uint32_t)type);
	ParsedExpressionIterator::EnumerateChildren(
	    *this, [&](const ParsedExpression &child) { hash = CombineHash(child.Hash(), hash); });
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
	case ExpressionClass::PARAMETER:
		result = ParameterExpression::Deserialize(type, source);
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

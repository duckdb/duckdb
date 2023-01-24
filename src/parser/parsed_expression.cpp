#include "duckdb/main/client_context.hpp"

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/parser/expression/list.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {

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

bool ParsedExpression::Equals(const BaseExpression *other) const {
	if (!BaseExpression::Equals(other)) {
		return false;
	}
	switch (expression_class) {
	case ExpressionClass::BETWEEN:
		return BetweenExpression::Equal((BetweenExpression *)this, (BetweenExpression *)other);
	case ExpressionClass::CASE:
		return CaseExpression::Equal((CaseExpression *)this, (CaseExpression *)other);
	case ExpressionClass::CAST:
		return CastExpression::Equal((CastExpression *)this, (CastExpression *)other);
	case ExpressionClass::COLLATE:
		return CollateExpression::Equal((CollateExpression *)this, (CollateExpression *)other);
	case ExpressionClass::COLUMN_REF:
		return ColumnRefExpression::Equal((ColumnRefExpression *)this, (ColumnRefExpression *)other);
	case ExpressionClass::COMPARISON:
		return ComparisonExpression::Equal((ComparisonExpression *)this, (ComparisonExpression *)other);
	case ExpressionClass::CONJUNCTION:
		return ConjunctionExpression::Equal((ConjunctionExpression *)this, (ConjunctionExpression *)other);
	case ExpressionClass::CONSTANT:
		return ConstantExpression::Equal((ConstantExpression *)this, (ConstantExpression *)other);
	case ExpressionClass::DEFAULT:
		return true;
	case ExpressionClass::FUNCTION:
		return FunctionExpression::Equal((FunctionExpression *)this, (FunctionExpression *)other);
	case ExpressionClass::LAMBDA:
		return LambdaExpression::Equal((LambdaExpression *)this, (LambdaExpression *)other);
	case ExpressionClass::OPERATOR:
		return OperatorExpression::Equal((OperatorExpression *)this, (OperatorExpression *)other);
	case ExpressionClass::PARAMETER:
		return ParameterExpression::Equal((ParameterExpression *)this, (ParameterExpression *)other);
	case ExpressionClass::POSITIONAL_REFERENCE:
		return PositionalReferenceExpression::Equal((PositionalReferenceExpression *)this,
		                                            (PositionalReferenceExpression *)other);
	case ExpressionClass::STAR:
		return StarExpression::Equal((StarExpression *)this, (StarExpression *)other);
	case ExpressionClass::SUBQUERY:
		return SubqueryExpression::Equal((SubqueryExpression *)this, (SubqueryExpression *)other);
	case ExpressionClass::WINDOW:
		return WindowExpression::Equal((WindowExpression *)this, (WindowExpression *)other);
	default:
		throw SerializationException("Unsupported type for expression comparison!");
	}
}

hash_t ParsedExpression::Hash() const {
	hash_t hash = duckdb::Hash<uint32_t>((uint32_t)type);
	ParsedExpressionIterator::EnumerateChildren(
	    *this, [&](const ParsedExpression &child) { hash = CombineHash(child.Hash(), hash); });
	return hash;
}

void ParsedExpression::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteField<ExpressionClass>(GetExpressionClass());
	writer.WriteField<ExpressionType>(type);
	writer.WriteString(alias);
	Serialize(writer);
	writer.Finalize();
}

unique_ptr<ParsedExpression> ParsedExpression::Deserialize(Deserializer &source) {
	FieldReader reader(source);
	auto expression_class = reader.ReadRequired<ExpressionClass>();
	auto type = reader.ReadRequired<ExpressionType>();
	auto alias = reader.ReadRequired<string>();
	unique_ptr<ParsedExpression> result;
	switch (expression_class) {
	case ExpressionClass::BETWEEN:
		result = BetweenExpression::Deserialize(type, reader);
		break;
	case ExpressionClass::CASE:
		result = CaseExpression::Deserialize(type, reader);
		break;
	case ExpressionClass::CAST:
		result = CastExpression::Deserialize(type, reader);
		break;
	case ExpressionClass::COLLATE:
		result = CollateExpression::Deserialize(type, reader);
		break;
	case ExpressionClass::COLUMN_REF:
		result = ColumnRefExpression::Deserialize(type, reader);
		break;
	case ExpressionClass::COMPARISON:
		result = ComparisonExpression::Deserialize(type, reader);
		break;
	case ExpressionClass::CONJUNCTION:
		result = ConjunctionExpression::Deserialize(type, reader);
		break;
	case ExpressionClass::CONSTANT:
		result = ConstantExpression::Deserialize(type, reader);
		break;
	case ExpressionClass::DEFAULT:
		result = DefaultExpression::Deserialize(type, reader);
		break;
	case ExpressionClass::FUNCTION:
		result = FunctionExpression::Deserialize(type, reader);
		break;
	case ExpressionClass::LAMBDA:
		result = LambdaExpression::Deserialize(type, reader);
		break;
	case ExpressionClass::OPERATOR:
		result = OperatorExpression::Deserialize(type, reader);
		break;
	case ExpressionClass::PARAMETER:
		result = ParameterExpression::Deserialize(type, reader);
		break;
	case ExpressionClass::POSITIONAL_REFERENCE:
		result = PositionalReferenceExpression::Deserialize(type, reader);
		break;
	case ExpressionClass::STAR:
		result = StarExpression::Deserialize(type, reader);
		break;
	case ExpressionClass::SUBQUERY:
		result = SubqueryExpression::Deserialize(type, reader);
		break;
	case ExpressionClass::WINDOW:
		result = WindowExpression::Deserialize(type, reader);
		break;
	default:
		throw SerializationException("Unsupported type for expression deserialization: '%s'!",
		                             ExpressionClassToString(expression_class));
	}
	result->alias = alias;
	reader.Finalize();
	return result;
}

} // namespace duckdb

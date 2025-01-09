#include "duckdb/main/client_context.hpp"

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/parser/expression/list.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/parser/expression_util.hpp"

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

bool ParsedExpression::Equals(const BaseExpression &other) const {
	if (!BaseExpression::Equals(other)) {
		return false;
	}
	switch (expression_class) {
	case ExpressionClass::BETWEEN:
		return BetweenExpression::Equal(Cast<BetweenExpression>(), other.Cast<BetweenExpression>());
	case ExpressionClass::CASE:
		return CaseExpression::Equal(Cast<CaseExpression>(), other.Cast<CaseExpression>());
	case ExpressionClass::CAST:
		return CastExpression::Equal(Cast<CastExpression>(), other.Cast<CastExpression>());
	case ExpressionClass::COLLATE:
		return CollateExpression::Equal(Cast<CollateExpression>(), other.Cast<CollateExpression>());
	case ExpressionClass::COLUMN_REF:
		return ColumnRefExpression::Equal(Cast<ColumnRefExpression>(), other.Cast<ColumnRefExpression>());
	case ExpressionClass::COMPARISON:
		return ComparisonExpression::Equal(Cast<ComparisonExpression>(), other.Cast<ComparisonExpression>());
	case ExpressionClass::CONJUNCTION:
		return ConjunctionExpression::Equal(Cast<ConjunctionExpression>(), other.Cast<ConjunctionExpression>());
	case ExpressionClass::CONSTANT:
		return ConstantExpression::Equal(Cast<ConstantExpression>(), other.Cast<ConstantExpression>());
	case ExpressionClass::DEFAULT:
		return true;
	case ExpressionClass::FUNCTION:
		return FunctionExpression::Equal(Cast<FunctionExpression>(), other.Cast<FunctionExpression>());
	case ExpressionClass::LAMBDA:
		return LambdaExpression::Equal(Cast<LambdaExpression>(), other.Cast<LambdaExpression>());
	case ExpressionClass::OPERATOR:
		return OperatorExpression::Equal(Cast<OperatorExpression>(), other.Cast<OperatorExpression>());
	case ExpressionClass::PARAMETER:
		return ParameterExpression::Equal(Cast<ParameterExpression>(), other.Cast<ParameterExpression>());
	case ExpressionClass::POSITIONAL_REFERENCE:
		return PositionalReferenceExpression::Equal(Cast<PositionalReferenceExpression>(),
		                                            other.Cast<PositionalReferenceExpression>());
	case ExpressionClass::STAR:
		return StarExpression::Equal(Cast<StarExpression>(), other.Cast<StarExpression>());
	case ExpressionClass::SUBQUERY:
		return SubqueryExpression::Equal(Cast<SubqueryExpression>(), other.Cast<SubqueryExpression>());
	case ExpressionClass::WINDOW:
		return WindowExpression::Equal(Cast<WindowExpression>(), other.Cast<WindowExpression>());
	default:
		throw SerializationException("Unsupported type for expression comparison!");
	}
}

hash_t ParsedExpression::Hash() const {
	hash_t hash = duckdb::Hash<uint32_t>(static_cast<uint32_t>(type));
	ParsedExpressionIterator::EnumerateChildren(
	    *this, [&](const ParsedExpression &child) { hash = CombineHash(child.Hash(), hash); });
	return hash;
}

bool ParsedExpression::Equals(const unique_ptr<ParsedExpression> &left, const unique_ptr<ParsedExpression> &right) {
	if (left.get() == right.get()) {
		return true;
	}
	if (!left || !right) {
		return false;
	}
	return left->Equals(*right);
}

bool ParsedExpression::ListEquals(const vector<unique_ptr<ParsedExpression>> &left,
                                  const vector<unique_ptr<ParsedExpression>> &right) {
	return ExpressionUtil::ListEquals(left, right);
}

} // namespace duckdb

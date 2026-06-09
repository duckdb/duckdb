#include "duckdb/parser/expression/subquery_expression.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

SubqueryExpression::SubqueryExpression()
    : ParsedExpression(ExpressionType::SUBQUERY, ExpressionClass::SUBQUERY), subquery_type(SubqueryType::INVALID),
      comparison_type(ExpressionType::INVALID) {
}

string SubqueryExpression::ToString() const {
	switch (subquery_type) {
	case SubqueryType::ANY:
		return "(" + child->ToString() + " " + ExpressionTypeToOperator(comparison_type) + " ANY(" +
		       subquery->ToString() + "))";
	case SubqueryType::EXISTS:
		return "EXISTS(" + subquery->ToString() + ")";
	case SubqueryType::NOT_EXISTS:
		return "NOT EXISTS(" + subquery->ToString() + ")";
	case SubqueryType::SCALAR:
		return "(" + subquery->ToString() + ")";
	default:
		throw InternalException("Unrecognized type for subquery");
	}
}

bool SubqueryExpression::Equal(const SubqueryExpression &a, const SubqueryExpression &b) {
	if (!a.subquery || !b.subquery) {
		return false;
	}
	if (!ParsedExpression::Equals(a.child, b.child)) {
		return false;
	}
	return a.comparison_type == b.comparison_type && a.subquery_type == b.subquery_type &&
	       a.subquery->Equals(*b.subquery);
}

unique_ptr<ParsedExpression> SubqueryExpression::Copy() const {
	auto copy = make_uniq<SubqueryExpression>();
	copy->CopyProperties(*this);
	copy->subquery = unique_ptr_cast<SQLStatement, SelectStatement>(subquery->Copy());
	copy->subquery_type = subquery_type;
	copy->child = child ? child->Copy() : nullptr;
	copy->comparison_type = comparison_type;
	return std::move(copy);
}

} // namespace duckdb

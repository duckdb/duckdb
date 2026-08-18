#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"

namespace duckdb {

BoundOperatorExpression::BoundOperatorExpression(ExpressionType type, LogicalType return_type)
    : Expression(type, ExpressionClass::BOUND_OPERATOR, std::move(return_type)) {
}

string BoundOperatorExpression::ToString() const {
	if (type == ExpressionType::ARGUMENT_PACK) {
		// print the packed arguments as they were written in the call, so that the enclosing function call reads
		// back the way the user wrote it. Keyword names live in the STRUCT return type.
		const auto named = return_type.id() == LogicalTypeId::STRUCT;
		string result;
		for (idx_t i = 0; i < children.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			if (named) {
				result += StructType::GetChildName(return_type, i).GetIdentifierName() + " := ";
			}
			result += children[i]->ToString();
		}
		return result;
	}
	return OperatorExpression::ToString<BoundOperatorExpression, Expression>(*this);
}

bool BoundOperatorExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundOperatorExpression>();
	if (!Expression::ListEquals(children, other.children)) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundOperatorExpression::Copy() const {
	auto copy = make_uniq<BoundOperatorExpression>(type, return_type);
	copy->CopyProperties(*this);
	for (auto &child : children) {
		copy->children.push_back(child->Copy());
	}
	return std::move(copy);
}

} // namespace duckdb

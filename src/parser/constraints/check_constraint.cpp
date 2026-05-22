#include "duckdb/parser/constraints/check_constraint.hpp"

namespace duckdb {

CheckConstraint::CheckConstraint(unique_ptr<ParsedExpression> expression)
    : Constraint(ConstraintType::CHECK), expression(std::move(expression)) {
}

string CheckConstraint::ToString() const {
	return "CHECK(" + expression->ToString() + ")";
}

unique_ptr<Constraint> CheckConstraint::Copy() const {
	auto copy = make_uniq<CheckConstraint>(expression->Copy());
	copy->constraint_name = constraint_name;
	return copy;
}

} // namespace duckdb

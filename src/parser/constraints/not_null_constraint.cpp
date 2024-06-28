#include "duckdb/parser/constraints/not_null_constraint.hpp"

namespace duckdb {

NotNullConstraint::NotNullConstraint(LogicalIndex index) : Constraint(ConstraintType::NOT_NULL), index(index) {
}

NotNullConstraint::~NotNullConstraint() {
}

string NotNullConstraint::ToString() const {
	return "NOT NULL";
}

unique_ptr<Constraint> NotNullConstraint::Copy() const {
	return make_uniq<NotNullConstraint>(index);
}

} // namespace duckdb

#include "duckdb/parser/constraints/not_null_constraint.hpp"

#include "duckdb/common/field_writer.hpp"

namespace duckdb {

NotNullConstraint::NotNullConstraint(LogicalIndex index) : Constraint(ConstraintType::NOT_NULL), index(index) {
}

NotNullConstraint::~NotNullConstraint() {
}

string NotNullConstraint::ToString() const {
	return "NOT NULL";
}

unique_ptr<Constraint> NotNullConstraint::Copy() const {
	return make_unique<NotNullConstraint>(index);
}

void NotNullConstraint::Serialize(FieldWriter &writer) const {
	writer.WriteField<idx_t>(index.index);
}

unique_ptr<Constraint> NotNullConstraint::Deserialize(FieldReader &source) {
	auto index = source.ReadRequired<idx_t>();
	return make_unique_base<Constraint, NotNullConstraint>(LogicalIndex(index));
}

} // namespace duckdb

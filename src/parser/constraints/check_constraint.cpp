#include "duckdb/parser/constraints/check_constraint.hpp"

#include "duckdb/common/field_writer.hpp"

namespace duckdb {

CheckConstraint::CheckConstraint(unique_ptr<ParsedExpression> expression)
    : Constraint(ConstraintType::CHECK), expression(std::move(expression)) {
}

string CheckConstraint::ToString() const {
	return "CHECK(" + expression->ToString() + ")";
}

unique_ptr<Constraint> CheckConstraint::Copy() const {
	return make_unique<CheckConstraint>(expression->Copy());
}

void CheckConstraint::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(*expression);
}

unique_ptr<Constraint> CheckConstraint::Deserialize(FieldReader &source) {
	auto expression = source.ReadRequiredSerializable<ParsedExpression>();
	return make_unique<CheckConstraint>(std::move(expression));
}

} // namespace duckdb

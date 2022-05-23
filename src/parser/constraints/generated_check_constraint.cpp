#include "duckdb/parser/constraints/generated_check_constraint.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

GeneratedCheckConstraint::GeneratedCheckConstraint(column_t index, unique_ptr<ParsedExpression> expression)
    : CheckConstraint(move(expression)), column_index(index) {
	type = ConstraintType::GENERATED;
}

string GeneratedCheckConstraint::ToString() const {
	return "GENERATED CHECK(" + expression->ToString() + ") ON COL ID " + std::to_string(column_index + 1);
}

unique_ptr<Constraint> GeneratedCheckConstraint::Copy() const {
	return make_unique<GeneratedCheckConstraint>(column_index, expression->Copy());
}

void GeneratedCheckConstraint::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(*expression);
	writer.WriteField<column_t>(column_index);
}

unique_ptr<Constraint> GeneratedCheckConstraint::Deserialize(FieldReader &source) {
	auto expression = source.ReadRequiredSerializable<ParsedExpression>();
	auto index = source.ReadRequired<column_t>();
	return make_unique<GeneratedCheckConstraint>(index, move(expression));
}

} // namespace duckdb

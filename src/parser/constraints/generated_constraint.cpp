#include "duckdb/parser/constraints/generated_constraint.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

GeneratedConstraint::GeneratedConstraint(uint64_t index, unique_ptr<ParsedExpression> expression)
    : Constraint(ConstraintType::GENERATED), index(index), expression(move(expression)) {
}

string GeneratedConstraint::ToString() const {
	D_ASSERT(index != DConstants::INVALID_INDEX);
	string base = "GENERATED(";
	base += std::to_string(index);
	return base + ")";
}

unique_ptr<Constraint> GeneratedConstraint::Copy() const {
	auto result = make_unique<GeneratedConstraint>(index, expression->Copy());
	return move(result);
}

void GeneratedConstraint::Serialize(FieldWriter &writer) const {
	writer.WriteField<uint64_t>(index);
	expression->Serialize(writer);
}

unique_ptr<Constraint> GeneratedConstraint::Deserialize(FieldReader &reader) {
	Deserializer& source = reader.GetSource();
	auto index = reader.ReadRequired<idx_t>();
	auto expression = ParsedExpression::Deserialize(source);
	return make_unique_base<Constraint, GeneratedConstraint>(index, move(expression));
}

} // namespace duckdb

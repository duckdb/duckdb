#include "duckdb/parser/constraints/generated_constraint.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

GeneratedConstraint::GeneratedConstraint(uint64_t index)
    : Constraint(ConstraintType::GENERATED), index(index) {
}

string GeneratedConstraint::ToString() const {
	D_ASSERT(index != DConstants::INVALID_INDEX);
	string base = "GENERATED(";
	base += std::to_string(index);
	return base + ")";
}

unique_ptr<Constraint> GeneratedConstraint::Copy() const {
	auto result = make_unique<GeneratedConstraint>(index);
	return move(result);
}

void GeneratedConstraint::Serialize(FieldWriter &writer) const {
	writer.WriteField<uint64_t>(index);
}

unique_ptr<Constraint> GeneratedConstraint::Deserialize(FieldReader &source) {
	auto index = source.ReadRequired<idx_t>();
	return make_unique_base<Constraint, GeneratedConstraint>(index);
}

} // namespace duckdb

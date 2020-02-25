#include "duckdb/parser/constraints/not_null_constraint.hpp"

#include "duckdb/common/serializer.hpp"

using namespace std;
using namespace duckdb;

string NotNullConstraint::ToString() const {
	return "NOT NULL Constraint";
}

unique_ptr<Constraint> NotNullConstraint::Copy() {
	return make_unique<NotNullConstraint>(index);
}

void NotNullConstraint::Serialize(Serializer &serializer) {
	Constraint::Serialize(serializer);
	serializer.Write<idx_t>(index);
}

unique_ptr<Constraint> NotNullConstraint::Deserialize(Deserializer &source) {
	auto index = source.Read<idx_t>();
	return make_unique_base<Constraint, NotNullConstraint>(index);
}

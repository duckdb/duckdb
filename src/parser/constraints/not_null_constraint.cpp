#include "parser/constraints/not_null_constraint.hpp"

#include "common/serializer.hpp"

using namespace std;
using namespace duckdb;

void NotNullConstraint::Serialize(Serializer &serializer) {
	Constraint::Serialize(serializer);
	serializer.Write<uint64_t>(index);
}

unique_ptr<Constraint> NotNullConstraint::Deserialize(Deserializer &source) {
	auto index = source.Read<uint64_t>();
	return make_unique_base<Constraint, NotNullConstraint>(index);
}

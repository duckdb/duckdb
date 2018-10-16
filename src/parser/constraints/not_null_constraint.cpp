
#include "common/serializer.hpp"

#include "parser/constraints/not_null_constraint.hpp"

using namespace std;
using namespace duckdb;

void NotNullConstraint::Serialize(Serializer &serializer) {
	Constraint::Serialize(serializer);
	serializer.Write<size_t>(index);
}

unique_ptr<Constraint> NotNullConstraint::Deserialize(Deserializer &source) {
	auto index = source.Read<size_t>();
	return make_unique_base<Constraint, NotNullConstraint>(index);
}

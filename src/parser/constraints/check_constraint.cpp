#include "parser/constraints/check_constraint.hpp"

#include "common/serializer.hpp"

using namespace std;
using namespace duckdb;

void CheckConstraint::Serialize(Serializer &serializer) {
	Constraint::Serialize(serializer);
	expression->Serialize(serializer);
}

unique_ptr<Constraint> CheckConstraint::Deserialize(Deserializer &source) {
	auto expression = ParsedExpression::Deserialize(source);
	if (!expression) {
		return nullptr;
	}
	return make_unique_base<Constraint, CheckConstraint>(move(expression));
}

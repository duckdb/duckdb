#include "parser/constraint.hpp"

#include "common/printer.hpp"
#include "common/serializer.hpp"
#include "parser/constraints/list.hpp"

using namespace duckdb;
using namespace std;

// FIXME: this is quite dirty, just copy by first serializing and then
// deserializing
unique_ptr<Constraint> Constraint::Copy() {
	Serializer serializer;
	Serialize(serializer);
	Deserializer source(serializer);
	return Constraint::Deserialize(source);
}

void Constraint::Serialize(Serializer &serializer) {
	serializer.Write<int>((int)type);
}

unique_ptr<Constraint> Constraint::Deserialize(Deserializer &source) {
	auto type = (ConstraintType)source.Read<int>();
	switch (type) {
	case ConstraintType::NOT_NULL:
		return NotNullConstraint::Deserialize(source);
	case ConstraintType::CHECK:
		return CheckConstraint::Deserialize(source);
	case ConstraintType::DUMMY:
		return ParsedConstraint::Deserialize(source);
	default:
		// don't know how to serialize this constraint type
		return nullptr;
	}
}

void Constraint::Print() {
	Printer::Print(ToString());
}

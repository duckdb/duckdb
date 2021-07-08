#include "duckdb/parser/constraint.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/parser/constraints/list.hpp"

namespace duckdb {

void Constraint::Serialize(Serializer &serializer) {
	serializer.Write<ConstraintType>(type);
}

unique_ptr<Constraint> Constraint::Deserialize(Deserializer &source) {
	auto type = source.Read<ConstraintType>();
	switch (type) {
	case ConstraintType::NOT_NULL:
		return NotNullConstraint::Deserialize(source);
	case ConstraintType::CHECK:
		return CheckConstraint::Deserialize(source);
	case ConstraintType::UNIQUE:
		return UniqueConstraint::Deserialize(source);
	default: // LCOV_EXCL_START
		// don't know how to serialize this constraint type
		return nullptr;
	} // LCOV_EXCL_STOP
}

void Constraint::Print() { // LCOV_EXCL_START
	Printer::Print(ToString());
} // LCOV_EXCL_STOP

} // namespace duckdb

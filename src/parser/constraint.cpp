#include "duckdb/parser/constraint.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/parser/constraints/list.hpp"

namespace duckdb {

Constraint::Constraint(ConstraintType type) : type(type) {
}

Constraint::~Constraint() {
}

void Constraint::Print() const {
	Printer::Print(ToString());
}

} // namespace duckdb

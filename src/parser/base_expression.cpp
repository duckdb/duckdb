#include "duckdb/parser/base_expression.hpp"

#include "duckdb/common/printer.hpp"

namespace duckdb {

void BaseExpression::Print() { // LCOV_EXCL_START
	Printer::Print(ToString());
} // LCOV_EXCL_STOP

string BaseExpression::GetName() const {
	return !alias.empty() ? alias : ToString();
}

bool BaseExpression::Equals(const BaseExpression *other) const {
	if (!other) {
		return false;
	}
	if (this->expression_class != other->expression_class || this->type != other->type) {
		return false;
	}
	return true;
}

} // namespace duckdb

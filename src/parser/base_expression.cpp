#include "duckdb/parser/base_expression.hpp"

#include "duckdb/common/printer.hpp"

using namespace duckdb;
using namespace std;

void BaseExpression::Print() {
	Printer::Print(ToString());
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

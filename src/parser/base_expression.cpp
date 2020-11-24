#include "duckdb/parser/base_expression.hpp"

#include "duckdb/common/printer.hpp"

namespace duckdb {
using namespace std;

void BaseExpression::Print() {
	Printer::Print(ToString());
}

string BaseExpression::GetName() const {
	if (alias.empty()) {
		return ToString();
	} else {
		return alias + " -> " + ToString();
	}
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


#include "parser/expression.hpp"

using namespace duckdb;
using namespace std;

bool Expression::IsAggregate() {
	bool is_aggregate = false;
	for (auto &child : children) {
		is_aggregate |= child->IsAggregate();
	}
	return is_aggregate;
}

bool Expression::IsScalar() {
	bool is_scalar = true;
	for (auto &child : children) {
		is_scalar &= child->IsScalar();
	}
	return is_scalar;
}

void Expression::GetAggregates(
    std::vector<AggregateExpression *> &expressions) {
	for (auto &child : children) {
		child->GetAggregates(expressions);
	}
}

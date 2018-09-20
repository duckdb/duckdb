
#include "parser/statement/select_statement.hpp"

#include "common/assert.hpp"

using namespace duckdb;
using namespace std;

string SelectStatement::ToString() const { return "Select"; }

bool SelectStatement::HasAggregation() {
	if (HasGroup()) {
		return true;
	}
	for (auto &expr : select_list) {
		if (expr->IsAggregate()) {
			return true;
		}
	}
	return false;
}

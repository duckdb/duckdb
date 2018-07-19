
#include "parser/statement/select_statement.hpp"

#include "common/assert.hpp"

using namespace duckdb;
using namespace std;

string SelectStatement::ToString() const { return "Select"; }

bool SelectStatement::HasAggregation() {
	if (HasGroup()) {
		return true;
	}
	bool has_aggregation = false;
	bool has_other_expression = false;
	for(auto& expr : select_list) {
		if (expr->IsAggregate()) {
			has_aggregation = true;
		} else {
			has_other_expression = true;
		}
	}
	if (has_aggregation && has_other_expression) {
		throw SyntaxException("Mixture of aggregate and non-aggregate expressions without group by!");
	}
 	return has_aggregation;
}
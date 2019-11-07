#include "duckdb/execution/column_binding_resolver.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

using namespace duckdb;
using namespace std;

ColumnBindingResolver::ColumnBindingResolver() : TableBindingResolver(true, true) {
}

unique_ptr<Expression> ColumnBindingResolver::VisitReplace(BoundColumnRefExpression &expr,
                                                           unique_ptr<Expression> *expr_ptr) {
	assert(expr.depth == 0);
	auto index = INVALID_INDEX;
	for (auto &binding : bound_tables) {
		if (binding.table_index == expr.binding.table_index) {
			index = (binding.column_offset + expr.binding.column_index);
			assert(expr.binding.column_index < binding.column_count);
			break;
		}
	}
	assert(index != INVALID_INDEX);
	if (index == INVALID_INDEX) {
		throw Exception("Failed to bind column ref");
	}
	return make_unique<BoundReferenceExpression>(expr.alias, expr.return_type, index);
}

#include "execution/column_binding_resolver.hpp"

#include "planner/expression/bound_columnref_expression.hpp"
#include "planner/expression/bound_reference_expression.hpp"

using namespace duckdb;
using namespace std;

ColumnBindingResolver::ColumnBindingResolver() : TableBindingResolver(true, true) {
}

unique_ptr<Expression> ColumnBindingResolver::VisitReplace(BoundColumnRefExpression &expr,
                                                           unique_ptr<Expression> *expr_ptr) {
	assert(expr.depth == 0);
	uint32_t index = (uint32_t)-1;
	for (auto &binding : bound_tables) {
		if (binding.table_index == expr.binding.table_index) {
			assert(binding.column_offset + expr.binding.column_index <= std::numeric_limits<uint32_t>::max());
			index = (uint32_t)(binding.column_offset + expr.binding.column_index);
			assert(expr.binding.column_index < binding.column_count);
			break;
		}
	}
	assert(index != (uint32_t)-1);
	if (index == (uint32_t)-1) {
		throw Exception("Failed to bind column ref");
	}
	return make_unique<BoundReferenceExpression>(expr.alias, expr.return_type, index);
}

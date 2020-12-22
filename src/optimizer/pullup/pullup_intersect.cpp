#include "duckdb/optimizer/filter_pullup.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {
using namespace std;

static void ReplaceFilterTableIndex(Expression &expr, LogicalSetOperation &setop) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = (BoundColumnRefExpression &)expr;
		D_ASSERT(colref.depth == 0);

		colref.binding.table_index = setop.table_index;
		return;
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](Expression &child) { ReplaceFilterTableIndex(child, setop); });
}

unique_ptr<LogicalOperator> FilterPullup::PullupIntersect(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_INTERSECT);
	is_set_operation=true;
    can_pullup = true;
	op = PullupBothSide(move(op));
	if(op->type == LogicalOperatorType::LOGICAL_FILTER) {
        auto &filter = (LogicalFilter &)*op;
		auto &setop = (LogicalSetOperation &)*filter.children[0];
		for(idx_t i=0; i < filter.expressions.size(); ++i) {
			ReplaceFilterTableIndex(*filter.expressions[i], setop);
		}
    }
	return op;
}

} // namespace duckdb

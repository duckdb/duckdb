// #include "optimizer/filter_pushdown.hpp"

// #include "planner/operator/logical_set_operation.hpp"

// using namespace duckdb;
// using namespace std;

// using Filter = FilterPushdown::Filter;

// unique_ptr<LogicalOperator> FilterPushdown::PushdownSetOperation(unique_ptr<LogicalOperator> op) {
// 	assert(op->type == LogicalOperatorType::UNION);

// 	// pushdown into UNION, can pushdown the expressions into both sides
// 	FilterPushdown left_pushdown(rewriter), right_pushdown(rewriter);
// 	for(auto &f : filters) {
// 		f->filter->Copy();
// 	}
	
// 	op->children[0] = left_pushdown.Rewrite(move(op->children[0]));
// 	op->children[1] = right_pushdown.Rewrite(move(op->children[1]));

// 	return op;
// }

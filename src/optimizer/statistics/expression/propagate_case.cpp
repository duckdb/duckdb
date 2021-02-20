#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"

namespace duckdb {

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(BoundCaseExpression &bound_case,
                                                                     unique_ptr<Expression> *expr_ptr) {
	// propagate in all the children
	auto check_stats = PropagateExpression(bound_case.check);
	auto res_if_true_stats = PropagateExpression(bound_case.result_if_true);
	auto res_if_false_stats = PropagateExpression(bound_case.result_if_false);
	// for a case statement, the resulting stats are the merged stats of the two children
	if (!res_if_true_stats || !res_if_false_stats) {
		return nullptr;
	}
	res_if_true_stats->Merge(*res_if_false_stats);
	return res_if_true_stats;
}

} // namespace duckdb

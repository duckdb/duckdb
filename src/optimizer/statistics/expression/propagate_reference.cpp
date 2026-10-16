#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(BoundReferenceExpression &ref,
                                                                     unique_ptr<Expression> &expr_ptr) {
	// references only carry statistics within a lambda body
	if (!lambda_ref_stats) {
		return nullptr;
	}
	auto &ref_stats = *lambda_ref_stats;
	auto index = ref.Index();
	if (index >= ref_stats.size() || !ref_stats[index]) {
		return nullptr;
	}
	return ref_stats[index]->ToUnique();
}

void StatisticsPropagator::PropagateLambdaStatistics(unique_ptr<Expression> &lambda_body,
                                                     const vector<unique_ptr<BaseStatistics>> &lambda_ref_stats_p) {
	// save and restore the reference statistics, so that nested lambdas are handled
	auto saved_ref_stats = lambda_ref_stats;
	lambda_ref_stats = &lambda_ref_stats_p;
	PropagateExpression(lambda_body);
	lambda_ref_stats = saved_ref_stats;
}

} // namespace duckdb

#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(BoundReferenceExpression &ref,
                                                                     unique_ptr<Expression> &expr_ptr) {
	// reference statistics are only seeded (lambda_ref_stats is non-null) for the duration of a
	// PropagateLambdaStatistics call; outside that, a BoundReferenceExpression propagates as unknown
	if (!lambda_ref_stats) {
		return nullptr;
	}
	auto &ref_stats = *lambda_ref_stats;
	auto index = ref.Index();
	if (index >= ref_stats.size() || !ref_stats[index]) {
		// no statistics seeded for this reference: propagate as unknown
		return nullptr;
	}
	return ref_stats[index]->ToUnique();
}

void StatisticsPropagator::PropagateLambdaStatistics(unique_ptr<Expression> &lambda_body,
                                                     const vector<unique_ptr<BaseStatistics>> &lambda_ref_stats_p) {
	// save and restore the reference statistics so this is re-entrant (nested lambdas, sibling expressions)
	auto saved_ref_stats = lambda_ref_stats;
	lambda_ref_stats = &lambda_ref_stats_p;
	try {
		PropagateExpression(lambda_body);
	} catch (...) {
		// exception-safe: restore lambda_ref_stats before propagating the exception
		lambda_ref_stats = saved_ref_stats;
		throw;
	}
	lambda_ref_stats = saved_ref_stats;
}

} // namespace duckdb

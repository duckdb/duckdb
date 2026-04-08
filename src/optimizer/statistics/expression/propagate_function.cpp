#include <memory>

#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(BoundFunctionExpression &func,
                                                                     unique_ptr<Expression> &expr_ptr) {
	vector<BaseStatistics> stats;
	stats.reserve(func.children.size());
	for (idx_t i = 0; i < func.children.size(); i++) {
		auto stat = PropagateExpression(func.children[i]);
		if (!stat) {
			stats.push_back(BaseStatistics::CreateUnknown(func.children[i]->return_type));
		} else {
			stats.push_back(stat->Copy());
		}
	}
	if (!func.function.HasStatisticsCallback()) {
		return nullptr;
	}
	FunctionStatisticsInput input(func, func.bind_info.get(), stats, &expr_ptr);
	return func.function.GetStatisticsCallback()(context, input);
}

} // namespace duckdb

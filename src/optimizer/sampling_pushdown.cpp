#include "duckdb/optimizer/sampling_pushdown.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_sample.hpp"
namespace duckdb {

unique_ptr<LogicalOperator> SamplingPushdown::Optimize(unique_ptr<LogicalOperator> op) {
	if (op->type == LogicalOperatorType::LOGICAL_SAMPLE && !op->children.empty() &&
	    op->children[0]->type == LogicalOperatorType::LOGICAL_GET && op->children[0]->children.empty()) {
		auto &sample_op = op->Cast<LogicalSample>();
		auto &get = op->children[0]->Cast<LogicalGet>();
		const auto &sample_options = *sample_op.sample_options;
		const bool can_push_system_sample = get.function.sampling_pushdown &&
		                                    sample_options.method == SampleMethod::SYSTEM_SAMPLE &&
		                                    sample_options.is_percentage;
		const bool can_push_reservoir_sample = get.function.reservoir_sampling_pushdown &&
		                                       sample_options.method == SampleMethod::RESERVOIR_SAMPLE &&
		                                       !sample_options.is_percentage;
		if (can_push_system_sample || can_push_reservoir_sample) {
			// set sampling option
			get.extra_info.sample_options = std::move(sample_op.sample_options);
			op = std::move(op->children[0]);
		}
	}
	for (auto &child : op->children) {
		child = Optimize(std::move(child));
	}
	return op;
}

} // namespace duckdb

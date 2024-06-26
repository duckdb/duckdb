#include "duckdb/optimizer/sampling_pushdown.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_sample.hpp"
#include "duckdb/common/types/value.hpp"
namespace duckdb {

unique_ptr<LogicalOperator> SamplingPushdown::Optimize(unique_ptr<LogicalOperator> op) {
	if (op->type == LogicalOperatorType::LOGICAL_SAMPLE &&
	    op->Cast<LogicalSample>().sample_options->method == SampleMethod::SYSTEM_SAMPLE &&
	    op->Cast<LogicalSample>().sample_options->is_percentage && !op->children.empty() &&
	    op->children[0]->type == LogicalOperatorType::LOGICAL_GET &&
	    op->children[0]->Cast<LogicalGet>().function.name == "seq_scan" && op->children[0]->children.empty()) {
		auto &get = op->children[0]->Cast<LogicalGet>();
		// set sampling pushdown options
		get.function.sampling_pushdown = true;
		get.function.sample_rate = op->Cast<LogicalSample>().sample_options->sample_size.GetValue<double>() / 100.0;
		op = std::move(op->children[0]);
	}
	for (auto &child : op->children) {
		child = Optimize(std::move(child));
	}
	return op;
}

} // namespace duckdb

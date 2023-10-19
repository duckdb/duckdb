#include "duckdb/planner/operator/logical_sample.hpp"

namespace duckdb {

LogicalSample::LogicalSample() : LogicalOperator(LogicalOperatorType::LOGICAL_SAMPLE) {
}

LogicalSample::LogicalSample(unique_ptr<SampleOptions> sample_options_p, unique_ptr<LogicalOperator> child)
    : LogicalOperator(LogicalOperatorType::LOGICAL_SAMPLE), sample_options(std::move(sample_options_p)) {
	children.push_back(std::move(child));
}

vector<ColumnBinding> LogicalSample::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

idx_t LogicalSample::EstimateCardinality(ClientContext &context) {
	auto child_cardinality = children[0]->EstimateCardinality(context);
	if (sample_options->is_percentage) {
		double sample_cardinality =
		    double(child_cardinality) * (sample_options->sample_size.GetValue<double>() / 100.0);
		if (sample_cardinality > double(child_cardinality)) {
			return child_cardinality;
		}
		return idx_t(sample_cardinality);
	} else {
		auto sample_size = sample_options->sample_size.GetValue<uint64_t>();
		if (sample_size < child_cardinality) {
			return sample_size;
		}
	}
	return child_cardinality;
}

void LogicalSample::ResolveTypes() {
	types = children[0]->types;
}

} // namespace duckdb

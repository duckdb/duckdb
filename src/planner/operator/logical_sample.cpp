#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/operator/logical_sample.hpp"

namespace duckdb {

LogicalSample::LogicalSample(unique_ptr<SampleOptions> sample_options_p, unique_ptr<LogicalOperator> child)
    : LogicalOperator(LogicalOperatorType::LOGICAL_SAMPLE), sample_options(move(sample_options_p)) {
	children.push_back(move(child));
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

void LogicalSample::Serialize(FieldWriter &writer) const {
	sample_options->Serialize(writer.GetSerializer());
}

unique_ptr<LogicalOperator> LogicalSample::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto sample_options = SampleOptions::Deserialize(reader.GetSource());
	// TODO(stephwang): review how to pass child LogicalOperator
	auto result = make_unique<LogicalSample>(move(sample_options), nullptr);
	return move(result);
}
} // namespace duckdb

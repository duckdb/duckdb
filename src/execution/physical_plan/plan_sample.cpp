#include "duckdb/execution/operator/helper/physical_reservoir_sample.hpp"
#include "duckdb/execution/operator/helper/physical_streaming_sample.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_sample.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalSample &op) {
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);

	unique_ptr<PhysicalOperator> sample;
	switch (op.sample_options->method) {
	case SampleMethod::RESERVOIR_SAMPLE:
		sample = make_uniq<PhysicalReservoirSample>(op.types, std::move(op.sample_options), op.estimated_cardinality);
		break;
	case SampleMethod::SYSTEM_SAMPLE:
	case SampleMethod::BERNOULLI_SAMPLE:
		if (!op.sample_options->is_percentage) {
			throw ParserException("Sample method %s cannot be used with a discrete sample count, either switch to "
			                      "reservoir sampling or use a sample_size",
			                      EnumUtil::ToString(op.sample_options->method));
		}
		sample = make_uniq<PhysicalStreamingSample>(op.types, op.sample_options->method,
		                                            op.sample_options->sample_size.GetValue<double>(),
		                                            op.sample_options->seed, op.estimated_cardinality);
		break;
	default:
		throw InternalException("Unimplemented sample method");
	}
	sample->children.push_back(std::move(plan));
	return sample;
}

} // namespace duckdb

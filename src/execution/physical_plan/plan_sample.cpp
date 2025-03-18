#include "duckdb/execution/operator/helper/physical_reservoir_sample.hpp"
#include "duckdb/execution/operator/helper/physical_streaming_sample.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_sample.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/random_engine.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalSample &op) {
	D_ASSERT(op.children.size() == 1);

	auto &plan = CreatePlan(*op.children[0]);
	if (!op.sample_options->seed.IsValid()) {
		auto &random_engine = RandomEngine::Get(context);
		op.sample_options->SetSeed(random_engine.NextRandomInteger());
	}

	switch (op.sample_options->method) {
	case SampleMethod::RESERVOIR_SAMPLE: {
		auto &sample = Make<PhysicalReservoirSample>(op.types, std::move(op.sample_options), op.estimated_cardinality);
		sample.children.push_back(plan);
		return sample;
	}
	case SampleMethod::SYSTEM_SAMPLE:
	case SampleMethod::BERNOULLI_SAMPLE: {
		if (!op.sample_options->is_percentage) {
			throw ParserException("Sample method %s cannot be used with a discrete sample count, either switch to "
			                      "reservoir sampling or use a sample_size",
			                      EnumUtil::ToString(op.sample_options->method));
		}
		auto &sample = Make<PhysicalStreamingSample>(op.types, std::move(op.sample_options), op.estimated_cardinality);
		sample.children.push_back(plan);
		return sample;
	}
	default:
		throw InternalException("Unimplemented sample method");
	}
}

} // namespace duckdb

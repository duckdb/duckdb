#include "duckdb/execution/operator/helper/physical_limit.hpp"
#include "duckdb/execution/operator/helper/physical_reservoir_sample.hpp"
#include "duckdb/execution/operator/helper/physical_streaming_sample.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_sample.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/exception/parser_exception.hpp"

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
	case SampleMethod::SYSTEM_SAMPLE: {
		const bool is_percentage = op.sample_options->is_percentage;
		int64_t rows = 0;
		if (!is_percentage) {
			rows = op.sample_options->sample_size.GetValue<int64_t>();
			// To ensure consistency between optimized and unoptimized paths,
			// we calculate the rate based on the base table cardinality if possible.
			idx_t base_cardinality = op.estimated_cardinality;
			LogicalOperator *current = &op;
			while (!current->children.empty()) {
				current = current->children[0].get();
				if (current->type == LogicalOperatorType::LOGICAL_GET) {
					base_cardinality = current->estimated_cardinality;
					break;
				}
			}
			if (base_cardinality > 0) {
				op.sample_options->sample_rate = static_cast<double>(rows) / static_cast<double>(base_cardinality);
			} else {
				op.sample_options->sample_rate = 1.0;
			}
		}

		auto &sample = Make<PhysicalStreamingSample>(op.types, std::move(op.sample_options), op.estimated_cardinality);
		sample.children.push_back(plan);

		if (!is_percentage) {
			// As the sampling operator uses a distributed chunk-based approach it may
			// oversample, so we wrap it with a LIMIT to ensure we stop as soon as the target is reached
			auto &limit = Make<PhysicalLimit>(op.types, BoundLimitNode::ConstantValue(rows), BoundLimitNode(),
			                                  op.estimated_cardinality);
			limit.children.push_back(sample);
			return limit;
		}
		return sample;
	}
	default:
		throw InternalException("Unimplemented sample method");
	}
}

} // namespace duckdb

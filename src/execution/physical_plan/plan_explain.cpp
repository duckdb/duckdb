#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/operator/helper/physical_explain_analyze.hpp"
#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_explain.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalExplain &op) {
	D_ASSERT(op.children.size() == 1);
	auto logical_plan_opt = op.children[0]->ToString(op.explain_format);
	auto &plan = CreatePlan(*op.children[0]);
	if (op.explain_type == ExplainType::EXPLAIN_ANALYZE) {
		auto &explain = Make<PhysicalExplainAnalyze>(op.types, op.explain_format);
		explain.children.push_back(plan);
		return explain;
	}

	// Format the plan and set the output of the EXPLAIN.
	op.physical_plan = plan.ToString(op.explain_format);
	vector<string> keys, values;
	switch (Settings::Get<ExplainOutputSetting>(context)) {
	case ExplainOutputType::OPTIMIZED_ONLY:
		keys = {"logical_opt"};
		values = {logical_plan_opt};
		break;
	case ExplainOutputType::PHYSICAL_ONLY:
		keys = {"physical_plan"};
		values = {op.physical_plan};
		break;
	default:
		keys = {"logical_plan", "logical_opt", "physical_plan"};
		values = {op.logical_plan_unopt, logical_plan_opt, op.physical_plan};
	}

	// Create a ColumnDataCollection: single "QUERY PLAN" column, one row per line
	// (PostgreSQL-compatible format).
	auto &allocator = Allocator::Get(context);
	vector<LogicalType> plan_types {LogicalType::VARCHAR};
	auto collection =
	    make_uniq<ColumnDataCollection>(context, plan_types, ColumnDataAllocatorType::IN_MEMORY_ALLOCATOR);

	DataChunk chunk;
	chunk.Initialize(allocator, plan_types);
	for (idx_t i = 0; i < values.size(); i++) {
		auto &val = values[i];
		idx_t pos = 0;
		while (pos < val.size()) {
			auto nl = val.find('\n', pos);
			auto line = val.substr(pos, nl == string::npos ? string::npos : nl - pos);
			pos = nl == string::npos ? val.size() : nl + 1;
			if (line.empty()) {
				continue;
			}
			chunk.SetValue(0, chunk.size(), Value(std::move(line)));
			chunk.SetCardinality(chunk.size() + 1);
			if (chunk.size() == STANDARD_VECTOR_SIZE) {
				collection->Append(chunk);
				chunk.Reset();
			}
		}
	}
	collection->Append(chunk);

	// Output the result via a chunk scan.
	return Make<PhysicalColumnDataScan>(plan_types, PhysicalOperatorType::COLUMN_DATA_SCAN, op.estimated_cardinality,
	                                    std::move(collection));
}

} // namespace duckdb

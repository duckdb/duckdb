#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/planner/logical_plan_sql_exporter.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/operator/helper/physical_explain_analyze.hpp"
#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_explain.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/common/enums/output_type.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalExplain &op) {
	D_ASSERT(op.children.size() == 1);
	vector<string> keys, values;
	if (op.explain_type == ExplainType::EXPLAIN_SQL) {
		LogicalPlanSQLExportOptions options;
		options.output_names = op.sql_output_names;
		auto exported = LogicalPlanSQLExporter::Export(context, *op.children[0], options);
		if (exported.HasError()) {
			auto &issue = exported.GetIssues()[0];
			string path = "logical_plan";
			if (issue.path) {
				for (auto &component : issue.path->components) {
					path += StringUtil::Format("/%s[%llu]", EnumUtil::ToString(component.type), component.ordinal);
				}
			}
			auto message = StringUtil::Format("EXPLAIN (SQL): %s during %s at %s: %s", EnumUtil::ToString(issue.code),
			                                  EnumUtil::ToString(issue.phase), path, issue.message);
			switch (issue.code) {
			case LogicalPlanVerificationIssueCode::UNSUPPORTED_OPERATOR:
			case LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPRESSION:
			case LogicalPlanVerificationIssueCode::UNSUPPORTED_FUNCTION:
			case LogicalPlanVerificationIssueCode::UNSUPPORTED_SOURCE:
			case LogicalPlanVerificationIssueCode::UNSUPPORTED_EXTENSION:
			case LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE:
				throw NotImplementedException(message);
			default:
				throw InternalException(message);
			}
		}
		keys = {"sql"};
		values = {exported.GetValue().query->ToString()};
	} else {
		// single-plan formats (e.g. FORMAT WEB) render only the final plan, so they produce a single artifact
		auto renderer = TreeRenderer::CreateRenderer(context, op.format);
		bool single_plan = renderer && renderer->RendersSinglePlan();
		bool analyze = op.explain_type == ExplainType::EXPLAIN_ANALYZE;
		// render the optimized logical plan before physical planning consumes it - but only when it will be shown
		string logical_plan_opt;
		if (!analyze && !single_plan) {
			logical_plan_opt = op.children[0]->ToString(context, op.format);
		}
		auto &plan = CreatePlan(*op.children[0]);
		if (analyze) {
			auto &explain = Make<PhysicalExplainAnalyze>(op.types, op.format);
			explain.children.push_back(plan);
			return explain;
		}

		// Format the plan and set the output of the EXPLAIN.
		op.physical_plan = plan.ToString(context, op.format);
		if (single_plan) {
			keys = {"physical_plan"};
			values = {op.physical_plan};
		} else {
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
		}
	}

	// Create a ColumnDataCollection from the output.
	auto &allocator = Allocator::Get(context);
	vector<LogicalType> plan_types {LogicalType::VARCHAR, LogicalType::VARCHAR};
	auto collection =
	    make_uniq<ColumnDataCollection>(context, plan_types, ColumnDataAllocatorType::IN_MEMORY_ALLOCATOR);

	DataChunk chunk;
	chunk.Initialize(allocator, op.types);
	for (idx_t i = 0; i < keys.size(); i++) {
		chunk.data[0].Append(Value(keys[i]));
		chunk.data[1].Append(Value(values[i]));
		if (chunk.size() == STANDARD_VECTOR_SIZE) {
			collection->Append(chunk);
			chunk.Reset();
		}
	}
	collection->Append(chunk);

	// Output the result via a chunk scan.
	return Make<PhysicalColumnDataScan>(op.types, PhysicalOperatorType::COLUMN_DATA_SCAN, op.estimated_cardinality,
	                                    std::move(collection));
}

} // namespace duckdb

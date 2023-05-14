#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/operator/logical_explain.hpp"

namespace duckdb {

void LogicalExplain::Serialize(FieldWriter &writer) const {
	writer.WriteField(explain_type);
	writer.WriteString(physical_plan);
	writer.WriteString(logical_plan_unopt);
	writer.WriteString(logical_plan_opt);
}

unique_ptr<LogicalOperator> LogicalExplain::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto explain_type = reader.ReadRequired<ExplainType>();
	// TODO(stephwang) review if unique_ptr<LogicalOperator> plan is needed
	auto result = unique_ptr<LogicalExplain>(new LogicalExplain(explain_type));
	result->physical_plan = reader.ReadRequired<string>();
	result->logical_plan_unopt = reader.ReadRequired<string>();
	result->logical_plan_opt = reader.ReadRequired<string>();
	return std::move(result);
}
} // namespace duckdb

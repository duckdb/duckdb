#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/operator/logical_explain.hpp"

namespace duckdb {

void LogicalExplain::Serialize(FieldWriter &writer) const {
	writer.WriteField(explain_type);
	writer.WriteString(physical_plan);
	writer.WriteString(logical_plan_unopt);
	writer.WriteString(logical_plan_opt);
}

unique_ptr<LogicalOperator> LogicalExplain::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                        FieldReader &reader) {
	auto explain_type = reader.ReadRequired<ExplainType>();
	auto physical_plan = reader.ReadRequired<std::string>();
	auto logical_plan_unopt = reader.ReadRequired<std::string>();
	auto logical_plan_opt = reader.ReadRequired<std::string>();
	// TODO(stephwang) review if unique_ptr<LogicalOperator> plan is needed
	auto result = make_unique<LogicalExplain>(nullptr, explain_type);
	result->physical_plan = physical_plan;
	result->logical_plan_unopt = logical_plan_unopt;
	result->logical_plan_opt = logical_plan_opt;
	return result;
}
} // namespace duckdb

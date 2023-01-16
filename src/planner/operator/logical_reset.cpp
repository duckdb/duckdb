#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/operator/logical_reset.hpp"

namespace duckdb {

void LogicalReset::Serialize(FieldWriter &writer) const {
	writer.WriteString(name);
	writer.WriteField(scope);
}

unique_ptr<LogicalOperator> LogicalReset::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto name = reader.ReadRequired<std::string>();
	auto scope = reader.ReadRequired<SetScope>();
	return make_unique<LogicalReset>(name, scope);
}

idx_t LogicalReset::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace duckdb

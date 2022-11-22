#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/operator/logical_set.hpp"

namespace duckdb {

void LogicalSet::Serialize(FieldWriter &writer) const {
	writer.WriteString(name);
	value.Serialize(writer.GetSerializer());
	writer.WriteField(scope);
}

unique_ptr<LogicalOperator> LogicalSet::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto name = reader.ReadRequired<std::string>();
	auto value = Value::Deserialize(reader.GetSource());
	auto scope = reader.ReadRequired<SetScope>();
	return make_unique<LogicalSet>(name, value, scope);
}

idx_t LogicalSet::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace duckdb

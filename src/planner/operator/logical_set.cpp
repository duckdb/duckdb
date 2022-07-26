#include "duckdb/planner/operator/logical_set.hpp"

namespace duckdb {

void LogicalSet::Serialize(FieldWriter &writer) const {
	writer.WriteString(name);
	value.Serialize(writer.GetSerializer());
	writer.WriteField(scope);
}

unique_ptr<LogicalOperator> LogicalSet::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                    FieldReader &reader) {
	auto name = reader.ReadRequired<std::string>();
	auto value = Value::Deserialize(reader.GetSource());
	auto scope = reader.ReadRequired<SetScope>();
	auto result = make_unique<LogicalSet>(name, value, scope);
	return result;
}

} // namespace duckdb

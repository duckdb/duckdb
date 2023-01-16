#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/operator/logical_show.hpp"

namespace duckdb {

void LogicalShow::Serialize(FieldWriter &writer) const {
	writer.WriteRegularSerializableList(types_select);
	writer.WriteList<string>(aliases);
}

unique_ptr<LogicalOperator> LogicalShow::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto types_select = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
	auto aliases = reader.ReadRequiredList<string>();

	// TODO(stephwang): review if we need to pass unique_ptr<LogicalOperator> plan
	auto result = unique_ptr<LogicalShow>(new LogicalShow());
	result->types_select = types_select;
	result->aliases = aliases;
	return std::move(result);
}
} // namespace duckdb

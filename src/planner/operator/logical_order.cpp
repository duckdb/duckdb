#include "duckdb/planner/operator/logical_order.hpp"

#include "duckdb/common/field_writer.hpp"

namespace duckdb {

void LogicalOrder::Serialize(FieldWriter &writer) const {
	writer.WriteRegularSerializableList(orders);
	writer.WriteField(table_index);
	writer.WriteSerializableList(projections);
}

unique_ptr<LogicalOperator> LogicalOrder::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto orders = reader.ReadRequiredSerializableList<BoundOrderByNode, BoundOrderByNode>(state.gstate);
	auto table_index = reader.ReadRequired<idx_t>();
	auto projections = reader.ReadRequiredSerializableList<Expression>(state.gstate);
	auto result = make_unique<LogicalOrder>(move(orders));
	result->projections = move(projections);
	result->table_index = table_index;
	return move(result);
}

} // namespace duckdb

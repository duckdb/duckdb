#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

void LogicalOrder::Serialize(FieldWriter &writer) const {
	writer.WriteRegularSerializableList(orders);
}

unique_ptr<LogicalOperator> LogicalOrder::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto orders = reader.ReadRequiredSerializableList<BoundOrderByNode, BoundOrderByNode>(state.gstate);
	return make_unique<LogicalOrder>(move(orders));
}

} // namespace duckdb

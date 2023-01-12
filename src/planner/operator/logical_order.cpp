#include "duckdb/planner/operator/logical_order.hpp"

#include "duckdb/common/field_writer.hpp"

namespace duckdb {

void LogicalOrder::Serialize(FieldWriter &writer) const {
	writer.WriteRegularSerializableList(orders);
	writer.WriteList<idx_t>(projections);
}

unique_ptr<LogicalOperator> LogicalOrder::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto orders = reader.ReadRequiredSerializableList<BoundOrderByNode, BoundOrderByNode>(state.gstate);
	auto projections = reader.ReadRequiredList<idx_t>();
	auto result = make_unique<LogicalOrder>(std::move(orders));
	result->projections = std::move(projections);
	return std::move(result);
}

} // namespace duckdb

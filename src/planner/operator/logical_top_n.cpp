#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

void LogicalTopN::Serialize(FieldWriter &writer) const {
	writer.WriteRegularSerializableList(orders);
	writer.WriteField(offset);
	writer.WriteField(limit);
}

unique_ptr<LogicalOperator> LogicalTopN::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto orders = reader.ReadRequiredSerializableList<BoundOrderByNode, BoundOrderByNode>(state.gstate);
	auto offset = reader.ReadRequired<idx_t>();
	auto limit = reader.ReadRequired<idx_t>();
	return make_unique<LogicalTopN>(move(orders), limit, offset);
}

idx_t LogicalTopN::EstimateCardinality(ClientContext &context) {
	auto child_cardinality = LogicalOperator::EstimateCardinality(context);
	if (limit >= 0 && child_cardinality < idx_t(limit)) {
		return limit;
	}
	return child_cardinality;
}

} // namespace duckdb

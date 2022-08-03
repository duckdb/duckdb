#include "duckdb/planner/operator/logical_top_n.hpp"

namespace duckdb {

void LogicalTopN::Serialize(FieldWriter &writer) const {
	writer.WriteRegularSerializableList(orders);
	writer.WriteField(offset);
	writer.WriteField(limit);
}

unique_ptr<LogicalOperator> LogicalTopN::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                     FieldReader &reader) {

	auto orders = reader.ReadRequiredSerializableList<BoundOrderByNode, BoundOrderByNode>(context);
	auto offset = reader.ReadRequired<idx_t>();
	auto limit = reader.ReadRequired<idx_t>();
	return make_unique<LogicalTopN>(move(orders), limit, offset);
}
} // namespace duckdb

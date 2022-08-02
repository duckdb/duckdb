#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

void LogicalOrder::Serialize(FieldWriter &writer) const {
	writer.WriteRegularSerializableList(orders);
}

unique_ptr<LogicalOperator> LogicalOrder::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                      FieldReader &reader) {
	auto orders = reader.ReadRequiredSerializableList<BoundOrderByNode, BoundOrderByNode>(context);
	return make_unique<LogicalOrder>(move(orders));
}

} // namespace duckdb

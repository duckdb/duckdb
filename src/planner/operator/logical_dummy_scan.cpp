#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"

namespace duckdb {

void LogicalDummyScan::Serialize(FieldWriter &writer) const {
	writer.WriteField(table_index);
}

unique_ptr<LogicalOperator> LogicalDummyScan::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                          FieldReader &reader) {
	auto table_index = reader.ReadRequired<idx_t>();
	return make_unique<LogicalDummyScan>(table_index);
}

} // namespace duckdb

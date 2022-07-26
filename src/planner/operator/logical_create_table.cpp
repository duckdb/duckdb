#include "duckdb/planner/operator/logical_create_table.hpp"

namespace duckdb {

void LogicalCreateTable::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

unique_ptr<LogicalOperator> LogicalCreateTable::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                            FieldReader &reader) {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb

#include "duckdb/planner/operator/logical_explain.hpp"

namespace duckdb {

void LogicalExplain::Serialize(FieldWriter &writer) const {
	throw NotImplementedException(LogicalOperatorToString(type));
}

unique_ptr<LogicalOperator> LogicalExplain::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                        FieldReader &reader) {
	throw NotImplementedException(LogicalOperatorToString(type));
}
} // namespace duckdb

#include "duckdb/planner/operator/logical_disconnect.hpp"

namespace duckdb {

LogicalDisconnect::LogicalDisconnect(unique_ptr<DisconnectInfo> info_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_DISCONNECT), info(std::move(info_p)) {
}

LogicalDisconnect::LogicalDisconnect(unique_ptr<ParseInfo> info_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_DISCONNECT),
      info(unique_ptr_cast<ParseInfo, DisconnectInfo>(std::move(info_p))) {
}

LogicalDisconnect::~LogicalDisconnect() {
}

idx_t LogicalDisconnect::EstimateCardinality(ClientContext &context) {
	return 1;
}

void LogicalDisconnect::ResolveTypes() {
	types.emplace_back(LogicalType::BOOLEAN);
}

} // namespace duckdb

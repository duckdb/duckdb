#include "duckdb/planner/operator/logical_connect.hpp"

namespace duckdb {

LogicalConnect::LogicalConnect(unique_ptr<ConnectInfo> info_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CONNECT), info(std::move(info_p)) {
}

LogicalConnect::LogicalConnect(unique_ptr<ParseInfo> info_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CONNECT),
      info(unique_ptr_cast<ParseInfo, ConnectInfo>(std::move(info_p))) {
}

LogicalConnect::~LogicalConnect() {
}

idx_t LogicalConnect::EstimateCardinality(ClientContext &context) {
	return 1;
}

void LogicalConnect::ResolveTypes() {
	types.emplace_back(LogicalType::BOOLEAN);
}

} // namespace duckdb

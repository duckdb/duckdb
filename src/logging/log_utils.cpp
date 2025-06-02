#include "duckdb/logging/log_utils.hpp"
#include "duckdb/logging/logger.hpp"

namespace duckdb {

PhysicalOperatorLogger::PhysicalOperatorLogger(ClientContext &context_p, const PhysicalOperator &physical_operator)
    : context(context_p), operator_type(physical_operator.type), parameters(physical_operator.ParamsToString()) {
}

void PhysicalOperatorLogger::Log(const string &info) const {
	DUCKDB_LOG(context, PhysicalOperatorLogType, operator_type, parameters, info);
}

} // namespace duckdb

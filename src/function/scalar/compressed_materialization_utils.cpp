#include "duckdb/function/scalar/compressed_materialization_utils.hpp"

namespace duckdb {

const vector<LogicalType> CMUtils::IntegralTypes() {
	return {LogicalType::UTINYINT, LogicalType::USMALLINT, LogicalType::UINTEGER, LogicalType::UBIGINT};
}

const vector<LogicalType> CMUtils::StringTypes() {
	return {LogicalType::UTINYINT, LogicalType::USMALLINT, LogicalType::UINTEGER, LogicalType::UBIGINT,
	        LogicalType::HUGEINT};
}

// LCOV_EXCL_START
unique_ptr<FunctionData> CMUtils::Bind(ClientContext &context, ScalarFunction &bound_function,
                                       vector<unique_ptr<Expression>> &arguments) {
	throw BinderException("Compressed materialization functions are for internal use only!");
}
// LCOV_EXCL_STOP

} // namespace duckdb

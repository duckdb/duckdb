#include "duckdb/function/scalar/compressed_materialization_functions.hpp"

namespace duckdb {

const vector<LogicalType> CompressedMaterializationFunctions::IntegralTypes() {
	return {LogicalType::UTINYINT, LogicalType::USMALLINT, LogicalType::UINTEGER, LogicalType::UBIGINT};
}

const vector<LogicalType> CompressedMaterializationFunctions::StringTypes() {
	return {LogicalType::UTINYINT, LogicalType::USMALLINT, LogicalType::UINTEGER, LogicalType::UBIGINT,
	        LogicalType::HUGEINT};
}

// LCOV_EXCL_START
unique_ptr<FunctionData> CompressedMaterializationFunctions::Bind(ClientContext &context,
                                                                  ScalarFunction &bound_function,
                                                                  vector<unique_ptr<Expression>> &arguments) {
	throw BinderException("Compressed materialization functions are for internal use only!");
}
// LCOV_EXCL_STOP

void BuiltinFunctions::RegisterCompressedMaterializationFunctions() {
	Register<CMIntegralCompressFun>();
	Register<CMIntegralDecompressFun>();
	Register<CMStringCompressFun>();
	Register<CMStringDecompressFun>();
}

} // namespace duckdb

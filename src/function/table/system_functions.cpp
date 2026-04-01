#include "duckdb/function/table/system_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterSQLiteFunctions() {
	PragmaVersion::RegisterFunction(*this);
	PragmaPlatform::RegisterFunction(*this);
	PragmaCollations::RegisterFunction(*this);
	PragmaTableInfo::RegisterFunction(*this);
	PragmaStorageInfo::RegisterFunction(*this);
	PragmaMetadataInfo::RegisterFunction(*this);
	PragmaDatabaseSize::RegisterFunction(*this);
	PragmaUserAgent::RegisterFunction(*this);

	DuckDBConnectionCountFun::RegisterFunction(*this);
	DuckDBApproxDatabaseCountFun::RegisterFunction(*this);
	DuckDBColumnsFun::RegisterFunction(*this);
	DuckDBConstraintsFun::RegisterFunction(*this);
	DuckDBCoordinateSystemsFun::RegisterFunction(*this);
	DuckDBDatabasesFun::RegisterFunction(*this);
	DuckDBFunctionsFun::RegisterFunction(*this);
	DuckDBKeywordsFun::RegisterFunction(*this);
	DuckDBPreparedStatementsFun::RegisterFunction(*this);
	DuckDBLogFun::RegisterFunction(*this);
	DuckDBLogContextFun::RegisterFunction(*this);
	DuckDBIndexesFun::RegisterFunction(*this);
	DuckDBSchemasFun::RegisterFunction(*this);
	DuckDBDependenciesFun::RegisterFunction(*this);
	DuckDBExtensionsFun::RegisterFunction(*this);
	DuckDBMemoryFun::RegisterFunction(*this);
	DuckDBEvictionQueuesFun::RegisterFunction(*this);
	DuckDBExternalFileCacheFun::RegisterFunction(*this);
	DuckDBOptimizersFun::RegisterFunction(*this);
	DuckDBSecretsFun::RegisterFunction(*this);
	DuckDBWhichSecretFun::RegisterFunction(*this);
	DuckDBSecretTypesFun::RegisterFunction(*this);
	DuckDBSequencesFun::RegisterFunction(*this);
	DuckDBSettingsFun::RegisterFunction(*this);
	DuckDBTablesFun::RegisterFunction(*this);
	DuckDBTableSample::RegisterFunction(*this);
	DuckDBTemporaryFilesFun::RegisterFunction(*this);
	DuckDBTypesFun::RegisterFunction(*this);
	DuckDBVariablesFun::RegisterFunction(*this);
	DuckDBViewsFun::RegisterFunction(*this);
	EnableLoggingFun::RegisterFunction(*this);
	EnableProfilingFun::RegisterFunction(*this);
	TestAllTypesFun::RegisterFunction(*this);
	TestVectorTypesFun::RegisterFunction(*this);
}

} // namespace duckdb

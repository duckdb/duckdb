#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterSQLiteFunctions() {
	PragmaVersion::RegisterFunction(*this);
	PragmaFunctionPragma::RegisterFunction(*this);
	PragmaCollations::RegisterFunction(*this);
	PragmaTableInfo::RegisterFunction(*this);
	PragmaStorageInfo::RegisterFunction(*this);
	PragmaDatabaseSize::RegisterFunction(*this);
	PragmaDatabaseList::RegisterFunction(*this);
	PragmaLastProfilingOutput::RegisterFunction(*this);
	PragmaDetailedProfilingOutput::RegisterFunction(*this);

	DuckDBColumnsFun::RegisterFunction(*this);
	DuckDBConstraintsFun::RegisterFunction(*this);
	DuckDBFunctionsFun::RegisterFunction(*this);
	DuckDBKeywordsFun::RegisterFunction(*this);
	DuckDBIndexesFun::RegisterFunction(*this);
	DuckDBSchemasFun::RegisterFunction(*this);
	DuckDBDependenciesFun::RegisterFunction(*this);
	DuckDBSequencesFun::RegisterFunction(*this);
	DuckDBSettingsFun::RegisterFunction(*this);
	DuckDBTablesFun::RegisterFunction(*this);
	DuckDBTypesFun::RegisterFunction(*this);
	DuckDBViewsFun::RegisterFunction(*this);
    DuckDBMatViewsFun::RegisterFunction(*this);
	TestAllTypesFun::RegisterFunction(*this);
}

} // namespace duckdb

#include "fts-extension.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

static void stem_function(DataChunk &args, ExpressionState &state, Vector &result) {
}
    
void FTSExtension::Load(DuckDB &db) {
    Connection con(db);
	con.BeginTransaction();

    ScalarFunction stem_func("stem", {LogicalType::VARCHAR}, LogicalType::VARCHAR, stem_function);
    CreateScalarFunctionInfo stem_info(stem_func);

    db.catalog->CreateFunction(*con.context, &stem_info);

    con.Commit();
}

} // namespace duckdb
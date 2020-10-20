#include "fts-extension.hpp"

#include "fts_stemmer.hpp"
#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

static void stem_function(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t, true>(args.data[0], result, args.size(), [&](string_t input) {
        string_t target = Stem(input);
        target.Finalize();
        return target;
    });
}

void FTSExtension::Load(DuckDB &db) {
	ScalarFunction stem_func("stem", {LogicalType::VARCHAR}, LogicalType::VARCHAR, stem_function);
	CreateScalarFunctionInfo stem_info(stem_func);

	Connection conn(db);
	conn.context->transaction.BeginTransaction();

	db.catalog->CreateFunction(*conn.context, &stem_info);

	conn.context->transaction.Commit();
}

} // namespace duckdb


#include "test_helper_extension.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

void TestHelperHello(DataChunk &args, ExpressionState &state, Vector &result) {
	result.Reference(Value("Hello!"));
}

void TestHelperExtension::Load(DuckDB &db) {
	CreateScalarFunctionInfo hello_info(ScalarFunction("test_helper_hello", {}, LogicalType::VARCHAR, TestHelperHello));

	Connection conn(db);
	conn.BeginTransaction();
	auto &catalog = Catalog::GetCatalog(*conn.context);
	catalog.CreateFunction(*conn.context, &hello_info);
	conn.Commit();
}

} // namespace duckdb
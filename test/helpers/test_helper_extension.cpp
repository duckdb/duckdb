
#include "test_helper_extension.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

void TestHelperHello(DataChunk &args, ExpressionState &state, Vector &result) {
	result.Reference(Value("Hello!"));
}

void TestHelperLastError(DataChunk &args, ExpressionState &state, Vector &result) {
	if (!TestHelperExtension::last_error) {
		result.Reference(Value(LogicalType::VARCHAR));
		return;
	}

	result.Reference(Value(*TestHelperExtension::last_error));
}

unique_ptr<string> TestHelperExtension::last_error = nullptr;

void TestHelperExtension::Load(DuckDB &db) {
	CreateScalarFunctionInfo hello_info(ScalarFunction("test_helper_hello", {}, LogicalType::VARCHAR, TestHelperHello));
	CreateScalarFunctionInfo last_error_info(
	    ScalarFunction("test_helper_last_error", {}, LogicalType::VARCHAR, TestHelperLastError));

	Connection conn(db);
	conn.BeginTransaction();
	auto &client_context = *conn.context;
	auto &catalog = Catalog::GetCatalog(client_context);
	catalog.CreateFunction(client_context, &hello_info);
	catalog.CreateFunction(client_context, &last_error_info);
	conn.Commit();
}

void TestHelperExtension::SetLastError(const string &error) {
	last_error = make_unique<string>(error);
}

void TestHelperExtension::ClearLastError() {
	last_error.reset();
}

std::string TestHelperExtension::Name() {
	return "test_helper";
}

} // namespace duckdb

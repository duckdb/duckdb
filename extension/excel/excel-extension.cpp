#define DUCKDB_EXTENSION_MAIN

#include "excel-extension.hpp"

#include "zformat.hxx"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/function/scalar_function.hpp"
// #include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
// #include "duckdb/parser/parsed_data/create_table_function_info.hpp"
// #include "duckdb/parser/parsed_data/create_view_info.hpp"
// #include "duckdb/parser/parser.hpp"
// #include "duckdb/parser/statement/select_statement.hpp"
#endif

namespace duckdb {

static string_t NumberFormatScalarFunction(Vector &result, double num_value, string_t format) {
	try {
		string in_str = format.GetString();
		string out_str = GetNumberFormatString(in_str, num_value);

		if (out_str.length() > 0) {
			auto result_string = StringVector::EmptyString(result, out_str.size());
			auto result_data = result_string.GetDataWriteable();
			memcpy(result_data, out_str.c_str(), out_str.size());
			result_string.Finalize();
			return result_string;
		} else {
			auto result_string = StringVector::EmptyString(result, 0);
			result_string.Finalize();
			return result_string;
		}
	} catch (...) {
		throw InternalException("Unexpected result for number format");
	}

	return string_t();
}

static void NumberFormatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &number_vector = args.data[0];
	auto &format_vector = args.data[1];
	BinaryExecutor::Execute<double, string_t, string_t>(
	    number_vector, format_vector, result, args.size(),
	    [&](double value, string_t format) { return NumberFormatScalarFunction(result, value, format); });
}

void EXCELExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();

	auto &catalog = Catalog::GetCatalog(*con.context);

	ScalarFunction text_func("text", {LogicalType::DOUBLE, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                         NumberFormatFunction);
	CreateScalarFunctionInfo text_info(text_func);
	catalog.CreateFunction(*con.context, &text_info);

	ScalarFunction excel_text_func("excel_text", {LogicalType::DOUBLE, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                               NumberFormatFunction);
	CreateScalarFunctionInfo excel_text_info(excel_text_func);
	catalog.CreateFunction(*con.context, &excel_text_info);

	con.Commit();
}

std::string EXCELExtension::Name() {
	return "excel";
}

} // namespace duckdb

extern "C" {
DUCKDB_EXTENSION_API void excel_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::EXCELExtension>();
}

DUCKDB_EXTENSION_API const char *excel_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

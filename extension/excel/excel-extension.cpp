#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "excel-extension.hpp"
#include "nf_calendar.h"
#include "nf_localedata.h"
#include "nf_zformat.h"

namespace duckdb {

static std::string GetNumberFormatString(std::string &format, double num_value) {
	duckdb_excel::LocaleData locale_data;
	duckdb_excel::ImpSvNumberInputScan input_scan(&locale_data);
	uint16_t nCheckPos;
	std::string out_str;

	duckdb_excel::SvNumberformat num_format(format, &locale_data, &input_scan, nCheckPos);

	if (!num_format.GetOutputString(num_value, out_str)) {
		return out_str;
	}

	return "";
}

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

	auto &catalog = Catalog::GetSystemCatalog(*con.context);

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

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif

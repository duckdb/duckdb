#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "excel_extension.hpp"
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

void ExcelExtension::Load(DuckDB &db) {
	auto &db_instance = *db.instance;

	ScalarFunction text_func("text", {LogicalType::DOUBLE, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                         NumberFormatFunction);
	ExtensionUtil::RegisterFunction(db_instance, text_func);

	ScalarFunction excel_text_func("excel_text", {LogicalType::DOUBLE, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                               NumberFormatFunction);
	ExtensionUtil::RegisterFunction(db_instance, excel_text_func);
}

std::string ExcelExtension::Name() {
	return "excel";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void excel_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::ExcelExtension>();
}

DUCKDB_EXTENSION_API const char *excel_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif

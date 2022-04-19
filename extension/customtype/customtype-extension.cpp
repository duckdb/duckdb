#define DUCKDB_EXTENSION_MAIN
#include "duckdb.hpp"
#include "customtype-extension.hpp"

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_custom_type_info.hpp"
#include "duckdb/catalog/catalog_entry/custom_type_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"

namespace duckdb {

static void BoxInFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &arg_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(arg_vector, result, args.size(), [&](string_t arg) {
		auto str = arg.GetString() + " Box In";
		auto str_len = str.size();
		string_t rv = StringVector::EmptyString(result, str_len);
		auto result_data = rv.GetDataWriteable();
		memcpy(result_data, str.c_str(), str_len);
		rv.Finalize();
		return rv;
	});
}

static void BoxOutFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &arg_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(arg_vector, result, args.size(), [&](string_t arg) {
		auto str = arg.GetString() + " Box Out";
		auto str_len = str.size();
		string_t rv = StringVector::EmptyString(result, str_len);
		auto result_data = rv.GetDataWriteable();
		memcpy(result_data, str.c_str(), str_len);
		rv.Finalize();
		return rv;
	});
}

void CustomTypeExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();
	auto &context = *con.context;
	auto &catalog = Catalog::GetCatalog(context);

	ScalarFunction box_in_func("box_in", {LogicalType::VARCHAR}, LogicalType::VARCHAR, BoxInFunction);
	CreateScalarFunctionInfo box_in_info(box_in_func);
	catalog.CreateFunction(context, &box_in_info);

	ScalarFunction box_out_func("box_out", {LogicalType::VARCHAR}, LogicalType::VARCHAR, BoxOutFunction);
	CreateScalarFunctionInfo box_out_info(box_out_func);
	catalog.CreateFunction(context, &box_out_info);

	con.Commit();
}

std::string CustomTypeExtension::Name() {
	return "customtype";
}
} // namespace duckdb
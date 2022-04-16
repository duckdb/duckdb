#define DUCKDB_EXTENSION_MAIN
#include "duckdb.hpp"
#include "custom-type-extension.hpp"

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

// static void BoxTestFunction(DataChunk &args, ExpressionState &state, Vector &result) {
// 	auto &arg_vector = args.data[0];
// 	UnaryExecutor::Execute<string_t, int>(
// 	    arg_vector, result, args.size(),
// 	    [&](string_t arg) {
// 			auto str = arg.GetString() + " Box Out";
// 			auto str_len = str.size();
// 			int rv = str_len;
// 			return rv;
// 		});
// }

// static void BoxAddEndFunction(DataChunk &args, ExpressionState &state, Vector &result) {
// 	auto &arg_vector = args.data[0];
// 	UnaryExecutor::Execute<string_t, string_t>(
// 	    arg_vector, result, args.size(),
// 	    [&](string_t arg) {
// 			auto str = arg.GetString() + " Add End";
// 			auto str_len = str.size();
// 			string_t rv = StringVector::EmptyString(result, str_len);
// 			auto result_data = rv.GetDataWriteable();
// 			memcpy(result_data, str.c_str(), str_len);
// 			rv.Finalize();
// 			return rv;
// 		});
// }

void CustomTypeExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();
	auto &context = *con.context;
	auto &catalog = Catalog::GetCatalog(context);

	// string custom_name = "box";

	// auto entry = catalog.GetEntry(context, CatalogType::TYPE_ENTRY, DEFAULT_SCHEMA, custom_name, true);

	// if (!entry) {

	ScalarFunction box_in_func("box_in", {LogicalType::VARCHAR}, LogicalType::VARCHAR, BoxInFunction);
	// ScalarFunction box_in_func("box_in", {LogicalType::INTEGER}, LogicalType::INTEGER,
	//                          BoxIntInFunction);
	CreateScalarFunctionInfo box_in_info(box_in_func);
	catalog.CreateFunction(context, &box_in_info);

	ScalarFunction box_out_func("box_out", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
							BoxOutFunction);
	// ScalarFunction box_out_func("box_out", {LogicalType::INTEGER}, LogicalType::INTEGER,
	//                          BoxIntOutFunction);
	CreateScalarFunctionInfo box_out_info(box_out_func);
	catalog.CreateFunction(context, &box_out_info);

	// 	map<CustomTypeParameterId, string> parameters;
	// 	parameters.insert(pair<CustomTypeParameterId, string>(CustomTypeParameterId::INPUT_FUNCTION, "box_in"));
	// 	parameters.insert(pair<CustomTypeParameterId, string>(CustomTypeParameterId::OUTPUT_FUNCTION, "box_out"));
	// 	CreateCustomTypeInfo ctinfo;
	// 	ctinfo.name = "box";
	// 	ctinfo.type = LogicalType::CUSTOM("box", parameters, LogicalTypeId::VARCHAR);
	// 	std::cout << "Create Custom Type =============================== 1" << std::endl;
	// 	catalog.CreateCustomType(context, &ctinfo);

	// 	auto entry = catalog.GetEntry(context, CatalogType::TYPE_CUSTOM_ENTRY, INVALID_SCHEMA, "box");
	// 	if (entry->type != CatalogType::TYPE_CUSTOM_ENTRY) {
	// 		throw Exception("entry requires a custom type");
	// 	}
	// 	auto box_entry = (CustomTypeCatalogEntry *)entry;
	// 	// std::cout << "Get Entry ============================= name ====== " << (int)box_entry->user_type.id() << std::endl;
	// 	ScalarFunction box_test_func("box_test", {box_entry->user_type}, LogicalType::INTEGER,
	// 							BoxTestFunction);
	// 	CreateScalarFunctionInfo box_test_info(box_test_func);
	// 	catalog.CreateFunction(context, &box_test_info);

	// 	ScalarFunction box_add_end_func("box_add_end", {box_entry->user_type}, box_entry->user_type,
	// 							BoxAddEndFunction);
	// 	CreateScalarFunctionInfo box_add_end_info(box_add_end_func);
	// 	catalog.CreateFunction(context, &box_add_end_info);
	// }

	con.Commit();
}

std::string CustomTypeExtension::Name() {
	return "custom-type";
}
} // namespace duckdb
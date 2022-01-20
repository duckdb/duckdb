#include "json-extension.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "json_functions.hpp"

namespace duckdb {

void JSONExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();

	auto &catalog = Catalog::GetCatalog(*con.context);

	for (const auto &fun : JSONFunctions::GetFunctions()) {
		CreateScalarFunctionInfo get_object_info(fun);
		catalog.CreateFunction(*con.context, &get_object_info);
	}

	con.Commit();
}

std::string JSONExtension::Name() {
	return "json";
}

} // namespace duckdb

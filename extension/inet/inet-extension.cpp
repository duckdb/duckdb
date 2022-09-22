#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/config.hpp"
#include "inet-extension.hpp"
#include "inet_functions.hpp"

namespace duckdb {

void INETExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();

	auto &catalog = Catalog::GetCatalog(*con.context);

	// add the "inet" type
	child_list_t<LogicalType> children;
	children.push_back(make_pair("ip_type", LogicalType::UTINYINT));
	children.push_back(make_pair("address", LogicalType::HUGEINT));
	children.push_back(make_pair("mask", LogicalType::USMALLINT));
	auto inet_type = LogicalType::STRUCT(move(children));
	inet_type.SetAlias("inet");

	CreateTypeInfo info("inet", inet_type);
	info.temporary = true;
	info.internal = true;
	catalog.CreateType(*con.context, &info);

	// add inet functions
	auto host_fun = ScalarFunction("host", {inet_type}, LogicalType::VARCHAR, INetFunctions::Host);
	CreateScalarFunctionInfo host_info(host_fun);
	catalog.CreateFunction(*con.context, &host_info);

	auto substract_fun = ScalarFunction("-", {inet_type, LogicalType::BIGINT}, inet_type, INetFunctions::Subtract);
	CreateScalarFunctionInfo subtract_info(substract_fun);
	subtract_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(*con.context, &subtract_info);

	// add inet casts
	auto &config = DBConfig::GetConfig(*con.context);

	auto &casts = config.GetCastFunctions();
	casts.RegisterCastFunction(LogicalType::VARCHAR, inet_type, INetFunctions::CastVarcharToINET, 100);
	casts.RegisterCastFunction(inet_type, LogicalType::VARCHAR, INetFunctions::CastINETToVarchar);

	con.Commit();
}

std::string INETExtension::Name() {
	return "inet";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void inet_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::INETExtension>();
}

DUCKDB_EXTENSION_API const char *inet_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif

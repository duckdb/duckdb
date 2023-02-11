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
#include "macaddr-extension.hpp"
#include "macaddr_functions.hpp"

namespace duckdb {

void MACAddrExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();

	auto &catalog = Catalog::GetSystemCatalog(*con.context);

	// add the "macaddr" type
	child_list_t<LogicalType> children;
	children.push_back(make_pair("a", LogicalType::USMALLINT));
	children.push_back(make_pair("b", LogicalType::USMALLINT));
	children.push_back(make_pair("c", LogicalType::USMALLINT));

	auto macaddr_type = LogicalType::STRUCT(std::move(children));
	macaddr_type.SetAlias("macaddr");

	CreateTypeInfo info("macaddr", macaddr_type);
	info.temporary = true;
	info.internal = true;
	catalog.CreateType(*con.context, &info);

	// add macaddr casts
	auto &config = DBConfig::GetConfig(*con.context);

	auto &casts = config.GetCastFunctions();
	casts.RegisterCastFunction(LogicalType::VARCHAR, macaddr_type, MACAddrFunctions::CastVarcharToMAC, 100);
	casts.RegisterCastFunction(macaddr_type, LogicalType::VARCHAR, MACAddrFunctions::CastMACToVarchar);

	con.Commit();
}

std::string MACAddrExtension::Name() {
	return "macaddr";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void macaddr_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::MACAddrExtension>();
}

DUCKDB_EXTENSION_API const char *macaddr_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif

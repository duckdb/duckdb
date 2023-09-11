#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/config.hpp"
#include "inet_extension.hpp"
#include "inet_functions.hpp"

namespace duckdb {

static constexpr auto INET_TYPE_NAME = "INET";

void InetExtension::Load(DuckDB &db) {
	// add the "inet" type
	child_list_t<LogicalType> children;
	children.push_back(make_pair("ip_type", LogicalType::UTINYINT));
	children.push_back(make_pair("address", LogicalType::HUGEINT));
	children.push_back(make_pair("mask", LogicalType::USMALLINT));
	auto inet_type = LogicalType::STRUCT(std::move(children));
	inet_type.SetAlias(INET_TYPE_NAME);
	ExtensionUtil::RegisterType(*db.instance, INET_TYPE_NAME, inet_type);

	// add the casts to and from INET type
	ExtensionUtil::RegisterCastFunction(*db.instance, LogicalType::VARCHAR, inet_type, INetFunctions::CastVarcharToINET,
	                                    100);
	ExtensionUtil::RegisterCastFunction(*db.instance, inet_type, LogicalType::VARCHAR,
	                                    INetFunctions::CastINETToVarchar);

	// add inet functions
	ExtensionUtil::RegisterFunction(*db.instance,
	                                ScalarFunction("host", {inet_type}, LogicalType::VARCHAR, INetFunctions::Host));

	// Add - function with ALTER_ON_CONFLICT
	Connection con(db);
	con.BeginTransaction();
	auto &catalog = Catalog::GetSystemCatalog(*con.context);
	auto substract_fun = ScalarFunction("-", {inet_type, LogicalType::BIGINT}, inet_type, INetFunctions::Subtract);
	CreateScalarFunctionInfo subtract_info(substract_fun);
	subtract_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(*con.context, subtract_info);
	con.Commit();
}

std::string InetExtension::Name() {
	return "inet";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void inet_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::InetExtension>();
}

DUCKDB_EXTENSION_API const char *inet_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif

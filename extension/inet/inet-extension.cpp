#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "inet-extension.hpp"
#include "inet_functions.hpp"

namespace duckdb {

void INETExtension::Load(DuckDB &db) {
	auto &db_instance = *db.instance;

	// add the "inet" type
	child_list_t<LogicalType> children;
	children.push_back(make_pair("ip_type", LogicalType::UTINYINT));
	children.push_back(make_pair("address", LogicalType::HUGEINT));
	children.push_back(make_pair("mask", LogicalType::USMALLINT));
	auto inet_type = LogicalType::STRUCT(std::move(children));
	inet_type.SetAlias("inet");

	ExtensionUtil::RegisterType(db_instance, "inet", inet_type);

	// add inet functions
	ScalarFunction host_fun("host", {inet_type}, LogicalType::VARCHAR, INetFunctions::Host);
	ExtensionUtil::RegisterFunction(db_instance, host_fun);

	ScalarFunction subtract_fun("-", {inet_type, LogicalType::BIGINT}, inet_type, INetFunctions::Subtract);
	ExtensionUtil::ExtendFunction(db_instance, subtract_fun);

	// add inet casts
	ExtensionUtil::RegisterCastFunction(db_instance, LogicalType::VARCHAR, inet_type, INetFunctions::CastVarcharToINET,
	                                    100);
	ExtensionUtil::RegisterCastFunction(db_instance, inet_type, LogicalType::VARCHAR, INetFunctions::CastINETToVarchar);
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

#define DUCKDB_EXTENSION_MAIN

#include "geo-extension.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "geo-functions.hpp"

namespace duckdb {
void GEOExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();

	auto &catalog = Catalog::GetCatalog(*con.context);

	auto geo_type = LogicalType(LogicalTypeId::BLOB);
	geo_type.SetAlias("GEOMETRY");

	CreateTypeInfo info("Geometry", geo_type);
	info.temporary = true;
	info.internal = true;
	catalog.CreateType(*con.context, &info);

	// add geo casts
	auto &config = DBConfig::GetConfig(*con.context);

	auto &casts = config.GetCastFunctions();
	casts.RegisterCastFunction(LogicalType::VARCHAR, geo_type, GeoFunctions::CastVarcharToGEO, 100);
	casts.RegisterCastFunction(geo_type, LogicalType::VARCHAR, GeoFunctions::CastGeoToVarchar);

	con.Commit();
}

std::string GEOExtension::Name() {
	return "geo";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void geo_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::GEOExtension>();
}

DUCKDB_EXTENSION_API const char *geo_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif

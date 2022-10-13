#define DUCKDB_EXTENSION_MAIN

#include "geo-extension.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
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

	// add geo functions
	// ST_MAKEPOINT
	ScalarFunctionSet make_point("st_makepoint");
	make_point.AddFunction(
	    ScalarFunction({LogicalType::DOUBLE, LogicalType::DOUBLE}, geo_type, GeoFunctions::MakePointFunction));
	make_point.AddFunction(ScalarFunction({LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE}, geo_type,
	                                      GeoFunctions::MakePointFunction));

	CreateScalarFunctionInfo make_point_func_info(make_point);
	catalog.AddFunction(*con.context, &make_point_func_info);

	// ST_ASTEXT
	ScalarFunctionSet as_text("st_astext");
	as_text.AddFunction(ScalarFunction({geo_type}, LogicalType::VARCHAR, GeoFunctions::GeometryAsTextFunction));

	CreateScalarFunctionInfo as_text_func_info(as_text);
	catalog.AddFunction(*con.context, &as_text_func_info);

	// ST_DISTANCE
	ScalarFunctionSet distance("st_distance");
	distance.AddFunction(
	    ScalarFunction({geo_type, geo_type}, LogicalType::DOUBLE, GeoFunctions::GeometryDistanceFunction));
	distance.AddFunction(ScalarFunction({geo_type, geo_type, LogicalType::BOOLEAN}, LogicalType::DOUBLE,
	                                    GeoFunctions::GeometryDistanceFunction));

	CreateScalarFunctionInfo distance_func_info(distance);
	catalog.AddFunction(*con.context, &distance_func_info);

	// ST_CENTROID
	ScalarFunctionSet centroid("st_centroid");
	centroid.AddFunction(ScalarFunction({geo_type}, geo_type, GeoFunctions::GeometryCentroidFunction));
	centroid.AddFunction(
	    ScalarFunction({geo_type, LogicalType::BOOLEAN}, geo_type, GeoFunctions::GeometryCentroidFunction));

	CreateScalarFunctionInfo centroid_func_info(centroid);
	catalog.AddFunction(*con.context, &centroid_func_info);

	// ST_GEOFROMTEXT
	ScalarFunctionSet from_text("st_geomfromtext");
	from_text.AddFunction(ScalarFunction({LogicalType::VARCHAR}, geo_type, GeoFunctions::GeometryFromTextFunction));
	from_text.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::INTEGER}, geo_type, GeoFunctions::GeometryFromTextFunction));

	CreateScalarFunctionInfo from_text_func_info(from_text);
	catalog.AddFunction(*con.context, &from_text_func_info);

	// ST_GEOFROMWKB
	ScalarFunctionSet from_wkb("st_geomfromwkb");
	from_wkb.AddFunction(ScalarFunction({LogicalType::BLOB}, geo_type, GeoFunctions::GeometryFromWKBFunction));
	from_wkb.AddFunction(
	    ScalarFunction({LogicalType::BLOB, LogicalType::INTEGER}, geo_type, GeoFunctions::GeometryFromWKBFunction));

	CreateScalarFunctionInfo from_wkb_func_info(from_wkb);
	catalog.AddFunction(*con.context, &from_wkb_func_info);

	// ST_X
	ScalarFunctionSet get_x("st_x");
	get_x.AddFunction(ScalarFunction({geo_type}, LogicalType::DOUBLE, GeoFunctions::GeometryGetXFunction));

	CreateScalarFunctionInfo get_x_func_info(get_x);
	catalog.AddFunction(*con.context, &get_x_func_info);

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

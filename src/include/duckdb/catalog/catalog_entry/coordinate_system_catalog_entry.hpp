
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/coordinate_system_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/parser/parsed_data/create_coordinate_system_info.hpp"

namespace duckdb {

//! A coordinate system catalog entry
class CoordinateSystemCatalogEntry : public StandardEntry {
public:
	static constexpr const CatalogType Type = CatalogType::COORDINATE_SYSTEM_ENTRY;
	static constexpr const char *Name = "coordinate system";

public:
	CoordinateSystemCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateCoordinateSystemInfo &info)
	    : StandardEntry(CatalogType::COORDINATE_SYSTEM_ENTRY, schema, catalog, info.name), authority(info.authority),
	      code(info.code), projjson_definition(info.projjson_definition),
	      wkt2_2019_definition(info.wkt2_2019_definition) {
	}

	//! The authority identifier of the coordinate system (e.g. "EPSG")
	string authority;

	//! The code identifier of the coordinate system (e.g. "4326")
	string code;

	//! The PROJJSON definition of the coordinate system
	string projjson_definition;

	//! The WKT2:2019 definition of the coordinate system
	string wkt2_2019_definition;
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_collation_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"

namespace duckdb {

struct CreateCoordinateSystemInfo : public CreateInfo {
	DUCKDB_API CreateCoordinateSystemInfo(string name_p, string authority, string code, string projjson,
	                                      string wkt2_2019);

	//! The name of the coordinate system
	//! This is typically in the format "AUTH:CODE", e.g. "OGC:CRS84"
	string name;

	//! The authority identifier of the coordinate system (e.g. "EPSG")
	string authority;

	//! The code identifier of the coordinate system (e.g. "4326")
	string code;

	//! The PROJJSON definition of the coordinate system
	string projjson_definition;

	//! The WKT2:2019 definition of the coordinate system
	string wkt2_2019_definition;

public:
	unique_ptr<CreateInfo> Copy() const override;
};

} // namespace duckdb

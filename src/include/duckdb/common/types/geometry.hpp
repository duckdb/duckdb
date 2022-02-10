//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/geometry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb_postgis{
struct GSERIALIZED;
} // namespace duckdb_libpgquery

namespace duckdb {

enum class DataFormatType : uint8_t {
	FORMAT_VALUE_TYPE_WKB,
	FORMAT_VALUE_TYPE_WKT,
	FORMAT_VALUE_TYPE_GEOJSON
};

//! The Geometry class is a static class that holds helper functions for the Geometry type.
class Geometry {
public:
	static string GetString(string_t geometry, DataFormatType ftype = DataFormatType::FORMAT_VALUE_TYPE_WKB);
	//! Converts a geometry to a string, writing the output to the designated output string.
	static void ToString(string_t geometry, char *output, DataFormatType ftype = DataFormatType::FORMAT_VALUE_TYPE_WKB);
	//! Convert a geometry object to a string
	static string ToString(string_t geometry, DataFormatType ftype = DataFormatType::FORMAT_VALUE_TYPE_WKB);

	static duckdb_postgis::GSERIALIZED* GetGserialized(string_t geom);

	//! Convert a string to a geometry. This function should ONLY be called after calling GetGeometrySize, since it does NOT
	//! perform data validation.
	static void ToGeometry(duckdb_postgis::GSERIALIZED* gser, data_ptr_t output);
	//! Convert a string object to a geometry
	static string ToGeometry(duckdb_postgis::GSERIALIZED* gser);
	static string ToGeometry(string_t text);

	static duckdb_postgis::GSERIALIZED* ToGserialized(string_t str);

	static idx_t GetGeometrySize(duckdb_postgis::GSERIALIZED *gser);

	static void DestroyGeometry(duckdb_postgis::GSERIALIZED *gser);
};
}
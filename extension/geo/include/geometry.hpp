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
#include "liblwgeom/liblwgeom.hpp"

namespace duckdb {

enum class DataFormatType : uint8_t { FORMAT_VALUE_TYPE_WKB, FORMAT_VALUE_TYPE_WKT, FORMAT_VALUE_TYPE_GEOJSON };

//! The Geometry class is a static class that holds helper functions for the Geometry type.
class Geometry {
public:
	static string GetString(string_t geometry, DataFormatType ftype = DataFormatType::FORMAT_VALUE_TYPE_WKB);
	//! Converts a geometry to a string, writing the output to the designated output string.
	static void ToString(string_t geometry, char *output, DataFormatType ftype = DataFormatType::FORMAT_VALUE_TYPE_WKB);
	//! Convert a geometry object to a string
	static string ToString(string_t geometry, DataFormatType ftype = DataFormatType::FORMAT_VALUE_TYPE_WKB);

	static GSERIALIZED *GetGserialized(string_t geom);

	//! Convert a string to a geometry. This function should ONLY be called after calling GetGeometrySize, since it does
	//! NOT perform data validation.
	static void ToGeometry(GSERIALIZED *gser, data_ptr_t output);
	//! Convert a string object to a geometry
	static string ToGeometry(GSERIALIZED *gser);
	static string ToGeometry(string_t text);

	static GSERIALIZED *ToGserialized(string_t str);

	static idx_t GetGeometrySize(GSERIALIZED *gser);

	static void DestroyGeometry(GSERIALIZED *gser);

	static data_ptr_t GetBase(GSERIALIZED *gser);

	static GSERIALIZED *MakePoint(double x, double y);

	static GSERIALIZED *MakePoint(double x, double y, double z);

	static std::string AsText(data_ptr_t base, size_t size);

	static double Distance(GSERIALIZED *g1, GSERIALIZED *g2);

	static double Distance(GSERIALIZED *g1, GSERIALIZED *g2, bool use_spheroid);

	static GSERIALIZED *FromText(char *text);

	static GSERIALIZED *FromText(char *text, int srid);

	static GSERIALIZED *FromWKB(const char *text, size_t byte_size);

	static GSERIALIZED *FromWKB(const char *text, size_t byte_size, int srid);

	static double XPoint(const void *data, size_t size);

	static GSERIALIZED *Centroid(GSERIALIZED *g);

	static GSERIALIZED *Centroid(GSERIALIZED *g, bool use_spheroid);
};
} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// json_geojson.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "json_common.hpp"

namespace duckdb {

class Vector;

//! Conversion between the core GEOMETRY type (little-endian ISO WKB) and GeoJSON geometry fragments.
//! GeoJSON has no M dimension, so M is dropped when writing and never produced when reading.
//! Any "crs" or "bbox" members are ignored: geometries are assumed to already be in the intended CRS.
struct JSONGeometry {
	//! Convert WKB to a GeoJSON geometry object, allocated in doc
	static yyjson_mut_val *ToGeoJSON(const string_t &wkb, yyjson_mut_doc *doc);
	//! Convert a GeoJSON geometry object to a GEOMETRY value stored in result_vector's string heap.
	//! Returns false and sets error if val is not a well-formed GeoJSON geometry.
	static bool FromGeoJSON(yyjson_val *val, string_t &result, Vector &result_vector, string &error);
	//! Whether val looks like a GeoJSON geometry fragment. Strict: "type" must be a string holding one of
	//! the seven GeoJSON geometry names, with a matching "coordinates" (or "geometries") array.
	static bool IsGeoJSONGeometry(yyjson_val *val);
};

} // namespace duckdb

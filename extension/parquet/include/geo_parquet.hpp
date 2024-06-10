//===----------------------------------------------------------------------===//
//                         DuckDB
//
// geo_parquet.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <algorithm>
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "parquet_types.h"
#include "column_writer.hpp"

namespace duckdb {

enum class GeometryType : uint8_t {
	POINT = 1,
	LINESTRING = 2,
	POLYGON = 3,
	MULTIPOINT = 4,
	MULTILINESTRING = 5,
	MULTIPOLYGON = 6,
	GEOMETRYCOLLECTION = 7,
};

struct GeometryTypes {
	static const char *ToString(GeometryType type);
};

enum class CoordType : uint8_t {
	XY = 1,
	XYZ = 2,
	XYM = 3,
	XYZM = 4,
};

struct GeometryBounds {
	double min_x = NumericLimits<double>::Maximum();
	double max_x = NumericLimits<double>::Minimum();
	double min_y = NumericLimits<double>::Maximum();
	double max_y = NumericLimits<double>::Minimum();
	double max_z = NumericLimits<double>::Minimum();
	double min_z = NumericLimits<double>::Maximum();

	GeometryBounds() = default;

	void Combine(const GeometryBounds &other) {
		min_x = std::min(min_x, other.min_x);
		max_x = std::max(max_x, other.max_x);
		min_y = std::min(min_y, other.min_y);
		max_y = std::max(max_y, other.max_y);
		min_z = std::min(min_z, other.min_z);
		max_z = std::max(max_z, other.max_z);
	}

	void Combine(const double &x, const double &y) {
		min_x = std::min(min_x, x);
		max_x = std::max(max_x, x);
		min_y = std::min(min_y, y);
		max_y = std::max(max_y, y);
	}

	void Combine(const double &x, const double &y, const double &z) {
		min_x = std::min(min_x, x);
		max_x = std::max(max_x, x);
		min_y = std::min(min_y, y);
		max_y = std::max(max_y, y);
		min_z = std::min(min_z, z);
		max_z = std::max(max_z, z);
	}

	void Combine(const double &min_x, const double &max_x, const double &min_y, const double &max_y) {
		this->min_x = std::min(this->min_x, min_x);
		this->max_x = std::max(this->max_x, max_x);
		this->min_y = std::min(this->min_y, min_y);
		this->max_y = std::max(this->max_y, max_y);
	}

	void Combine(const double &min_x, const double &max_x, const double &min_y, const double &max_y,
	             const double &min_z, const double &max_z) {
		this->min_x = std::min(this->min_x, min_x);
		this->max_x = std::max(this->max_x, max_x);
		this->min_y = std::min(this->min_y, min_y);
		this->max_y = std::max(this->max_y, max_y);
		this->min_z = std::min(this->min_z, min_z);
		this->max_z = std::max(this->max_z, max_z);
	}
};

struct GeometryColumnData {
	GeometryBounds bounds;
	unordered_set<GeometryType> types;
	bool has_z = false;

	void Update(Vector &vector, idx_t count);

	void Combine(const GeometryColumnData &other) {
		bounds.Combine(other.bounds);
		for (auto &type : other.types) {
			types.insert(type);
		}
	}
};

struct GeoParquetData {
	bool is_geoparquet = false;
	unordered_map<string, GeometryColumnData> columns;
	void WriteMetadata(duckdb_parquet::format::FileMetaData &file_meta_data) const;
};

} // namespace duckdb

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
#include "duckdb/common/pair.hpp"
#include <limits>
#include <cmath>

namespace duckdb {

struct GeometryStatsData;

enum class GeometryType : uint8_t {
	INVALID = 0,
	POINT = 1,
	LINESTRING = 2,
	POLYGON = 3,
	MULTIPOINT = 4,
	MULTILINESTRING = 5,
	MULTIPOLYGON = 6,
	GEOMETRYCOLLECTION = 7,
};

enum class VertexType : uint8_t { XY = 0, XYZ = 1, XYM = 2, XYZM = 3 };

struct VertexXY {
	static constexpr auto TYPE = VertexType::XY;
	static constexpr auto HAS_Z = false;
	static constexpr auto HAS_M = false;

	double x;
	double y;

	bool AllNan() const {
		return std::isnan(x) && std::isnan(y);
	}
};

struct VertexXYZ {
	static constexpr auto TYPE = VertexType::XYZ;
	static constexpr auto HAS_Z = true;
	static constexpr auto HAS_M = false;

	double x;
	double y;
	double z;

	bool AllNan() const {
		return std::isnan(x) && std::isnan(y) && std::isnan(z);
	}
};
struct VertexXYM {
	static constexpr auto TYPE = VertexType::XYM;
	static constexpr auto HAS_M = true;
	static constexpr auto HAS_Z = false;

	double x;
	double y;
	double m;

	bool AllNan() const {
		return std::isnan(x) && std::isnan(y) && std::isnan(m);
	}
};

struct VertexXYZM {
	static constexpr auto TYPE = VertexType::XYZM;
	static constexpr auto HAS_Z = true;
	static constexpr auto HAS_M = true;

	double x;
	double y;
	double z;
	double m;

	bool AllNan() const {
		return std::isnan(x) && std::isnan(y) && std::isnan(z) && std::isnan(m);
	}
};

class GeometryExtent {
public:
	static constexpr auto UNKNOWN_MIN = -std::numeric_limits<double>::infinity();
	static constexpr auto UNKNOWN_MAX = +std::numeric_limits<double>::infinity();

	static constexpr auto EMPTY_MIN = +std::numeric_limits<double>::infinity();
	static constexpr auto EMPTY_MAX = -std::numeric_limits<double>::infinity();

	// "Unknown" extent means we don't know the bounding box.
	// Merging with an unknown extent results in an unknown extent.
	// Everything intersects with an unknown extent.
	static GeometryExtent Unknown() {
		return GeometryExtent {UNKNOWN_MIN, UNKNOWN_MIN, UNKNOWN_MIN, UNKNOWN_MIN,
		                       UNKNOWN_MAX, UNKNOWN_MAX, UNKNOWN_MAX, UNKNOWN_MAX};
	}

	// "Empty" extent means the smallest possible bounding box.
	// Merging with an empty extent has no effect.
	// Nothing intersects with an empty extent.
	static GeometryExtent Empty() {
		return GeometryExtent {EMPTY_MIN, EMPTY_MIN, EMPTY_MIN, EMPTY_MIN, EMPTY_MAX, EMPTY_MAX, EMPTY_MAX, EMPTY_MAX};
	}

	// Does this extent have any X/Y values set?
	// In other words, is the range of the x/y axes not empty and not unknown?
	bool HasXY() const {
		return std::isfinite(x_min) && std::isfinite(y_min) && std::isfinite(x_max) && std::isfinite(y_max);
	}
	// Does this extent have any Z values set?
	// In other words, is the range of the Z-axis not empty and not unknown?
	bool HasZ() const {
		return std::isfinite(z_min) && std::isfinite(z_max);
	}
	// Does this extent have any M values set?
	// In other words, is the range of the M-axis not empty and not unknown?
	bool HasM() const {
		return std::isfinite(m_min) && std::isfinite(m_max);
	}

	void Extend(const VertexXY &vertex) {
		x_min = MinValue(x_min, vertex.x);
		x_max = MaxValue(x_max, vertex.x);
		y_min = MinValue(y_min, vertex.y);
		y_max = MaxValue(y_max, vertex.y);
	}

	void Extend(const VertexXYZ &vertex) {
		x_min = MinValue(x_min, vertex.x);
		x_max = MaxValue(x_max, vertex.x);
		y_min = MinValue(y_min, vertex.y);
		y_max = MaxValue(y_max, vertex.y);
		z_min = MinValue(z_min, vertex.z);
		z_max = MaxValue(z_max, vertex.z);
	}

	void Extend(const VertexXYM &vertex) {
		x_min = MinValue(x_min, vertex.x);
		x_max = MaxValue(x_max, vertex.x);
		y_min = MinValue(y_min, vertex.y);
		y_max = MaxValue(y_max, vertex.y);
		m_min = MinValue(m_min, vertex.m);
		m_max = MaxValue(m_max, vertex.m);
	}

	void Extend(const VertexXYZM &vertex) {
		x_min = MinValue(x_min, vertex.x);
		x_max = MaxValue(x_max, vertex.x);
		y_min = MinValue(y_min, vertex.y);
		y_max = MaxValue(y_max, vertex.y);
		z_min = MinValue(z_min, vertex.z);
		z_max = MaxValue(z_max, vertex.z);
		m_min = MinValue(m_min, vertex.m);
		m_max = MaxValue(m_max, vertex.m);
	}

	void Merge(const GeometryExtent &other) {
		x_min = MinValue(x_min, other.x_min);
		y_min = MinValue(y_min, other.y_min);
		z_min = MinValue(z_min, other.z_min);
		m_min = MinValue(m_min, other.m_min);

		x_max = MaxValue(x_max, other.x_max);
		y_max = MaxValue(y_max, other.y_max);
		z_max = MaxValue(z_max, other.z_max);
		m_max = MaxValue(m_max, other.m_max);
	}

	bool IntersectsXY(const GeometryExtent &other) const {
		return !(x_min > other.x_max || x_max < other.x_min || y_min > other.y_max || y_max < other.y_min);
	}

	bool IntersectsXYZM(const GeometryExtent &other) const {
		return !(x_min > other.x_max || x_max < other.x_min || y_min > other.y_max || y_max < other.y_min ||
		         z_min > other.z_max || z_max < other.z_min || m_min > other.m_max || m_max < other.m_min);
	}

	bool ContainsXY(const GeometryExtent &other) const {
		return x_min <= other.x_min && x_max >= other.x_max && y_min <= other.y_min && y_max >= other.y_max;
	}

	double x_min;
	double y_min;
	double z_min;
	double m_min;

	double x_max;
	double y_max;
	double z_max;
	double m_max;
};

class Geometry {
public:
	static constexpr idx_t MAX_RECURSION_DEPTH = 16;

	//! Convert from WKT
	DUCKDB_API static bool FromString(const string_t &wkt_text, string_t &result, Vector &result_vector, bool strict);

	//! Convert to WKT
	DUCKDB_API static string_t ToString(Vector &result, const string_t &geom);

	//! Convert from WKB
	DUCKDB_API static bool FromBinary(const string_t &wkb, string_t &result, Vector &result_vector, bool strict);
	DUCKDB_API static bool FromBinary(Vector &source, Vector &result, idx_t count, bool strict);

	//! Convert to WKB
	DUCKDB_API static void ToBinary(Vector &source, Vector &result, idx_t count);

	//! Get the geometry type and vertex type from the WKB
	DUCKDB_API static pair<GeometryType, VertexType> GetType(const string_t &wkb);

	//! Update the bounding box, return number of vertices processed
	DUCKDB_API static uint32_t GetExtent(const string_t &wkb, GeometryExtent &extent);
};

} // namespace duckdb

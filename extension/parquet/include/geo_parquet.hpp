//===----------------------------------------------------------------------===//
//                         DuckDB
//
// geo_parquet.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_writer.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "parquet_types.h"

namespace duckdb {

enum class WKBGeometryType : uint16_t {
	POINT = 1,
	LINESTRING = 2,
	POLYGON = 3,
	MULTIPOINT = 4,
	MULTILINESTRING = 5,
	MULTIPOLYGON = 6,
	GEOMETRYCOLLECTION = 7,

	POINT_Z = 1001,
	LINESTRING_Z = 1002,
	POLYGON_Z = 1003,
	MULTIPOINT_Z = 1004,
	MULTILINESTRING_Z = 1005,
	MULTIPOLYGON_Z = 1006,
	GEOMETRYCOLLECTION_Z = 1007,
};

struct WKBGeometryTypes {
	static const char *ToString(WKBGeometryType type);
};

struct GeometryBounds {
	double min_x = NumericLimits<double>::Maximum();
	double max_x = NumericLimits<double>::Minimum();
	double min_y = NumericLimits<double>::Maximum();
	double max_y = NumericLimits<double>::Minimum();

	GeometryBounds() = default;

	void Combine(const GeometryBounds &other) {
		min_x = std::min(min_x, other.min_x);
		max_x = std::max(max_x, other.max_x);
		min_y = std::min(min_y, other.min_y);
		max_y = std::max(max_y, other.max_y);
	}

	void Combine(const double &x, const double &y) {
		min_x = std::min(min_x, x);
		max_x = std::max(max_x, x);
		min_y = std::min(min_y, y);
		max_y = std::max(max_y, y);
	}

	void Combine(const double &min_x, const double &max_x, const double &min_y, const double &max_y) {
		this->min_x = std::min(this->min_x, min_x);
		this->max_x = std::max(this->max_x, max_x);
		this->min_y = std::min(this->min_y, min_y);
		this->max_y = std::max(this->max_y, max_y);
	}
};

//------------------------------------------------------------------------------
// GeoParquetMetadata
//------------------------------------------------------------------------------
class ParquetReader;
class ColumnReader;
class ClientContext;
class ExpressionExecutor;

enum class GeoParquetColumnEncoding : uint8_t {
	WKB = 1,
	POINT,
	LINESTRING,
	POLYGON,
	MULTIPOINT,
	MULTILINESTRING,
	MULTIPOLYGON,
};

struct GeoParquetColumnMetadata {
	// The encoding of the geometry column
	GeoParquetColumnEncoding geometry_encoding;

	// The geometry types that are present in the column
	set<WKBGeometryType> geometry_types;

	// The bounds of the geometry column
	GeometryBounds bbox;

	// The crs of the geometry column (if any) in PROJJSON format
	string projjson;
};

class GeoParquetColumnMetadataWriter {
	unique_ptr<ExpressionExecutor> executor;
	DataChunk input_chunk;
	DataChunk result_chunk;

	unique_ptr<Expression> type_expr;
	unique_ptr<Expression> flag_expr;
	unique_ptr<Expression> bbox_expr;

public:
	explicit GeoParquetColumnMetadataWriter(ClientContext &context);
	void Update(GeoParquetColumnMetadata &meta, Vector &vector, idx_t count);
};

class GeoParquetFileMetadata {
public:
	// Try to read GeoParquet metadata. Returns nullptr if not found, invalid or the required spatial extension is not
	// available.
	static unique_ptr<GeoParquetFileMetadata> TryRead(const duckdb_parquet::format::FileMetaData &file_meta_data,
	                                                  const ClientContext &context);
	void Write(duckdb_parquet::format::FileMetaData &file_meta_data) const;

	void FlushColumnMeta(const string &column_name, const GeoParquetColumnMetadata &meta);
	const unordered_map<string, GeoParquetColumnMetadata> &GetColumnMeta() const;

	unique_ptr<ColumnReader> CreateColumnReader(ParquetReader &reader, const LogicalType &logical_type,
	                                            const duckdb_parquet::format::SchemaElement &s_ele, idx_t schema_idx_p,
	                                            idx_t max_define_p, idx_t max_repeat_p, ClientContext &context);

	bool IsGeometryColumn(const string &column_name) const;
	void RegisterGeometryColumn(const string &column_name);

	static bool IsGeoParquetConversionEnabled(const ClientContext &context);

private:
	mutex write_lock;
	string version = "1.1.0";
	string primary_geometry_column;
	unordered_map<string, GeoParquetColumnMetadata> geometry_columns;
};

} // namespace duckdb

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

struct ParquetColumnSchema;
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

	// The statistics of the geometry column
	GeometryStatsData stats;

	// The crs of the geometry column (if any) in PROJJSON format
	string projjson;

	// Used to track the "primary" geometry column (if any)
	idx_t insertion_index = 0;
};

class GeoParquetFileMetadata {
public:
	void AddGeoParquetStats(const string &column_name, const LogicalType &type, const GeometryStatsData &stats);
	void Write(duckdb_parquet::FileMetaData &file_meta_data);

	// Try to read GeoParquet metadata. Returns nullptr if not found, invalid or the required spatial extension is not
	// available.
	static unique_ptr<GeoParquetFileMetadata> TryRead(const duckdb_parquet::FileMetaData &file_meta_data,
	                                                  const ClientContext &context);
	const unordered_map<string, GeoParquetColumnMetadata> &GetColumnMeta() const;

	static unique_ptr<ColumnReader> CreateColumnReader(ParquetReader &reader, const ParquetColumnSchema &schema,
	                                                   ClientContext &context);

	bool IsGeometryColumn(const string &column_name) const;

	static bool IsGeoParquetConversionEnabled(const ClientContext &context);
	static LogicalType GeometryType();

private:
	mutex write_lock;
	string version = "1.1.0";
	unordered_map<string, GeoParquetColumnMetadata> geometry_columns;
};

} // namespace duckdb

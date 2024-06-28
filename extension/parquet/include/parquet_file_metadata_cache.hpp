//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_file_metadata_cache.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/storage/object_cache.hpp"
#include "geo_parquet.hpp"
#endif
#include "parquet_types.h"
namespace duckdb {

//! ParquetFileMetadataCache
class ParquetFileMetadataCache : public ObjectCacheEntry {
public:
	ParquetFileMetadataCache() : metadata(nullptr) {
	}
	ParquetFileMetadataCache(unique_ptr<duckdb_parquet::format::FileMetaData> file_metadata, time_t r_time,
	                         unique_ptr<GeoParquetFileMetadata> geo_metadata)
	    : metadata(std::move(file_metadata)), read_time(r_time), geo_metadata(std::move(geo_metadata)) {
	}

	~ParquetFileMetadataCache() override = default;

	//! Parquet file metadata
	unique_ptr<const duckdb_parquet::format::FileMetaData> metadata;

	//! read time
	time_t read_time;

	//! GeoParquet metadata
	unique_ptr<GeoParquetFileMetadata> geo_metadata;

public:
	static string ObjectType() {
		return "parquet_metadata";
	}

	string GetObjectType() override {
		return ObjectType();
	}
};
} // namespace duckdb

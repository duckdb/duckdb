//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_file_metadata_cache.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "geo_parquet.hpp"
#include "parquet_types.h"

namespace duckdb {
struct CachingFileHandle;

enum class ParquetCacheValidity { VALID, INVALID, UNKNOWN };

class ParquetFileMetadataCache : public ObjectCacheEntry {
public:
	ParquetFileMetadataCache(unique_ptr<duckdb_parquet::FileMetaData> file_metadata, CachingFileHandle &handle,
	                         unique_ptr<GeoParquetFileMetadata> geo_metadata);
	~ParquetFileMetadataCache() override = default;

	//! Parquet file metadata
	unique_ptr<const duckdb_parquet::FileMetaData> metadata;

	//! GeoParquet metadata
	unique_ptr<GeoParquetFileMetadata> geo_metadata;

public:
	static string ObjectType();
	string GetObjectType() override;

	bool IsValid(CachingFileHandle &new_handle) const;
	//! Check if a cache entry is valid based ONLY on the OpenFileInfo (without doing any file system calls)
	//! If the OpenFileInfo does not have enough information this can return UNKNOWN
	ParquetCacheValidity IsValid(const OpenFileInfo &info) const;

private:
	bool validate;
	time_t last_modified;
	string version_tag;
};

} // namespace duckdb

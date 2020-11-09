//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_file_metadata_cache.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/storage/object_cache.hpp" // ObjectCache
#include "parquet_types.h"                 // parquet::format::FileMetaData

namespace duckdb {

//! ParquetFileMetadataCache
class ParquetFileMetadataCache : public ObjectCache {
public:
	ParquetFileMetadataCache() : metadata(nullptr) {}
	ParquetFileMetadataCache(std::unique_ptr<parquet::format::FileMetaData> file_metadata, time_t r_time)
	    : metadata(std::move(file_metadata)), read_time(r_time) {}

	~ParquetFileMetadataCache() override = default;

	//! Parquet file metadata
	std::unique_ptr<parquet::format::FileMetaData> metadata;

	//! read time
	time_t read_time;
};
} // namespace duckdb

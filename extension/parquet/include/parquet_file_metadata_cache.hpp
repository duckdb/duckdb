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
#endif
#include "parquet_types.h"

namespace duckdb {

//! ParquetFileMetadataCache
class ParquetFileMetadataCache : public ObjectCacheEntry {
public:
	ParquetFileMetadataCache() : metadata(nullptr) {
	}
	ParquetFileMetadataCache(unique_ptr<duckdb_parquet::format::FileMetaData> file_metadata, time_t r_time)
	    : metadata(std::move(file_metadata)), read_time(r_time) {
	}

	~ParquetFileMetadataCache() override = default;

	//! Parquet file metadata
	unique_ptr<const duckdb_parquet::format::FileMetaData> metadata;

	//! read time
	time_t read_time;

public:
	static string ObjectType() {
		return "parquet_metadata";
	}

	string GetObjectType() override {
		return ObjectType();
	}
};
} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#endif

#include "parquet_types.h"
#include "column_writer.hpp"
#include "thrift/protocol/TCompactProtocol.h"

namespace duckdb {
class FileSystem;
class FileOpener;

class ParquetWriter {
	//! We limit uncompressed pages to 1B bytes
	//! This is because Parquet limits pages to 2^31 bytes as they use an int32 to represent page size
	//! Since compressed page size can theoretically be larger than uncompressed page size
	//! We conservatively choose to limit it to around half of this
	static constexpr const idx_t MAX_UNCOMPRESSED_PAGE_SIZE = 1000000000;

public:
	ParquetWriter(FileSystem &fs, string file_name, FileOpener *file_opener, vector<LogicalType> types,
	              vector<string> names, duckdb_parquet::format::CompressionCodec::type codec);

public:
	void Flush(ChunkCollection &buffer);
	void Finalize();

private:
	string file_name;
	vector<LogicalType> sql_types;
	vector<string> column_names;
	duckdb_parquet::format::CompressionCodec::type codec;

	unique_ptr<BufferedFileWriter> writer;
	shared_ptr<duckdb_apache::thrift::protocol::TProtocol> protocol;
	duckdb_parquet::format::FileMetaData file_meta_data;
	std::mutex lock;

	vector<unique_ptr<ColumnWriter>> column_writers;

private:
	//! Returns how many rows we can write from the given column, starting at the given position
	idx_t MaxWriteCount(ChunkCollection &buffer, idx_t col_idx, idx_t chunk_idx, idx_t index_in_chunk,
	                    idx_t &out_max_chunk, idx_t &out_max_index_in_chunk);

	void WriteColumn(ChunkCollection &collection, idx_t col_idx, idx_t chunk_idx, idx_t index_in_chunk, idx_t max_chunk,
	                 idx_t max_index_in_chunk, idx_t write_count);
};

} // namespace duckdb

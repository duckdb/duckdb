//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/types/chunk_collection.hpp"

#include "parquet_types.h"
#include "thrift/protocol/TCompactProtocol.h"

namespace duckdb {
class FileSystem;

class ParquetWriter {
public:
	ParquetWriter(FileSystem &fs, string file_name, vector<LogicalType> types, vector<string> names);

public:
	void Flush(ChunkCollection &buffer);
	void Finalize();

private:
	string file_name;
	vector<LogicalType> sql_types;
	vector<string> column_names;

	unique_ptr<BufferedFileWriter> writer;
	shared_ptr<apache::thrift::protocol::TProtocol> protocol;
	parquet::format::FileMetaData file_meta_data;
	std::mutex lock;
};

} // namespace duckdb

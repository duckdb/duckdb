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
	friend class ColumnWriter;
	friend class BasicColumnWriter;
	friend class ListColumnWriter;
	friend class StructColumnWriter;

public:
	ParquetWriter(FileSystem &fs, string file_name, FileOpener *file_opener, vector<LogicalType> types,
	              vector<string> names, duckdb_parquet::format::CompressionCodec::type codec);

public:
	void Flush(ChunkCollection &buffer);
	void Finalize();

	static duckdb_parquet::format::Type::type DuckDBTypeToParquetType(const LogicalType &duckdb_type);
	static void SetSchemaProperties(const LogicalType &duckdb_type, duckdb_parquet::format::SchemaElement &schema_ele);

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
};

} // namespace duckdb

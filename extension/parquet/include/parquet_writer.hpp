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
#include "duckdb/common/types/column/column_data_collection.hpp"
#endif

#include "column_writer.hpp"
#include "parquet_types.h"
#include "thrift/protocol/TCompactProtocol.h"

namespace duckdb {
class FileSystem;
class FileOpener;

struct PreparedRowGroup {
	duckdb_parquet::format::RowGroup row_group;
	vector<duckdb::unique_ptr<ColumnWriterState>> states;
};

struct FieldID;
struct ChildFieldIDs {
	unordered_map<string, FieldID> ids;
};

struct FieldID {
	explicit FieldID(int32_t field_id_p) : field_id(field_id_p) {
	}
	int32_t field_id;
	// TODO: current unused, implement at some point
	ChildFieldIDs child_field_ids;
};

class ParquetWriter {
public:
	ParquetWriter(FileSystem &fs, string file_name, vector<LogicalType> types, vector<string> names,
	              duckdb_parquet::format::CompressionCodec::type codec, unordered_map<string, FieldID> field_ids);

public:
	void PrepareRowGroup(ColumnDataCollection &buffer, PreparedRowGroup &result);
	void FlushRowGroup(PreparedRowGroup &row_group);
	void Flush(ColumnDataCollection &buffer);
	void Finalize();

	static duckdb_parquet::format::Type::type DuckDBTypeToParquetType(const LogicalType &duckdb_type);
	static void SetSchemaProperties(const LogicalType &duckdb_type, duckdb_parquet::format::SchemaElement &schema_ele);

	duckdb_apache::thrift::protocol::TProtocol *GetProtocol() {
		return protocol.get();
	}
	duckdb_parquet::format::CompressionCodec::type GetCodec() {
		return codec;
	}
	duckdb_parquet::format::Type::type GetType(idx_t schema_idx) {
		return file_meta_data.schema[schema_idx].type;
	}
	BufferedFileWriter &GetWriter() {
		return *writer;
	}

private:
	string file_name;
	vector<LogicalType> sql_types;
	vector<string> column_names;
	duckdb_parquet::format::CompressionCodec::type codec;
	unordered_map<string, FieldID> field_ids;

	duckdb::unique_ptr<BufferedFileWriter> writer;
	shared_ptr<duckdb_apache::thrift::protocol::TProtocol> protocol;
	duckdb_parquet::format::FileMetaData file_meta_data;
	std::mutex lock;

	vector<duckdb::unique_ptr<ColumnWriter>> column_writers;
};

} // namespace duckdb

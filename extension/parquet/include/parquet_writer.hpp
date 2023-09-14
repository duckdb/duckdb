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
#include "duckdb/function/copy_function.hpp"
#endif

#include "column_writer.hpp"
#include "parquet_types.h"
#include "thrift/protocol/TCompactProtocol.h"

namespace duckdb {
class FileSystem;
class FileOpener;

struct PreparedRowGroup {
	duckdb_parquet::format::RowGroup row_group;
	vector<unique_ptr<ColumnWriterState>> states;
	vector<shared_ptr<StringHeap>> heaps;
};

struct FieldID;
struct ChildFieldIDs {
	ChildFieldIDs();
	ChildFieldIDs Copy() const;
	unique_ptr<case_insensitive_map_t<FieldID>> ids;
};

struct FieldID {
	static constexpr const auto DUCKDB_FIELD_ID = "__duckdb_field_id";
	FieldID();
	explicit FieldID(int32_t field_id);
	FieldID Copy() const;
	bool set;
	int32_t field_id;
	ChildFieldIDs child_field_ids;
};

class ParquetWriter {
public:
	ParquetWriter(FileSystem &fs, string file_name, vector<LogicalType> types, vector<string> names,
	              duckdb_parquet::format::CompressionCodec::type codec, ChildFieldIDs field_ids);

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

	static CopyTypeSupport TypeIsSupported(const LogicalType &type);

private:
	static CopyTypeSupport DuckDBTypeToParquetTypeInternal(const LogicalType &duckdb_type,
	                                                       duckdb_parquet::format::Type::type &type);
	string file_name;
	vector<LogicalType> sql_types;
	vector<string> column_names;
	duckdb_parquet::format::CompressionCodec::type codec;
	ChildFieldIDs field_ids;

	unique_ptr<BufferedFileWriter> writer;
	shared_ptr<duckdb_apache::thrift::protocol::TProtocol> protocol;
	duckdb_parquet::format::FileMetaData file_meta_data;
	std::mutex lock;

	vector<unique_ptr<ColumnWriter>> column_writers;
};

} // namespace duckdb

#include "parquet_metadata.hpp"

#include "parquet_statistics.hpp"

#include <sstream>

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/main/config.hpp"
#endif

namespace duckdb {

struct ParquetMetaDataBindData : public TableFunctionData {
	vector<LogicalType> return_types;
	shared_ptr<MultiFileList> file_list;
	unique_ptr<MultiFileReader> multi_file_reader;
};

struct ParquetBloomProbeBindData : public ParquetMetaDataBindData {
	string probe_column_name;
	Value probe_constant;
};

enum class ParquetMetadataOperatorType : uint8_t {
	META_DATA,
	SCHEMA,
	KEY_VALUE_META_DATA,
	FILE_META_DATA,
	BLOOM_PROBE
};

struct ParquetMetaDataOperatorData : public GlobalTableFunctionState {
	explicit ParquetMetaDataOperatorData(ClientContext &context, const vector<LogicalType> &types)
	    : collection(context, types) {
	}

	ColumnDataCollection collection;
	ColumnDataScanState scan_state;

	MultiFileListScanData file_list_scan;
	string current_file;

public:
	static void BindMetaData(vector<LogicalType> &return_types, vector<string> &names);
	static void BindSchema(vector<LogicalType> &return_types, vector<string> &names);
	static void BindKeyValueMetaData(vector<LogicalType> &return_types, vector<string> &names);
	static void BindFileMetaData(vector<LogicalType> &return_types, vector<string> &names);
	static void BindBloomProbe(vector<LogicalType> &return_types, vector<string> &names);

	void LoadRowGroupMetadata(ClientContext &context, const vector<LogicalType> &return_types, const string &file_path);
	void LoadSchemaData(ClientContext &context, const vector<LogicalType> &return_types, const string &file_path);
	void LoadKeyValueMetaData(ClientContext &context, const vector<LogicalType> &return_types, const string &file_path);
	void LoadFileMetaData(ClientContext &context, const vector<LogicalType> &return_types, const string &file_path);
	void ExecuteBloomProbe(ClientContext &context, const vector<LogicalType> &return_types, const string &file_path,
	                       const string &column_name, const Value &probe);
};

template <class T>
string ConvertParquetElementToString(T &&entry) {
	std::stringstream ss;
	ss << entry;
	return ss.str();
}

template <class T>
string PrintParquetElementToString(T &&entry) {
	std::stringstream ss;
	entry.printTo(ss);
	return ss.str();
}

template <class T>
Value ParquetElementString(T &&value, bool is_set) {
	if (!is_set) {
		return Value();
	}
	return Value(ConvertParquetElementToString(value));
}

Value ParquetElementStringVal(const string &value, bool is_set) {
	if (!is_set) {
		return Value();
	}
	return Value(value);
}

template <class T>
Value ParquetElementInteger(T &&value, bool is_iset) {
	if (!is_iset) {
		return Value();
	}
	return Value::INTEGER(value);
}

template <class T>
Value ParquetElementBigint(T &&value, bool is_iset) {
	if (!is_iset) {
		return Value();
	}
	return Value::BIGINT(value);
}

//===--------------------------------------------------------------------===//
// Row Group Meta Data
//===--------------------------------------------------------------------===//
void ParquetMetaDataOperatorData::BindMetaData(vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("file_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("row_group_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("row_group_num_rows");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("row_group_num_columns");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("row_group_bytes");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("column_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("file_offset");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("num_values");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("path_in_schema");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("stats_min");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("stats_max");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("stats_null_count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("stats_distinct_count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("stats_min_value");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("stats_max_value");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("compression");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("encodings");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("index_page_offset");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("dictionary_page_offset");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("data_page_offset");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("total_compressed_size");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("total_uncompressed_size");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("key_value_metadata");
	return_types.emplace_back(LogicalType::MAP(LogicalType::BLOB, LogicalType::BLOB));

	names.emplace_back("bloom_filter_offset");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("bloom_filter_length");
	return_types.emplace_back(LogicalType::BIGINT);
}

Value ConvertParquetStats(const LogicalType &type, const duckdb_parquet::SchemaElement &schema_ele, bool stats_is_set,
                          const std::string &stats) {
	if (!stats_is_set) {
		return Value(LogicalType::VARCHAR);
	}
	return ParquetStatisticsUtils::ConvertValue(type, schema_ele, stats).DefaultCastAs(LogicalType::VARCHAR);
}

void ParquetMetaDataOperatorData::LoadRowGroupMetadata(ClientContext &context, const vector<LogicalType> &return_types,
                                                       const string &file_path) {
	collection.Reset();
	ParquetOptions parquet_options(context);
	auto reader = make_uniq<ParquetReader>(context, file_path, parquet_options);
	idx_t count = 0;
	DataChunk current_chunk;
	current_chunk.Initialize(context, return_types);
	auto meta_data = reader->GetFileMetadata();
	vector<LogicalType> column_types;
	vector<idx_t> schema_indexes;
	for (idx_t schema_idx = 0; schema_idx < meta_data->schema.size(); schema_idx++) {
		auto &schema_element = meta_data->schema[schema_idx];
		if (schema_element.num_children > 0) {
			continue;
		}
		column_types.push_back(ParquetReader::DeriveLogicalType(schema_element, false));
		schema_indexes.push_back(schema_idx);
	}

	for (idx_t row_group_idx = 0; row_group_idx < meta_data->row_groups.size(); row_group_idx++) {
		auto &row_group = meta_data->row_groups[row_group_idx];

		if (row_group.columns.size() > column_types.size()) {
			throw InternalException("Too many column in row group: corrupt file?");
		}
		for (idx_t col_idx = 0; col_idx < row_group.columns.size(); col_idx++) {
			auto &column = row_group.columns[col_idx];
			auto &col_meta = column.meta_data;
			auto &stats = col_meta.statistics;
			auto &schema_element = meta_data->schema[schema_indexes[col_idx]];
			auto &column_type = column_types[col_idx];

			// file_name, LogicalType::VARCHAR
			current_chunk.SetValue(0, count, file_path);

			// row_group_id, LogicalType::BIGINT
			current_chunk.SetValue(1, count, Value::BIGINT(UnsafeNumericCast<int64_t>(row_group_idx)));

			// row_group_num_rows, LogicalType::BIGINT
			current_chunk.SetValue(2, count, Value::BIGINT(row_group.num_rows));

			// row_group_num_columns, LogicalType::BIGINT
			current_chunk.SetValue(3, count, Value::BIGINT(UnsafeNumericCast<int64_t>(row_group.columns.size())));

			// row_group_bytes, LogicalType::BIGINT
			current_chunk.SetValue(4, count, Value::BIGINT(row_group.total_byte_size));

			// column_id, LogicalType::BIGINT
			current_chunk.SetValue(5, count, Value::BIGINT(UnsafeNumericCast<int64_t>(col_idx)));

			// file_offset, LogicalType::BIGINT
			current_chunk.SetValue(6, count, ParquetElementBigint(column.file_offset, row_group.__isset.file_offset));

			// num_values, LogicalType::BIGINT
			current_chunk.SetValue(7, count, Value::BIGINT(col_meta.num_values));

			// path_in_schema, LogicalType::VARCHAR
			current_chunk.SetValue(8, count, StringUtil::Join(col_meta.path_in_schema, ", "));

			// type, LogicalType::VARCHAR
			current_chunk.SetValue(9, count, ConvertParquetElementToString(col_meta.type));

			// stats_min, LogicalType::VARCHAR
			current_chunk.SetValue(10, count,
			                       ConvertParquetStats(column_type, schema_element, stats.__isset.min, stats.min));

			// stats_max, LogicalType::VARCHAR
			current_chunk.SetValue(11, count,
			                       ConvertParquetStats(column_type, schema_element, stats.__isset.max, stats.max));

			// stats_null_count, LogicalType::BIGINT
			current_chunk.SetValue(12, count, ParquetElementBigint(stats.null_count, stats.__isset.null_count));

			// stats_distinct_count, LogicalType::BIGINT
			current_chunk.SetValue(13, count, ParquetElementBigint(stats.distinct_count, stats.__isset.distinct_count));

			// stats_min_value, LogicalType::VARCHAR
			current_chunk.SetValue(
			    14, count, ConvertParquetStats(column_type, schema_element, stats.__isset.min_value, stats.min_value));

			// stats_max_value, LogicalType::VARCHAR
			current_chunk.SetValue(
			    15, count, ConvertParquetStats(column_type, schema_element, stats.__isset.max_value, stats.max_value));

			// compression, LogicalType::VARCHAR
			current_chunk.SetValue(16, count, ConvertParquetElementToString(col_meta.codec));

			// encodings, LogicalType::VARCHAR
			vector<string> encoding_string;
			encoding_string.reserve(col_meta.encodings.size());
			for (auto &encoding : col_meta.encodings) {
				encoding_string.push_back(ConvertParquetElementToString(encoding));
			}
			current_chunk.SetValue(17, count, Value(StringUtil::Join(encoding_string, ", ")));

			// index_page_offset, LogicalType::BIGINT
			current_chunk.SetValue(
			    18, count, ParquetElementBigint(col_meta.index_page_offset, col_meta.__isset.index_page_offset));

			// dictionary_page_offset, LogicalType::BIGINT
			current_chunk.SetValue(
			    19, count,
			    ParquetElementBigint(col_meta.dictionary_page_offset, col_meta.__isset.dictionary_page_offset));

			// data_page_offset, LogicalType::BIGINT
			current_chunk.SetValue(20, count, Value::BIGINT(col_meta.data_page_offset));

			// total_compressed_size, LogicalType::BIGINT
			current_chunk.SetValue(21, count, Value::BIGINT(col_meta.total_compressed_size));

			// total_uncompressed_size, LogicalType::BIGINT
			current_chunk.SetValue(22, count, Value::BIGINT(col_meta.total_uncompressed_size));

			// key_value_metadata, LogicalType::MAP(LogicalType::BLOB, LogicalType::BLOB)
			vector<Value> map_keys, map_values;
			for (auto &entry : col_meta.key_value_metadata) {
				map_keys.push_back(Value::BLOB_RAW(entry.key));
				map_values.push_back(Value::BLOB_RAW(entry.value));
			}
			current_chunk.SetValue(
			    23, count,
			    Value::MAP(LogicalType::BLOB, LogicalType::BLOB, std::move(map_keys), std::move(map_values)));

			// bloom_filter_offset, LogicalType::BIGINT
			current_chunk.SetValue(
			    24, count, ParquetElementBigint(col_meta.bloom_filter_offset, col_meta.__isset.bloom_filter_offset));

			// bloom_filter_length, LogicalType::BIGINT
			current_chunk.SetValue(
			    25, count, ParquetElementBigint(col_meta.bloom_filter_length, col_meta.__isset.bloom_filter_length));

			count++;
			if (count >= STANDARD_VECTOR_SIZE) {
				current_chunk.SetCardinality(count);
				collection.Append(current_chunk);

				count = 0;
				current_chunk.Reset();
			}
		}
	}
	current_chunk.SetCardinality(count);
	collection.Append(current_chunk);

	collection.InitializeScan(scan_state);
}

//===--------------------------------------------------------------------===//
// Schema Data
//===--------------------------------------------------------------------===//
void ParquetMetaDataOperatorData::BindSchema(vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("file_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("type_length");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("repetition_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("num_children");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("converted_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("scale");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("precision");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("field_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("logical_type");
	return_types.emplace_back(LogicalType::VARCHAR);
}

Value ParquetLogicalTypeToString(const duckdb_parquet::LogicalType &type, bool is_set) {
	if (!is_set) {
		return Value();
	}
	if (type.__isset.STRING) {
		return Value(PrintParquetElementToString(type.STRING));
	}
	if (type.__isset.MAP) {
		return Value(PrintParquetElementToString(type.MAP));
	}
	if (type.__isset.LIST) {
		return Value(PrintParquetElementToString(type.LIST));
	}
	if (type.__isset.ENUM) {
		return Value(PrintParquetElementToString(type.ENUM));
	}
	if (type.__isset.DECIMAL) {
		return Value(PrintParquetElementToString(type.DECIMAL));
	}
	if (type.__isset.DATE) {
		return Value(PrintParquetElementToString(type.DATE));
	}
	if (type.__isset.TIME) {
		return Value(PrintParquetElementToString(type.TIME));
	}
	if (type.__isset.TIMESTAMP) {
		return Value(PrintParquetElementToString(type.TIMESTAMP));
	}
	if (type.__isset.INTEGER) {
		return Value(PrintParquetElementToString(type.INTEGER));
	}
	if (type.__isset.UNKNOWN) {
		return Value(PrintParquetElementToString(type.UNKNOWN));
	}
	if (type.__isset.JSON) {
		return Value(PrintParquetElementToString(type.JSON));
	}
	if (type.__isset.BSON) {
		return Value(PrintParquetElementToString(type.BSON));
	}
	if (type.__isset.UUID) {
		return Value(PrintParquetElementToString(type.UUID));
	}
	return Value();
}

void ParquetMetaDataOperatorData::LoadSchemaData(ClientContext &context, const vector<LogicalType> &return_types,
                                                 const string &file_path) {
	collection.Reset();
	ParquetOptions parquet_options(context);
	auto reader = make_uniq<ParquetReader>(context, file_path, parquet_options);
	idx_t count = 0;
	DataChunk current_chunk;
	current_chunk.Initialize(context, return_types);
	auto meta_data = reader->GetFileMetadata();
	for (idx_t col_idx = 0; col_idx < meta_data->schema.size(); col_idx++) {
		auto &column = meta_data->schema[col_idx];

		// file_name, LogicalType::VARCHAR
		current_chunk.SetValue(0, count, file_path);

		// name, LogicalType::VARCHAR
		current_chunk.SetValue(1, count, column.name);

		// type, LogicalType::VARCHAR
		current_chunk.SetValue(2, count, ParquetElementString(column.type, column.__isset.type));

		// type_length, LogicalType::INTEGER
		current_chunk.SetValue(3, count, ParquetElementInteger(column.type_length, column.__isset.type_length));

		// repetition_type, LogicalType::VARCHAR
		current_chunk.SetValue(4, count, ParquetElementString(column.repetition_type, column.__isset.repetition_type));

		// num_children, LogicalType::BIGINT
		current_chunk.SetValue(5, count, ParquetElementBigint(column.num_children, column.__isset.num_children));

		// converted_type, LogicalType::VARCHAR
		current_chunk.SetValue(6, count, ParquetElementString(column.converted_type, column.__isset.converted_type));

		// scale, LogicalType::BIGINT
		current_chunk.SetValue(7, count, ParquetElementBigint(column.scale, column.__isset.scale));

		// precision, LogicalType::BIGINT
		current_chunk.SetValue(8, count, ParquetElementBigint(column.precision, column.__isset.precision));

		// field_id, LogicalType::BIGINT
		current_chunk.SetValue(9, count, ParquetElementBigint(column.field_id, column.__isset.field_id));

		// logical_type, LogicalType::VARCHAR
		current_chunk.SetValue(10, count, ParquetLogicalTypeToString(column.logicalType, column.__isset.logicalType));

		count++;
		if (count >= STANDARD_VECTOR_SIZE) {
			current_chunk.SetCardinality(count);
			collection.Append(current_chunk);

			count = 0;
			current_chunk.Reset();
		}
	}
	current_chunk.SetCardinality(count);
	collection.Append(current_chunk);

	collection.InitializeScan(scan_state);
}

//===--------------------------------------------------------------------===//
// KV Meta Data
//===--------------------------------------------------------------------===//
void ParquetMetaDataOperatorData::BindKeyValueMetaData(vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("file_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("key");
	return_types.emplace_back(LogicalType::BLOB);

	names.emplace_back("value");
	return_types.emplace_back(LogicalType::BLOB);
}

void ParquetMetaDataOperatorData::LoadKeyValueMetaData(ClientContext &context, const vector<LogicalType> &return_types,
                                                       const string &file_path) {
	collection.Reset();
	ParquetOptions parquet_options(context);
	auto reader = make_uniq<ParquetReader>(context, file_path, parquet_options);
	idx_t count = 0;
	DataChunk current_chunk;
	current_chunk.Initialize(context, return_types);
	auto meta_data = reader->GetFileMetadata();

	for (idx_t col_idx = 0; col_idx < meta_data->key_value_metadata.size(); col_idx++) {
		auto &entry = meta_data->key_value_metadata[col_idx];

		current_chunk.SetValue(0, count, Value(file_path));
		current_chunk.SetValue(1, count, Value::BLOB_RAW(entry.key));
		current_chunk.SetValue(2, count, Value::BLOB_RAW(entry.value));

		count++;
		if (count >= STANDARD_VECTOR_SIZE) {
			current_chunk.SetCardinality(count);
			collection.Append(current_chunk);

			count = 0;
			current_chunk.Reset();
		}
	}
	current_chunk.SetCardinality(count);
	collection.Append(current_chunk);
	collection.InitializeScan(scan_state);
}

//===--------------------------------------------------------------------===//
// File Meta Data
//===--------------------------------------------------------------------===//
void ParquetMetaDataOperatorData::BindFileMetaData(vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("file_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("created_by");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("num_rows");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("num_row_groups");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("format_version");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("encryption_algorithm");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("footer_signing_key_metadata");
	return_types.emplace_back(LogicalType::VARCHAR);
}

void ParquetMetaDataOperatorData::LoadFileMetaData(ClientContext &context, const vector<LogicalType> &return_types,
                                                   const string &file_path) {
	collection.Reset();
	ParquetOptions parquet_options(context);
	auto reader = make_uniq<ParquetReader>(context, file_path, parquet_options);
	DataChunk current_chunk;
	current_chunk.Initialize(context, return_types);
	auto meta_data = reader->GetFileMetadata();

	//	file_name
	current_chunk.SetValue(0, 0, Value(file_path));
	//	created_by
	current_chunk.SetValue(1, 0, ParquetElementStringVal(meta_data->created_by, meta_data->__isset.created_by));
	//	num_rows
	current_chunk.SetValue(2, 0, Value::BIGINT(meta_data->num_rows));
	//	num_row_groups
	current_chunk.SetValue(3, 0, Value::BIGINT(UnsafeNumericCast<int64_t>(meta_data->row_groups.size())));
	//	format_version
	current_chunk.SetValue(4, 0, Value::BIGINT(meta_data->version));
	//	encryption_algorithm
	current_chunk.SetValue(
	    5, 0, ParquetElementString(meta_data->encryption_algorithm, meta_data->__isset.encryption_algorithm));
	//	footer_signing_key_metadata
	current_chunk.SetValue(6, 0,
	                       ParquetElementStringVal(meta_data->footer_signing_key_metadata,
	                                               meta_data->__isset.footer_signing_key_metadata));
	current_chunk.SetCardinality(1);
	collection.Append(current_chunk);
	collection.InitializeScan(scan_state);
}

//===--------------------------------------------------------------------===//
// Bloom Probe
//===--------------------------------------------------------------------===//
void ParquetMetaDataOperatorData::BindBloomProbe(vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("file_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("row_group_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("bloom_filter_excludes");
	return_types.emplace_back(LogicalType::BOOLEAN);
}

void ParquetMetaDataOperatorData::ExecuteBloomProbe(ClientContext &context, const vector<LogicalType> &return_types,
                                                    const string &file_path, const string &column_name,
                                                    const Value &probe) {
	collection.Reset();
	ParquetOptions parquet_options(context);
	auto reader = make_uniq<ParquetReader>(context, file_path, parquet_options);
	idx_t count = 0;
	DataChunk current_chunk;
	current_chunk.Initialize(context, return_types);
	auto meta_data = reader->GetFileMetadata();

	optional_idx probe_column_idx;
	for (idx_t column_idx = 0; column_idx < reader->columns.size(); column_idx++) {
		if (reader->columns[column_idx].name == column_name) {
			probe_column_idx = column_idx;
		}
	}

	if (!probe_column_idx.IsValid()) {
		throw InvalidInputException("Column %s not found in %s", column_name, file_path);
	}

	auto &allocator = Allocator::DefaultAllocator();
	auto transport = std::make_shared<ThriftFileTransport>(allocator, reader->GetHandle(), false);
	auto protocol =
	    make_uniq<duckdb_apache::thrift::protocol::TCompactProtocolT<ThriftFileTransport>>(std::move(transport));

	D_ASSERT(!probe.IsNull());
	ConstantFilter filter(ExpressionType::COMPARE_EQUAL,
	                      probe.CastAs(context, reader->GetColumns()[probe_column_idx.GetIndex()].type));

	for (idx_t row_group_idx = 0; row_group_idx < meta_data->row_groups.size(); row_group_idx++) {
		auto &row_group = meta_data->row_groups[row_group_idx];
		auto &column = row_group.columns[probe_column_idx.GetIndex()];

		auto bloom_excludes =
		    ParquetStatisticsUtils::BloomFilterExcludes(filter, column.meta_data, *protocol, allocator);
		current_chunk.SetValue(0, count, Value(file_path));
		current_chunk.SetValue(1, count, Value::BIGINT(NumericCast<int64_t>(row_group_idx)));
		current_chunk.SetValue(2, count, Value::BOOLEAN(bloom_excludes));

		count++;
		if (count >= STANDARD_VECTOR_SIZE) {
			current_chunk.SetCardinality(count);
			collection.Append(current_chunk);

			count = 0;
			current_chunk.Reset();
		}
	}

	current_chunk.SetCardinality(count);
	collection.Append(current_chunk);
	collection.InitializeScan(scan_state);
}

//===--------------------------------------------------------------------===//
// Bind
//===--------------------------------------------------------------------===//
template <ParquetMetadataOperatorType TYPE>
unique_ptr<FunctionData> ParquetMetaDataBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ParquetMetaDataBindData>();

	switch (TYPE) {
	case ParquetMetadataOperatorType::SCHEMA:
		ParquetMetaDataOperatorData::BindSchema(return_types, names);
		break;
	case ParquetMetadataOperatorType::META_DATA:
		ParquetMetaDataOperatorData::BindMetaData(return_types, names);
		break;
	case ParquetMetadataOperatorType::KEY_VALUE_META_DATA:
		ParquetMetaDataOperatorData::BindKeyValueMetaData(return_types, names);
		break;
	case ParquetMetadataOperatorType::FILE_META_DATA:
		ParquetMetaDataOperatorData::BindFileMetaData(return_types, names);
		break;
	case ParquetMetadataOperatorType::BLOOM_PROBE: {
		auto probe_bind_data = make_uniq<ParquetBloomProbeBindData>();
		D_ASSERT(input.inputs.size() == 3);
		if (input.inputs[1].IsNull() || input.inputs[2].IsNull()) {
			throw InvalidInputException("Can't have NULL parameters for parquet_bloom_probe");
		}
		probe_bind_data->probe_column_name = input.inputs[1].CastAs(context, LogicalType::VARCHAR).GetValue<string>();
		probe_bind_data->probe_constant = input.inputs[2];
		result = std::move(probe_bind_data);
		ParquetMetaDataOperatorData::BindBloomProbe(return_types, names);
		break;
	}
	default:
		throw InternalException("Unsupported ParquetMetadataOperatorType");
	}

	result->return_types = return_types;
	result->multi_file_reader = MultiFileReader::Create(input.table_function);
	result->file_list = result->multi_file_reader->CreateFileList(context, input.inputs[0]);

	return std::move(result);
}

template <ParquetMetadataOperatorType TYPE>
unique_ptr<GlobalTableFunctionState> ParquetMetaDataInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ParquetMetaDataBindData>();

	auto result = make_uniq<ParquetMetaDataOperatorData>(context, bind_data.return_types);

	bind_data.file_list->InitializeScan(result->file_list_scan);
	bind_data.file_list->Scan(result->file_list_scan, result->current_file);

	D_ASSERT(!bind_data.file_list->IsEmpty());

	switch (TYPE) {
	case ParquetMetadataOperatorType::SCHEMA:
		result->LoadSchemaData(context, bind_data.return_types, bind_data.file_list->GetFirstFile());
		break;
	case ParquetMetadataOperatorType::META_DATA:
		result->LoadRowGroupMetadata(context, bind_data.return_types, bind_data.file_list->GetFirstFile());
		break;
	case ParquetMetadataOperatorType::KEY_VALUE_META_DATA:
		result->LoadKeyValueMetaData(context, bind_data.return_types, bind_data.file_list->GetFirstFile());
		break;
	case ParquetMetadataOperatorType::FILE_META_DATA:
		result->LoadFileMetaData(context, bind_data.return_types, bind_data.file_list->GetFirstFile());
		break;
	case ParquetMetadataOperatorType::BLOOM_PROBE: {
		auto &bloom_probe_bind_data = input.bind_data->Cast<ParquetBloomProbeBindData>();
		result->ExecuteBloomProbe(context, bind_data.return_types, bind_data.file_list->GetFirstFile(),
		                          bloom_probe_bind_data.probe_column_name, bloom_probe_bind_data.probe_constant);
		break;
	}
	default:
		throw InternalException("Unsupported ParquetMetadataOperatorType");
	}

	return std::move(result);
}

template <ParquetMetadataOperatorType TYPE>
void ParquetMetaDataImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<ParquetMetaDataOperatorData>();
	auto &bind_data = data_p.bind_data->Cast<ParquetMetaDataBindData>();

	while (true) {
		if (!data.collection.Scan(data.scan_state, output)) {

			// Try get next file
			if (!bind_data.file_list->Scan(data.file_list_scan, data.current_file)) {
				return;
			}

			switch (TYPE) {
			case ParquetMetadataOperatorType::SCHEMA:
				data.LoadSchemaData(context, bind_data.return_types, data.current_file);
				break;
			case ParquetMetadataOperatorType::META_DATA:
				data.LoadRowGroupMetadata(context, bind_data.return_types, data.current_file);
				break;
			case ParquetMetadataOperatorType::KEY_VALUE_META_DATA:
				data.LoadKeyValueMetaData(context, bind_data.return_types, data.current_file);
				break;
			case ParquetMetadataOperatorType::FILE_META_DATA:
				data.LoadFileMetaData(context, bind_data.return_types, data.current_file);
				break;
			case ParquetMetadataOperatorType::BLOOM_PROBE: {
				auto &bloom_probe_bind_data = data_p.bind_data->Cast<ParquetBloomProbeBindData>();
				data.ExecuteBloomProbe(context, bind_data.return_types, bind_data.file_list->GetFirstFile(),
				                       bloom_probe_bind_data.probe_column_name, bloom_probe_bind_data.probe_constant);
				break;
			}
			default:
				throw InternalException("Unsupported ParquetMetadataOperatorType");
			}
			continue;
		}
		if (output.size() != 0) {
			return;
		}
	}
}

ParquetMetaDataFunction::ParquetMetaDataFunction()
    : TableFunction("parquet_metadata", {LogicalType::VARCHAR},
                    ParquetMetaDataImplementation<ParquetMetadataOperatorType::META_DATA>,
                    ParquetMetaDataBind<ParquetMetadataOperatorType::META_DATA>,
                    ParquetMetaDataInit<ParquetMetadataOperatorType::META_DATA>) {
}

ParquetSchemaFunction::ParquetSchemaFunction()
    : TableFunction("parquet_schema", {LogicalType::VARCHAR},
                    ParquetMetaDataImplementation<ParquetMetadataOperatorType::SCHEMA>,
                    ParquetMetaDataBind<ParquetMetadataOperatorType::SCHEMA>,
                    ParquetMetaDataInit<ParquetMetadataOperatorType::SCHEMA>) {
}

ParquetKeyValueMetadataFunction::ParquetKeyValueMetadataFunction()
    : TableFunction("parquet_kv_metadata", {LogicalType::VARCHAR},
                    ParquetMetaDataImplementation<ParquetMetadataOperatorType::KEY_VALUE_META_DATA>,
                    ParquetMetaDataBind<ParquetMetadataOperatorType::KEY_VALUE_META_DATA>,
                    ParquetMetaDataInit<ParquetMetadataOperatorType::KEY_VALUE_META_DATA>) {
}

ParquetFileMetadataFunction::ParquetFileMetadataFunction()
    : TableFunction("parquet_file_metadata", {LogicalType::VARCHAR},
                    ParquetMetaDataImplementation<ParquetMetadataOperatorType::FILE_META_DATA>,
                    ParquetMetaDataBind<ParquetMetadataOperatorType::FILE_META_DATA>,
                    ParquetMetaDataInit<ParquetMetadataOperatorType::FILE_META_DATA>) {
}

ParquetBloomProbeFunction::ParquetBloomProbeFunction()
    : TableFunction("parquet_bloom_probe", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::ANY},
                    ParquetMetaDataImplementation<ParquetMetadataOperatorType::BLOOM_PROBE>,
                    ParquetMetaDataBind<ParquetMetadataOperatorType::BLOOM_PROBE>,
                    ParquetMetaDataInit<ParquetMetadataOperatorType::BLOOM_PROBE>) {
}

} // namespace duckdb

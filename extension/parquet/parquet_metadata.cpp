#include "parquet_metadata.hpp"

#include "parquet_statistics.hpp"

#include <sstream>

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"

namespace duckdb {

struct ParquetMetaDataBindData : public TableFunctionData {
	vector<string> file_paths;
	vector<LogicalType> return_types;
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

class ParquetMetadataFileProcessor {
public:
	ParquetMetadataFileProcessor(ClientContext &context, const string &file_path, ParquetMetadataOperatorType op_type);
	virtual ~ParquetMetadataFileProcessor() = default;

	virtual idx_t ReadChunk(ClientContext &context, DataChunk &output, idx_t output_offset, idx_t max_rows) = 0;
	virtual bool IsExhausted() const {
		return exhausted;
	}

protected:
	string file_path;
	ParquetMetadataOperatorType operation_type;
	unique_ptr<ParquetReader> reader;
	ClientContext *context_ptr = nullptr;
	bool initialized = false;
	bool exhausted = false;

	virtual void InitializeReader(ClientContext &context);
};

struct ParquetMetaDataBindData;

template <ParquetMetadataOperatorType OP_TYPE>
class ParquetMetaDataOperator {
public:
	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names);
	static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context, TableFunctionInitInput &input);
	static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext &context, TableFunctionInitInput &input,
	                                                     GlobalTableFunctionState *global_state);
	static void Function(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
	static double Progress(ClientContext &context, const FunctionData *bind_data_p,
	                       const GlobalTableFunctionState *global_state);

	static void BindSchema(vector<LogicalType> &return_types, vector<string> &names);

private:
	static unique_ptr<ParquetMetadataFileProcessor> CreateProcessor(ClientContext &context, const string &file_path,
	                                                                const ParquetMetaDataBindData &bind_data);
};

struct ParquetMetadataGlobalState : public GlobalTableFunctionState {
	ParquetMetadataGlobalState(vector<string> file_paths_p, ClientContext &context)
	    : file_paths(std::move(file_paths_p)), current_file_idx(0) {
		idx_t system_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
		max_threads = MinValue<idx_t>(file_paths.size(), system_threads);
	}

	idx_t MaxThreads() const override {
		return max_threads;
	}

	optional_idx GetNextFileIndex() {
		idx_t file_idx = current_file_idx.fetch_add(1);
		if (file_idx >= file_paths.size()) {
			return {};
		}
		return file_idx;
	}

	double GetProgress() const {
		// Not the most accurate, instantly assumes all files are done and equal
		return static_cast<double>(current_file_idx.load()) / file_paths.size();
	}

	vector<string> file_paths;
	idx_t max_threads;
	atomic<idx_t> current_file_idx;
};

struct ParquetMetadataLocalState : public LocalTableFunctionState {
	ParquetMetadataLocalState() : current_file_idx(DConstants::INVALID_INDEX) {
	}

	idx_t current_file_idx;
	unique_ptr<ParquetMetadataFileProcessor> processor;
	bool file_exhausted = true;
};

template <class T>
static string ConvertParquetElementToString(T &&entry) {
	duckdb::stringstream ss;
	ss << entry;
	return ss.str();
}

template <class T>
static string PrintParquetElementToString(T &&entry) {
	duckdb::stringstream ss;
	entry.printTo(ss);
	return ss.str();
}

template <class T>
static Value ParquetElementString(T &&value, bool is_set) {
	if (!is_set) {
		return Value();
	}
	return Value(ConvertParquetElementToString(value));
}

static Value ParquetElementStringVal(const string &value, bool is_set) {
	if (!is_set) {
		return Value();
	}
	return Value(value);
}

template <class T>
static Value ParquetElementInteger(T &&value, bool is_iset) {
	if (!is_iset) {
		return Value();
	}
	return Value::INTEGER(value);
}

template <class T>
static Value ParquetElementBigint(T &&value, bool is_iset) {
	if (!is_iset) {
		return Value();
	}
	return Value::BIGINT(value);
}

static Value ParquetElementBoolean(bool value, bool is_iset) {
	if (!is_iset) {
		return Value();
	}
	return Value::BOOLEAN(value);
}

ParquetMetadataFileProcessor::ParquetMetadataFileProcessor(ClientContext &context, const string &file_path_p,
                                                           ParquetMetadataOperatorType op_type)
    : file_path(file_path_p), operation_type(op_type) {
}

void ParquetMetadataFileProcessor::InitializeReader(ClientContext &context) {
	if (!initialized) {
		ParquetOptions parquet_options(context);
		reader = make_uniq<ParquetReader>(context, file_path, parquet_options);
		initialized = true;
	}
}

//===--------------------------------------------------------------------===//
// Row Group Meta Data
//===--------------------------------------------------------------------===//

class ParquetRowGroupMetadataProcessor : public ParquetMetadataFileProcessor {
public:
	ParquetRowGroupMetadataProcessor(ClientContext &context, const string &file_path);

	idx_t ReadChunk(ClientContext &context, DataChunk &output, idx_t output_offset, idx_t max_rows) override;

protected:
	void InitializeReader(ClientContext &context) override;

private:
	idx_t current_row_group = 0;
	idx_t current_column = 0;
	vector<ParquetColumnSchema> column_schemas;

	void ProcessRowGroupColumn(DataChunk &output, idx_t output_idx, idx_t row_group_idx, idx_t col_idx);
};

template <>
void ParquetMetaDataOperator<ParquetMetadataOperatorType::META_DATA>::BindSchema(vector<LogicalType> &return_types,
                                                                                 vector<string> &names) {
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

	names.emplace_back("min_is_exact");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("max_is_exact");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("row_group_compressed_bytes");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("geo_bbox");
	return_types.emplace_back(LogicalType::STRUCT({
	    {"xmin", LogicalType::DOUBLE},
	    {"xmax", LogicalType::DOUBLE},
	    {"ymin", LogicalType::DOUBLE},
	    {"ymax", LogicalType::DOUBLE},
	    {"zmin", LogicalType::DOUBLE},
	    {"zmax", LogicalType::DOUBLE},
	    {"mmin", LogicalType::DOUBLE},
	    {"mmax", LogicalType::DOUBLE},
	}));

	names.emplace_back("geo_types");
	return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));
}

static Value ConvertParquetStats(const LogicalType &type, const ParquetColumnSchema &schema_ele, bool stats_is_set,
                                 const std::string &stats) {
	if (!stats_is_set) {
		return Value(LogicalType::VARCHAR);
	}
	return ParquetStatisticsUtils::ConvertValue(type, schema_ele, stats).DefaultCastAs(LogicalType::VARCHAR);
}

static Value ConvertParquetGeoStatsBBOX(const duckdb_parquet::GeospatialStatistics &stats) {
	if (!stats.__isset.bbox) {
		return Value(LogicalType::STRUCT({
		    {"xmin", LogicalType::DOUBLE},
		    {"xmax", LogicalType::DOUBLE},
		    {"ymin", LogicalType::DOUBLE},
		    {"ymax", LogicalType::DOUBLE},
		    {"zmin", LogicalType::DOUBLE},
		    {"zmax", LogicalType::DOUBLE},
		    {"mmin", LogicalType::DOUBLE},
		    {"mmax", LogicalType::DOUBLE},
		}));
	}

	return Value::STRUCT({
	    {"xmin", Value::DOUBLE(stats.bbox.xmin)},
	    {"xmax", Value::DOUBLE(stats.bbox.xmax)},
	    {"ymin", Value::DOUBLE(stats.bbox.ymin)},
	    {"ymax", Value::DOUBLE(stats.bbox.ymax)},
	    {"zmin", stats.bbox.__isset.zmin ? Value::DOUBLE(stats.bbox.zmin) : Value(LogicalTypeId::DOUBLE)},
	    {"zmax", stats.bbox.__isset.zmax ? Value::DOUBLE(stats.bbox.zmax) : Value(LogicalTypeId::DOUBLE)},
	    {"mmin", stats.bbox.__isset.mmin ? Value::DOUBLE(stats.bbox.mmin) : Value(LogicalTypeId::DOUBLE)},
	    {"mmax", stats.bbox.__isset.mmax ? Value::DOUBLE(stats.bbox.mmax) : Value(LogicalTypeId::DOUBLE)},
	});
}

static Value ConvertParquetGeoStatsTypes(const duckdb_parquet::GeospatialStatistics &stats) {
	if (!stats.__isset.geospatial_types) {
		return Value(LogicalType::LIST(LogicalType::VARCHAR));
	}
	vector<Value> types;
	types.reserve(stats.geospatial_types.size());

	GeometryKindSet kind_set;
	for (auto &type : stats.geospatial_types) {
		kind_set.Add(type);
	}
	for (auto &type_name : kind_set.ToString(true)) {
		types.push_back(Value(type_name));
	}
	return Value::LIST(LogicalType::VARCHAR, types);
}

ParquetRowGroupMetadataProcessor::ParquetRowGroupMetadataProcessor(ClientContext &context, const string &file_path)
    : ParquetMetadataFileProcessor(context, file_path, ParquetMetadataOperatorType::META_DATA) {
}

void ParquetRowGroupMetadataProcessor::InitializeReader(ClientContext &context) {
	bool was_initialized = initialized;
	ParquetMetadataFileProcessor::InitializeReader(context);

	if (!was_initialized) {
		auto meta_data = reader->GetFileMetadata();
		for (idx_t schema_idx = 0; schema_idx < meta_data->schema.size(); schema_idx++) {
			auto &schema_element = meta_data->schema[schema_idx];
			if (schema_element.num_children > 0) {
				continue;
			}
			ParquetColumnSchema column_schema;
			column_schema.type = reader->DeriveLogicalType(schema_element, column_schema);
			column_schemas.push_back(std::move(column_schema));
		}
	}
}

idx_t ParquetRowGroupMetadataProcessor::ReadChunk(ClientContext &context, DataChunk &output, idx_t output_offset,
                                                  idx_t max_rows) {
	InitializeReader(context);

	auto meta_data = reader->GetFileMetadata();

	idx_t rows_written = 0;

	while (rows_written < max_rows && !exhausted) {
		if (current_row_group >= meta_data->row_groups.size()) {
			exhausted = true;
			break;
		}

		auto &row_group = meta_data->row_groups[current_row_group];
		if (current_column >= row_group.columns.size()) {
			current_row_group++;
			current_column = 0;
			continue;
		}

		ProcessRowGroupColumn(output, output_offset + rows_written, current_row_group, current_column);
		rows_written++;
		current_column++;
	}

	return rows_written;
}

void ParquetRowGroupMetadataProcessor::ProcessRowGroupColumn(DataChunk &output, idx_t output_idx, idx_t row_group_idx,
                                                             idx_t col_idx) {
	auto meta_data = reader->GetFileMetadata();
	auto &row_group = meta_data->row_groups[row_group_idx];

	auto &column = row_group.columns[col_idx];
	auto &column_schema = column_schemas[col_idx];
	auto &col_meta = column.meta_data;
	auto &stats = col_meta.statistics;
	auto &column_type = column_schema.type;

	// file_name
	output.SetValue(0, output_idx, file_path);
	// row_group_id
	output.SetValue(1, output_idx, Value::BIGINT(UnsafeNumericCast<int64_t>(row_group_idx)));
	// row_group_num_rows
	output.SetValue(2, output_idx, Value::BIGINT(row_group.num_rows));
	// row_group_num_columns
	output.SetValue(3, output_idx, Value::BIGINT(UnsafeNumericCast<int64_t>(row_group.columns.size())));
	// row_group_bytes
	output.SetValue(4, output_idx, Value::BIGINT(row_group.total_byte_size));
	// column_id
	output.SetValue(5, output_idx, Value::BIGINT(UnsafeNumericCast<int64_t>(col_idx)));
	// file_offset
	output.SetValue(6, output_idx, ParquetElementBigint(column.file_offset, row_group.__isset.file_offset));
	// num_values
	output.SetValue(7, output_idx, Value::BIGINT(col_meta.num_values));
	// path_in_schema
	output.SetValue(8, output_idx, StringUtil::Join(col_meta.path_in_schema, ", "));
	// type
	output.SetValue(9, output_idx, ConvertParquetElementToString(col_meta.type));
	// stats_min
	output.SetValue(10, output_idx, ConvertParquetStats(column_type, column_schema, stats.__isset.min, stats.min));
	// stats_max
	output.SetValue(11, output_idx, ConvertParquetStats(column_type, column_schema, stats.__isset.max, stats.max));
	// stats_null_count
	output.SetValue(12, output_idx, ParquetElementBigint(stats.null_count, stats.__isset.null_count));
	// stats_distinct_count
	output.SetValue(13, output_idx, ParquetElementBigint(stats.distinct_count, stats.__isset.distinct_count));
	// stats_min_value
	output.SetValue(14, output_idx,
	                ConvertParquetStats(column_type, column_schema, stats.__isset.min_value, stats.min_value));
	// stats_max_value
	output.SetValue(15, output_idx,
	                ConvertParquetStats(column_type, column_schema, stats.__isset.max_value, stats.max_value));
	// compression
	output.SetValue(16, output_idx, ConvertParquetElementToString(col_meta.codec));
	// encodings
	vector<string> encoding_string;
	encoding_string.reserve(col_meta.encodings.size());
	for (auto &encoding : col_meta.encodings) {
		encoding_string.push_back(ConvertParquetElementToString(encoding));
	}
	output.SetValue(17, output_idx, Value(StringUtil::Join(encoding_string, ", ")));
	// index_page_offset
	output.SetValue(18, output_idx,
	                ParquetElementBigint(col_meta.index_page_offset, col_meta.__isset.index_page_offset));
	// dictionary_page_offset
	output.SetValue(19, output_idx,
	                ParquetElementBigint(col_meta.dictionary_page_offset, col_meta.__isset.dictionary_page_offset));
	// data_page_offset
	output.SetValue(20, output_idx, Value::BIGINT(col_meta.data_page_offset));
	// total_compressed_size
	output.SetValue(21, output_idx, Value::BIGINT(col_meta.total_compressed_size));
	// total_uncompressed_size
	output.SetValue(22, output_idx, Value::BIGINT(col_meta.total_uncompressed_size));
	// key_value_metadata
	vector<Value> map_keys, map_values;
	for (auto &entry : col_meta.key_value_metadata) {
		map_keys.push_back(Value::BLOB_RAW(entry.key));
		map_values.push_back(Value::BLOB_RAW(entry.value));
	}
	output.SetValue(23, output_idx,
	                Value::MAP(LogicalType::BLOB, LogicalType::BLOB, std::move(map_keys), std::move(map_values)));
	// bloom_filter_offset
	output.SetValue(24, output_idx,
	                ParquetElementBigint(col_meta.bloom_filter_offset, col_meta.__isset.bloom_filter_offset));
	// bloom_filter_length
	output.SetValue(25, output_idx,
	                ParquetElementBigint(col_meta.bloom_filter_length, col_meta.__isset.bloom_filter_length));
	// min_is_exact
	output.SetValue(26, output_idx, ParquetElementBoolean(stats.is_min_value_exact, stats.__isset.is_min_value_exact));
	// max_is_exact
	output.SetValue(27, output_idx, ParquetElementBoolean(stats.is_max_value_exact, stats.__isset.is_max_value_exact));
	// row_group_compressed_bytes
	output.SetValue(28, output_idx,
	                ParquetElementBigint(row_group.total_compressed_size, row_group.__isset.total_compressed_size));
	// geo_stats_bbox, LogicalType::STRUCT(...)
	output.SetValue(29, output_idx, ConvertParquetGeoStatsBBOX(col_meta.geospatial_statistics));

	// geo_stats_types, LogicalType::LIST(LogicalType::VARCHAR)
	output.SetValue(30, output_idx, ConvertParquetGeoStatsTypes(col_meta.geospatial_statistics));
}

//===--------------------------------------------------------------------===//
// Schema Data
//===--------------------------------------------------------------------===//

class ParquetSchemaProcessor : public ParquetMetadataFileProcessor {
public:
	ParquetSchemaProcessor(ClientContext &context, const string &file_path);

	idx_t ReadChunk(ClientContext &context, DataChunk &output, idx_t output_offset, idx_t max_rows) override;

private:
	idx_t current_schema_element = 0;

	void ProcessSchemaElement(DataChunk &output, idx_t output_idx, idx_t schema_idx);
};

template <>
void ParquetMetaDataOperator<ParquetMetadataOperatorType::SCHEMA>::BindSchema(vector<LogicalType> &return_types,
                                                                              vector<string> &names) {
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

	names.emplace_back("duckdb_type");
	return_types.emplace_back(LogicalType::VARCHAR);
}

ParquetSchemaProcessor::ParquetSchemaProcessor(ClientContext &context, const string &file_path)
    : ParquetMetadataFileProcessor(context, file_path, ParquetMetadataOperatorType::SCHEMA) {
}

idx_t ParquetSchemaProcessor::ReadChunk(ClientContext &context, DataChunk &output, idx_t output_offset,
                                        idx_t max_rows) {
	InitializeReader(context);

	auto meta_data = reader->GetFileMetadata();
	idx_t rows_written = 0;

	while (rows_written < max_rows && !exhausted) {
		if (current_schema_element >= meta_data->schema.size()) {
			exhausted = true;
			break;
		}

		ProcessSchemaElement(output, output_offset + rows_written, current_schema_element);
		rows_written++;
		current_schema_element++;
	}

	return rows_written;
}

static Value ParquetLogicalTypeToString(const duckdb_parquet::LogicalType &type, bool is_set) {
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
	if (type.__isset.FLOAT16) {
		return Value(PrintParquetElementToString(type.FLOAT16));
	}
	if (type.__isset.GEOMETRY) {
		return Value(PrintParquetElementToString(type.GEOMETRY));
	}
	if (type.__isset.GEOGRAPHY) {
		return Value(PrintParquetElementToString(type.GEOGRAPHY));
	}
	return Value();
}

void ParquetSchemaProcessor::ProcessSchemaElement(DataChunk &output, idx_t output_idx, idx_t schema_idx) {
	auto meta_data = reader->GetFileMetadata();
	auto &column = meta_data->schema[schema_idx];

	// file_name
	output.SetValue(0, output_idx, file_path);
	// name
	output.SetValue(1, output_idx, column.name);
	// type
	output.SetValue(2, output_idx, ParquetElementString(column.type, column.__isset.type));
	// type_length
	output.SetValue(3, output_idx, ParquetElementInteger(column.type_length, column.__isset.type_length));
	// repetition_type
	output.SetValue(4, output_idx, ParquetElementString(column.repetition_type, column.__isset.repetition_type));
	// num_children
	output.SetValue(5, output_idx, ParquetElementBigint(column.num_children, column.__isset.num_children));
	// converted_type
	output.SetValue(6, output_idx, ParquetElementString(column.converted_type, column.__isset.converted_type));
	// scale
	output.SetValue(7, output_idx, ParquetElementBigint(column.scale, column.__isset.scale));
	// precision
	output.SetValue(8, output_idx, ParquetElementBigint(column.precision, column.__isset.precision));
	// field_id
	output.SetValue(9, output_idx, ParquetElementBigint(column.field_id, column.__isset.field_id));
	// logical_type
	output.SetValue(10, output_idx, ParquetLogicalTypeToString(column.logicalType, column.__isset.logicalType));
	// duckdb_type
	ParquetColumnSchema column_schema;
	Value duckdb_type;
	if (column.__isset.type) {
		duckdb_type = reader->DeriveLogicalType(column, column_schema).ToString();
	}
	output.SetValue(11, output_idx, duckdb_type);
}

//===--------------------------------------------------------------------===//
// KV Meta Data
//===--------------------------------------------------------------------===//

class ParquetKeyValueMetadataProcessor : public ParquetMetadataFileProcessor {
public:
	ParquetKeyValueMetadataProcessor(ClientContext &context, const string &file_path);

	idx_t ReadChunk(ClientContext &context, DataChunk &output, idx_t output_offset, idx_t max_rows) override;

private:
	idx_t current_kv_entry = 0;

	void ProcessKeyValueEntry(DataChunk &output, idx_t output_idx, idx_t kv_idx);
};

template <>
void ParquetMetaDataOperator<ParquetMetadataOperatorType::KEY_VALUE_META_DATA>::BindSchema(
    vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("file_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("key");
	return_types.emplace_back(LogicalType::BLOB);

	names.emplace_back("value");
	return_types.emplace_back(LogicalType::BLOB);
}

ParquetKeyValueMetadataProcessor::ParquetKeyValueMetadataProcessor(ClientContext &context, const string &file_path)
    : ParquetMetadataFileProcessor(context, file_path, ParquetMetadataOperatorType::KEY_VALUE_META_DATA) {
}

idx_t ParquetKeyValueMetadataProcessor::ReadChunk(ClientContext &context, DataChunk &output, idx_t output_offset,
                                                  idx_t max_rows) {
	InitializeReader(context);

	auto meta_data = reader->GetFileMetadata();
	idx_t rows_written = 0;

	while (rows_written < max_rows && !exhausted) {
		if (current_kv_entry >= meta_data->key_value_metadata.size()) {
			exhausted = true;
			break;
		}

		ProcessKeyValueEntry(output, output_offset + rows_written, current_kv_entry);
		rows_written++;
		current_kv_entry++;
	}

	return rows_written;
}

void ParquetKeyValueMetadataProcessor::ProcessKeyValueEntry(DataChunk &output, idx_t output_idx, idx_t kv_idx) {
	auto meta_data = reader->GetFileMetadata();
	auto &entry = meta_data->key_value_metadata[kv_idx];

	output.SetValue(0, output_idx, Value(file_path));
	output.SetValue(1, output_idx, Value::BLOB_RAW(entry.key));
	output.SetValue(2, output_idx, Value::BLOB_RAW(entry.value));
}

//===--------------------------------------------------------------------===//
// File Meta Data
//===--------------------------------------------------------------------===//

class ParquetFileMetadataProcessor : public ParquetMetadataFileProcessor {
public:
	ParquetFileMetadataProcessor(ClientContext &context, const string &file_path);

	idx_t ReadChunk(ClientContext &context, DataChunk &output, idx_t output_offset, idx_t max_rows) override;

private:
	bool processed = false;

	void ProcessFileMetadata(DataChunk &output, idx_t output_idx);
};

template <>
void ParquetMetaDataOperator<ParquetMetadataOperatorType::FILE_META_DATA>::BindSchema(vector<LogicalType> &return_types,
                                                                                      vector<string> &names) {
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

	names.emplace_back("file_size_bytes");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("footer_size");
	return_types.emplace_back(LogicalType::UBIGINT);
}

ParquetFileMetadataProcessor::ParquetFileMetadataProcessor(ClientContext &context, const string &file_path)
    : ParquetMetadataFileProcessor(context, file_path, ParquetMetadataOperatorType::FILE_META_DATA) {
}

idx_t ParquetFileMetadataProcessor::ReadChunk(ClientContext &context, DataChunk &output, idx_t output_offset,
                                              idx_t max_rows) {
	if (processed || max_rows == 0) {
		return 0;
	}

	InitializeReader(context);
	ProcessFileMetadata(output, output_offset);
	processed = true;
	return 1;
}

void ParquetFileMetadataProcessor::ProcessFileMetadata(DataChunk &output, idx_t output_idx) {
	auto meta_data = reader->GetFileMetadata();

	// file_name
	output.SetValue(0, output_idx, Value(file_path));
	// created_by
	output.SetValue(1, output_idx, ParquetElementStringVal(meta_data->created_by, meta_data->__isset.created_by));
	// num_rows
	output.SetValue(2, output_idx, Value::BIGINT(meta_data->num_rows));
	// num_row_groups
	output.SetValue(3, output_idx, Value::BIGINT(UnsafeNumericCast<int64_t>(meta_data->row_groups.size())));
	// format_version
	output.SetValue(4, output_idx, Value::BIGINT(meta_data->version));
	// encryption_algorithm
	output.SetValue(5, output_idx,
	                ParquetElementString(meta_data->encryption_algorithm, meta_data->__isset.encryption_algorithm));
	// footer_signing_key_metadata
	output.SetValue(6, output_idx,
	                ParquetElementStringVal(meta_data->footer_signing_key_metadata,
	                                        meta_data->__isset.footer_signing_key_metadata));
	// file_size_bytes
	output.SetValue(7, output_idx, Value::UBIGINT(reader->GetHandle().GetFileSize()));
	// footer_size
	output.SetValue(8, output_idx, Value::UBIGINT(reader->metadata->footer_size));
}

//===--------------------------------------------------------------------===//
// Bloom Probe
//===--------------------------------------------------------------------===//

class ParquetBloomProbeProcessor : public ParquetMetadataFileProcessor {
public:
	ParquetBloomProbeProcessor(ClientContext &context, const string &file_path, const string &probe_column,
	                           const Value &probe_value);

	idx_t ReadChunk(ClientContext &context, DataChunk &output, idx_t output_offset, idx_t max_rows) override;

private:
	string probe_column_name;
	Value probe_constant;
	idx_t current_row_group = 0;
	optional_idx probe_column_idx;

	void ProcessRowGroupBloomProbe(ClientContext &context, DataChunk &output, idx_t output_idx, idx_t row_group_idx);
	void InitializeProbeColumn();
};

template <>
void ParquetMetaDataOperator<ParquetMetadataOperatorType::BLOOM_PROBE>::BindSchema(vector<LogicalType> &return_types,
                                                                                   vector<string> &names) {
	names.emplace_back("file_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("row_group_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("bloom_filter_excludes");
	return_types.emplace_back(LogicalType::BOOLEAN);
}

ParquetBloomProbeProcessor::ParquetBloomProbeProcessor(ClientContext &context, const string &file_path,
                                                       const string &probe_column, const Value &probe_value)
    : ParquetMetadataFileProcessor(context, file_path, ParquetMetadataOperatorType::BLOOM_PROBE),
      probe_column_name(probe_column), probe_constant(probe_value) {
}

idx_t ParquetBloomProbeProcessor::ReadChunk(ClientContext &context, DataChunk &output, idx_t output_offset,
                                            idx_t max_rows) {
	InitializeReader(context);

	if (!probe_column_idx.IsValid()) {
		InitializeProbeColumn();
	}

	auto meta_data = reader->GetFileMetadata();
	idx_t rows_written = 0;

	while (rows_written < max_rows && !exhausted) {
		if (current_row_group >= meta_data->row_groups.size()) {
			exhausted = true;
			break;
		}

		ProcessRowGroupBloomProbe(context, output, output_offset + rows_written, current_row_group);
		rows_written++;
		current_row_group++;
	}

	return rows_written;
}

void ParquetBloomProbeProcessor::InitializeProbeColumn() {
	for (idx_t column_idx = 0; column_idx < reader->columns.size(); column_idx++) {
		if (reader->columns[column_idx].name == probe_column_name) {
			probe_column_idx = column_idx;
			break;
		}
	}

	if (!probe_column_idx.IsValid()) {
		throw InvalidInputException("Column %s not found in %s", probe_column_name, file_path);
	}
}

void ParquetBloomProbeProcessor::ProcessRowGroupBloomProbe(ClientContext &context, DataChunk &output, idx_t output_idx,
                                                           idx_t row_group_idx) {
	auto meta_data = reader->GetFileMetadata();
	auto &row_group = meta_data->row_groups[row_group_idx];
	auto &column = row_group.columns[probe_column_idx.GetIndex()];

	auto &allocator = BufferAllocator::Get(context);
	auto transport = duckdb_base_std::make_shared<ThriftFileTransport>(reader->GetHandle(), false);
	auto protocol =
	    make_uniq<duckdb_apache::thrift::protocol::TCompactProtocolT<ThriftFileTransport>>(std::move(transport));

	D_ASSERT(!probe_constant.IsNull());
	ConstantFilter filter(ExpressionType::COMPARE_EQUAL,
	                      probe_constant.CastAs(context, reader->GetColumns()[probe_column_idx.GetIndex()].type));

	auto bloom_excludes = ParquetStatisticsUtils::BloomFilterExcludes(filter, column.meta_data, *protocol, allocator);

	output.SetValue(0, output_idx, Value(file_path));
	output.SetValue(1, output_idx, Value::BIGINT(NumericCast<int64_t>(row_group_idx)));
	output.SetValue(2, output_idx, Value::BOOLEAN(bloom_excludes));
}

//===--------------------------------------------------------------------===//
// Template Function Implementation
//===--------------------------------------------------------------------===//

template <ParquetMetadataOperatorType OP_TYPE>
unique_ptr<FunctionData> ParquetMetaDataOperator<OP_TYPE>::Bind(ClientContext &context, TableFunctionBindInput &input,
                                                                vector<LogicalType> &return_types,
                                                                vector<string> &names) {
	// Extract file paths from input using MultiFileReader (handles both single files and arrays)
	auto multi_file_reader = MultiFileReader::CreateDefault("ParquetMetadata");
	auto glob_input = FileGlobInput(FileGlobOptions::FALLBACK_GLOB, "parquet");
	auto file_list = multi_file_reader->CreateFileList(context, input.inputs[0], glob_input);

	vector<string> file_paths;
	MultiFileListScanData scan_data;
	file_list->InitializeScan(scan_data);

	OpenFileInfo current_file;
	while (file_list->Scan(scan_data, current_file)) {
		file_paths.push_back(current_file.path);
	}

	D_ASSERT(!file_paths.empty());

	auto result = make_uniq<ParquetMetaDataBindData>();

	// Bind schema based on operation type
	if (OP_TYPE == ParquetMetadataOperatorType::BLOOM_PROBE) {
		auto probe_bind_data = make_uniq<ParquetBloomProbeBindData>();
		D_ASSERT(input.inputs.size() == 3);
		if (input.inputs[1].IsNull() || input.inputs[2].IsNull()) {
			throw InvalidInputException("Can't have NULL parameters for parquet_bloom_probe");
		}
		probe_bind_data->probe_column_name = input.inputs[1].CastAs(context, LogicalType::VARCHAR).GetValue<string>();
		probe_bind_data->probe_constant = input.inputs[2];
		result = std::move(probe_bind_data);
	}

	BindSchema(return_types, names);
	result->file_paths = std::move(file_paths);
	result->return_types = return_types;

	return std::move(result);
}

template <ParquetMetadataOperatorType OP_TYPE>
unique_ptr<GlobalTableFunctionState> ParquetMetaDataOperator<OP_TYPE>::InitGlobal(ClientContext &context,
                                                                                  TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ParquetMetaDataBindData>();
	return make_uniq<ParquetMetadataGlobalState>(bind_data.file_paths, context);
}

template <ParquetMetadataOperatorType OP_TYPE>
unique_ptr<LocalTableFunctionState>
ParquetMetaDataOperator<OP_TYPE>::InitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                            GlobalTableFunctionState *global_state) {
	return make_uniq<ParquetMetadataLocalState>();
}

template <ParquetMetadataOperatorType OP_TYPE>
void ParquetMetaDataOperator<OP_TYPE>::Function(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<ParquetMetaDataBindData>();
	auto &global_state = data_p.global_state->Cast<ParquetMetadataGlobalState>();
	auto &local_state = data_p.local_state->Cast<ParquetMetadataLocalState>();

	idx_t output_count = 0;

	while (output_count < STANDARD_VECTOR_SIZE) {
		// Check if we need a new file
		if (local_state.file_exhausted) {
			auto next_file_idx = global_state.GetNextFileIndex();
			if (!next_file_idx.IsValid()) {
				break; // No more files to process
			}

			// Initialize processor for new file
			local_state.current_file_idx = next_file_idx.GetIndex();
			const auto &file_path = bind_data.file_paths[local_state.current_file_idx];
			local_state.processor = CreateProcessor(context, file_path, bind_data);
			local_state.file_exhausted = false;
		}

		idx_t rows_read =
		    local_state.processor->ReadChunk(context, output, output_count, STANDARD_VECTOR_SIZE - output_count);

		output_count += rows_read;

		if (local_state.processor->IsExhausted()) {
			local_state.file_exhausted = true;
			local_state.processor.reset();
		}

		// Break if no more data from current file and couldn't read full chunk
		if (rows_read == 0) {
			local_state.file_exhausted = true;
		}
	}

	output.SetCardinality(output_count);
}

template <ParquetMetadataOperatorType OP_TYPE>
double ParquetMetaDataOperator<OP_TYPE>::Progress(ClientContext &context, const FunctionData *bind_data_p,
                                                  const GlobalTableFunctionState *global_state) {
	auto &global_data = global_state->Cast<ParquetMetadataGlobalState>();
	return global_data.GetProgress() * 100.0;
}

template <ParquetMetadataOperatorType OP_TYPE>
unique_ptr<ParquetMetadataFileProcessor>
ParquetMetaDataOperator<OP_TYPE>::CreateProcessor(ClientContext &context, const string &file_path,
                                                  const ParquetMetaDataBindData &bind_data) {
	switch (OP_TYPE) {
	case ParquetMetadataOperatorType::META_DATA:
		return make_uniq<ParquetRowGroupMetadataProcessor>(context, file_path);
	case ParquetMetadataOperatorType::SCHEMA:
		return make_uniq<ParquetSchemaProcessor>(context, file_path);
	case ParquetMetadataOperatorType::KEY_VALUE_META_DATA:
		return make_uniq<ParquetKeyValueMetadataProcessor>(context, file_path);
	case ParquetMetadataOperatorType::FILE_META_DATA:
		return make_uniq<ParquetFileMetadataProcessor>(context, file_path);
	case ParquetMetadataOperatorType::BLOOM_PROBE: {
		const auto &probe_bind_data = static_cast<const ParquetBloomProbeBindData &>(bind_data);
		return make_uniq<ParquetBloomProbeProcessor>(context, file_path, probe_bind_data.probe_column_name,
		                                             probe_bind_data.probe_constant);
	}
	default:
		throw InternalException("Unsupported ParquetMetadataOperatorType");
	}
}

ParquetMetaDataFunction::ParquetMetaDataFunction()
    : TableFunction("parquet_metadata", {LogicalType::VARCHAR},
                    ParquetMetaDataOperator<ParquetMetadataOperatorType::META_DATA>::Function,
                    ParquetMetaDataOperator<ParquetMetadataOperatorType::META_DATA>::Bind,
                    ParquetMetaDataOperator<ParquetMetadataOperatorType::META_DATA>::InitGlobal,
                    ParquetMetaDataOperator<ParquetMetadataOperatorType::META_DATA>::InitLocal) {
	table_scan_progress = ParquetMetaDataOperator<ParquetMetadataOperatorType::META_DATA>::Progress;
}

ParquetSchemaFunction::ParquetSchemaFunction()
    : TableFunction("parquet_schema", {LogicalType::VARCHAR},
                    ParquetMetaDataOperator<ParquetMetadataOperatorType::SCHEMA>::Function,
                    ParquetMetaDataOperator<ParquetMetadataOperatorType::SCHEMA>::Bind,
                    ParquetMetaDataOperator<ParquetMetadataOperatorType::SCHEMA>::InitGlobal,
                    ParquetMetaDataOperator<ParquetMetadataOperatorType::SCHEMA>::InitLocal) {
	table_scan_progress = ParquetMetaDataOperator<ParquetMetadataOperatorType::SCHEMA>::Progress;
}

ParquetKeyValueMetadataFunction::ParquetKeyValueMetadataFunction()
    : TableFunction("parquet_kv_metadata", {LogicalType::VARCHAR},
                    ParquetMetaDataOperator<ParquetMetadataOperatorType::KEY_VALUE_META_DATA>::Function,
                    ParquetMetaDataOperator<ParquetMetadataOperatorType::KEY_VALUE_META_DATA>::Bind,
                    ParquetMetaDataOperator<ParquetMetadataOperatorType::KEY_VALUE_META_DATA>::InitGlobal,
                    ParquetMetaDataOperator<ParquetMetadataOperatorType::KEY_VALUE_META_DATA>::InitLocal) {
	table_scan_progress = ParquetMetaDataOperator<ParquetMetadataOperatorType::KEY_VALUE_META_DATA>::Progress;
}

ParquetFileMetadataFunction::ParquetFileMetadataFunction()
    : TableFunction("parquet_file_metadata", {LogicalType::VARCHAR},
                    ParquetMetaDataOperator<ParquetMetadataOperatorType::FILE_META_DATA>::Function,
                    ParquetMetaDataOperator<ParquetMetadataOperatorType::FILE_META_DATA>::Bind,
                    ParquetMetaDataOperator<ParquetMetadataOperatorType::FILE_META_DATA>::InitGlobal,
                    ParquetMetaDataOperator<ParquetMetadataOperatorType::FILE_META_DATA>::InitLocal) {
	table_scan_progress = ParquetMetaDataOperator<ParquetMetadataOperatorType::FILE_META_DATA>::Progress;
}

ParquetBloomProbeFunction::ParquetBloomProbeFunction()
    : TableFunction("parquet_bloom_probe", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::ANY},
                    ParquetMetaDataOperator<ParquetMetadataOperatorType::BLOOM_PROBE>::Function,
                    ParquetMetaDataOperator<ParquetMetadataOperatorType::BLOOM_PROBE>::Bind,
                    ParquetMetaDataOperator<ParquetMetadataOperatorType::BLOOM_PROBE>::InitGlobal,
                    ParquetMetaDataOperator<ParquetMetadataOperatorType::BLOOM_PROBE>::InitLocal) {
	table_scan_progress = ParquetMetaDataOperator<ParquetMetadataOperatorType::BLOOM_PROBE>::Progress;
}
} // namespace duckdb

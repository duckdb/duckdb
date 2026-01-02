#include "parquet_metadata.hpp"

#include "parquet_statistics.hpp"

#include <sstream>

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "parquet_reader.hpp"
#include "duckdb/common/numeric_utils.hpp"

namespace duckdb {

struct ParquetMetadataFilePaths {
	MultiFileListScanData scan_data;
	shared_ptr<MultiFileList> file_list;
	mutex file_lock;

	bool NextFile(OpenFileInfo &result) {
		D_ASSERT(file_list);
		unique_lock<mutex> lock(file_lock);
		return file_list->Scan(scan_data, result);
	}

	FileExpandResult GetExpandResult() {
		D_ASSERT(file_list);
		unique_lock<mutex> lock(file_lock);
		return file_list->GetExpandResult();
	}
};

struct ParquetMetaDataBindData : public TableFunctionData {
	unique_ptr<ParquetMetadataFilePaths> file_paths;
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
	BLOOM_PROBE,
	FULL_METADATA
};

class ParquetMetadataFileProcessor {
public:
	ParquetMetadataFileProcessor() = default;
	virtual ~ParquetMetadataFileProcessor() = default;
	void Initialize(ClientContext &context, ParquetReader &reader) {
		InitializeInternal(context, reader);
	}
	virtual void InitializeInternal(ClientContext &context, ParquetReader &reader) {};
	virtual idx_t TotalRowCount(ParquetReader &reader) = 0;
	virtual void ReadRow(vector<reference<Vector>> &output, idx_t output_idx, idx_t row_idx, ParquetReader &reader) = 0;
	virtual bool ForceFlush() {
		return false;
	}
};

struct ParquetMetaDataBindData;

class ParquetMetaDataOperator {
public:
	template <ParquetMetadataOperatorType OP_TYPE>
	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names);
	static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context, TableFunctionInitInput &input);
	template <ParquetMetadataOperatorType OP_TYPE>
	static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext &context, TableFunctionInitInput &input,
	                                                     GlobalTableFunctionState *global_state);
	template <ParquetMetadataOperatorType OP_TYPE>
	static void Function(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
	static double Progress(ClientContext &context, const FunctionData *bind_data_p,
	                       const GlobalTableFunctionState *global_state);

	template <ParquetMetadataOperatorType OP_TYPE>
	static void BindSchema(vector<LogicalType> &return_types, vector<string> &names);
};

struct ParquetMetadataGlobalState : public GlobalTableFunctionState {
	ParquetMetadataGlobalState(unique_ptr<ParquetMetadataFilePaths> file_paths_p, ClientContext &context)
	    : file_paths(std::move(file_paths_p)) {
		auto expand_result = file_paths->GetExpandResult();
		if (expand_result == FileExpandResult::MULTIPLE_FILES) {
			max_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
		} else {
			max_threads = 1;
		}
	}

	idx_t MaxThreads() const override {
		return max_threads;
	}

	bool NextFile(ClientContext &context, OpenFileInfo &result) {
		return file_paths->NextFile(result);
	}

	double GetProgress() const {
		// Not the most accurate, instantly assumes all files are done and equal
		unique_lock<mutex> lock(file_paths->file_lock);
		return static_cast<double>(file_paths->scan_data.current_file_idx) / file_paths->file_list->GetTotalFileCount();
	}

	unique_ptr<ParquetMetadataFilePaths> file_paths;
	idx_t max_threads;
};

struct ParquetMetadataLocalState : public LocalTableFunctionState {
	unique_ptr<ParquetReader> reader;
	unique_ptr<ParquetMetadataFileProcessor> processor;
	bool file_exhausted = true;
	idx_t row_idx = 0;
	idx_t total_rows = 0;

	void Initialize(ClientContext &context, OpenFileInfo &file_info) {
		ParquetOptions parquet_options(context);
		reader = make_uniq<ParquetReader>(context, file_info, parquet_options);
		processor->Initialize(context, *reader);
		total_rows = processor->TotalRowCount(*reader);
		row_idx = 0;
		file_exhausted = false;
	}
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

//===--------------------------------------------------------------------===//
// Row Group Meta Data
//===--------------------------------------------------------------------===//

class ParquetRowGroupMetadataProcessor : public ParquetMetadataFileProcessor {
public:
	void InitializeInternal(ClientContext &context, ParquetReader &reader) override;
	idx_t TotalRowCount(ParquetReader &reader) override;
	void ReadRow(vector<reference<Vector>> &output, idx_t output_idx, idx_t row_idx, ParquetReader &reader) override;

private:
	vector<ParquetColumnSchema> column_schemas;
};

template <>
void ParquetMetaDataOperator::BindSchema<ParquetMetadataOperatorType::META_DATA>(vector<LogicalType> &return_types,
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

	GeometryTypeSet type_set = GeometryTypeSet::Empty();
	for (auto &type : stats.geospatial_types) {
		const auto geom_type = (type % 1000);
		const auto vert_type = (type / 1000);
		if (geom_type < 1 || geom_type > 7) {
			throw InvalidInputException("Unsupported geometry type in Parquet geo metadata");
		}
		if (vert_type < 0 || vert_type > 3) {
			throw InvalidInputException("Unsupported geometry vertex type in Parquet geo metadata");
		}
		type_set.Add(static_cast<GeometryType>(geom_type), static_cast<VertexType>(vert_type));
	}

	for (auto &type_name : type_set.ToString(true)) {
		types.push_back(Value(type_name));
	}
	return Value::LIST(LogicalType::VARCHAR, types);
}

void ParquetRowGroupMetadataProcessor::InitializeInternal(ClientContext &context, ParquetReader &reader) {
	auto meta_data = reader.GetFileMetadata();
	column_schemas.clear();
	for (idx_t schema_idx = 0; schema_idx < meta_data->schema.size(); schema_idx++) {
		auto &schema_element = meta_data->schema[schema_idx];
		if (schema_element.num_children > 0) {
			continue;
		}
		ParquetColumnSchema column_schema;
		column_schema.type = reader.DeriveLogicalType(schema_element, column_schema);
		column_schemas.push_back(std::move(column_schema));
	}
}

idx_t ParquetRowGroupMetadataProcessor::TotalRowCount(ParquetReader &reader) {
	auto meta_data = reader.GetFileMetadata();
	return meta_data->row_groups.size() * column_schemas.size();
}

void ParquetRowGroupMetadataProcessor::ReadRow(vector<reference<Vector>> &output, idx_t output_idx, idx_t row_idx,
                                               ParquetReader &reader) {
	auto meta_data = reader.GetFileMetadata();
	idx_t col_idx = row_idx % column_schemas.size();
	idx_t row_group_idx = row_idx / column_schemas.size();

	auto &row_group = meta_data->row_groups[row_group_idx];

	auto &column = row_group.columns[col_idx];
	auto &column_schema = column_schemas[col_idx];
	auto &col_meta = column.meta_data;
	auto &stats = col_meta.statistics;
	auto &column_type = column_schema.type;

	// file_name
	output[0].get().SetValue(output_idx, reader.file.path);
	// row_group_id
	output[1].get().SetValue(output_idx, Value::BIGINT(UnsafeNumericCast<int64_t>(row_group_idx)));
	// row_group_num_rows
	output[2].get().SetValue(output_idx, Value::BIGINT(row_group.num_rows));
	// row_group_num_columns
	output[3].get().SetValue(output_idx, Value::BIGINT(UnsafeNumericCast<int64_t>(row_group.columns.size())));
	// row_group_bytes
	output[4].get().SetValue(output_idx, Value::BIGINT(row_group.total_byte_size));
	// column_id
	output[5].get().SetValue(output_idx, Value::BIGINT(UnsafeNumericCast<int64_t>(col_idx)));
	// file_offset
	output[6].get().SetValue(output_idx, ParquetElementBigint(column.file_offset, row_group.__isset.file_offset));
	// num_values
	output[7].get().SetValue(output_idx, Value::BIGINT(col_meta.num_values));
	// path_in_schema
	output[8].get().SetValue(output_idx, StringUtil::Join(col_meta.path_in_schema, ", "));
	// type
	output[9].get().SetValue(output_idx, ConvertParquetElementToString(col_meta.type));
	// stats_min
	output[10].get().SetValue(output_idx,
	                          ConvertParquetStats(column_type, column_schema, stats.__isset.min, stats.min));
	// stats_max
	output[11].get().SetValue(output_idx,
	                          ConvertParquetStats(column_type, column_schema, stats.__isset.max, stats.max));
	// stats_null_count
	output[12].get().SetValue(output_idx, ParquetElementBigint(stats.null_count, stats.__isset.null_count));
	// stats_distinct_count
	output[13].get().SetValue(output_idx, ParquetElementBigint(stats.distinct_count, stats.__isset.distinct_count));
	// stats_min_value
	output[14].get().SetValue(
	    output_idx, ConvertParquetStats(column_type, column_schema, stats.__isset.min_value, stats.min_value));
	// stats_max_value
	output[15].get().SetValue(
	    output_idx, ConvertParquetStats(column_type, column_schema, stats.__isset.max_value, stats.max_value));
	// compression
	output[16].get().SetValue(output_idx, ConvertParquetElementToString(col_meta.codec));
	// encodings
	vector<string> encoding_string;
	encoding_string.reserve(col_meta.encodings.size());
	for (auto &encoding : col_meta.encodings) {
		encoding_string.push_back(ConvertParquetElementToString(encoding));
	}
	output[17].get().SetValue(output_idx, Value(StringUtil::Join(encoding_string, ", ")));
	// index_page_offset
	output[18].get().SetValue(output_idx,
	                          ParquetElementBigint(col_meta.index_page_offset, col_meta.__isset.index_page_offset));
	// dictionary_page_offset
	output[19].get().SetValue(
	    output_idx, ParquetElementBigint(col_meta.dictionary_page_offset, col_meta.__isset.dictionary_page_offset));
	// data_page_offset
	output[20].get().SetValue(output_idx, Value::BIGINT(col_meta.data_page_offset));
	// total_compressed_size
	output[21].get().SetValue(output_idx, Value::BIGINT(col_meta.total_compressed_size));
	// total_uncompressed_size
	output[22].get().SetValue(output_idx, Value::BIGINT(col_meta.total_uncompressed_size));
	// key_value_metadata
	vector<Value> map_keys, map_values;
	for (auto &entry : col_meta.key_value_metadata) {
		map_keys.push_back(Value::BLOB_RAW(entry.key));
		map_values.push_back(Value::BLOB_RAW(entry.value));
	}
	output[23].get().SetValue(
	    output_idx, Value::MAP(LogicalType::BLOB, LogicalType::BLOB, std::move(map_keys), std::move(map_values)));
	// bloom_filter_offset
	output[24].get().SetValue(output_idx,
	                          ParquetElementBigint(col_meta.bloom_filter_offset, col_meta.__isset.bloom_filter_offset));
	// bloom_filter_length
	output[25].get().SetValue(output_idx,
	                          ParquetElementBigint(col_meta.bloom_filter_length, col_meta.__isset.bloom_filter_length));
	// min_is_exact
	output[26].get().SetValue(output_idx,
	                          ParquetElementBoolean(stats.is_min_value_exact, stats.__isset.is_min_value_exact));
	// max_is_exact
	output[27].get().SetValue(output_idx,
	                          ParquetElementBoolean(stats.is_max_value_exact, stats.__isset.is_max_value_exact));
	// row_group_compressed_bytes
	output[28].get().SetValue(
	    output_idx, ParquetElementBigint(row_group.total_compressed_size, row_group.__isset.total_compressed_size));
	// geo_stats_bbox, LogicalType::STRUCT(...)
	output[29].get().SetValue(output_idx, ConvertParquetGeoStatsBBOX(col_meta.geospatial_statistics));

	// geo_stats_types, LogicalType::LIST(LogicalType::VARCHAR)
	output[30].get().SetValue(output_idx, ConvertParquetGeoStatsTypes(col_meta.geospatial_statistics));
}

//===--------------------------------------------------------------------===//
// Schema Data
//===--------------------------------------------------------------------===//

class ParquetSchemaProcessor : public ParquetMetadataFileProcessor {
public:
	idx_t TotalRowCount(ParquetReader &reader) override;
	void ReadRow(vector<reference<Vector>> &output, idx_t output_idx, idx_t row_idx, ParquetReader &reader) override;
};

template <>
void ParquetMetaDataOperator::BindSchema<ParquetMetadataOperatorType::SCHEMA>(vector<LogicalType> &return_types,
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

	names.emplace_back("column_id");
	return_types.emplace_back(LogicalType::BIGINT);
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

idx_t ParquetSchemaProcessor::TotalRowCount(ParquetReader &reader) {
	return reader.GetFileMetadata()->schema.size();
}

void ParquetSchemaProcessor::ReadRow(vector<reference<Vector>> &output, idx_t output_idx, idx_t row_idx,
                                     ParquetReader &reader) {
	auto meta_data = reader.GetFileMetadata();
	const auto &column = meta_data->schema[row_idx];

	// file_name
	output[0].get().SetValue(output_idx, reader.file.path);
	// name
	output[1].get().SetValue(output_idx, column.name);
	// type
	output[2].get().SetValue(output_idx, ParquetElementString(column.type, column.__isset.type));
	// type_length
	output[3].get().SetValue(output_idx, ParquetElementInteger(column.type_length, column.__isset.type_length));
	// repetition_type
	output[4].get().SetValue(output_idx, ParquetElementString(column.repetition_type, column.__isset.repetition_type));
	// num_children
	output[5].get().SetValue(output_idx, ParquetElementBigint(column.num_children, column.__isset.num_children));
	// converted_type
	output[6].get().SetValue(output_idx, ParquetElementString(column.converted_type, column.__isset.converted_type));
	// scale
	output[7].get().SetValue(output_idx, ParquetElementBigint(column.scale, column.__isset.scale));
	// precision
	output[8].get().SetValue(output_idx, ParquetElementBigint(column.precision, column.__isset.precision));
	// field_id
	output[9].get().SetValue(output_idx, ParquetElementBigint(column.field_id, column.__isset.field_id));
	// logical_type
	output[10].get().SetValue(output_idx, ParquetLogicalTypeToString(column.logicalType, column.__isset.logicalType));
	// duckdb_type
	ParquetColumnSchema column_schema;
	Value duckdb_type;
	if (column.__isset.type) {
		duckdb_type = reader.DeriveLogicalType(column, column_schema).ToString();
	}
	output[11].get().SetValue(output_idx, duckdb_type);
	// column_id
	output[12].get().SetValue(output_idx, Value::BIGINT(UnsafeNumericCast<int64_t>(row_idx)));
}

//===--------------------------------------------------------------------===//
// KV Meta Data
//===--------------------------------------------------------------------===//

class ParquetKeyValueMetadataProcessor : public ParquetMetadataFileProcessor {
public:
	idx_t TotalRowCount(ParquetReader &reader) override;
	void ReadRow(vector<reference<Vector>> &output, idx_t output_idx, idx_t row_idx, ParquetReader &reader) override;
};

template <>
void ParquetMetaDataOperator::BindSchema<ParquetMetadataOperatorType::KEY_VALUE_META_DATA>(
    vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("file_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("key");
	return_types.emplace_back(LogicalType::BLOB);

	names.emplace_back("value");
	return_types.emplace_back(LogicalType::BLOB);
}

idx_t ParquetKeyValueMetadataProcessor::TotalRowCount(ParquetReader &reader) {
	return reader.GetFileMetadata()->key_value_metadata.size();
}

void ParquetKeyValueMetadataProcessor::ReadRow(vector<reference<Vector>> &output, idx_t output_idx, idx_t row_idx,
                                               ParquetReader &reader) {
	auto meta_data = reader.GetFileMetadata();
	auto &entry = meta_data->key_value_metadata[row_idx];

	output[0].get().SetValue(output_idx, Value(reader.file.path));
	output[1].get().SetValue(output_idx, Value::BLOB_RAW(entry.key));
	output[2].get().SetValue(output_idx, Value::BLOB_RAW(entry.value));
}

//===--------------------------------------------------------------------===//
// File Meta Data
//===--------------------------------------------------------------------===//

class ParquetFileMetadataProcessor : public ParquetMetadataFileProcessor {
public:
	idx_t TotalRowCount(ParquetReader &reader) override;
	void ReadRow(vector<reference<Vector>> &output, idx_t output_idx, idx_t row_idx, ParquetReader &reader) override;
};

template <>
void ParquetMetaDataOperator::BindSchema<ParquetMetadataOperatorType::FILE_META_DATA>(vector<LogicalType> &return_types,
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

idx_t ParquetFileMetadataProcessor::TotalRowCount(ParquetReader &reader) {
	return 1;
}

void ParquetFileMetadataProcessor::ReadRow(vector<reference<Vector>> &output, idx_t output_idx, idx_t row_idx,
                                           ParquetReader &reader) {
	auto meta_data = reader.GetFileMetadata();

	// file_name
	output[0].get().SetValue(output_idx, Value(reader.file.path));
	// created_by
	output[1].get().SetValue(output_idx, ParquetElementStringVal(meta_data->created_by, meta_data->__isset.created_by));
	// num_rows
	output[2].get().SetValue(output_idx, Value::BIGINT(meta_data->num_rows));
	// num_row_groups
	output[3].get().SetValue(output_idx, Value::BIGINT(UnsafeNumericCast<int64_t>(meta_data->row_groups.size())));
	// format_version
	output[4].get().SetValue(output_idx, Value::BIGINT(meta_data->version));
	// encryption_algorithm
	output[5].get().SetValue(
	    output_idx, ParquetElementString(meta_data->encryption_algorithm, meta_data->__isset.encryption_algorithm));
	// footer_signing_key_metadata
	output[6].get().SetValue(output_idx, ParquetElementStringVal(meta_data->footer_signing_key_metadata,
	                                                             meta_data->__isset.footer_signing_key_metadata));
	// file_size_bytes
	output[7].get().SetValue(output_idx, Value::UBIGINT(reader.GetHandle().GetFileSize()));
	// footer_size
	output[8].get().SetValue(output_idx, Value::UBIGINT(reader.metadata->footer_size));
}

//===--------------------------------------------------------------------===//
// Bloom Probe
//===--------------------------------------------------------------------===//

class ParquetBloomProbeProcessor : public ParquetMetadataFileProcessor {
public:
	ParquetBloomProbeProcessor(const string &probe_column, const Value &probe_value);

	void InitializeInternal(ClientContext &context, ParquetReader &reader) override;
	idx_t TotalRowCount(ParquetReader &reader) override;
	void ReadRow(vector<reference<Vector>> &output, idx_t output_idx, idx_t row_idx, ParquetReader &reader) override;

private:
	string probe_column_name;
	Value probe_constant;
	optional_idx probe_column_idx;

	unique_ptr<duckdb_apache::thrift::protocol::TCompactProtocolT<ThriftFileTransport>> protocol;
	optional_ptr<Allocator> allocator;
	unique_ptr<ConstantFilter> filter;
};

template <>
void ParquetMetaDataOperator::BindSchema<ParquetMetadataOperatorType::BLOOM_PROBE>(vector<LogicalType> &return_types,
                                                                                   vector<string> &names) {
	names.emplace_back("file_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("row_group_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("bloom_filter_excludes");
	return_types.emplace_back(LogicalType::BOOLEAN);
}

ParquetBloomProbeProcessor::ParquetBloomProbeProcessor(const string &probe_column, const Value &probe_value)
    : probe_column_name(probe_column), probe_constant(probe_value) {
}

void ParquetBloomProbeProcessor::InitializeInternal(ClientContext &context, ParquetReader &reader) {
	probe_column_idx = optional_idx::Invalid();

	for (idx_t column_idx = 0; column_idx < reader.columns.size(); column_idx++) {
		if (reader.columns[column_idx].name == probe_column_name) {
			probe_column_idx = column_idx;
			break;
		}
	}

	if (!probe_column_idx.IsValid()) {
		throw InvalidInputException("Column %s not found in %s", probe_column_name, reader.file.path);
	}

	auto transport = duckdb_base_std::make_shared<ThriftFileTransport>(reader.GetHandle(), false);
	protocol = make_uniq<duckdb_apache::thrift::protocol::TCompactProtocolT<ThriftFileTransport>>(std::move(transport));
	allocator = &BufferAllocator::Get(context);
	filter = make_uniq<ConstantFilter>(
	    ExpressionType::COMPARE_EQUAL,
	    probe_constant.CastAs(context, reader.GetColumns()[probe_column_idx.GetIndex()].type));
}

idx_t ParquetBloomProbeProcessor::TotalRowCount(ParquetReader &reader) {
	return reader.GetFileMetadata()->row_groups.size();
}

void ParquetBloomProbeProcessor::ReadRow(vector<reference<Vector>> &output, idx_t output_idx, idx_t row_idx,
                                         ParquetReader &reader) {
	auto meta_data = reader.GetFileMetadata();
	auto &row_group = meta_data->row_groups[row_idx];
	auto &column = row_group.columns[probe_column_idx.GetIndex()];

	D_ASSERT(!probe_constant.IsNull());

	auto bloom_excludes = ParquetStatisticsUtils::BloomFilterExcludes(*filter, column.meta_data, *protocol, *allocator);

	output[0].get().SetValue(output_idx, Value(reader.file.path));
	output[1].get().SetValue(output_idx, Value::BIGINT(NumericCast<int64_t>(row_idx)));
	output[2].get().SetValue(output_idx, Value::BOOLEAN(bloom_excludes));
}

//===--------------------------------------------------------------------===//
// Full Metadata
//===--------------------------------------------------------------------===//

class FullMetadataProcessor : public ParquetMetadataFileProcessor {
public:
	FullMetadataProcessor() = default;

	void InitializeInternal(ClientContext &context, ParquetReader &reader) override;
	idx_t TotalRowCount(ParquetReader &reader) override;
	void ReadRow(vector<reference<Vector>> &output, idx_t output_idx, idx_t row_idx, ParquetReader &reader) override;
	bool ForceFlush() override {
		return true;
	}

private:
	void PopulateMetadata(ParquetMetadataFileProcessor &processor, Vector &output, idx_t output_idx,
	                      ParquetReader &reader);

	ParquetFileMetadataProcessor file_processor;
	ParquetRowGroupMetadataProcessor row_group_processor;
	ParquetSchemaProcessor schema_processor;
	ParquetKeyValueMetadataProcessor kv_processor;
};

void FullMetadataProcessor::PopulateMetadata(ParquetMetadataFileProcessor &processor, Vector &output, idx_t output_idx,
                                             ParquetReader &reader) {
	auto count = processor.TotalRowCount(reader);
	auto *result_data = FlatVector::GetData<list_entry_t>(output);
	auto &result_struct = ListVector::GetEntry(output);
	auto &result_struct_entries = StructVector::GetEntries(result_struct);

	ListVector::SetListSize(output, count);
	ListVector::Reserve(output, count);

	result_data[output_idx].offset = 0;
	result_data[output_idx].length = count;

	FlatVector::Validity(output).SetValid(output_idx);

	vector<reference<Vector>> vectors;
	for (auto &entry : result_struct_entries) {
		vectors.push_back(std::ref(*entry.get()));
		entry->SetVectorType(VectorType::FLAT_VECTOR);
		auto &validity = FlatVector::Validity(*entry);
		validity.Initialize(count);
	}
	for (idx_t i = 0; i < count; i++) {
		processor.ReadRow(vectors, i, i, reader);
	}
}

template <>
void ParquetMetaDataOperator::BindSchema<ParquetMetadataOperatorType::FULL_METADATA>(vector<LogicalType> &return_types,
                                                                                     vector<string> &names) {
	names.emplace_back("parquet_file_metadata");
	vector<LogicalType> file_meta_types;
	vector<string> file_meta_names;
	ParquetMetaDataOperator::BindSchema<ParquetMetadataOperatorType::FILE_META_DATA>(file_meta_types, file_meta_names);
	child_list_t<LogicalType> file_meta_children;
	for (idx_t i = 0; i < file_meta_types.size(); i++) {
		file_meta_children.push_back(make_pair(file_meta_names[i], file_meta_types[i]));
	}
	return_types.emplace_back(LogicalType::LIST(LogicalType::STRUCT(std::move(file_meta_children))));

	names.emplace_back("parquet_metadata");
	vector<LogicalType> row_group_types;
	vector<string> row_group_names;
	ParquetMetaDataOperator::BindSchema<ParquetMetadataOperatorType::META_DATA>(row_group_types, row_group_names);
	child_list_t<LogicalType> row_group_children;
	for (idx_t i = 0; i < row_group_types.size(); i++) {
		row_group_children.push_back(make_pair(row_group_names[i], row_group_types[i]));
	}
	return_types.emplace_back(LogicalType::LIST(LogicalType::STRUCT(std::move(row_group_children))));

	names.emplace_back("parquet_schema");
	vector<LogicalType> schema_types;
	vector<string> schema_names;
	ParquetMetaDataOperator::BindSchema<ParquetMetadataOperatorType::SCHEMA>(schema_types, schema_names);
	child_list_t<LogicalType> schema_children;
	for (idx_t i = 0; i < schema_types.size(); i++) {
		schema_children.push_back(make_pair(schema_names[i], schema_types[i]));
	}
	return_types.emplace_back(LogicalType::LIST(LogicalType::STRUCT(std::move(schema_children))));

	names.emplace_back("parquet_kv_metadata");
	vector<LogicalType> kv_types;
	vector<string> kv_names;
	ParquetMetaDataOperator::BindSchema<ParquetMetadataOperatorType::KEY_VALUE_META_DATA>(kv_types, kv_names);
	child_list_t<LogicalType> kv_children;
	for (idx_t i = 0; i < kv_types.size(); i++) {
		kv_children.push_back(make_pair(kv_names[i], kv_types[i]));
	}
	return_types.emplace_back(LogicalType::LIST(LogicalType::STRUCT(std::move(kv_children))));
}

void FullMetadataProcessor::InitializeInternal(ClientContext &context, ParquetReader &reader) {
	file_processor.Initialize(context, reader);
	row_group_processor.Initialize(context, reader);
	schema_processor.Initialize(context, reader);
	kv_processor.Initialize(context, reader);
}

idx_t FullMetadataProcessor::TotalRowCount(ParquetReader &reader) {
	return 1;
}

void FullMetadataProcessor::ReadRow(vector<reference<Vector>> &output, idx_t output_idx, idx_t row_idx,
                                    ParquetReader &reader) {
	PopulateMetadata(file_processor, output[0].get(), output_idx, reader);
	PopulateMetadata(row_group_processor, output[1].get(), output_idx, reader);
	PopulateMetadata(schema_processor, output[2].get(), output_idx, reader);
	PopulateMetadata(kv_processor, output[3].get(), output_idx, reader);
}

//===--------------------------------------------------------------------===//
// Template Function Implementation
//===--------------------------------------------------------------------===//

template <ParquetMetadataOperatorType OP_TYPE>
unique_ptr<FunctionData> ParquetMetaDataOperator::Bind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	// Extract file paths from input using MultiFileReader (handles both single files and arrays)
	auto multi_file_reader = MultiFileReader::CreateDefault("ParquetMetadata");
	auto glob_input = FileGlobInput(FileGlobOptions::FALLBACK_GLOB, "parquet");

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

	result->file_paths = make_uniq<ParquetMetadataFilePaths>();
	result->file_paths->file_list = multi_file_reader->CreateFileList(context, input.inputs[0], glob_input);
	D_ASSERT(!result->file_paths->file_list->IsEmpty());
	result->file_paths->file_list->InitializeScan(result->file_paths->scan_data);

	BindSchema<OP_TYPE>(return_types, names);

	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> ParquetMetaDataOperator::InitGlobal(ClientContext &context,
                                                                         TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<ParquetMetaDataBindData>();
	return make_uniq<ParquetMetadataGlobalState>(std::move(bind_data.file_paths), context);
}

template <ParquetMetadataOperatorType OP_TYPE>
unique_ptr<LocalTableFunctionState> ParquetMetaDataOperator::InitLocal(ExecutionContext &context,
                                                                       TableFunctionInitInput &input,
                                                                       GlobalTableFunctionState *global_state) {
	auto &bind_data = input.bind_data->Cast<ParquetMetaDataBindData>();
	auto res = make_uniq<ParquetMetadataLocalState>();
	switch (OP_TYPE) {
	case ParquetMetadataOperatorType::META_DATA:
		res->processor = make_uniq<ParquetRowGroupMetadataProcessor>();
		break;
	case ParquetMetadataOperatorType::SCHEMA:
		res->processor = make_uniq<ParquetSchemaProcessor>();
		break;
	case ParquetMetadataOperatorType::KEY_VALUE_META_DATA:
		res->processor = make_uniq<ParquetKeyValueMetadataProcessor>();
		break;
	case ParquetMetadataOperatorType::FILE_META_DATA:
		res->processor = make_uniq<ParquetFileMetadataProcessor>();
		break;
	case ParquetMetadataOperatorType::BLOOM_PROBE: {
		const auto &probe_bind_data = static_cast<const ParquetBloomProbeBindData &>(bind_data);
		res->processor =
		    make_uniq<ParquetBloomProbeProcessor>(probe_bind_data.probe_column_name, probe_bind_data.probe_constant);
		break;
	}
	case ParquetMetadataOperatorType::FULL_METADATA: {
		res->processor = make_uniq<FullMetadataProcessor>();
		break;
	}
	default:
		throw InternalException("Unsupported ParquetMetadataOperatorType");
	}
	return unique_ptr_cast<LocalTableFunctionState, ParquetMetadataLocalState>(std::move(res));
}

template <ParquetMetadataOperatorType OP_TYPE>
void ParquetMetaDataOperator::Function(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &global_state = data_p.global_state->Cast<ParquetMetadataGlobalState>();
	auto &local_state = data_p.local_state->Cast<ParquetMetadataLocalState>();

	idx_t output_count = 0;

	vector<reference<Vector>> output_vectors;
	for (idx_t i = 0; i < output.ColumnCount(); i++) {
		output_vectors.push_back(std::ref(output.data[i]));
	}

	while (output_count < STANDARD_VECTOR_SIZE) {
		// Check if we need a new file
		if (local_state.file_exhausted) {
			OpenFileInfo next_file;
			if (!global_state.file_paths->NextFile(next_file)) {
				break; // No more files to process
			}

			local_state.Initialize(context, next_file);
		}

		idx_t left_in_vector = STANDARD_VECTOR_SIZE - output_count;
		idx_t left_in_file = local_state.total_rows - local_state.row_idx;
		idx_t rows_to_output = 0;
		if (left_in_file <= left_in_vector) {
			local_state.file_exhausted = true;
			rows_to_output = left_in_file;
		} else {
			rows_to_output = left_in_vector;
		}

		output.SetCardinality(output_count + rows_to_output);

		for (idx_t i = 0; i < rows_to_output; ++i) {
			local_state.processor->ReadRow(output_vectors, output_count + i, local_state.row_idx + i,
			                               *local_state.reader);
		}
		output_count += rows_to_output;
		local_state.row_idx += rows_to_output;

		if (local_state.processor->ForceFlush()) {
			break;
		}
	}
}

double ParquetMetaDataOperator::Progress(ClientContext &context, const FunctionData *bind_data_p,
                                         const GlobalTableFunctionState *global_state) {
	auto &global_data = global_state->Cast<ParquetMetadataGlobalState>();
	return global_data.GetProgress() * 100.0;
}

ParquetMetaDataFunction::ParquetMetaDataFunction()
    : TableFunction("parquet_metadata", {LogicalType::VARCHAR},
                    ParquetMetaDataOperator::Function<ParquetMetadataOperatorType::META_DATA>,
                    ParquetMetaDataOperator::Bind<ParquetMetadataOperatorType::META_DATA>,
                    ParquetMetaDataOperator::InitGlobal,
                    ParquetMetaDataOperator::InitLocal<ParquetMetadataOperatorType::META_DATA>) {
	table_scan_progress = ParquetMetaDataOperator::Progress;
}

ParquetSchemaFunction::ParquetSchemaFunction()
    : TableFunction("parquet_schema", {LogicalType::VARCHAR},
                    ParquetMetaDataOperator::Function<ParquetMetadataOperatorType::SCHEMA>,
                    ParquetMetaDataOperator::Bind<ParquetMetadataOperatorType::SCHEMA>,
                    ParquetMetaDataOperator::InitGlobal,
                    ParquetMetaDataOperator::InitLocal<ParquetMetadataOperatorType::SCHEMA>) {
	table_scan_progress = ParquetMetaDataOperator::Progress;
}

ParquetKeyValueMetadataFunction::ParquetKeyValueMetadataFunction()
    : TableFunction("parquet_kv_metadata", {LogicalType::VARCHAR},
                    ParquetMetaDataOperator::Function<ParquetMetadataOperatorType::KEY_VALUE_META_DATA>,
                    ParquetMetaDataOperator::Bind<ParquetMetadataOperatorType::KEY_VALUE_META_DATA>,
                    ParquetMetaDataOperator::InitGlobal,
                    ParquetMetaDataOperator::InitLocal<ParquetMetadataOperatorType::KEY_VALUE_META_DATA>) {
	table_scan_progress = ParquetMetaDataOperator::Progress;
}

ParquetFileMetadataFunction::ParquetFileMetadataFunction()
    : TableFunction("parquet_file_metadata", {LogicalType::VARCHAR},
                    ParquetMetaDataOperator::Function<ParquetMetadataOperatorType::FILE_META_DATA>,
                    ParquetMetaDataOperator::Bind<ParquetMetadataOperatorType::FILE_META_DATA>,
                    ParquetMetaDataOperator::InitGlobal,
                    ParquetMetaDataOperator::InitLocal<ParquetMetadataOperatorType::FILE_META_DATA>) {
	table_scan_progress = ParquetMetaDataOperator::Progress;
}

ParquetBloomProbeFunction::ParquetBloomProbeFunction()
    : TableFunction("parquet_bloom_probe", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::ANY},
                    ParquetMetaDataOperator::Function<ParquetMetadataOperatorType::BLOOM_PROBE>,
                    ParquetMetaDataOperator::Bind<ParquetMetadataOperatorType::BLOOM_PROBE>,
                    ParquetMetaDataOperator::InitGlobal,
                    ParquetMetaDataOperator::InitLocal<ParquetMetadataOperatorType::BLOOM_PROBE>) {
	table_scan_progress = ParquetMetaDataOperator::Progress;
}

ParquetFullMetadataFunction::ParquetFullMetadataFunction()
    : TableFunction("parquet_full_metadata", {LogicalType::VARCHAR},
                    ParquetMetaDataOperator::Function<ParquetMetadataOperatorType::FULL_METADATA>,
                    ParquetMetaDataOperator::Bind<ParquetMetadataOperatorType::FULL_METADATA>,
                    ParquetMetaDataOperator::InitGlobal,
                    ParquetMetaDataOperator::InitLocal<ParquetMetadataOperatorType::FULL_METADATA>) {
	table_scan_progress = ParquetMetaDataOperator::Progress;
}
} // namespace duckdb

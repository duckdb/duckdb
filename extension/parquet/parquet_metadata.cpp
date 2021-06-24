#include "parquet_metadata.hpp"
#include <sstream>

namespace duckdb {

struct ParquetMetaDataBindData : public FunctionData {
	vector<LogicalType> return_types;
	vector<string> files;
};

struct ParquetMetaDataOperatorData : public FunctionOperatorData {
	idx_t file_index;
	ChunkCollection collection;

	static void BindMetaData(vector<LogicalType> &return_types, vector<string> &names);
	static void BindSchema(vector<LogicalType> &return_types, vector<string> &names);

	void LoadFileMetaData(ClientContext &context, const vector<LogicalType> &return_types, const string &file_path);
	void LoadSchemaData(ClientContext &context, const vector<LogicalType> &return_types, const string &file_path);
};

template<class T>
string ConvertParquetElementToString(T &&entry) {
	std::stringstream ss;
	ss << entry;
	return ss.str();
}

template<class T>
string PrintParquetElementToString(T &&entry) {
	std::stringstream ss;
	entry.printTo(ss);
	return ss.str();
}

void ParquetMetaDataOperatorData::BindMetaData(vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("file_name");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("row_group_id");
	return_types.push_back(LogicalType::BIGINT);

	names.emplace_back("row_group_num_rows");
	return_types.push_back(LogicalType::BIGINT);

	names.emplace_back("row_group_num_columns");
	return_types.push_back(LogicalType::BIGINT);

	names.emplace_back("row_group_bytes");
	return_types.push_back(LogicalType::BIGINT);

	names.emplace_back("column_id");
	return_types.push_back(LogicalType::BIGINT);

	names.emplace_back("file_offset");
	return_types.push_back(LogicalType::BIGINT);

	names.emplace_back("num_values");
	return_types.push_back(LogicalType::BIGINT);

	names.emplace_back("path_in_schema");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("stats_min");
	return_types.push_back(LogicalType::BLOB);

	names.emplace_back("stats_max");
	return_types.push_back(LogicalType::BLOB);

	names.emplace_back("stats_null_count");
	return_types.push_back(LogicalType::BIGINT);

	names.emplace_back("stats_distinct_count");
	return_types.push_back(LogicalType::BIGINT);

	names.emplace_back("stats_min_value");
	return_types.push_back(LogicalType::BLOB);

	names.emplace_back("stats_max_value");
	return_types.push_back(LogicalType::BLOB);

	names.emplace_back("compression");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("encodings");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("index_page_offset");
	return_types.push_back(LogicalType::BIGINT);

	names.emplace_back("dictionary_page_offset");
	return_types.push_back(LogicalType::BIGINT);

	names.emplace_back("data_page_offset");
	return_types.push_back(LogicalType::BIGINT);

	names.emplace_back("total_compressed_size");
	return_types.push_back(LogicalType::BIGINT);

	names.emplace_back("total_uncompressed_size");
	return_types.push_back(LogicalType::BIGINT);
}

void ParquetMetaDataOperatorData::LoadFileMetaData(ClientContext &context, const vector<LogicalType> &return_types, const string &file_path) {
	collection.Reset();

	auto reader = make_unique<ParquetReader>(context, file_path);
	idx_t count = 0;
	DataChunk current_chunk;
	current_chunk.Initialize(return_types);
	auto meta_data = reader->GetFileMetadata();
	for(idx_t row_group_idx = 0; row_group_idx < meta_data->row_groups.size(); row_group_idx++) {
		auto &row_group = meta_data->row_groups[row_group_idx];

		for(idx_t col_idx = 0; col_idx < row_group.columns.size(); col_idx++) {
			auto &column = row_group.columns[col_idx];
			auto &col_meta = column.meta_data;
			auto &stats = col_meta.statistics;

			// file_name, LogicalType::VARCHAR
			current_chunk.SetValue(0, count, file_path);

			// row_group_id, LogicalType::BIGINT
			current_chunk.SetValue(1, count, Value::BIGINT(row_group_idx));

			// row_group_num_rows, LogicalType::BIGINT
			current_chunk.SetValue(2, count, Value::BIGINT(row_group.num_rows));

			// row_group_num_columns, LogicalType::BIGINT
			current_chunk.SetValue(3, count, Value::BIGINT(row_group.columns.size()));

			// row_group_bytes, LogicalType::BIGINT
			current_chunk.SetValue(4, count, Value::BIGINT(row_group.total_byte_size));

			// column_id, LogicalType::BIGINT
			current_chunk.SetValue(5, count, Value::BIGINT(col_idx));

			// file_offset, LogicalType::BIGINT
			current_chunk.SetValue(6, count, Value::BIGINT(column.file_offset));

			// num_values, LogicalType::BIGINT
			current_chunk.SetValue(7, count, Value::BIGINT(col_meta.num_values));

			// path_in_schema, LogicalType::VARCHAR
			current_chunk.SetValue(8, count, StringUtil::Join(col_meta.path_in_schema, ", "));

			// stats_min, LogicalType::BLOB
			current_chunk.SetValue(9, count, stats.__isset.min ? Value::BLOB_RAW(stats.min) : Value(LogicalType::BLOB));

			// stats_max, LogicalType::BLOB
			current_chunk.SetValue(10, count, stats.__isset.max ? Value::BLOB_RAW(stats.max) : Value(LogicalType::BLOB));

			// stats_null_count, LogicalType::BIGINT
			current_chunk.SetValue(11, count, stats.__isset.null_count ? Value::BIGINT(stats.null_count) : Value(LogicalType::BIGINT));

			// stats_distinct_count, LogicalType::BIGINT
			current_chunk.SetValue(12, count, stats.__isset.distinct_count ? Value::BIGINT(stats.distinct_count) : Value(LogicalType::BIGINT));

			// stats_min_value, LogicalType::BLOB
			current_chunk.SetValue(13, count, stats.__isset.min_value ? Value::BLOB_RAW(stats.min_value) : Value(LogicalType::BLOB));

			// stats_max_value, LogicalType::BLOB
			current_chunk.SetValue(14, count, stats.__isset.max_value ? Value::BLOB_RAW(stats.max_value) : Value(LogicalType::BLOB));

			// compression, LogicalType::VARCHAR
			current_chunk.SetValue(15, count, ConvertParquetElementToString(col_meta.codec));

			// encodings, LogicalType::VARCHAR
			vector<string> encoding_string;
			for(auto &encoding : col_meta.encodings) {
				encoding_string.push_back(ConvertParquetElementToString(encoding));
			}
			current_chunk.SetValue(16, count, Value(StringUtil::Join(encoding_string, ", ")));

			// index_page_offset, LogicalType::BIGINT
			current_chunk.SetValue(17, count, Value::BIGINT(col_meta.index_page_offset));

			// dictionary_page_offset, LogicalType::BIGINT
			current_chunk.SetValue(18, count, Value::BIGINT(col_meta.dictionary_page_offset));

			// data_page_offset, LogicalType::BIGINT
			current_chunk.SetValue(19, count, Value::BIGINT(col_meta.data_page_offset));

			// total_compressed_size, LogicalType::BIGINT
			current_chunk.SetValue(20, count, Value::BIGINT(col_meta.total_compressed_size));

			// total_uncompressed_size, LogicalType::BIGINT
			current_chunk.SetValue(21, count, Value::BIGINT(col_meta.total_uncompressed_size));

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
}

void ParquetMetaDataOperatorData::BindSchema(vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("file_name");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("name");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("type");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("type_length");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("repetition_type");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("num_children");
	return_types.push_back(LogicalType::BIGINT);

	names.emplace_back("converted_type");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("scale");
	return_types.push_back(LogicalType::BIGINT);

	names.emplace_back("precision");
	return_types.push_back(LogicalType::BIGINT);

	names.emplace_back("field_id");
	return_types.push_back(LogicalType::BIGINT);

	names.emplace_back("logical_type");
	return_types.push_back(LogicalType::VARCHAR);
}



Value ParquetLogicalTypeToString(const duckdb_parquet::format::LogicalType &type) {

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

void ParquetMetaDataOperatorData::LoadSchemaData(ClientContext &context, const vector<LogicalType> &return_types, const string &file_path) {
	collection.Reset();

	auto reader = make_unique<ParquetReader>(context, file_path);
	idx_t count = 0;
	DataChunk current_chunk;
	current_chunk.Initialize(return_types);
	auto meta_data = reader->GetFileMetadata();
	for(idx_t col_idx = 0; col_idx < meta_data->schema.size(); col_idx++) {
		auto &column = meta_data->schema[col_idx];

		// file_name, LogicalType::VARCHAR
		current_chunk.SetValue(0, count, file_path);

		// name, LogicalType::VARCHAR
		current_chunk.SetValue(1, count, column.name);

		// type, LogicalType::VARCHAR
		current_chunk.SetValue(2, count, ConvertParquetElementToString(column.type));

		// type_length, LogicalType::VARCHAR
		current_chunk.SetValue(3, count, Value::INTEGER(column.type_length));

		// repetition_type, LogicalType::VARCHAR
		current_chunk.SetValue(4, count, ConvertParquetElementToString(column.repetition_type));

		// num_children, LogicalType::BIGINT
		current_chunk.SetValue(5, count, Value::BIGINT(column.num_children));

		// converted_type, LogicalType::VARCHAR
		current_chunk.SetValue(6, count, ConvertParquetElementToString(column.converted_type));

		// scale, LogicalType::BIGINT
		current_chunk.SetValue(7, count, Value::BIGINT(column.scale));

		// precision, LogicalType::BIGINT
		current_chunk.SetValue(8, count, Value::BIGINT(column.precision));

		// field_id, LogicalType::BIGINT
		current_chunk.SetValue(9, count, Value::BIGINT(column.field_id));

		// logical_type, LogicalType::VARCHAR
		current_chunk.SetValue(10, count, ParquetLogicalTypeToString(column.logicalType));

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
}



template<bool SCHEMA>
unique_ptr<FunctionData> ParquetMetaDataBind(ClientContext &context, vector<Value> &inputs,
													unordered_map<string, Value> &named_parameters,
													vector<LogicalType> &input_table_types,
													vector<string> &input_table_names,
													vector<LogicalType> &return_types, vector<string> &names) {
	if (SCHEMA) {
		ParquetMetaDataOperatorData::BindSchema(return_types, names);
	} else {
		ParquetMetaDataOperatorData::BindMetaData(return_types, names);
	}

	auto file_name = inputs[0].GetValue<string>();
	auto result = make_unique<ParquetMetaDataBindData>();

	FileSystem &fs = FileSystem::GetFileSystem(context);
	result->return_types = return_types;
	result->files = fs.Glob(file_name);
	if (result->files.empty()) {
		throw IOException("No files found that match the pattern \"%s\"", file_name);
	}
	return result;
}

template<bool SCHEMA>
unique_ptr<FunctionOperatorData> ParquetMetaDataInit(ClientContext &context, const FunctionData *bind_data_p,
															const vector<column_t> &column_ids,
															TableFilterCollection *filters) {
	auto &bind_data = (ParquetMetaDataBindData &)*bind_data_p;
	D_ASSERT(!bind_data.files.empty());

	auto result = make_unique<ParquetMetaDataOperatorData>();
	if (SCHEMA) {
		result->LoadSchemaData(context, bind_data.return_types, bind_data.files[0]);
	} else {
		result->LoadFileMetaData(context, bind_data.return_types, bind_data.files[0]);
	}
	result->file_index = 0;
	return move(result);
}

template<bool SCHEMA>
void ParquetMetaDataImplementation(ClientContext &context, const FunctionData *bind_data_p,
											FunctionOperatorData *operator_state, DataChunk *input,
											DataChunk &output) {
	auto &data = (ParquetMetaDataOperatorData &)*operator_state;
	auto &bind_data = (ParquetMetaDataBindData &)*bind_data_p;
	while(true) {
		auto chunk = data.collection.Fetch();
		if (!chunk) {
			if (data.file_index + 1 < bind_data.files.size()) {
				// load the metadata for the next file
				data.file_index++;
				if (SCHEMA) {
					data.LoadSchemaData(context, bind_data.return_types, bind_data.files[data.file_index]);
				} else {
					data.LoadFileMetaData(context, bind_data.return_types, bind_data.files[data.file_index]);
				}
				continue;
			} else {
				// no files remaining: done
				return;
			}
		}
		output.Move(*chunk);
		if (output.size() != 0) {
			return;
		}
	}
}

ParquetMetaDataFunction::ParquetMetaDataFunction()
	: TableFunction("parquet_metadata", {LogicalType::VARCHAR}, ParquetMetaDataImplementation<false>, ParquetMetaDataBind<false>,
					ParquetMetaDataInit<false>, /* statistics */ nullptr, /* cleanup */ nullptr,
					/* dependency */ nullptr, nullptr,
					/* pushdown_complex_filter */ nullptr, /* to_string */ nullptr, nullptr, nullptr, nullptr,
					nullptr, nullptr, false, false, nullptr) {
}


ParquetSchemaFunction::ParquetSchemaFunction()
	: TableFunction("parquet_schema", {LogicalType::VARCHAR}, ParquetMetaDataImplementation<true>, ParquetMetaDataBind<true>,
					ParquetMetaDataInit<true>, /* statistics */ nullptr, /* cleanup */ nullptr,
					/* dependency */ nullptr, nullptr,
					/* pushdown_complex_filter */ nullptr, /* to_string */ nullptr, nullptr, nullptr, nullptr,
					nullptr, nullptr, false, false, nullptr) {
}

}

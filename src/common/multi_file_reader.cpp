#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

void MultiFileReader::AddParameters(TableFunction &table_function) {
	table_function.named_parameters["filename"] = LogicalType::BOOLEAN;
	table_function.named_parameters["hive_partitioning"] = LogicalType::BOOLEAN;
	table_function.named_parameters["union_by_name"] = LogicalType::BOOLEAN;
}

vector<string> MultiFileReader::GetFileList(ClientContext &context, const Value &input, const string &name,
                                            FileGlobOptions options) {
	auto &config = DBConfig::GetConfig(context);
	if (!config.options.enable_external_access) {
		throw PermissionException("Scanning %s files is disabled through configuration", name);
	}
	if (input.IsNull()) {
		throw ParserException("%s reader cannot take NULL list as parameter", name);
	}
	FileSystem &fs = FileSystem::GetFileSystem(context);
	vector<string> files;
	if (input.type().id() == LogicalTypeId::VARCHAR) {
		auto file_name = StringValue::Get(input);
		files = fs.GlobFiles(file_name, context, options);
	} else if (input.type().id() == LogicalTypeId::LIST) {
		for (auto &val : ListValue::GetChildren(input)) {
			if (val.IsNull()) {
				throw ParserException("%s reader cannot take NULL input as parameter", name);
			}
			auto glob_files = fs.GlobFiles(StringValue::Get(val), context, options);
			files.insert(files.end(), glob_files.begin(), glob_files.end());
		}
	} else {
		throw InternalException("Unsupported type for MultiFileReader::GetFileList");
	}
	if (files.empty() && options == FileGlobOptions::DISALLOW_EMPTY) {
		throw IOException("%s reader needs at least one file to read", name);
	}
	return files;
}

bool MultiFileReader::ParseOption(const string &key, const Value &val, MultiFileReaderOptions &options) {
	auto loption = StringUtil::Lower(key);
	if (loption == "filename") {
		options.filename = BooleanValue::Get(val);
	} else if (loption == "hive_partitioning") {
		options.hive_partitioning = BooleanValue::Get(val);
	} else if (loption == "union_by_name") {
		options.union_by_name = BooleanValue::Get(val);
	} else {
		return false;
	}
	return true;
}

bool MultiFileReader::ParseCopyOption(const string &key, const vector<Value> &values, MultiFileReaderOptions &options) {
	auto loption = StringUtil::Lower(key);
	if (loption == "filename") {
		options.filename = true;
	} else if (loption == "hive_partitioning") {
		options.hive_partitioning = true;
	} else if (loption == "union_by_name") {
		options.union_by_name = true;
	} else {
		return false;
	}
	return true;
}

bool MultiFileReader::ComplexFilterPushdown(ClientContext &context, vector<string> &files,
                                            const MultiFileReaderOptions &options, LogicalGet &get,
                                            vector<unique_ptr<Expression>> &filters) {
	if (files.empty()) {
		return false;
	}
	if (!options.hive_partitioning && !options.filename) {
		return false;
	}
	auto initial_filename = files[0];

	unordered_map<string, column_t> column_map;
	for (idx_t i = 0; i < get.column_ids.size(); i++) {
		column_map.insert({get.names[get.column_ids[i]], i});
	}

	HivePartitioning::ApplyFiltersToFileList(context, files, filters, column_map, get.table_index,
	                                         options.hive_partitioning, options.filename);
	if (files.empty() || initial_filename != files[0]) {
		return true;
	}
	return false;
}

MultiFileReaderBindData MultiFileReader::BindOptions(MultiFileReaderOptions &options, const vector<string> &files,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	MultiFileReaderBindData bind_data;
	// Add generated constant column for filename
	if (options.filename) {
		if (std::find(names.begin(), names.end(), "filename") != names.end()) {
			throw BinderException("Using filename option on file with column named filename is not supported");
		}
		bind_data.filename_idx = names.size();
		return_types.emplace_back(LogicalType::VARCHAR);
		names.emplace_back("filename");
	}

	// Add generated constant column for filename
	if (options.hive_partitioning) {
		D_ASSERT(!files.empty());
		auto partitions = HivePartitioning::Parse(files[0]);
		// verify that all files have the same hive partitioning scheme
		for (auto &f : files) {
			auto file_partitions = HivePartitioning::Parse(f);
			for (auto &part_info : partitions) {
				if (file_partitions.find(part_info.first) == file_partitions.end()) {
					throw BinderException(
					    "Hive partition mismatch between file \"%s\" and \"%s\": key \"%s\" not found", files[0], f,
					    part_info.first);
				}
			}
			if (partitions.size() != file_partitions.size()) {
				throw BinderException("Hive partition mismatch between file \"%s\" and \"%s\"", files[0], f);
			}
		}
		for (auto &part : partitions) {
			idx_t hive_partitioning_index = DConstants::INVALID_INDEX;
			auto lookup = std::find(names.begin(), names.end(), part.first);
			if (lookup != names.end()) {
				// hive partitioning column also exists in file - override
				auto idx = lookup - names.begin();
				hive_partitioning_index = idx;
				return_types[idx] = LogicalType::VARCHAR;
			} else {
				// hive partitioning column does not exist in file - add a new column containing the key
				hive_partitioning_index = names.size();
				return_types.emplace_back(LogicalType::VARCHAR);
				names.emplace_back(part.first);
			}
			bind_data.hive_partitioning_indexes.push_back(make_pair(part.first, hive_partitioning_index));
		}
	}
	return bind_data;
}

void MultiFileReader::FinalizeBind(const MultiFileReaderBindData &options, const string &filename,
                                   const vector<column_t> &global_column_ids, MultiFileReaderData &reader_data) {
	for (idx_t i = 0; i < global_column_ids.size(); i++) {
		auto column_id = global_column_ids[i];
		if (IsRowIdColumnId(column_id)) {
			// row-id
			reader_data.constant_map.push_back(make_pair(i, Value::BIGINT(42)));
			continue;
		}
		if (column_id == options.filename_idx) {
			// filename
			reader_data.constant_map.push_back(make_pair(i, Value(filename)));
			continue;
		}
		if (!reader_data.union_null_cols.empty()) {
			// union by name NULL column
			if (reader_data.union_null_cols[column_id]) {
				reader_data.constant_map.push_back(make_pair(i, Value(reader_data.union_col_types[column_id])));
				continue;
			}
		}
		if (!options.hive_partitioning_indexes.empty()) {
			// hive partition constants
			auto partitions = HivePartitioning::Parse(filename);
			D_ASSERT(partitions.size() == options.hive_partitioning_indexes.size());
			for (auto &entry : options.hive_partitioning_indexes) {
				if (column_id == entry.second) {
					reader_data.constant_map.push_back(make_pair(i, Value(partitions[entry.first])));
					break;
				}
			}
		}
	}
}

void MultiFileReader::CreateMapping(const string &file_name, const vector<LogicalType> &file_types,
                                    const vector<string> &file_names, const vector<LogicalType> &global_types,
                                    const vector<string> &global_names, const vector<column_t> &global_column_ids,
                                    optional_ptr<TableFilterSet> filters, MultiFileReaderData &reader_data) {
	// we have expected types: create a map of name -> column index
	unordered_map<string, idx_t> name_map;
	for (idx_t col_idx = 0; col_idx < file_names.size(); col_idx++) {
		name_map[file_names[col_idx]] = col_idx;
	}
	for (idx_t i = 0; i < global_column_ids.size(); i++) {
		// check if this is a constant column
		bool constant = false;
		for (auto &entry : reader_data.constant_map) {
			if (entry.first == i) {
				constant = true;
				break;
			}
		}
		if (constant) {
			// this column is constant for this file
			continue;
		}
		// not constant - look up the column in the name map
		auto global_id = global_column_ids[i];
		auto &global_name = global_names[global_id];
		auto entry = name_map.find(global_name);
		if (entry == name_map.end()) {
			throw IOException(
			    StringUtil::Format("Failed to read file \"%s\": schema mismatch in glob: column \"%s\" was read from "
			                       "the original file, but could not be found in file \"%s\".\n\nIf you are trying to "
			                       "read files with different schemas, try setting union_by_name=True",
			                       file_name, global_name, file_name));
		}
		// we found the column in the local file - check if the types are the same
		auto local_id = entry->second;
		auto &global_type = global_types[global_id];
		auto &local_type = file_types[local_id];
		if (global_type != local_type) {
			reader_data.cast_map[local_id] = global_type;
		}
		// the types are the same - create the mapping
		reader_data.column_mapping.push_back(i);
		reader_data.column_ids.push_back(local_id);
	}
	if (filters) {
		reader_data.filter_map.resize(global_types.size());
		for (idx_t c = 0; c < reader_data.column_mapping.size(); c++) {
			auto map_index = reader_data.column_mapping[c];
			reader_data.filter_map[map_index].index = c;
			reader_data.filter_map[map_index].is_constant = false;
		}
		for (idx_t c = 0; c < reader_data.constant_map.size(); c++) {
			auto constant_index = reader_data.constant_map[c].first;
			reader_data.filter_map[constant_index].index = c;
			reader_data.filter_map[constant_index].is_constant = true;
		}
	}
}

void MultiFileReader::FinalizeChunk(const MultiFileReaderBindData &bind_data, const MultiFileReaderData &reader_data,
                                    DataChunk &chunk) {
	// reference all the constants set up in MultiFileReader::FinalizeBind
	for (auto &entry : reader_data.constant_map) {
		chunk.data[entry.first].Reference(entry.second);
	}
	chunk.Verify();
}

void MultiFileReaderOptions::Serialize(FieldWriter &writer) const {
	writer.WriteField<bool>(filename);
	writer.WriteField<bool>(hive_partitioning);
	writer.WriteField<bool>(union_by_name);
}

void MultiFileReaderOptions::Deserialize(FieldReader &reader) {
	filename = reader.ReadRequired<bool>();
	hive_partitioning = reader.ReadRequired<bool>();
	union_by_name = reader.ReadRequired<bool>();
}

void MultiFileReaderOptions::AddBatchInfo(BindInfo &bind_info) {
	bind_info.InsertOption("filename", Value::BOOLEAN(filename));
	bind_info.InsertOption("hive_partitioning", Value::BOOLEAN(hive_partitioning));
	bind_info.InsertOption("union_by_name", Value::BOOLEAN(union_by_name));
}

} // namespace duckdb

#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

void MultiFileReader::AddParameters(TableFunction &table_function) {
	table_function.named_parameters["filename"] = LogicalType::BOOLEAN;
	table_function.named_parameters["hive_partitioning"] = LogicalType::BOOLEAN;
	table_function.named_parameters["union_by_name"] = LogicalType::BOOLEAN;
	table_function.named_parameters["hive_types"] = LogicalType::ANY;
	table_function.named_parameters["hive_types_autocast"] = LogicalType::BOOLEAN;
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
			if (val.type().id() != LogicalTypeId::VARCHAR) {
				throw ParserException("%s reader can only take a list of strings as a parameter", name);
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

bool MultiFileReader::ParseOption(const string &key, const Value &val, MultiFileReaderOptions &options,
                                  ClientContext &context) {
	auto loption = StringUtil::Lower(key);
	if (loption == "filename") {
		options.filename = BooleanValue::Get(val);
	} else if (loption == "hive_partitioning") {
		options.hive_partitioning = BooleanValue::Get(val);
		options.auto_detect_hive_partitioning = false;
	} else if (loption == "union_by_name") {
		options.union_by_name = BooleanValue::Get(val);
	} else if (loption == "hive_types_autocast" || loption == "hive_type_autocast") {
		options.hive_types_autocast = BooleanValue::Get(val);
	} else if (loption == "hive_types" || loption == "hive_type") {
		if (val.type().id() != LogicalTypeId::STRUCT) {
			throw InvalidInputException(
			    "'hive_types' only accepts a STRUCT('name':VARCHAR, ...), but '%s' was provided",
			    val.type().ToString());
		}
		// verify that that all the children of the struct value are VARCHAR
		auto &children = StructValue::GetChildren(val);
		for (idx_t i = 0; i < children.size(); i++) {
			const Value &child = children[i];
			if (child.type().id() != LogicalType::VARCHAR) {
				throw InvalidInputException("hive_types: '%s' must be a VARCHAR, instead: '%s' was provided",
				                            StructType::GetChildName(val.type(), i), child.type().ToString());
			}
			// for every child of the struct, get the logical type
			LogicalType transformed_type = TransformStringToLogicalType(child.ToString(), context);
			const string &name = StructType::GetChildName(val.type(), i);
			options.hive_types_schema[name] = transformed_type;
		}
		D_ASSERT(!options.hive_types_schema.empty());
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

	unordered_map<string, column_t> column_map;
	for (idx_t i = 0; i < get.column_ids.size(); i++) {
		column_map.insert({get.names[get.column_ids[i]], i});
	}

	auto start_files = files.size();
	HivePartitioning::ApplyFiltersToFileList(context, files, filters, column_map, get, options.hive_partitioning,
	                                         options.filename);

	if (files.size() != start_files) {
		// we have pruned files
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

	// Add generated constant columns from hive partitioning scheme
	if (options.hive_partitioning) {
		D_ASSERT(!files.empty());
		auto partitions = HivePartitioning::Parse(files[0]);
		// verify that all files have the same hive partitioning scheme
		for (auto &f : files) {
			auto file_partitions = HivePartitioning::Parse(f);
			for (auto &part_info : partitions) {
				if (file_partitions.find(part_info.first) == file_partitions.end()) {
					string error = "Hive partition mismatch between file \"%s\" and \"%s\": key \"%s\" not found";
					if (options.auto_detect_hive_partitioning == true) {
						throw InternalException(error + "(hive partitioning was autodetected)", files[0], f,
						                        part_info.first);
					}
					throw BinderException(error.c_str(), files[0], f, part_info.first);
				}
			}
			if (partitions.size() != file_partitions.size()) {
				string error_msg = "Hive partition mismatch between file \"%s\" and \"%s\"";
				if (options.auto_detect_hive_partitioning == true) {
					throw InternalException(error_msg + "(hive partitioning was autodetected)", files[0], f);
				}
				throw BinderException(error_msg.c_str(), files[0], f);
			}
		}

		if (!options.hive_types_schema.empty()) {
			// verify that all hive_types are existing partitions
			options.VerifyHiveTypesArePartitions(partitions);
		}

		for (auto &part : partitions) {
			idx_t hive_partitioning_index = DConstants::INVALID_INDEX;
			auto lookup = std::find(names.begin(), names.end(), part.first);
			if (lookup != names.end()) {
				// hive partitioning column also exists in file - override
				auto idx = lookup - names.begin();
				hive_partitioning_index = idx;
				return_types[idx] = options.GetHiveLogicalType(part.first);
			} else {
				// hive partitioning column does not exist in file - add a new column containing the key
				hive_partitioning_index = names.size();
				return_types.emplace_back(options.GetHiveLogicalType(part.first));
				names.emplace_back(part.first);
			}
			bind_data.hive_partitioning_indexes.emplace_back(part.first, hive_partitioning_index);
		}
	}
	return bind_data;
}

void MultiFileReader::FinalizeBind(const MultiFileReaderOptions &file_options, const MultiFileReaderBindData &options,
                                   const string &filename, const vector<string> &local_names,
                                   const vector<LogicalType> &global_types, const vector<string> &global_names,
                                   const vector<column_t> &global_column_ids, MultiFileReaderData &reader_data,
                                   ClientContext &context) {

	// create a map of name -> column index
	case_insensitive_map_t<idx_t> name_map;
	if (file_options.union_by_name) {
		for (idx_t col_idx = 0; col_idx < local_names.size(); col_idx++) {
			name_map[local_names[col_idx]] = col_idx;
		}
	}
	for (idx_t i = 0; i < global_column_ids.size(); i++) {
		auto column_id = global_column_ids[i];
		if (IsRowIdColumnId(column_id)) {
			// row-id
			reader_data.constant_map.emplace_back(i, Value::BIGINT(42));
			continue;
		}
		if (column_id == options.filename_idx) {
			// filename
			reader_data.constant_map.emplace_back(i, Value(filename));
			continue;
		}
		if (!options.hive_partitioning_indexes.empty()) {
			// hive partition constants
			auto partitions = HivePartitioning::Parse(filename);
			D_ASSERT(partitions.size() == options.hive_partitioning_indexes.size());
			bool found_partition = false;
			for (auto &entry : options.hive_partitioning_indexes) {
				if (column_id == entry.index) {
					Value value = file_options.GetHivePartitionValue(partitions[entry.value], entry.value, context);
					reader_data.constant_map.emplace_back(i, value);
					found_partition = true;
					break;
				}
			}
			if (found_partition) {
				continue;
			}
		}
		if (file_options.union_by_name) {
			auto &global_name = global_names[column_id];
			auto entry = name_map.find(global_name);
			bool not_present_in_file = entry == name_map.end();
			if (not_present_in_file) {
				// we need to project a column with name \"global_name\" - but it does not exist in the current file
				// push a NULL value of the specified type
				reader_data.constant_map.emplace_back(i, Value(global_types[column_id]));
				continue;
			}
		}
	}
}

void MultiFileReader::CreateNameMapping(const string &file_name, const vector<LogicalType> &local_types,
                                        const vector<string> &local_names, const vector<LogicalType> &global_types,
                                        const vector<string> &global_names, const vector<column_t> &global_column_ids,
                                        MultiFileReaderData &reader_data, const string &initial_file) {
	D_ASSERT(global_types.size() == global_names.size());
	D_ASSERT(local_types.size() == local_names.size());
	// we have expected types: create a map of name -> column index
	case_insensitive_map_t<idx_t> name_map;
	for (idx_t col_idx = 0; col_idx < local_names.size(); col_idx++) {
		name_map[local_names[col_idx]] = col_idx;
	}
	for (idx_t i = 0; i < global_column_ids.size(); i++) {
		// check if this is a constant column
		bool constant = false;
		for (auto &entry : reader_data.constant_map) {
			if (entry.column_id == i) {
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
		if (global_id >= global_types.size()) {
			throw InternalException(
			    "MultiFileReader::CreatePositionalMapping - global_id is out of range in global_types for this file");
		}
		auto &global_name = global_names[global_id];
		auto entry = name_map.find(global_name);
		if (entry == name_map.end()) {
			string candidate_names;
			for (auto &local_name : local_names) {
				if (!candidate_names.empty()) {
					candidate_names += ", ";
				}
				candidate_names += local_name;
			}
			throw IOException(
			    StringUtil::Format("Failed to read file \"%s\": schema mismatch in glob: column \"%s\" was read from "
			                       "the original file \"%s\", but could not be found in file \"%s\".\nCandidate names: "
			                       "%s\nIf you are trying to "
			                       "read files with different schemas, try setting union_by_name=True",
			                       file_name, global_name, initial_file, file_name, candidate_names));
		}
		// we found the column in the local file - check if the types are the same
		auto local_id = entry->second;
		D_ASSERT(global_id < global_types.size());
		D_ASSERT(local_id < local_types.size());
		auto &global_type = global_types[global_id];
		auto &local_type = local_types[local_id];
		if (global_type != local_type) {
			reader_data.cast_map[local_id] = global_type;
		}
		// the types are the same - create the mapping
		reader_data.column_mapping.push_back(i);
		reader_data.column_ids.push_back(local_id);
	}
	reader_data.empty_columns = reader_data.column_ids.empty();
}

void MultiFileReader::CreateMapping(const string &file_name, const vector<LogicalType> &local_types,
                                    const vector<string> &local_names, const vector<LogicalType> &global_types,
                                    const vector<string> &global_names, const vector<column_t> &global_column_ids,
                                    optional_ptr<TableFilterSet> filters, MultiFileReaderData &reader_data,
                                    const string &initial_file) {
	CreateNameMapping(file_name, local_types, local_names, global_types, global_names, global_column_ids, reader_data,
	                  initial_file);
	if (filters) {
		reader_data.filter_map.resize(global_types.size());
		for (idx_t c = 0; c < reader_data.column_mapping.size(); c++) {
			auto map_index = reader_data.column_mapping[c];
			reader_data.filter_map[map_index].index = c;
			reader_data.filter_map[map_index].is_constant = false;
		}
		for (idx_t c = 0; c < reader_data.constant_map.size(); c++) {
			auto constant_index = reader_data.constant_map[c].column_id;
			reader_data.filter_map[constant_index].index = c;
			reader_data.filter_map[constant_index].is_constant = true;
		}
	}
}

void MultiFileReader::FinalizeChunk(const MultiFileReaderBindData &bind_data, const MultiFileReaderData &reader_data,
                                    DataChunk &chunk) {
	// reference all the constants set up in MultiFileReader::FinalizeBind
	for (auto &entry : reader_data.constant_map) {
		chunk.data[entry.column_id].Reference(entry.value);
	}
	chunk.Verify();
}

TableFunctionSet MultiFileReader::CreateFunctionSet(TableFunction table_function) {
	TableFunctionSet function_set(table_function.name);
	function_set.AddFunction(table_function);
	D_ASSERT(table_function.arguments.size() == 1 && table_function.arguments[0] == LogicalType::VARCHAR);
	table_function.arguments[0] = LogicalType::LIST(LogicalType::VARCHAR);
	function_set.AddFunction(std::move(table_function));
	return function_set;
}

HivePartitioningIndex::HivePartitioningIndex(string value_p, idx_t index) : value(std::move(value_p)), index(index) {
}

void MultiFileReaderOptions::AddBatchInfo(BindInfo &bind_info) const {
	bind_info.InsertOption("filename", Value::BOOLEAN(filename));
	bind_info.InsertOption("hive_partitioning", Value::BOOLEAN(hive_partitioning));
	bind_info.InsertOption("auto_detect_hive_partitioning", Value::BOOLEAN(auto_detect_hive_partitioning));
	bind_info.InsertOption("union_by_name", Value::BOOLEAN(union_by_name));
	bind_info.InsertOption("hive_types_autocast", Value::BOOLEAN(hive_types_autocast));
}

void UnionByName::CombineUnionTypes(const vector<string> &col_names, const vector<LogicalType> &sql_types,
                                    vector<LogicalType> &union_col_types, vector<string> &union_col_names,
                                    case_insensitive_map_t<idx_t> &union_names_map) {
	D_ASSERT(col_names.size() == sql_types.size());

	for (idx_t col = 0; col < col_names.size(); ++col) {
		auto union_find = union_names_map.find(col_names[col]);

		if (union_find != union_names_map.end()) {
			// given same name , union_col's type must compatible with col's type
			auto &current_type = union_col_types[union_find->second];
			LogicalType compatible_type;
			compatible_type = LogicalType::MaxLogicalType(current_type, sql_types[col]);
			union_col_types[union_find->second] = compatible_type;
		} else {
			union_names_map[col_names[col]] = union_col_names.size();
			union_col_names.emplace_back(col_names[col]);
			union_col_types.emplace_back(sql_types[col]);
		}
	}
}

bool MultiFileReaderOptions::AutoDetectHivePartitioningInternal(const vector<string> &files, ClientContext &context) {
	std::unordered_set<string> partitions;
	auto &fs = FileSystem::GetFileSystem(context);

	auto splits_first_file = StringUtil::Split(files.front(), fs.PathSeparator(files.front()));
	if (splits_first_file.size() < 2) {
		return false;
	}
	for (auto it = splits_first_file.begin(); it != splits_first_file.end(); it++) {
		auto partition = StringUtil::Split(*it, "=");
		if (partition.size() == 2) {
			partitions.insert(partition.front());
		}
	}
	if (partitions.empty()) {
		return false;
	}
	for (auto &file : files) {
		auto splits = StringUtil::Split(file, fs.PathSeparator(file));
		if (splits.size() != splits_first_file.size()) {
			return false;
		}
		for (auto it = splits.begin(); it != std::prev(splits.end()); it++) {
			auto part = StringUtil::Split(*it, "=");
			if (part.size() != 2) {
				continue;
			}
			if (partitions.find(part.front()) == partitions.end()) {
				return false;
			}
		}
	}
	return true;
}
void MultiFileReaderOptions::AutoDetectHiveTypesInternal(const string &file, ClientContext &context) {
	auto &fs = FileSystem::GetFileSystem(context);

	std::map<string, string> partitions;
	auto splits = StringUtil::Split(file, fs.PathSeparator(file));
	if (splits.size() < 2) {
		return;
	}
	for (auto it = splits.begin(); it != std::prev(splits.end()); it++) {
		auto part = StringUtil::Split(*it, "=");
		if (part.size() == 2) {
			partitions[part.front()] = part.back();
		}
	}
	if (partitions.empty()) {
		return;
	}

	const LogicalType candidates[] = {LogicalType::DATE, LogicalType::TIMESTAMP, LogicalType::BIGINT};
	for (auto &part : partitions) {
		const string &name = part.first;
		if (hive_types_schema.find(name) != hive_types_schema.end()) {
			continue;
		}
		Value value(part.second);
		for (auto &candidate : candidates) {
			const bool success = value.TryCastAs(context, candidate);
			if (success) {
				hive_types_schema[name] = candidate;
				break;
			}
		}
	}
}
void MultiFileReaderOptions::AutoDetectHivePartitioning(const vector<string> &files, ClientContext &context) {
	D_ASSERT(!files.empty());
	const bool hp_explicitly_disabled = !auto_detect_hive_partitioning && !hive_partitioning;
	const bool ht_enabled = !hive_types_schema.empty();
	if (hp_explicitly_disabled && ht_enabled) {
		throw InvalidInputException("cannot disable hive_partitioning when hive_types is enabled");
	}
	if (ht_enabled && auto_detect_hive_partitioning && !hive_partitioning) {
		// hive_types flag implies hive_partitioning
		hive_partitioning = true;
		auto_detect_hive_partitioning = false;
	}
	if (auto_detect_hive_partitioning) {
		hive_partitioning = AutoDetectHivePartitioningInternal(files, context);
	}
	if (hive_partitioning && hive_types_autocast) {
		AutoDetectHiveTypesInternal(files.front(), context);
	}
}
void MultiFileReaderOptions::VerifyHiveTypesArePartitions(const std::map<string, string> &partitions) const {
	for (auto &hive_type : hive_types_schema) {
		if (partitions.find(hive_type.first) == partitions.end()) {
			throw InvalidInputException("Unknown hive_type: \"%s\" does not appear to be a partition", hive_type.first);
		}
	}
}
LogicalType MultiFileReaderOptions::GetHiveLogicalType(const string &hive_partition_column) const {
	if (!hive_types_schema.empty()) {
		auto it = hive_types_schema.find(hive_partition_column);
		if (it != hive_types_schema.end()) {
			return it->second;
		}
	}
	return LogicalType::VARCHAR;
}
Value MultiFileReaderOptions::GetHivePartitionValue(const string &base, const string &entry,
                                                    ClientContext &context) const {
	Value value(base);
	auto it = hive_types_schema.find(entry);
	if (it == hive_types_schema.end()) {
		return value;
	}

	// Handle nulls
	if (base.empty() || StringUtil::CIEquals(base, "NULL")) {
		return Value(it->second);
	}

	if (!value.TryCastAs(context, it->second)) {
		throw InvalidInputException("Unable to cast '%s' (from hive partition column '%s') to: '%s'", value.ToString(),
		                            StringUtil::Upper(it->first), it->second.ToString());
	}
	return value;
}

} // namespace duckdb

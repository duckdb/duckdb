#include "duckdb/common/multi_file_reader.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/common/string_util.hpp"

#include <algorithm>

namespace duckdb {

MultiFileReaderGlobalState::~MultiFileReaderGlobalState() {
}

MultiFileReader::~MultiFileReader() {
}

unique_ptr<MultiFileReader> MultiFileReader::Create(const TableFunction &table_function) {
	unique_ptr<MultiFileReader> res;
	if (table_function.get_multi_file_reader) {
		res = table_function.get_multi_file_reader();
		res->function_name = table_function.name;
	} else {
		res = make_uniq<MultiFileReader>();
		res->function_name = table_function.name;
	}
	return res;
}

unique_ptr<MultiFileReader> MultiFileReader::CreateDefault(const string &function_name) {
	auto res = make_uniq<MultiFileReader>();
	res->function_name = function_name;
	return res;
}

void MultiFileReader::AddParameters(TableFunction &table_function) {
	table_function.named_parameters["filename"] = LogicalType::ANY;
	table_function.named_parameters["hive_partitioning"] = LogicalType::BOOLEAN;
	table_function.named_parameters["union_by_name"] = LogicalType::BOOLEAN;
	table_function.named_parameters["hive_types"] = LogicalType::ANY;
	table_function.named_parameters["hive_types_autocast"] = LogicalType::BOOLEAN;
}

vector<string> MultiFileReader::ParsePaths(const Value &input) {
	if (input.IsNull()) {
		throw ParserException("%s cannot take NULL list as parameter", function_name);
	}

	if (input.type().id() == LogicalTypeId::VARCHAR) {
		return {StringValue::Get(input)};
	} else if (input.type().id() == LogicalTypeId::LIST) {
		vector<string> paths;
		for (auto &val : ListValue::GetChildren(input)) {
			if (val.IsNull()) {
				throw ParserException("%s reader cannot take NULL input as parameter", function_name);
			}
			if (val.type().id() != LogicalTypeId::VARCHAR) {
				throw ParserException("%s reader can only take a list of strings as a parameter", function_name);
			}
			paths.push_back(StringValue::Get(val));
		}
		return paths;
	} else {
		throw InternalException("Unsupported type for MultiFileReader::ParsePaths called with: '%s'");
	}
}

unique_ptr<MultiFileList> MultiFileReader::CreateFileList(ClientContext &context, const vector<string> &paths,
                                                          FileGlobOptions options) {
	auto &config = DBConfig::GetConfig(context);
	if (!config.options.enable_external_access) {
		throw PermissionException("Scanning %s files is disabled through configuration", function_name);
	}
	vector<string> result_files;

	auto res = make_uniq<GlobMultiFileList>(context, paths, options);
	if (res->GetExpandResult() == FileExpandResult::NO_FILES && options == FileGlobOptions::DISALLOW_EMPTY) {
		throw IOException("%s needs at least one file to read", function_name);
	}
	return std::move(res);
}

unique_ptr<MultiFileList> MultiFileReader::CreateFileList(ClientContext &context, const Value &input,
                                                          FileGlobOptions options) {
	auto paths = ParsePaths(input);
	return CreateFileList(context, paths, options);
}

bool MultiFileReader::ParseOption(const string &key, const Value &val, MultiFileReaderOptions &options,
                                  ClientContext &context) {
	auto loption = StringUtil::Lower(key);
	if (loption == "filename") {
		if (val.type() == LogicalType::VARCHAR) {
			// If not, we interpret it as the name of the column containing the filename
			options.filename = true;
			options.filename_column = StringValue::Get(val);
		} else {
			Value boolean_value;
			string error_message;
			if (val.DefaultTryCastAs(LogicalType::BOOLEAN, boolean_value, &error_message)) {
				// If the argument can be cast to boolean, we just interpret it as a boolean
				options.filename = BooleanValue::Get(boolean_value);
			}
		}
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

unique_ptr<MultiFileList> MultiFileReader::ComplexFilterPushdown(ClientContext &context, MultiFileList &files,
                                                                 const MultiFileReaderOptions &options, LogicalGet &get,
                                                                 vector<unique_ptr<Expression>> &filters) {
	return files.ComplexFilterPushdown(context, options, get, filters);
}

bool MultiFileReader::Bind(MultiFileReaderOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
                           vector<string> &names, MultiFileReaderBindData &bind_data) {
	// The Default MultiFileReader can not perform any binding as it uses MultiFileLists with no schema information.
	return false;
}

void MultiFileReader::BindOptions(MultiFileReaderOptions &options, MultiFileList &files,
                                  vector<LogicalType> &return_types, vector<string> &names,
                                  MultiFileReaderBindData &bind_data) {
	// Add generated constant column for filename
	if (options.filename) {
		if (std::find(names.begin(), names.end(), options.filename_column) != names.end()) {
			throw BinderException("Option filename adds column \"%s\", but a column with this name is also in the "
			                      "file. Try setting a different name: filename='<filename column name>'",
			                      options.filename_column);
		}
		bind_data.filename_idx = names.size();
		return_types.emplace_back(LogicalType::VARCHAR);
		names.emplace_back(options.filename_column);
	}

	// Add generated constant columns from hive partitioning scheme
	if (options.hive_partitioning) {
		D_ASSERT(files.GetExpandResult() != FileExpandResult::NO_FILES);
		auto partitions = HivePartitioning::Parse(files.GetFirstFile());
		// verify that all files have the same hive partitioning scheme
		for (const auto &file : files.Files()) {
			auto file_partitions = HivePartitioning::Parse(file);
			for (auto &part_info : partitions) {
				if (file_partitions.find(part_info.first) == file_partitions.end()) {
					string error = "Hive partition mismatch between file \"%s\" and \"%s\": key \"%s\" not found";
					if (options.auto_detect_hive_partitioning == true) {
						throw InternalException(error + "(hive partitioning was autodetected)", files.GetFirstFile(),
						                        file, part_info.first);
					}
					throw BinderException(error.c_str(), files.GetFirstFile(), file, part_info.first);
				}
			}
			if (partitions.size() != file_partitions.size()) {
				string error_msg = "Hive partition mismatch between file \"%s\" and \"%s\"";
				if (options.auto_detect_hive_partitioning == true) {
					throw InternalException(error_msg + "(hive partitioning was autodetected)", files.GetFirstFile(),
					                        file);
				}
				throw BinderException(error_msg.c_str(), files.GetFirstFile(), file);
			}
		}

		if (!options.hive_types_schema.empty()) {
			// verify that all hive_types are existing partitions
			options.VerifyHiveTypesArePartitions(partitions);
		}

		for (auto &part : partitions) {
			idx_t hive_partitioning_index;
			auto lookup = std::find(names.begin(), names.end(), part.first);
			if (lookup != names.end()) {
				// hive partitioning column also exists in file - override
				auto idx = NumericCast<idx_t>(lookup - names.begin());
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
}

void MultiFileReader::FinalizeBind(const MultiFileReaderOptions &file_options, const MultiFileReaderBindData &options,
                                   const string &filename, const vector<string> &local_names,
                                   const vector<LogicalType> &global_types, const vector<string> &global_names,
                                   const vector<column_t> &global_column_ids, MultiFileReaderData &reader_data,
                                   ClientContext &context, optional_ptr<MultiFileReaderGlobalState> global_state) {

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

unique_ptr<MultiFileReaderGlobalState>
MultiFileReader::InitializeGlobalState(ClientContext &context, const MultiFileReaderOptions &file_options,
                                       const MultiFileReaderBindData &bind_data, const MultiFileList &file_list,
                                       const vector<LogicalType> &global_types, const vector<string> &global_names,
                                       const vector<column_t> &global_column_ids) {
	// By default, the multifilereader does not require any global state
	return nullptr;
}

void MultiFileReader::CreateNameMapping(const string &file_name, const vector<LogicalType> &local_types,
                                        const vector<string> &local_names, const vector<LogicalType> &global_types,
                                        const vector<string> &global_names, const vector<column_t> &global_column_ids,
                                        MultiFileReaderData &reader_data, const string &initial_file,
                                        optional_ptr<MultiFileReaderGlobalState> global_state) {
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
                                    const string &initial_file, const MultiFileReaderBindData &options,
                                    optional_ptr<MultiFileReaderGlobalState> global_state) {
	CreateNameMapping(file_name, local_types, local_names, global_types, global_names, global_column_ids, reader_data,
	                  initial_file, global_state);
	CreateFilterMap(global_types, filters, reader_data, global_state);
}

void MultiFileReader::CreateFilterMap(const vector<LogicalType> &global_types, optional_ptr<TableFilterSet> filters,
                                      MultiFileReaderData &reader_data,
                                      optional_ptr<MultiFileReaderGlobalState> global_state) {
	if (filters) {
		auto filter_map_size = global_types.size();
		if (global_state) {
			filter_map_size += global_state->extra_columns.size();
		}
		reader_data.filter_map.resize(filter_map_size);

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

void MultiFileReader::FinalizeChunk(ClientContext &context, const MultiFileReaderBindData &bind_data,
                                    const MultiFileReaderData &reader_data, DataChunk &chunk,
                                    optional_ptr<MultiFileReaderGlobalState> global_state) {
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
	bind_info.InsertOption("filename", Value(filename_column));
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
			auto compatible_type = LogicalType::ForceMaxLogicalType(current_type, sql_types[col]);
			union_col_types[union_find->second] = compatible_type;
		} else {
			union_names_map[col_names[col]] = union_col_names.size();
			union_col_names.emplace_back(col_names[col]);
			union_col_types.emplace_back(sql_types[col]);
		}
	}
}

bool MultiFileReaderOptions::AutoDetectHivePartitioningInternal(MultiFileList &files, ClientContext &context) {
	std::unordered_set<string> partitions;
	auto &fs = FileSystem::GetFileSystem(context);

	auto first_file = files.GetFirstFile();
	auto splits_first_file = StringUtil::Split(first_file, fs.PathSeparator(first_file));
	if (splits_first_file.size() < 2) {
		return false;
	}
	for (auto &split : splits_first_file) {
		auto partition = StringUtil::Split(split, "=");
		if (partition.size() == 2) {
			partitions.insert(partition.front());
		}
	}
	if (partitions.empty()) {
		return false;
	}

	for (const auto &file : files.Files()) {
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
void MultiFileReaderOptions::AutoDetectHiveTypesInternal(MultiFileList &files, ClientContext &context) {
	const LogicalType candidates[] = {LogicalType::DATE, LogicalType::TIMESTAMP, LogicalType::BIGINT};

	auto &fs = FileSystem::GetFileSystem(context);

	unordered_map<string, LogicalType> detected_types;
	for (const auto &file : files.Files()) {
		unordered_map<string, string> partitions;
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

		for (auto &part : partitions) {
			const string &name = part.first;
			if (hive_types_schema.find(name) != hive_types_schema.end()) {
				// type was explicitly provided by the user
				continue;
			}
			LogicalType detected_type = LogicalType::VARCHAR;
			Value value(part.second);
			for (auto &candidate : candidates) {
				const bool success = value.TryCastAs(context, candidate, true);
				if (success) {
					detected_type = candidate;
					break;
				}
			}
			auto entry = detected_types.find(name);
			if (entry == detected_types.end()) {
				// type was not yet detected - insert it
				detected_types.insert(make_pair(name, std::move(detected_type)));
			} else {
				// type was already detected - check if the type matches
				// if not promote to VARCHAR
				if (entry->second != detected_type) {
					entry->second = LogicalType::VARCHAR;
				}
			}
		}
	}
	for (auto &entry : detected_types) {
		hive_types_schema.insert(make_pair(entry.first, std::move(entry.second)));
	}
}
void MultiFileReaderOptions::AutoDetectHivePartitioning(MultiFileList &files, ClientContext &context) {
	D_ASSERT(files.GetExpandResult() != FileExpandResult::NO_FILES);
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
		AutoDetectHiveTypesInternal(files, context);
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

bool MultiFileReaderOptions::AnySet() {
	return filename || hive_partitioning || union_by_name;
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

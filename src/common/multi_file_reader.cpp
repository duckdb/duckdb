#include "duckdb/common/multi_file_reader.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/filter/list.hpp"

#include <algorithm>

namespace duckdb {

constexpr column_t MultiFileReader::COLUMN_IDENTIFIER_FILENAME;

MultiFileReaderGlobalState::~MultiFileReaderGlobalState() {
}

MultiFileReader::~MultiFileReader() {
}

unique_ptr<MultiFileReader> MultiFileReader::Create(const TableFunction &table_function) {
	unique_ptr<MultiFileReader> res;
	if (table_function.get_multi_file_reader) {
		res = table_function.get_multi_file_reader(table_function);
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

Value MultiFileReader::CreateValueFromFileList(const vector<string> &file_list) {
	vector<Value> files;
	for (auto &file : file_list) {
		files.push_back(file);
	}
	return Value::LIST(LogicalType::VARCHAR, std::move(files));
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

shared_ptr<MultiFileList> MultiFileReader::CreateFileList(ClientContext &context, const vector<string> &paths,
                                                          FileGlobOptions options) {
	auto res = make_uniq<GlobMultiFileList>(context, paths, options);
	if (res->GetExpandResult() == FileExpandResult::NO_FILES && options == FileGlobOptions::DISALLOW_EMPTY) {
		throw IOException("%s needs at least one file to read", function_name);
	}
	return std::move(res);
}

shared_ptr<MultiFileList> MultiFileReader::CreateFileList(ClientContext &context, const Value &input,
                                                          FileGlobOptions options) {
	auto paths = ParsePaths(input);
	return CreateFileList(context, paths, options);
}

bool MultiFileReader::ParseOption(const string &key, const Value &val, MultiFileReaderOptions &options,
                                  ClientContext &context) {
	auto loption = StringUtil::Lower(key);
	if (loption == "filename") {
		if (val.IsNull()) {
			throw InvalidInputException("Cannot use NULL as argument for \"%s\"", key);
		}
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
		if (val.IsNull()) {
			throw InvalidInputException("Cannot use NULL as argument for \"%s\"", key);
		}
		options.hive_partitioning = BooleanValue::Get(val);
		options.auto_detect_hive_partitioning = false;
	} else if (loption == "union_by_name") {
		if (val.IsNull()) {
			throw InvalidInputException("Cannot use NULL as argument for \"%s\"", key);
		}
		options.union_by_name = BooleanValue::Get(val);
	} else if (loption == "hive_types_autocast" || loption == "hive_type_autocast") {
		if (val.IsNull()) {
			throw InvalidInputException("Cannot use NULL as argument for \"%s\"", key);
		}
		options.hive_types_autocast = BooleanValue::Get(val);
	} else if (loption == "hive_types" || loption == "hive_type") {
		if (val.IsNull()) {
			throw InvalidInputException("Cannot use NULL as argument for \"%s\"", key);
		}
		if (val.type().id() != LogicalTypeId::STRUCT) {
			throw InvalidInputException(
			    "'hive_types' only accepts a STRUCT('name':VARCHAR, ...), but '%s' was provided",
			    val.type().ToString());
		}
		// verify that all the children of the struct value are VARCHAR
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
                                                                 const MultiFileReaderOptions &options,
                                                                 MultiFilePushdownInfo &info,
                                                                 vector<unique_ptr<Expression>> &filters) {
	return files.ComplexFilterPushdown(context, options, info, filters);
}

unique_ptr<MultiFileList> MultiFileReader::DynamicFilterPushdown(ClientContext &context, const MultiFileList &files,
                                                                 const MultiFileReaderOptions &options,
                                                                 const vector<string> &names,
                                                                 const vector<LogicalType> &types,
                                                                 const vector<column_t> &column_ids,
                                                                 TableFilterSet &filters) {
	return files.DynamicFilterPushdown(context, options, names, types, column_ids, filters);
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
			auto lookup = std::find_if(names.begin(), names.end(), [&](const string &col_name) {
				return StringUtil::CIEquals(col_name, part.first);
			});
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

void MultiFileReader::GetVirtualColumns(ClientContext &context, MultiFileReaderBindData &bind_data,
                                        virtual_column_map_t &result) {
	if (bind_data.filename_idx == DConstants::INVALID_INDEX || bind_data.filename_idx == COLUMN_IDENTIFIER_FILENAME) {
		bind_data.filename_idx = COLUMN_IDENTIFIER_FILENAME;
		result.insert(make_pair(COLUMN_IDENTIFIER_FILENAME, TableColumn("filename", LogicalType::VARCHAR)));
	}
}

void MultiFileReader::FinalizeBind(const MultiFileReaderOptions &file_options, const MultiFileReaderBindData &options,
                                   const string &filename, const vector<MultiFileReaderColumnDefinition> &local_columns,
                                   const vector<MultiFileReaderColumnDefinition> &global_columns,
                                   const vector<ColumnIndex> &global_column_ids, MultiFileReaderData &reader_data,
                                   ClientContext &context, optional_ptr<MultiFileReaderGlobalState> global_state) {

	// create a map of name -> column index
	case_insensitive_map_t<idx_t> name_map;
	if (file_options.union_by_name) {
		for (idx_t col_idx = 0; col_idx < local_columns.size(); col_idx++) {
			auto &column = local_columns[col_idx];
			name_map[column.name] = col_idx;
		}
	}
	for (idx_t i = 0; i < global_column_ids.size(); i++) {
		auto global_idx = MultiFileGlobalIndex(i);
		auto &col_id = global_column_ids[i];
		auto column_id = col_id.GetPrimaryIndex();
		if (column_id == options.filename_idx) {
			// filename
			reader_data.constant_map.Add(global_idx, Value(filename));
			continue;
		}
		if (IsVirtualColumn(column_id)) {
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
					reader_data.constant_map.Add(global_idx, value);
					found_partition = true;
					break;
				}
			}
			if (found_partition) {
				continue;
			}
		}
		if (file_options.union_by_name) {
			auto &column = global_columns[column_id];
			auto &name = column.name;
			auto &type = column.type;

			auto entry = name_map.find(name);
			bool not_present_in_file = entry == name_map.end();
			if (not_present_in_file) {
				// we need to project a column with name \"global_name\" - but it does not exist in the current file
				// push a NULL value of the specified type
				reader_data.constant_map.Add(global_idx, Value(type));
				continue;
			}
		}
	}
}

unique_ptr<MultiFileReaderGlobalState>
MultiFileReader::InitializeGlobalState(ClientContext &context, const MultiFileReaderOptions &file_options,
                                       const MultiFileReaderBindData &bind_data, const MultiFileList &file_list,
                                       const vector<MultiFileReaderColumnDefinition> &global_columns,
                                       const vector<ColumnIndex> &global_column_ids) {
	// By default, the multifilereader does not require any global state
	return nullptr;
}

void MultiFileReader::CreateColumnMappingByName(const string &file_name,
                                                const vector<MultiFileReaderColumnDefinition> &local_columns,
                                                const vector<MultiFileReaderColumnDefinition> &global_columns,
                                                const vector<ColumnIndex> &global_column_ids,
                                                MultiFileReaderData &reader_data,
                                                const MultiFileReaderBindData &bind_data,
                                                const virtual_column_map_t &virtual_columns, const string &initial_file,
                                                optional_ptr<MultiFileReaderGlobalState> global_state) {

	// we have expected types: create a map of name -> (local) column id
	case_insensitive_map_t<MultiFileLocalColumnId> name_map;
	for (idx_t col_idx = 0; col_idx < local_columns.size(); col_idx++) {
		auto &column = local_columns[col_idx];
		name_map.emplace(column.name, MultiFileLocalColumnId(col_idx));
	}

	auto &expressions = reader_data.expressions;
	for (idx_t i = 0; i < global_column_ids.size(); i++) {
		auto global_idx = MultiFileGlobalIndex(i);
		// check if this is a constant column
		optional_idx constant_idx;
		for (idx_t j = 0; j < reader_data.constant_map.size(); j++) {
			auto constant_index = MultiFileConstantMapIndex(j);
			auto &entry = reader_data.constant_map[constant_index];
			if (entry.column_idx.GetIndex() == i) {
				constant_idx = j;
				break;
			}
		}
		if (constant_idx.IsValid()) {
			// this column is constant for this file
			auto constant_index = MultiFileConstantMapIndex(constant_idx.GetIndex());
			auto &constant_entry = reader_data.constant_map[constant_index];
			expressions.push_back(make_uniq<BoundConstantExpression>(constant_entry.value));
			continue;
		}
		// not constant - look up the column in the name map
		auto &global_id = global_column_ids[i];
		auto global_column_id = global_id.GetPrimaryIndex();
		if (IsVirtualColumn(global_column_id)) {
			// virtual column - these are emitted for every file
			auto virtual_entry = virtual_columns.find(global_column_id);
			if (virtual_entry == virtual_columns.end()) {
				throw InternalException("Virtual column id %d not found in virtual columns map", global_column_id);
			}
			expressions.push_back(make_uniq<BoundConstantExpression>(Value(virtual_entry->second.type)));
			continue;
		}
		if (global_column_id >= global_columns.size()) {
			throw InternalException(
			    "MultiFileReader::CreateColumnMappingByName - global_id is out of range in global_types for this file");
		}
		auto &global_column = global_columns[global_column_id];
		auto identifier = global_column.GetIdentifierName();
		auto entry = name_map.find(identifier);
		if (entry == name_map.end()) {
			// identiier not present in file, use default value
			if (global_column.default_expression) {
				reader_data.constant_map.Add(global_idx, global_column.GetDefaultValue());
				expressions.push_back(make_uniq<BoundConstantExpression>(global_column.GetDefaultValue()));
				continue;
			} else {
				string candidate_names;
				for (auto &column : local_columns) {
					if (!candidate_names.empty()) {
						candidate_names += ", ";
					}
					candidate_names += column.name;
				}
				throw IOException(StringUtil::Format(
				    "Failed to read file \"%s\": schema mismatch in glob: column \"%s\" was read from "
				    "the original file \"%s\", but could not be found in file \"%s\".\nCandidate names: "
				    "%s\nIf you are trying to "
				    "read files with different schemas, try setting union_by_name=True",
				    file_name, identifier, initial_file, file_name, candidate_names));
			}
		}
		// we found the column in the local file - check if the types are the same
		auto local_id = entry->second;
		D_ASSERT(global_column_id < global_columns.size());
		D_ASSERT(local_id.GetId() < local_columns.size());
		auto &global_type = global_columns[global_column_id].type;
		auto &local_type = local_columns[local_id.GetId()].type;
		ColumnIndex local_index(local_id.GetId());

		auto local_idx = reader_data.column_ids.size();
		auto expected_type = local_type;
		if (global_type != local_type) {
			// the types are not the same - add a cast
			reader_data.cast_map[local_id] = global_type;
			expected_type = global_type;
		} else {
			//! FIXME: local fields are not guaranteed to match with the global fields for this struct
			local_index = ColumnIndex(local_id.GetId(), global_id.GetChildIndexes());
		}
		expressions.push_back(make_uniq<BoundReferenceExpression>(expected_type, local_idx));
		// create the mapping
		reader_data.column_mapping.push_back(global_idx);
		reader_data.column_ids.push_back(local_id);
		reader_data.column_indexes.push_back(std::move(local_index));
	}
	D_ASSERT(global_column_ids.size() == reader_data.expressions.size());

	reader_data.empty_columns = reader_data.column_indexes.empty();
}

void MultiFileReader::CreateColumnMappingByFieldId(
    const string &file_name, const vector<MultiFileReaderColumnDefinition> &local_columns,
    const vector<MultiFileReaderColumnDefinition> &global_columns, const vector<ColumnIndex> &global_column_ids,
    MultiFileReaderData &reader_data, const MultiFileReaderBindData &bind_data,
    const virtual_column_map_t &virtual_columns, const string &initial_file,
    optional_ptr<MultiFileReaderGlobalState> global_state) {
#ifdef DEBUG
	//! Make sure the global columns have field_ids to match on
	for (auto &column : global_columns) {
		D_ASSERT(!column.identifier.IsNull());
		D_ASSERT(column.identifier.type().id() == LogicalTypeId::INTEGER);
	}
#endif

	// we have expected types: create a map of field_id -> column index
	unordered_map<int32_t, MultiFileLocalColumnId> field_id_map;
	for (idx_t col_idx = 0; col_idx < local_columns.size(); col_idx++) {
		auto &column = local_columns[col_idx];
		if (column.identifier.IsNull()) {
			// Extra columns at the end will not have a field_id
			break;
		}
		auto field_id = column.GetIdentifierFieldId();
		field_id_map.emplace(field_id, MultiFileLocalColumnId(col_idx));
	}

	// loop through the schema definition
	auto &expressions = reader_data.expressions;
	for (idx_t i = 0; i < global_column_ids.size(); i++) {
		auto global_idx = MultiFileGlobalIndex(i);

		optional_idx constant_idx;
		for (idx_t j = 0; j < reader_data.constant_map.size(); j++) {
			auto constant_index = MultiFileConstantMapIndex(j);
			auto &entry = reader_data.constant_map[constant_index];
			if (entry.column_idx.GetIndex() == i) {
				constant_idx = j;
				break;
			}
		}
		if (constant_idx.IsValid()) {
			// this column is constant for this file
			auto constant_index = MultiFileConstantMapIndex(constant_idx.GetIndex());
			auto &constant_entry = reader_data.constant_map[constant_index];
			expressions.push_back(make_uniq<BoundConstantExpression>(constant_entry.value));
			continue;
		}

		// Handle any generate columns that are not in the schema (currently only file_row_number)
		auto &global_id = global_column_ids[i];
		auto global_column_id = global_column_ids[i].GetPrimaryIndex();

		if (IsVirtualColumn(global_column_id)) {
			// virtual column - these are emitted for every file
			auto virtual_entry = virtual_columns.find(global_column_id);
			if (virtual_entry == virtual_columns.end()) {
				throw InternalException("Virtual column id %d not found in virtual columns map", global_column_id);
			}
			expressions.push_back(make_uniq<BoundConstantExpression>(Value(virtual_entry->second.type)));
			continue;
		}

		auto local_idx = MultiFileLocalIndex(reader_data.column_ids.size());
		if (global_column_id >= global_columns.size()) {
			if (bind_data.file_row_number_idx == global_column_id) {
				reader_data.column_mapping.push_back(MultiFileGlobalIndex(i));
				// FIXME: this needs a more extensible solution
				auto new_column_id = MultiFileLocalColumnId(field_id_map.size());
				reader_data.column_ids.push_back(new_column_id);
				reader_data.column_indexes.emplace_back(field_id_map.size());
				//! FIXME: what to do here???
				expressions.push_back(make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, local_idx));
			} else {
				throw InternalException("Unexpected generated column");
			}
			continue;
		}

		const auto &global_column = global_columns[global_column_id];
		D_ASSERT(!global_column.identifier.IsNull());
		auto it = field_id_map.find(global_column.GetIdentifierFieldId());
		if (it == field_id_map.end()) {
			// field id not present in file, use default value
			auto &default_val = global_column.default_expression;
			D_ASSERT(default_val);
			if (default_val->type != ExpressionType::VALUE_CONSTANT) {
				throw NotImplementedException("Default expression that isn't constant is not supported yet");
			}
			auto &constant_expr = default_val->Cast<ConstantExpression>();
			expressions.push_back(make_uniq<BoundConstantExpression>(constant_expr.value));
			reader_data.constant_map.Add(global_idx, constant_expr.value);
			continue;
		}

		const auto &local_id = it->second;
		auto &local_column = local_columns[local_id.GetId()];
		ColumnIndex local_index(local_id.GetId());

		auto expected_type = local_column.type;
		if (local_column.type != global_column.type) {
			// differing types, wrap in a cast column reader
			reader_data.cast_map[local_id] = global_column.type;
			expected_type = global_column.type;
		} else {
			//! FIXME: local fields are not guaranteed to match with the global fields for this struct
			local_index = ColumnIndex(local_id.GetId(), global_id.GetChildIndexes());
		}
		expressions.push_back(make_uniq<BoundReferenceExpression>(expected_type, local_idx));

		reader_data.column_mapping.push_back(MultiFileGlobalIndex(i));
		reader_data.column_ids.push_back(local_id);
		reader_data.column_indexes.push_back(std::move(local_index));
	}
	D_ASSERT(global_column_ids.size() == reader_data.expressions.size());

	reader_data.empty_columns = reader_data.column_ids.empty();
}

void MultiFileReader::CreateColumnMapping(const string &file_name,
                                          const vector<MultiFileReaderColumnDefinition> &local_columns,
                                          const vector<MultiFileReaderColumnDefinition> &global_columns,
                                          const vector<ColumnIndex> &global_column_ids,
                                          MultiFileReaderData &reader_data, const MultiFileReaderBindData &bind_data,
                                          const virtual_column_map_t &virtual_columns, const string &initial_file,
                                          optional_ptr<MultiFileReaderGlobalState> global_state) {
	switch (bind_data.mapping) {
	case MultiFileReaderColumnMappingMode::BY_NAME: {
		CreateColumnMappingByName(file_name, local_columns, global_columns, global_column_ids, reader_data, bind_data,
		                          virtual_columns, initial_file, global_state);
		break;
	}
	case MultiFileReaderColumnMappingMode::BY_FIELD_ID: {
		CreateColumnMappingByFieldId(file_name, local_columns, global_columns, global_column_ids, reader_data,
		                             bind_data, virtual_columns, initial_file, global_state);
		break;
	}
	default: {
		throw InternalException("Unsupported MultiFileReaderColumnMappingMode type");
	}
	}
}

static bool EvaluateFilterAgainstConstant(TableFilter &filter, const Value &constant) {
	const auto type = filter.filter_type;

	switch (type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		if (constant.IsNull()) {
			return false;
		}
		return constant_filter.Compare(constant);
	}
	case TableFilterType::IS_NULL: {
		return constant.IsNull();
	}
	case TableFilterType::IS_NOT_NULL: {
		return !constant.IsNull();
	}
	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		for (auto &val : in_filter.values) {
			if (!constant.IsNull() && val == constant) {
				return true;
			}
		}
		return false;
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &or_filter = filter.Cast<ConjunctionOrFilter>();
		for (auto &it : or_filter.child_filters) {
			if (EvaluateFilterAgainstConstant(*it, constant)) {
				return true;
			}
		}
		return false;
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &and_filter = filter.Cast<ConjunctionAndFilter>();
		auto res = make_uniq<ConjunctionAndFilter>();
		for (auto &it : and_filter.child_filters) {
			if (!EvaluateFilterAgainstConstant(*it, constant)) {
				return false;
			}
		}
		return true;
	}
	case TableFilterType::STRUCT_EXTRACT: {
		auto &struct_filter = filter.Cast<StructFilter>();
		auto &child_filter = struct_filter.child_filter;

		if (constant.type().id() != LogicalTypeId::STRUCT) {
			throw InternalException(
			    "Constant for this column is not of type struct, but used in a STRUCT_EXTRACT TableFilter");
		}
		auto &struct_fields = StructValue::GetChildren(constant);
		auto field_index = struct_filter.child_idx;
		if (field_index >= struct_fields.size()) {
			throw InternalException("STRUCT_EXTRACT looks for child_idx %d, but constant only has %d children",
			                        field_index, struct_fields.size());
		}
		auto &field_name = StructType::GetChildName(constant.type(), field_index);
		if (!StringUtil::CIEquals(field_name, struct_filter.child_name)) {
			throw InternalException("STRUCT_EXTRACT looks for a child with name '%s' at index %d, but constant has a "
			                        "field with '%s' as the name for that index",
			                        struct_filter.child_name, field_index, field_name);
		}
		auto &child_constant = struct_fields[field_index];
		return EvaluateFilterAgainstConstant(*child_filter, child_constant);
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		if (optional_filter.child_filter) {
			return EvaluateFilterAgainstConstant(*optional_filter.child_filter, constant);
		}
		return true;
	}
	case TableFilterType::DYNAMIC_FILTER: {
		auto &dynamic_filter = filter.Cast<DynamicFilter>();
		if (!dynamic_filter.filter_data) {
			//! No filter_data assigned (does this mean the DynamicFilter is broken??)
			return true;
		}
		if (!dynamic_filter.filter_data->initialized) {
			//! Not initialized
			return true;
		}
		if (!dynamic_filter.filter_data->filter) {
			//! No filter present
			return true;
		}
		return EvaluateFilterAgainstConstant(*dynamic_filter.filter_data->filter, constant);
	}
	default:
		throw NotImplementedException("Can't evaluate TableFilterType (%s) against a constant",
		                              EnumUtil::ToString(type));
	}
}

namespace {

struct EvaluationResult {
	//! Whether evaluation of any of the filters against the global constants failed.
	bool can_skip_file = false;
	//! The remaining filters that need to be converted to local filters.
	map<idx_t, reference<TableFilter>> remaining_filters;
};

} // namespace

static EvaluationResult EvaluateConstantFilters(optional_ptr<TableFilterSet> filters, const string &filename,
                                                const vector<MultiFileReaderColumnDefinition> &global_columns,
                                                const vector<ColumnIndex> &global_column_ids,
                                                const virtual_column_map_t &virtual_columns,
                                                MultiFileReaderData &reader_data,
                                                unordered_map<idx_t, MultiFileIndexMapping> &global_to_local) {
	EvaluationResult result;
	if (!filters) {
		return result;
	}

	for (auto &it : filters->filters) {
		auto &global_index = it.first;
		auto &global_filter = it.second;

		auto local_it = global_to_local.find(it.first);
		if (local_it != global_to_local.end()) {
			//! File has this column, filter needs to be evaluated later
			result.remaining_filters.emplace(global_index, *global_filter);
			continue;
		}

		//! FIXME: this does not check for filters against struct fields that are not present in the file
		auto global_column_id = global_column_ids[global_index].GetPrimaryIndex();
		Value constant_value;
		auto virtual_it = virtual_columns.find(global_column_ids[global_index].GetPrimaryIndex());
		if (virtual_it != virtual_columns.end()) {
			auto &virtual_column = virtual_it->second;
			if (virtual_column.name == "filename") {
				constant_value = Value(filename);
			} else {
				throw InternalException("Unrecognized virtual column found: %s", virtual_column.name);
			}
		} else {
			bool has_constant = false;
			for (idx_t i = 0; i < reader_data.constant_map.size(); i++) {
				auto &constant_map_entry = reader_data.constant_map[MultiFileConstantMapIndex(i)];
				if (constant_map_entry.column_idx.GetIndex() == global_index) {
					has_constant = true;
					constant_value = constant_map_entry.value;
					break;
				}
			}
			if (!has_constant) {
				auto &global_column = global_columns[global_column_id];
				throw InternalException(
				    "Column '%s' is not present in the file, but no constant_map entry exists for it!",
				    global_column.name);
			}
		}

		if (!EvaluateFilterAgainstConstant(*global_filter, constant_value)) {
			result.can_skip_file = true;
			return result;
		}
	}

	return result;
}

bool MultiFileReader::CreateMapping(const string &file_name,
                                    const vector<MultiFileReaderColumnDefinition> &local_columns,
                                    const vector<MultiFileReaderColumnDefinition> &global_columns,
                                    const vector<ColumnIndex> &global_column_ids, optional_ptr<TableFilterSet> filters,
                                    MultiFileReaderData &reader_data, const string &initial_file,
                                    const MultiFileReaderBindData &bind_data,
                                    const virtual_column_map_t &virtual_columns,
                                    optional_ptr<MultiFileReaderGlobalState> global_state) {
	// copy global columns and inject any different defaults
	CreateColumnMapping(file_name, local_columns, global_columns, global_column_ids, reader_data, bind_data,
	                    virtual_columns, initial_file, global_state);

	unordered_map<idx_t, MultiFileIndexMapping> global_to_local;
	for (idx_t i = 0; i < reader_data.column_mapping.size(); i++) {
		auto local_idx = MultiFileLocalIndex(i);
		auto global_idx = reader_data.column_mapping[local_idx];
		global_to_local.emplace(global_idx.GetIndex(), i);

		// auto &local_column_id = reader_data.column_indexes[local_idx];
		// auto &global_column_id = global_column_ids[global_idx];

		// auto &local_column = local_columns[local_column_id.GetPrimaryIndex()];
		// auto &global_column = global_columns[global_column_id.GetPrimaryIndex()];
		//! FIXME: The `column_indexes` created in the mapping methods are not respecting/expecting differences in
		//! struct schemas
	}

	//! Evaluate the filters against the column(s) that are constant for this file (not present in the local schema)
	//! If any of these fail, the file can be skipped entirely
	auto evaluation_result = EvaluateConstantFilters(filters, file_name, global_columns, global_column_ids,
	                                                 virtual_columns, reader_data, global_to_local);
	if (evaluation_result.can_skip_file) {
		return false;
	}

	reader_data.filters = CreateFilters(evaluation_result.remaining_filters, global_to_local);
	return true;
}

static unique_ptr<TableFilter> ConvertFilterFromGlobalToLocal(const TableFilter &global_filter,
                                                              MultiFileIndexMapping &mapping) {
	auto type = global_filter.filter_type;

	unique_ptr<TableFilter> result;
	switch (type) {
	case TableFilterType::CONSTANT_COMPARISON:
	case TableFilterType::IS_NULL:
	case TableFilterType::IS_NOT_NULL:
	case TableFilterType::IN_FILTER:
	case TableFilterType::DYNAMIC_FILTER:
		return global_filter.Copy();
	case TableFilterType::CONJUNCTION_OR: {
		auto &or_filter = global_filter.Cast<ConjunctionOrFilter>();
		auto res = make_uniq<ConjunctionOrFilter>();
		for (auto &it : or_filter.child_filters) {
			auto child_filter = ConvertFilterFromGlobalToLocal(*it, mapping);
			if (child_filter) {
				res->child_filters.push_back(std::move(child_filter));
			}
		}
		return std::move(res);
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &and_filter = global_filter.Cast<ConjunctionAndFilter>();
		auto res = make_uniq<ConjunctionAndFilter>();
		for (auto &it : and_filter.child_filters) {
			auto child_filter = ConvertFilterFromGlobalToLocal(*it, mapping);
			if (child_filter) {
				res->child_filters.push_back(std::move(child_filter));
			}
		}
		return std::move(res);
	}
	case TableFilterType::STRUCT_EXTRACT: {
		auto &struct_filter = global_filter.Cast<StructFilter>();
		auto &child_filter = struct_filter.child_filter;

		//! FXIME: The previous step should ensure that filters that target fields that are not present in the file are
		//! evaluated earlier
		//! For now we will assume the mapping is 1-to-1
		MultiFileIndexMapping mapping(struct_filter.child_idx);
		auto new_child_filter = ConvertFilterFromGlobalToLocal(*child_filter, mapping);
		if (!new_child_filter) {
			return nullptr;
		}
		//! TODO: renaming fields should probably be respected here?
		auto child_name = struct_filter.child_name;
		return make_uniq<StructFilter>(mapping.index, child_name, std::move(new_child_filter));
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = global_filter.Cast<OptionalFilter>();
		unique_ptr<TableFilter> child;
		if (optional_filter.child_filter) {
			child = ConvertFilterFromGlobalToLocal(*optional_filter.child_filter, mapping);
		}
		return make_uniq<OptionalFilter>(std::move(child));
	}
	default:
		throw NotImplementedException("Can't convert TableFilterType (%s) from global to local indexes",
		                              EnumUtil::ToString(type));
	}
	return result;
}

unique_ptr<TableFilterSet>
MultiFileReader::CreateFilters(map<idx_t, reference<TableFilter>> &filters,
                               unordered_map<idx_t, MultiFileIndexMapping> &global_to_local) {
	if (filters.empty()) {
		return nullptr;
	}

	auto result = make_uniq<TableFilterSet>();
	for (auto &it : filters) {
		auto &global_index = it.first;
		auto &global_filter = it.second.get();

		auto local_it = global_to_local.find(global_index);
		if (local_it == global_to_local.end()) {
			throw InternalException(
			    "Error in 'EvaluateConstantFilters', this filter should not end up in CreateFilters!");
		}
		auto &mapping = local_it->second;
		auto local_filter = ConvertFilterFromGlobalToLocal(global_filter, mapping);
		if (local_filter) {
			result->filters.emplace(mapping.index, std::move(local_filter));
		}
	}
	return result;
}

void MultiFileReader::FinalizeChunk(ClientContext &context, const MultiFileReaderBindData &bind_data,
                                    const MultiFileReaderData &reader_data, DataChunk &input_chunk,
                                    DataChunk &output_chunk, ExpressionExecutor &executor,
                                    optional_ptr<MultiFileReaderGlobalState> global_state) {
	executor.Execute(input_chunk, output_chunk);
	output_chunk.SetCardinality(input_chunk.size());
	output_chunk.Verify();
}

void MultiFileReader::GetPartitionData(ClientContext &context, const MultiFileReaderBindData &bind_data,
                                       const MultiFileReaderData &reader_data,
                                       optional_ptr<MultiFileReaderGlobalState> global_state,
                                       const OperatorPartitionInfo &partition_info,
                                       OperatorPartitionData &partition_data) {
	for (idx_t col : partition_info.partition_columns) {
		bool found_constant = false;
		for (auto &constant : reader_data.constant_map) {
			if (constant.column_idx.GetIndex() == col) {
				found_constant = true;
				partition_data.partition_data.emplace_back(constant.value);
				break;
			}
		}
		if (!found_constant) {
			throw InternalException(
			    "MultiFileReader::GetPartitionData - did not find constant for the given partition");
		}
	}
}

TablePartitionInfo MultiFileReader::GetPartitionInfo(ClientContext &context, const MultiFileReaderBindData &bind_data,
                                                     TableFunctionPartitionInput &input) {
	// check if all of the columns are in the hive partition set
	for (auto &partition_col : input.partition_ids) {
		// check if this column is in the hive partitioned set
		bool found = false;
		for (auto &partition : bind_data.hive_partitioning_indexes) {
			if (partition.index == partition_col) {
				found = true;
				break;
			}
		}
		if (!found) {
			// the column is not partitioned - hive partitioning alone can't guarantee the groups are partitioned
			return TablePartitionInfo::NOT_PARTITIONED;
		}
	}
	// if all columns are in the hive partitioning set, we know that each partition will only have a single value
	// i.e. if the hive partitioning is by (YEAR, MONTH), each partition will have a single unique (YEAR, MONTH)
	return TablePartitionInfo::SINGLE_VALUE_PARTITIONS;
}

TableFunctionSet MultiFileReader::CreateFunctionSet(TableFunction table_function) {
	TableFunctionSet function_set(table_function.name);
	function_set.AddFunction(table_function);
	D_ASSERT(!table_function.arguments.empty() && table_function.arguments[0] == LogicalType::VARCHAR);
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
	auto first_file = files.GetFirstFile();
	auto partitions = HivePartitioning::Parse(first_file);
	if (partitions.empty()) {
		// no partitions found in first file
		return false;
	}

	for (const auto &file : files.Files()) {
		auto new_partitions = HivePartitioning::Parse(file);
		if (new_partitions.size() != partitions.size()) {
			// partition count mismatch
			return false;
		}
		for (auto &part : new_partitions) {
			auto entry = partitions.find(part.first);
			if (entry == partitions.end()) {
				// differing partitions between files
				return false;
			}
		}
	}
	return true;
}
void MultiFileReaderOptions::AutoDetectHiveTypesInternal(MultiFileList &files, ClientContext &context) {
	const LogicalType candidates[] = {LogicalType::DATE, LogicalType::TIMESTAMP, LogicalType::BIGINT};

	unordered_map<string, LogicalType> detected_types;
	for (const auto &file : files.Files()) {
		auto partitions = HivePartitioning::Parse(file);
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
	if (files.GetExpandResult() == FileExpandResult::NO_FILES) {
		return;
	}
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

bool MultiFileReaderOptions::AnySet() const {
	return filename || hive_partitioning || union_by_name;
}

Value MultiFileReaderOptions::GetHivePartitionValue(const string &value, const string &key,
                                                    ClientContext &context) const {
	auto it = hive_types_schema.find(key);
	if (it == hive_types_schema.end()) {
		return HivePartitioning::GetValue(context, key, value, LogicalType::VARCHAR);
	}
	return HivePartitioning::GetValue(context, key, value, it->second);
}

} // namespace duckdb

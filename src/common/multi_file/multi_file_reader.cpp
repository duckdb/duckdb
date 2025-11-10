#include "duckdb/common/multi_file/multi_file_reader.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/multi_file/multi_file_column_mapper.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/common/multi_file/union_by_name.hpp"
#include <algorithm>

namespace duckdb {

constexpr column_t MultiFileReader::COLUMN_IDENTIFIER_FILENAME;
constexpr column_t MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER;
constexpr column_t MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX;
constexpr int32_t MultiFileReader::ORDINAL_FIELD_ID;
constexpr int32_t MultiFileReader::FILENAME_FIELD_ID;
constexpr int32_t MultiFileReader::ROW_ID_FIELD_ID;
constexpr int32_t MultiFileReader::LAST_UPDATED_SEQUENCE_NUMBER_ID;

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

unique_ptr<MultiFileReader> MultiFileReader::Copy() const {
	return CreateDefault(function_name);
}

MultiFileBindData::~MultiFileBindData() {
}

unique_ptr<FunctionData> MultiFileBindData::Copy() const {
	auto result = make_uniq<MultiFileBindData>();
	if (bind_data) {
		result->bind_data = unique_ptr_cast<FunctionData, TableFunctionData>(bind_data->Copy());
	}
	result->file_list = file_list->Copy();
	result->multi_file_reader = multi_file_reader->Copy();
	result->interface = interface->Copy();
	result->columns = columns;
	result->reader_bind = reader_bind;
	result->file_options = file_options;
	result->types = types;
	result->names = names;
	result->virtual_columns = virtual_columns;
	result->table_columns = table_columns;
	return std::move(result);
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
                                                          const FileGlobInput &glob_input) {
	vector<OpenFileInfo> open_files;
	for (auto &path : paths) {
		open_files.emplace_back(path);
	}
	auto res = make_uniq<GlobMultiFileList>(context, std::move(open_files), glob_input);
	if (res->GetExpandResult() == FileExpandResult::NO_FILES && glob_input.behavior != FileGlobOptions::ALLOW_EMPTY) {
		throw IOException("%s needs at least one file to read", function_name);
	}
	return std::move(res);
}

shared_ptr<MultiFileList> MultiFileReader::CreateFileList(ClientContext &context, const Value &input,
                                                          const FileGlobInput &glob_input) {
	auto paths = ParsePaths(input);
	return CreateFileList(context, paths, glob_input);
}

bool MultiFileReader::ParseOption(const string &key, const Value &val, MultiFileOptions &options,
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
                                                                 const MultiFileOptions &options,
                                                                 MultiFilePushdownInfo &info,
                                                                 vector<unique_ptr<Expression>> &filters) {
	return files.ComplexFilterPushdown(context, options, info, filters);
}

unique_ptr<MultiFileList> MultiFileReader::DynamicFilterPushdown(
    ClientContext &context, const MultiFileList &files, const MultiFileOptions &options, const vector<string> &names,
    const vector<LogicalType> &types, const vector<column_t> &column_ids, TableFilterSet &filters) {
	return files.DynamicFilterPushdown(context, options, names, types, column_ids, filters);
}

bool MultiFileReader::Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
                           vector<string> &names, MultiFileReaderBindData &bind_data) {
	// The Default MultiFileReader can not perform any binding as it uses MultiFileLists with no schema information.
	return false;
}

void MultiFileReader::BindOptions(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
                                  vector<string> &names, MultiFileReaderBindData &bind_data) {
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
		auto partitions = HivePartitioning::Parse(files.GetFirstFile().path);
		// verify that all files have the same hive partitioning scheme
		for (const auto &file : files.Files()) {
			auto file_partitions = HivePartitioning::Parse(file.path);
			for (auto &part_info : partitions) {
				if (file_partitions.find(part_info.first) == file_partitions.end()) {
					string error = "Hive partition mismatch between file \"%s\" and \"%s\": key \"%s\" not found";
					if (options.auto_detect_hive_partitioning == true) {
						throw InternalException(error + "(hive partitioning was autodetected)",
						                        files.GetFirstFile().path, file.path, part_info.first);
					}
					throw BinderException(error.c_str(), files.GetFirstFile().path, file.path, part_info.first);
				}
			}
			if (partitions.size() != file_partitions.size()) {
				string error_msg = "Hive partition mismatch between file \"%s\" and \"%s\"";
				if (options.auto_detect_hive_partitioning == true) {
					throw InternalException(error_msg + "(hive partitioning was autodetected)",
					                        files.GetFirstFile().path, file.path);
				}
				throw BinderException(error_msg.c_str(), files.GetFirstFile().path, file.path);
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
	if (!bind_data.filename_idx.IsValid() || bind_data.filename_idx == COLUMN_IDENTIFIER_FILENAME) {
		bind_data.filename_idx = COLUMN_IDENTIFIER_FILENAME;
		result.insert(make_pair(COLUMN_IDENTIFIER_FILENAME, TableColumn("filename", LogicalType::VARCHAR)));
	}
	result.insert(make_pair(COLUMN_IDENTIFIER_FILE_INDEX, TableColumn("file_index", LogicalType::UBIGINT)));
	result.insert(make_pair(COLUMN_IDENTIFIER_EMPTY, TableColumn("", LogicalType::BOOLEAN)));
}

void MultiFileReader::FinalizeBind(MultiFileReaderData &reader_data, const MultiFileOptions &file_options,
                                   const MultiFileReaderBindData &options,
                                   const vector<MultiFileColumnDefinition> &global_columns,
                                   const vector<ColumnIndex> &global_column_ids, ClientContext &context,
                                   optional_ptr<MultiFileReaderGlobalState> global_state) {
	// create a map of name -> column index
	auto &local_columns = reader_data.reader->GetColumns();
	auto &filename = reader_data.reader->GetFileName();
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
		if ((options.filename_idx.IsValid() && column_id == options.filename_idx.GetIndex()) ||
		    column_id == COLUMN_IDENTIFIER_FILENAME) {
			// filename
			reader_data.constant_map.Add(global_idx, Value(filename));
			continue;
		}
		if (column_id == COLUMN_IDENTIFIER_FILE_INDEX) {
			// filename
			reader_data.constant_map.Add(global_idx, Value::UBIGINT(reader_data.reader->file_list_idx.GetIndex()));
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
MultiFileReader::InitializeGlobalState(ClientContext &context, const MultiFileOptions &file_options,
                                       const MultiFileReaderBindData &bind_data, const MultiFileList &file_list,
                                       const vector<MultiFileColumnDefinition> &global_columns,
                                       const vector<ColumnIndex> &global_column_ids) {
	// By default, the multifilereader does not require any global state
	return nullptr;
}

ReaderInitializeType
MultiFileReader::CreateMapping(ClientContext &context, MultiFileReaderData &reader_data,
                               const vector<MultiFileColumnDefinition> &global_columns,
                               const vector<ColumnIndex> &global_column_ids, optional_ptr<TableFilterSet> filters,
                               MultiFileList &multi_file_list, const MultiFileReaderBindData &bind_data,
                               const virtual_column_map_t &virtual_columns, MultiFileColumnMappingMode mapping_mode) {
	MultiFileColumnMapper column_mapper(context, *this, reader_data, global_columns, global_column_ids, filters,
	                                    multi_file_list, virtual_columns);
	return column_mapper.CreateMapping(mapping_mode);
}

ReaderInitializeType MultiFileReader::CreateMapping(
    ClientContext &context, MultiFileReaderData &reader_data, const vector<MultiFileColumnDefinition> &global_columns,
    const vector<ColumnIndex> &global_column_ids, optional_ptr<TableFilterSet> filters, MultiFileList &multi_file_list,
    const MultiFileReaderBindData &bind_data, const virtual_column_map_t &virtual_columns) {
	return CreateMapping(context, reader_data, global_columns, global_column_ids, filters, multi_file_list, bind_data,
	                     virtual_columns, bind_data.mapping);
}

string GetExtendedMultiFileError(const MultiFileBindData &bind_data, const Expression &expr, BaseFileReader &reader,
                                 idx_t expr_idx, string &first_message) {
	if (expr.type != ExpressionType::OPERATOR_CAST) {
		// not a cast
		return string();
	}
	auto &cast_expr = expr.Cast<BoundCastExpression>();
	if (cast_expr.child->type != ExpressionType::BOUND_REF) {
		return string();
	}
	auto &ref = cast_expr.child->Cast<BoundReferenceExpression>();
	auto &source_type = ref.return_type;
	auto &target_type = cast_expr.return_type;
	auto &columns = reader.GetColumns();
	auto local_col_id = reader.column_indexes[ref.index].GetPrimaryIndex();
	auto &local_col = columns[local_col_id];

	auto reader_type = reader.GetReaderType();
	auto function_name = "read_" + StringUtil::Lower(reader_type);
	string extended_error;
	if (!bind_data.table_columns.empty()) {
		// COPY .. FROM
		string target_column;
		if (expr_idx < bind_data.table_columns.size()) {
			target_column = "\"" + bind_data.table_columns[expr_idx] + "\" ";
		}
		extended_error = StringUtil::Format(
		    "In file \"%s\" the column \"%s\" has type %s, but we are trying to load it into column %swith type "
		    "%s.\nThis means the %s schema does not match the schema of the table.\nPossible solutions:\n* Insert by "
		    "name instead of by position using \"INSERT INTO tbl BY NAME SELECT * FROM %s(...)\"\n* Manually specify "
		    "which columns to insert using \"INSERT INTO tbl SELECT ... FROM %s(...)\"",
		    reader.GetFileName(), local_col.name, source_type, target_column, target_type, reader_type, function_name,
		    function_name);
	} else {
		// read_parquet() with multiple files
		extended_error = StringUtil::Format(
		    "In file \"%s\" the column \"%s\" has type %s, but we are trying to read it as type %s."
		    "\nThis can happen when reading multiple %s files. The schema information is taken from "
		    "the first %s file by default. Possible solutions:\n"
		    "* Enable the union_by_name=True option to combine the schema of all %s files "
		    "(https://duckdb.org/docs/stable/data/multiple_files/combining_schemas)\n"
		    "* Use a COPY statement to automatically derive types from an existing table.",
		    reader.GetFileName(), local_col.name, source_type, target_type, reader_type, reader_type, reader_type);
	}
	first_message = StringUtil::Format("failed to cast column \"%s\" from type %s to %s: ", local_col.name, source_type,
	                                   target_type);
	return extended_error;
}

void MultiFileReader::FinalizeChunk(ClientContext &context, const MultiFileBindData &bind_data, BaseFileReader &reader,
                                    const MultiFileReaderData &reader_data, DataChunk &input_chunk,
                                    DataChunk &output_chunk, ExpressionExecutor &executor,
                                    optional_ptr<MultiFileReaderGlobalState> global_state) {
	executor.SetChunk(input_chunk);
	for (idx_t i = 0; i < executor.expressions.size(); i++) {
		try {
			executor.ExecuteExpression(i, output_chunk.data[i]);
		} catch (std::exception &ex) {
			// error while converting - try to create a nice error message
			ErrorData error(ex);
			if (error.Type() == ExceptionType::INTERNAL) {
				throw;
			}
			auto &original_error = error.RawMessage();
			string first_message;
			string extended_error =
			    GetExtendedMultiFileError(bind_data, *executor.expressions[i], reader, i, first_message);
			throw ConversionException("Error while reading file \"%s\": %s: %s\n\n%s", reader.GetFileName(),
			                          first_message, original_error, extended_error);
		}
	}
	output_chunk.SetCardinality(input_chunk.size());
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

unique_ptr<Expression> MultiFileReader::GetConstantVirtualColumn(MultiFileReaderData &reader_data, idx_t column_id,
                                                                 const LogicalType &type) {
	if (column_id == COLUMN_IDENTIFIER_EMPTY || column_id == COLUMN_IDENTIFIER_FILENAME) {
		return make_uniq<BoundConstantExpression>(Value(type));
	}
	return nullptr;
}

unique_ptr<Expression> MultiFileReader::GetVirtualColumnExpression(ClientContext &, MultiFileReaderData &,
                                                                   const vector<MultiFileColumnDefinition> &,
                                                                   idx_t &column_id, const LogicalType &type,
                                                                   MultiFileLocalIndex local_idx,
                                                                   optional_ptr<MultiFileColumnDefinition> &) {
	return make_uniq<BoundReferenceExpression>(type, local_idx.GetIndex());
}

MultiFileReaderBindData MultiFileReader::BindUnionReader(ClientContext &context, vector<LogicalType> &return_types,
                                                         vector<string> &names, MultiFileList &files,
                                                         MultiFileBindData &result, BaseFileReaderOptions &options,
                                                         MultiFileOptions &file_options) {
	D_ASSERT(file_options.union_by_name);
	vector<string> union_col_names;
	vector<LogicalType> union_col_types;

	// obtain the set of union column names + types by unifying the types of all of the files
	// note that this requires opening readers for each file and reading the metadata of each file
	// note also that it requires fully expanding the MultiFileList
	auto materialized_file_list = files.GetAllFiles();
	auto union_readers = UnionByName::UnionCols(context, materialized_file_list, union_col_types, union_col_names,
	                                            options, file_options, *this, *result.interface);

	std::move(union_readers.begin(), union_readers.end(), std::back_inserter(result.union_readers));
	// perform the binding on the obtained set of names + types
	MultiFileReaderBindData bind_data;
	BindOptions(file_options, files, union_col_types, union_col_names, bind_data);
	names = union_col_names;
	return_types = union_col_types;
	result.Initialize(context, *result.union_readers[0]);
	D_ASSERT(names.size() == return_types.size());
	return bind_data;
}

MultiFileReaderBindData MultiFileReader::BindReader(ClientContext &context, vector<LogicalType> &return_types,
                                                    vector<string> &names, MultiFileList &files,
                                                    MultiFileBindData &result, BaseFileReaderOptions &options,
                                                    MultiFileOptions &file_options) {
	if (file_options.union_by_name) {
		return BindUnionReader(context, return_types, names, files, result, options, file_options);
	} else {
		shared_ptr<BaseFileReader> reader;
		reader = CreateReader(context, files.GetFirstFile(), options, file_options, *result.interface);
		auto &columns = reader->GetColumns();
		for (auto &column : columns) {
			return_types.emplace_back(column.type);
			names.emplace_back(column.name);
		}
		result.Initialize(std::move(reader));
		MultiFileReaderBindData bind_data;
		BindOptions(file_options, files, return_types, names, bind_data);
		return bind_data;
	}
}

ReaderInitializeType MultiFileReader::InitializeReader(MultiFileReaderData &reader_data,
                                                       const MultiFileBindData &bind_data,
                                                       const vector<MultiFileColumnDefinition> &global_columns,
                                                       const vector<ColumnIndex> &global_column_ids,
                                                       optional_ptr<TableFilterSet> table_filters,
                                                       ClientContext &context, MultiFileGlobalState &gstate) {
	FinalizeBind(reader_data, bind_data.file_options, bind_data.reader_bind, global_columns, global_column_ids, context,
	             gstate.multi_file_reader_state.get());
	return CreateMapping(context, reader_data, global_columns, global_column_ids, table_filters, gstate.file_list,
	                     bind_data.reader_bind, bind_data.virtual_columns);
}

shared_ptr<BaseFileReader> MultiFileReader::CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
                                                         BaseUnionData &union_data,
                                                         const MultiFileBindData &bind_data) {
	return bind_data.interface->CreateReader(context, gstate, union_data, bind_data);
}

shared_ptr<BaseFileReader> MultiFileReader::CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
                                                         const OpenFileInfo &file, idx_t file_idx,
                                                         const MultiFileBindData &bind_data) {
	return bind_data.interface->CreateReader(context, gstate, file, file_idx, bind_data);
}

shared_ptr<BaseFileReader> MultiFileReader::CreateReader(ClientContext &context, const OpenFileInfo &file,
                                                         BaseFileReaderOptions &options,
                                                         const MultiFileOptions &file_options,
                                                         MultiFileReaderInterface &interface) {
	return interface.CreateReader(context, file, options, file_options);
}

void MultiFileReader::PruneReaders(MultiFileBindData &data, MultiFileList &file_list) {
	unordered_set<string> file_set;

	// Avoid materializing the file list if there's nothing to prune
	if (!data.initial_reader && data.union_readers.empty()) {
		return;
	}

	for (const auto &file : file_list.Files()) {
		file_set.insert(file.path);
	}

	if (data.initial_reader) {
		// check if the initial reader should still be read
		auto entry = file_set.find(data.initial_reader->GetFileName());
		if (entry == file_set.end()) {
			data.initial_reader.reset();
		}
	}
	for (idx_t r = 0; r < data.union_readers.size(); r++) {
		if (!data.union_readers[r]) {
			data.union_readers.erase_at(r);
			r--;
			continue;
		}
		// check if the union reader should still be read or not
		auto entry = file_set.find(data.union_readers[r]->GetFileName());
		if (entry == file_set.end()) {
			data.union_readers.erase_at(r);
			r--;
			continue;
		}
	}
}

FileGlobInput MultiFileReader::GetGlobInput(MultiFileReaderInterface &interface) {
	return interface.GetGlobInput();
}

HivePartitioningIndex::HivePartitioningIndex(string value_p, idx_t index) : value(std::move(value_p)), index(index) {
}

void MultiFileOptions::AddBatchInfo(BindInfo &bind_info) const {
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

bool MultiFileOptions::AutoDetectHivePartitioningInternal(MultiFileList &files, ClientContext &context) {
	auto first_file = files.GetFirstFile();
	auto partitions = HivePartitioning::Parse(first_file.path);
	if (partitions.empty()) {
		// no partitions found in first file
		return false;
	}

	for (const auto &file : files.Files()) {
		auto new_partitions = HivePartitioning::Parse(file.path);
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
void MultiFileOptions::AutoDetectHiveTypesInternal(MultiFileList &files, ClientContext &context) {
	const LogicalType candidates[] = {LogicalType::DATE, LogicalType::TIMESTAMP, LogicalType::BIGINT};

	unordered_map<string, LogicalType> detected_types;
	for (const auto &file : files.Files()) {
		auto partitions = HivePartitioning::Parse(file.path);
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
void MultiFileOptions::AutoDetectHivePartitioning(MultiFileList &files, ClientContext &context) {
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
void MultiFileOptions::VerifyHiveTypesArePartitions(const std::map<string, string> &partitions) const {
	for (auto &hive_type : hive_types_schema) {
		if (partitions.find(hive_type.first) == partitions.end()) {
			throw InvalidInputException("Unknown hive_type: \"%s\" does not appear to be a partition", hive_type.first);
		}
	}
}
LogicalType MultiFileOptions::GetHiveLogicalType(const string &hive_partition_column) const {
	if (!hive_types_schema.empty()) {
		auto it = hive_types_schema.find(hive_partition_column);
		if (it != hive_types_schema.end()) {
			return it->second;
		}
	}
	return LogicalType::VARCHAR;
}

bool MultiFileOptions::AnySet() const {
	return filename || hive_partitioning || union_by_name;
}

Value MultiFileOptions::GetHivePartitionValue(const string &value, const string &key, ClientContext &context) const {
	auto it = hive_types_schema.find(key);
	if (it == hive_types_schema.end()) {
		return HivePartitioning::GetValue(context, key, value, LogicalType::VARCHAR);
	}
	return HivePartitioning::GetValue(context, key, value, it->second);
}

} // namespace duckdb

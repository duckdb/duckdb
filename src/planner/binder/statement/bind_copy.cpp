#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/bind_helpers.hpp"
#include "duckdb/common/filename_pattern.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression_binder/table_function_binder.hpp"
#include "duckdb/common/algorithm.hpp"

#include "duckdb/main/extension_entries.hpp"

namespace duckdb {

static bool GetBooleanArg(ClientContext &context, const vector<Value> &arg) {
	return arg.empty() || arg[0].CastAs(context, LogicalType::BOOLEAN).GetValue<bool>();
}

void IsFormatExtensionKnown(const string &format) {
	for (auto &file_postfixes : EXTENSION_FILE_POSTFIXES) {
		if (format == file_postfixes.name + 1) {
			// It's a match, we must throw
			throw CatalogException(
			    "Copy Function with name \"%s\" is not in the catalog, but it exists in the %s extension.", format,
			    std::string(file_postfixes.extension));
		}
	}
}

case_insensitive_map_t<CopyOption> Binder::GetFullCopyOptionsList(const CopyFunction &function, CopyOptionMode mode) {
	case_insensitive_map_t<CopyOption> copy_options;
	CopyOptionsInput input(copy_options);
	function.copy_options(context, input);

	// first erase all options that don't match this type
	if (mode != CopyOptionMode::READ_WRITE) {
		vector<string> erased_options;
		for (auto &entry : copy_options) {
			if (entry.second.mode == CopyOptionMode::READ_WRITE) {
				// used for both
				continue;
			}
			if (entry.second.mode != mode) {
				erased_options.push_back(entry.first);
			}
		}
		for (auto &erased : erased_options) {
			copy_options.erase(erased);
		}
	}

	// now we have a list of all options for this copy type
	// add generic options
	if (mode != CopyOptionMode::READ_ONLY) {
		copy_options["use_tmp_file"] = CopyOption(LogicalType::BOOLEAN, CopyOptionMode::WRITE_ONLY);
		copy_options["overwrite_or_ignore"] = CopyOption(LogicalType::BOOLEAN, CopyOptionMode::WRITE_ONLY);
		copy_options["overwrite"] = CopyOption(LogicalType::BOOLEAN, CopyOptionMode::WRITE_ONLY);
		copy_options["append"] = CopyOption(LogicalType::BOOLEAN, CopyOptionMode::WRITE_ONLY);
		copy_options["filename_pattern"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::WRITE_ONLY);
		copy_options["file_extension"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::WRITE_ONLY);
		copy_options["per_thread_output"] = CopyOption(LogicalType::BOOLEAN, CopyOptionMode::WRITE_ONLY);
		copy_options["file_size_bytes"] = CopyOption(LogicalType::ANY, CopyOptionMode::WRITE_ONLY);
		copy_options["partition_by"] = CopyOption(LogicalType::ANY, CopyOptionMode::WRITE_ONLY);
		copy_options["return_files"] = CopyOption(LogicalType::BOOLEAN, CopyOptionMode::WRITE_ONLY);
		copy_options["preserve_order"] = CopyOption(LogicalType::BOOLEAN, CopyOptionMode::WRITE_ONLY);
		copy_options["return_stats"] = CopyOption(LogicalType::BOOLEAN, CopyOptionMode::WRITE_ONLY);
		copy_options["write_partition_columns"] = CopyOption(LogicalType::BOOLEAN, CopyOptionMode::WRITE_ONLY);
		copy_options["write_empty_file"] = CopyOption(LogicalType::BOOLEAN, CopyOptionMode::WRITE_ONLY);
		copy_options["hive_file_pattern"] = CopyOption(LogicalType::BOOLEAN, CopyOptionMode::WRITE_ONLY);
	}
	return copy_options;
}

BoundStatement Binder::BindCopyTo(CopyStatement &stmt, const CopyFunction &function, CopyToType copy_to_type) {
	if (function.plan) {
		// plan rewrite COPY TO
		return function.plan(*this, stmt);
	}

	auto &copy_info = *stmt.info;
	// bind the select statement
	auto node_copy = copy_info.select_statement->Copy();
	auto select_node = Bind(*node_copy);

	if (!function.copy_to_bind) {
		throw NotImplementedException("COPY TO is not supported for FORMAT \"%s\"", stmt.info->format);
	}

	bool use_tmp_file = true;
	CopyOverwriteMode overwrite_mode = CopyOverwriteMode::COPY_ERROR_ON_CONFLICT;
	FilenamePattern filename_pattern;
	bool user_set_use_tmp_file = false;
	bool per_thread_output = false;
	optional_idx file_size_bytes;
	vector<idx_t> partition_cols;
	bool seen_overwrite_mode = false;
	bool seen_filepattern = false;
	bool write_partition_columns = false;
	bool write_empty_file = true;
	bool hive_file_pattern = true;
	PreserveOrderType preserve_order = PreserveOrderType::AUTOMATIC;
	CopyFunctionReturnType return_type = CopyFunctionReturnType::CHANGED_ROWS;

	CopyFunctionBindInput bind_input(*stmt.info, function.function_info);

	bind_input.file_extension = function.extension;

	auto original_options = stmt.info->options;
	stmt.info->options.clear();
	for (auto &option : original_options) {
		auto loption = StringUtil::Lower(option.first);
		if (loption == "use_tmp_file") {
			use_tmp_file = GetBooleanArg(context, option.second);
			user_set_use_tmp_file = true;
		} else if (loption == "overwrite_or_ignore" || loption == "overwrite" || loption == "append") {
			if (seen_overwrite_mode) {
				throw BinderException("Can only set one of OVERWRITE_OR_IGNORE, OVERWRITE or APPEND");
			}
			seen_overwrite_mode = true;

			auto boolean = GetBooleanArg(context, option.second);
			if (boolean) {
				if (loption == "overwrite_or_ignore") {
					overwrite_mode = CopyOverwriteMode::COPY_OVERWRITE_OR_IGNORE;
				} else if (loption == "overwrite") {
					overwrite_mode = CopyOverwriteMode::COPY_OVERWRITE;
				} else if (loption == "append") {
					if (!seen_filepattern) {
						filename_pattern.SetFilenamePattern("{uuid}");
					}
					overwrite_mode = CopyOverwriteMode::COPY_APPEND;
				}
			}
		} else if (loption == "filename_pattern") {
			if (option.second.empty()) {
				throw IOException("FILENAME_PATTERN cannot be empty");
			}
			filename_pattern.SetFilenamePattern(
			    option.second[0].CastAs(context, LogicalType::VARCHAR).GetValue<string>());
			seen_filepattern = true;
		} else if (loption == "file_extension") {
			if (option.second.empty()) {
				throw IOException("FILE_EXTENSION cannot be empty");
			}
			bind_input.file_extension = option.second[0].CastAs(context, LogicalType::VARCHAR).GetValue<string>();
		} else if (loption == "per_thread_output") {
			per_thread_output = GetBooleanArg(context, option.second);
		} else if (loption == "file_size_bytes") {
			if (option.second.empty()) {
				throw BinderException("FILE_SIZE_BYTES cannot be empty");
			}
			if (!function.rotate_files) {
				throw NotImplementedException("FILE_SIZE_BYTES not implemented for FORMAT \"%s\"", stmt.info->format);
			}
			if (option.second[0].GetTypeMutable().id() == LogicalTypeId::VARCHAR) {
				file_size_bytes = DBConfig::ParseMemoryLimit(option.second[0].ToString());
			} else {
				file_size_bytes = option.second[0].GetValue<uint64_t>();
			}
		} else if (loption == "partition_by") {
			auto converted = ConvertVectorToValue(std::move(option.second));
			partition_cols = ParseColumnsOrdered(converted, select_node.names, loption);
		} else if (loption == "return_files") {
			if (GetBooleanArg(context, option.second)) {
				return_type = CopyFunctionReturnType::CHANGED_ROWS_AND_FILE_LIST;
			}
		} else if (loption == "preserve_order") {
			if (GetBooleanArg(context, option.second)) {
				preserve_order = PreserveOrderType::PRESERVE_ORDER;
			} else {
				preserve_order = PreserveOrderType::DONT_PRESERVE_ORDER;
			}
		} else if (loption == "return_stats") {
			if (GetBooleanArg(context, option.second)) {
				return_type = CopyFunctionReturnType::WRITTEN_FILE_STATISTICS;
			}
		} else if (loption == "write_partition_columns") {
			write_partition_columns = GetBooleanArg(context, option.second);
		} else if (loption == "write_empty_file") {
			write_empty_file = GetBooleanArg(context, option.second);
		} else if (loption == "hive_file_pattern") {
			hive_file_pattern = GetBooleanArg(context, option.second);
		} else {
			stmt.info->options[option.first] = option.second;
		}
	}
	if (overwrite_mode == CopyOverwriteMode::COPY_APPEND && !filename_pattern.HasUUID()) {
		throw BinderException("APPEND mode requires a {uuid} label in filename_pattern");
	}
	if (user_set_use_tmp_file && per_thread_output) {
		throw NotImplementedException("Can't combine USE_TMP_FILE and PER_THREAD_OUTPUT for COPY");
	}
	if (user_set_use_tmp_file && file_size_bytes.IsValid()) {
		throw NotImplementedException("Can't combine USE_TMP_FILE and FILE_SIZE_BYTES for COPY");
	}
	if (user_set_use_tmp_file && !partition_cols.empty()) {
		throw NotImplementedException("Can't combine USE_TMP_FILE and PARTITION_BY for COPY");
	}
	if (per_thread_output && !partition_cols.empty()) {
		throw NotImplementedException("Can't combine PER_THREAD_OUTPUT and PARTITION_BY for COPY");
	}
	if (file_size_bytes.IsValid() && !partition_cols.empty()) {
		throw NotImplementedException("Can't combine FILE_SIZE_BYTES and PARTITION_BY for COPY");
	}
	if (!write_partition_columns) {
		if (partition_cols.size() == select_node.names.size()) {
			throw NotImplementedException("No column to write as all columns are specified as partition columns. "
			                              "WRITE_PARTITION_COLUMNS option can be used to write partition columns.");
		}
	}
	bool is_remote_file = FileSystem::IsRemoteFile(stmt.info->file_path);
	if (is_remote_file) {
		use_tmp_file = false;
	} else {
		auto &fs = FileSystem::GetFileSystem(context);
		bool is_file_and_exists = fs.FileExists(stmt.info->file_path);
		bool is_stdout = stmt.info->file_path == "/dev/stdout";
		if (!user_set_use_tmp_file) {
			use_tmp_file = is_file_and_exists && !per_thread_output && partition_cols.empty() && !is_stdout;
		}
	}

	// Allow the copy function to intercept the select list and types and push a new projection on top of the plan
	if (function.copy_to_select) {
		auto bindings = select_node.plan->GetColumnBindings();

		CopyToSelectInput input = {context, stmt.info->options, {}, copy_to_type};
		input.select_list.reserve(bindings.size());

		// Create column references for the select list
		for (idx_t i = 0; i < bindings.size(); i++) {
			auto &binding = bindings[i];
			auto &name = select_node.names[i];
			auto &type = select_node.types[i];
			input.select_list.push_back(make_uniq<BoundColumnRefExpression>(name, type, binding));
		}

		auto new_select_list = function.copy_to_select(input);
		if (!new_select_list.empty()) {
			// We have a new select list, create a projection on top of the current plan
			auto projection = make_uniq<LogicalProjection>(GenerateTableIndex(), std::move(new_select_list));
			projection->children.push_back(std::move(select_node.plan));
			projection->ResolveOperatorTypes();

			// Update the names and types of the select node
			select_node.names.clear();
			select_node.types.clear();
			for (auto &expr : projection->expressions) {
				select_node.names.push_back(expr->GetName());
				select_node.types.push_back(expr->return_type);
			}
			select_node.plan = std::move(projection);
		}
	}

	auto unique_column_names = select_node.names;
	QueryResult::DeduplicateColumns(unique_column_names);
	auto file_path = stmt.info->file_path;

	auto names_to_write =
	    LogicalCopyToFile::GetNamesWithoutPartitions(unique_column_names, partition_cols, write_partition_columns);
	auto types_to_write =
	    LogicalCopyToFile::GetTypesWithoutPartitions(select_node.types, partition_cols, write_partition_columns);
	auto function_data = function.copy_to_bind(context, bind_input, names_to_write, types_to_write);

	const auto rotate = function.rotate_files && function.rotate_files(*function_data, file_size_bytes);
	if (rotate) {
		if (!function.rotate_next_file) {
			throw InternalException("rotate_next_file not implemented for \"%s\"", function.extension);
		}
		if (user_set_use_tmp_file) {
			throw NotImplementedException(
			    "Can't combine USE_TMP_FILE and file rotation (e.g., ROW_GROUPS_PER_FILE) for COPY");
		}
		if (!partition_cols.empty()) {
			throw NotImplementedException(
			    "Can't combine file rotation (e.g., ROW_GROUPS_PER_FILE) and PARTITION_BY for COPY");
		}
	}
	if (!write_empty_file) {
		if (per_thread_output) {
			throw NotImplementedException("Can't combine WRITE_EMPTY_FILE false with PER_THREAD_OUTPUT");
		}
		if (!partition_cols.empty()) {
			throw NotImplementedException("Can't combine WRITE_EMPTY_FILE false with PARTITION_BY");
		}
	}
	if (return_type == CopyFunctionReturnType::WRITTEN_FILE_STATISTICS && !function.copy_to_get_written_statistics) {
		throw NotImplementedException("RETURN_STATS is not supported for the \"%s\" copy format", stmt.info->format);
	}

	// now create the copy information
	auto copy = make_uniq<LogicalCopyToFile>(function, std::move(function_data), std::move(stmt.info));
	copy->file_path = file_path;
	copy->use_tmp_file = use_tmp_file;
	copy->overwrite_mode = overwrite_mode;
	copy->filename_pattern = filename_pattern;
	copy->file_extension = bind_input.file_extension;
	copy->per_thread_output = per_thread_output;
	if (file_size_bytes.IsValid()) {
		copy->file_size_bytes = file_size_bytes;
	}
	copy->rotate = rotate;
	copy->partition_output = !partition_cols.empty();
	copy->write_partition_columns = write_partition_columns;
	copy->partition_columns = std::move(partition_cols);
	copy->write_empty_file = write_empty_file;
	copy->return_type = return_type;
	copy->preserve_order = preserve_order;
	copy->hive_file_pattern = hive_file_pattern;

	copy->names = unique_column_names;
	copy->expected_types = select_node.types;

	copy->AddChild(std::move(select_node.plan));

	auto &properties = GetStatementProperties();
	switch (copy->return_type) {
	case CopyFunctionReturnType::CHANGED_ROWS:
		properties.return_type = StatementReturnType::CHANGED_ROWS;
		break;
	case CopyFunctionReturnType::CHANGED_ROWS_AND_FILE_LIST:
	case CopyFunctionReturnType::WRITTEN_FILE_STATISTICS:
		properties.return_type = StatementReturnType::QUERY_RESULT;
		break;
	default:
		throw NotImplementedException("Unknown CopyFunctionReturnType");
	}

	BoundStatement result;
	result.names = GetCopyFunctionReturnNames(copy->return_type);
	result.types = GetCopyFunctionReturnLogicalTypes(copy->return_type);
	result.plan = std::move(copy);

	return result;
}

BoundStatement Binder::BindCopyFrom(CopyStatement &stmt, const CopyFunction &function) {
	BoundStatement result;
	result.types = {LogicalType::BIGINT};
	result.names = {"Count"};

	if (stmt.info->table.empty()) {
		throw ParserException("COPY FROM requires a table name to be specified");
	}
	if (!function.copy_from_bind) {
		throw NotImplementedException("COPY FROM is not supported for FORMAT \"%s\"", stmt.info->format);
	}
	// COPY FROM a file
	// generate an insert statement for the to-be-inserted table
	InsertStatement insert;
	insert.table = stmt.info->table;
	insert.schema = stmt.info->schema;
	insert.catalog = stmt.info->catalog;
	insert.columns = stmt.info->select_list;

	// bind the insert statement to the base table
	auto insert_statement = Bind(insert);
	D_ASSERT(insert_statement.plan->type == LogicalOperatorType::LOGICAL_INSERT);

	auto &bound_insert = insert_statement.plan->Cast<LogicalInsert>();

	// lookup the table to copy into
	BindSchemaOrCatalog(stmt.info->catalog, stmt.info->schema);
	auto &table =
	    Catalog::GetEntry<TableCatalogEntry>(context, stmt.info->catalog, stmt.info->schema, stmt.info->table);
	vector<string> expected_names;
	if (!bound_insert.column_index_map.empty()) {
		expected_names.resize(bound_insert.expected_types.size());
		for (auto &col : table.GetColumns().Physical()) {
			auto i = col.Physical();
			if (bound_insert.column_index_map[i] != DConstants::INVALID_INDEX) {
				expected_names[bound_insert.column_index_map[i]] = col.Name();
			}
		}
	} else {
		expected_names.reserve(bound_insert.expected_types.size());
		for (auto &col : table.GetColumns().Physical()) {
			expected_names.push_back(col.Name());
		}
	}
	auto copy_from_function = function.copy_from_function;
	CopyFromFunctionBindInput input(*stmt.info, copy_from_function);
	auto function_data = function.copy_from_bind(context, input, expected_names, bound_insert.expected_types);
	auto get = make_uniq<LogicalGet>(GenerateTableIndex(), std::move(copy_from_function), std::move(function_data),
	                                 bound_insert.expected_types, expected_names);
	for (idx_t i = 0; i < bound_insert.expected_types.size(); i++) {
		get->AddColumnId(i);
	}
	insert_statement.plan->children.push_back(std::move(get));
	result.plan = std::move(insert_statement.plan);
	return result;
}

vector<Value> BindCopyOption(ClientContext &context, TableFunctionBinder &option_binder, const string &name,
                             unique_ptr<ParsedExpression> &expr) {
	vector<Value> result;
	if (!expr) {
		return result;
	}
	if (expr->type == ExpressionType::STAR) {
		auto &star = expr->Cast<StarExpression>();
		// for compatibility with previous copy implementation - turn a raw * into a * string literal
		if (star.relation_name.empty() && star.exclude_list.empty() && star.replace_list.empty() &&
		    star.rename_list.empty() && !star.expr && !star.columns) {
			result.push_back("*");
			return result;
		}
	}
	auto bound_expr = option_binder.Bind(expr);
	auto val = ExpressionExecutor::EvaluateScalar(context, *bound_expr);
	if (val.IsNull()) {
		throw BinderException("NULL is not supported as a valid option for COPY option \"" + name + "\"");
	}
	if (val.type().id() == LogicalTypeId::STRUCT && StructType::IsUnnamed(val.type())) {
		// unpack unnamed structs into a list of options
		return StructValue::GetChildren(val);
	}
	result.push_back(std::move(val));
	return result;
}

string ExtractFormat(const string &file_path) {
	auto format = StringUtil::Lower(file_path);
	// We first remove extension suffixes
	if (StringUtil::EndsWith(format, CompressionExtensionFromType(FileCompressionType::GZIP))) {
		format = format.substr(0, format.size() - 3);
	} else if (StringUtil::EndsWith(format, CompressionExtensionFromType(FileCompressionType::ZSTD))) {
		format = format.substr(0, format.size() - 4);
	}
	// Now lets check for the last .
	size_t dot_pos = format.rfind('.');
	if (dot_pos == std::string::npos || dot_pos == format.length() - 1) {
		// No format found
		return "";
	}
	// We found something
	return format.substr(dot_pos + 1);
}

void Binder::BindCopyOptions(CopyInfo &info) {
	TableFunctionBinder option_binder(*this, context, "Copy", "Copy options");
	if (info.file_path_expression) {
		auto inputs = BindCopyOption(context, option_binder, "filename", info.file_path_expression);
		if (inputs.size() != 1 || inputs[0].type().id() != LogicalTypeId::VARCHAR) {
			throw InternalException("Unsupported parameter type for filename: expected e.g. TARGET 'file.parquet'");
		}
		if (!info.file_path.empty()) {
			throw InternalException("Both a file path and a file path expression were provided for COPY - only one of "
			                        "the two can be provided");
		}
		info.file_path = inputs[0].ToString();
		info.file_path_expression.reset();
	}
	for (auto &entry : info.parsed_options) {
		auto inputs = BindCopyOption(context, option_binder, entry.first, entry.second);
		if (StringUtil::CIEquals(entry.first, "format")) {
			// format specifier: interpret this option
			if (inputs.size() != 1 || inputs[0].type().id() != LogicalTypeId::VARCHAR) {
				throw ParserException("Unsupported parameter type for FORMAT: expected e.g. FORMAT 'csv', 'parquet'");
			}
			info.format = StringUtil::Lower(inputs[0].ToString());
			info.is_format_auto_detected = false;
			continue;
		}
		info.options[entry.first] = std::move(inputs);
	}
	if (info.is_format_auto_detected && info.format.empty()) {
		info.format = ExtractFormat(info.file_path);
	}
	info.parsed_options.clear();
}

BoundStatement Binder::Bind(CopyStatement &stmt, CopyToType copy_to_type) {
	// bind the copy options
	BindCopyOptions(*stmt.info);

	if (!stmt.info->is_from && !stmt.info->select_statement) {
		// copy table into file without a query
		// generate SELECT * FROM table;
		auto ref = make_uniq<BaseTableRef>();
		ref->catalog_name = stmt.info->catalog;
		ref->schema_name = stmt.info->schema;
		ref->table_name = stmt.info->table;

		auto statement = make_uniq<SelectNode>();
		statement->from_table = std::move(ref);
		if (!stmt.info->select_list.empty()) {
			for (auto &name : stmt.info->select_list) {
				statement->select_list.push_back(make_uniq<ColumnRefExpression>(name));
			}
		} else {
			statement->select_list.push_back(make_uniq<StarExpression>());
		}
		stmt.info->select_statement = std::move(statement);
	}

	// Let's first bind our format
	// lookup the format in the catalog
	auto on_entry_do =
	    stmt.info->is_format_auto_detected ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	CatalogEntryRetriever entry_retriever {context};
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto entry = catalog.GetEntry(entry_retriever, DEFAULT_SCHEMA,
	                              {CatalogType::COPY_FUNCTION_ENTRY, stmt.info->format}, on_entry_do);

	if (!entry) {
		IsFormatExtensionKnown(stmt.info->format);
		// If we did not find an entry, we default to a CSV
		entry = catalog.GetEntry(entry_retriever, DEFAULT_SCHEMA, {CatalogType::COPY_FUNCTION_ENTRY, "csv"},
		                         OnEntryNotFound::THROW_EXCEPTION);
	}
	auto &copy_function = entry->Cast<CopyFunctionCatalogEntry>();
	auto &function = copy_function.function;

	if (function.copy_options) {
		// list all copy options - then bind them and offer alternatives
		auto copy_mode = stmt.info->is_from ? CopyOptionMode::READ_ONLY : CopyOptionMode::WRITE_ONLY;
		auto copy_options = GetFullCopyOptionsList(function, CopyOptionMode::READ_WRITE);
		for (auto &provided_entry : stmt.info->options) {
			auto &provided_option = provided_entry.first;
			auto option_entry = copy_options.find(provided_option);
			if (option_entry == copy_options.end()) {
				// option not found - offer an alternative suggestion
				vector<string> candidates;
				for (auto &copy_entry : copy_options) {
					candidates.push_back(copy_entry.first);
				}
				string candidate_str = StringUtil::CandidatesMessage(
				    StringUtil::TopNJaroWinkler(candidates, provided_option), "Candidate options");

				throw NotImplementedException("Unrecognized option \"%s\" for %s\n%s", provided_option,
				                              stmt.info->format, candidate_str);
			}
			auto &copy_option = option_entry->second;
			// check if this matches the mode
			if (copy_option.mode != CopyOptionMode::READ_WRITE && copy_option.mode != copy_mode) {
				throw InvalidInputException("Option \"%s\" is not supported for %s - only for %s", provided_option,
				                            std::string(stmt.info->is_from ? "reading" : "writing"),
				                            std::string(stmt.info->is_from ? "writing" : "reading"));
			}
			if (copy_option.type.id() != LogicalTypeId::ANY) {
				if (provided_entry.second.empty()) {
					if (copy_option.type.id() == LogicalTypeId::BOOLEAN) {
						// boolean can be empty (e.g. "HEADER")
						continue;
					}
					throw InvalidInputException("Copy option \"%s\" requires an argument of type %s", provided_option,
					                            copy_option.type.ToString());
				}
				if (provided_entry.second.size() > 1) {
					throw InvalidInputException("Copy option \"%s\" did not expect a list as argument",
					                            provided_option);
				}
				auto &original_value = provided_entry.second[0];
				if (copy_option.type == original_value.type()) {
					// types match
					continue;
				}
				bool can_cast =
				    CastFunctionSet::ImplicitCastCost(context, original_value.type(), copy_option.type) >= 0;
				if (!can_cast) {
					// for backwards compatibility - we are more lax on casting rules for copy options
					if (copy_option.type.IsNumeric()) {
						can_cast = original_value.type().IsNumeric();
					} else if (copy_option.type.id() == LogicalTypeId::BOOLEAN) {
						can_cast = original_value.type().IsIntegral();
					}
					if (original_value.type().id() == LogicalTypeId::VARCHAR) {
						can_cast = true;
					}
				}

				Value new_value;
				if (!can_cast || !original_value.TryCastAs(context, copy_option.type, new_value, nullptr)) {
					throw InvalidInputException("Copy option \"%s\" expected an argument of type %s - the argument "
					                            "\"%s\" of type %s could not be cast as this type",
					                            provided_option, copy_option.type, original_value.ToString(),
					                            original_value.type());
				}
				original_value = std::move(new_value);
			}
		}
	}

	auto &properties = GetStatementProperties();
	properties.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	properties.return_type = StatementReturnType::CHANGED_ROWS;
	if (stmt.info->is_from) {
		return BindCopyFrom(stmt, function);
	} else {
		return BindCopyTo(stmt, function, copy_to_type);
	}
}

} // namespace duckdb

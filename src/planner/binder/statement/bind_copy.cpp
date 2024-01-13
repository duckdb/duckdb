#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/bind_helpers.hpp"
#include "duckdb/common/filename_pattern.hpp"
#include "duckdb/common/local_file_system.hpp"
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

#include <algorithm>

namespace duckdb {

vector<string> GetUniqueNames(const vector<string> &original_names) {
	unordered_set<string> name_set;
	vector<string> unique_names;
	unique_names.reserve(original_names.size());

	for (auto &name : original_names) {
		auto insert_result = name_set.insert(name);
		if (insert_result.second == false) {
			// Could not be inserted, name already exists
			idx_t index = 1;
			string postfixed_name;
			while (true) {
				postfixed_name = StringUtil::Format("%s:%d", name, index);
				auto res = name_set.insert(postfixed_name);
				if (!res.second) {
					index++;
					continue;
				}
				break;
			}
			unique_names.push_back(postfixed_name);
		} else {
			unique_names.push_back(name);
		}
	}
	return unique_names;
}

static bool GetBooleanArg(ClientContext &context, const vector<Value> &arg) {
	return arg.empty() || arg[0].CastAs(context, LogicalType::BOOLEAN).GetValue<bool>();
}

BoundStatement Binder::BindCopyTo(CopyStatement &stmt) {
	// COPY TO a file
	auto &config = DBConfig::GetConfig(context);
	if (!config.options.enable_external_access) {
		throw PermissionException("COPY TO is disabled by configuration");
	}
	BoundStatement result;
	result.types = {LogicalType::BIGINT};
	result.names = {"Count"};

	// lookup the format in the catalog
	auto &copy_function =
	    Catalog::GetEntry<CopyFunctionCatalogEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA, stmt.info->format);
	if (copy_function.function.plan) {
		// plan rewrite COPY TO
		return copy_function.function.plan(*this, stmt);
	}

	// bind the select statement
	auto select_node = Bind(*stmt.select_statement);

	if (!copy_function.function.copy_to_bind) {
		throw NotImplementedException("COPY TO is not supported for FORMAT \"%s\"", stmt.info->format);
	}
	bool use_tmp_file = true;
	bool overwrite_or_ignore = false;
	FilenamePattern filename_pattern;
	string file_extension = copy_function.function.extension;
	bool user_set_use_tmp_file = false;
	bool per_thread_output = false;
	optional_idx file_size_bytes;
	vector<idx_t> partition_cols;

	auto original_options = stmt.info->options;
	stmt.info->options.clear();

	for (auto &option : original_options) {
		auto loption = StringUtil::Lower(option.first);
		if (loption == "use_tmp_file") {
			use_tmp_file = GetBooleanArg(context, option.second);
			user_set_use_tmp_file = true;
		} else if (loption == "overwrite_or_ignore") {
			overwrite_or_ignore = GetBooleanArg(context, option.second);
		} else if (loption == "filename_pattern") {
			if (option.second.empty()) {
				throw IOException("FILENAME_PATTERN cannot be empty");
			}
			filename_pattern.SetFilenamePattern(
			    option.second[0].CastAs(context, LogicalType::VARCHAR).GetValue<string>());
		} else if (loption == "file_extension") {
			if (option.second.empty()) {
				throw IOException("FILE_EXTENSION cannot be empty");
			}
			file_extension = option.second[0].CastAs(context, LogicalType::VARCHAR).GetValue<string>();
		} else if (loption == "per_thread_output") {
			per_thread_output = GetBooleanArg(context, option.second);
		} else if (loption == "file_size_bytes") {
			if (option.second.empty()) {
				throw BinderException("FILE_SIZE_BYTES cannot be empty");
			}
			if (!copy_function.function.file_size_bytes) {
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
		} else {
			stmt.info->options[option.first] = option.second;
		}
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
	bool is_remote_file = config.file_system->IsRemoteFile(stmt.info->file_path);
	if (is_remote_file) {
		use_tmp_file = false;
	} else {
		bool is_file_and_exists = config.file_system->FileExists(stmt.info->file_path);
		bool is_stdout = stmt.info->file_path == "/dev/stdout";
		if (!user_set_use_tmp_file) {
			use_tmp_file = is_file_and_exists && !per_thread_output && partition_cols.empty() && !is_stdout;
		}
	}

	auto unique_column_names = GetUniqueNames(select_node.names);
	auto file_path = stmt.info->file_path;

	auto function_data =
	    copy_function.function.copy_to_bind(context, *stmt.info, unique_column_names, select_node.types);
	// now create the copy information
	auto copy = make_uniq<LogicalCopyToFile>(copy_function.function, std::move(function_data), std::move(stmt.info));
	copy->file_path = file_path;
	copy->use_tmp_file = use_tmp_file;
	copy->overwrite_or_ignore = overwrite_or_ignore;
	copy->filename_pattern = filename_pattern;
	copy->file_extension = file_extension;
	copy->per_thread_output = per_thread_output;
	if (file_size_bytes.IsValid()) {
		copy->file_size_bytes = file_size_bytes;
	}
	copy->partition_output = !partition_cols.empty();
	copy->partition_columns = std::move(partition_cols);

	copy->names = unique_column_names;
	copy->expected_types = select_node.types;

	copy->AddChild(std::move(select_node.plan));

	result.plan = std::move(copy);

	return result;
}

BoundStatement Binder::BindCopyFrom(CopyStatement &stmt) {
	auto &config = DBConfig::GetConfig(context);
	if (!config.options.enable_external_access) {
		throw PermissionException("COPY FROM is disabled by configuration");
	}
	BoundStatement result;
	result.types = {LogicalType::BIGINT};
	result.names = {"Count"};

	if (stmt.info->table.empty()) {
		throw ParserException("COPY FROM requires a table name to be specified");
	}
	// COPY FROM a file
	// generate an insert statement for the the to-be-inserted table
	InsertStatement insert;
	insert.table = stmt.info->table;
	insert.schema = stmt.info->schema;
	insert.catalog = stmt.info->catalog;
	insert.columns = stmt.info->select_list;

	// bind the insert statement to the base table
	auto insert_statement = Bind(insert);
	D_ASSERT(insert_statement.plan->type == LogicalOperatorType::LOGICAL_INSERT);

	auto &bound_insert = insert_statement.plan->Cast<LogicalInsert>();

	// lookup the format in the catalog
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &copy_function = catalog.GetEntry<CopyFunctionCatalogEntry>(context, DEFAULT_SCHEMA, stmt.info->format);
	if (!copy_function.function.copy_from_bind) {
		throw NotImplementedException("COPY FROM is not supported for FORMAT \"%s\"", stmt.info->format);
	}
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

	auto function_data =
	    copy_function.function.copy_from_bind(context, *stmt.info, expected_names, bound_insert.expected_types);
	auto get = make_uniq<LogicalGet>(GenerateTableIndex(), copy_function.function.copy_from_function,
	                                 std::move(function_data), bound_insert.expected_types, expected_names);
	for (idx_t i = 0; i < bound_insert.expected_types.size(); i++) {
		get->column_ids.push_back(i);
	}
	insert_statement.plan->children.push_back(std::move(get));
	result.plan = std::move(insert_statement.plan);
	return result;
}

BoundStatement Binder::Bind(CopyStatement &stmt) {
	if (!stmt.info->is_from && !stmt.select_statement) {
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
		stmt.select_statement = std::move(statement);
	}
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::CHANGED_ROWS;
	if (stmt.info->is_from) {
		return BindCopyFrom(stmt);
	} else {
		return BindCopyTo(stmt);
	}
}

} // namespace duckdb

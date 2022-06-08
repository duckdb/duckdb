#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/execution/operator/persistent/buffered_csv_reader.hpp"
#include "duckdb/function/table/read_csv.hpp"
#include <algorithm>

namespace duckdb {

BoundStatement Binder::BindCopyTo(CopyStatement &stmt) {
	// COPY TO a file
	auto &config = DBConfig::GetConfig(context);
	if (!config.enable_external_access) {
		throw PermissionException("COPY TO is disabled by configuration");
	}
	BoundStatement result;
	result.types = {LogicalType::BIGINT};
	result.names = {"Count"};

	// bind the select statement
	auto select_node = Bind(*stmt.select_statement);

	// lookup the format in the catalog
	auto &catalog = Catalog::GetCatalog(context);
	auto copy_function = catalog.GetEntry<CopyFunctionCatalogEntry>(context, DEFAULT_SCHEMA, stmt.info->format);
	if (!copy_function->function.copy_to_bind) {
		throw NotImplementedException("COPY TO is not supported for FORMAT \"%s\"", stmt.info->format);
	}
	bool use_tmp_file = true;
	for (auto &option : stmt.info->options) {
		auto loption = StringUtil::Lower(option.first);
		if (loption == "use_tmp_file") {
			use_tmp_file = option.second[0].CastAs(LogicalType::BOOLEAN).GetValue<bool>();
			stmt.info->options.erase(option.first);
			break;
		}
	}
	auto function_data =
	    copy_function->function.copy_to_bind(context, *stmt.info, select_node.names, select_node.types);
	// now create the copy information
	auto copy = make_unique<LogicalCopyToFile>(copy_function->function, move(function_data));
	copy->file_path = stmt.info->file_path;
	copy->use_tmp_file = use_tmp_file;
	copy->is_file_and_exists = config.file_system->FileExists(copy->file_path);

	copy->AddChild(move(select_node.plan));

	result.plan = move(copy);

	return result;
}

BoundStatement Binder::BindCopyFrom(CopyStatement &stmt) {
	auto &config = DBConfig::GetConfig(context);
	if (!config.enable_external_access) {
		throw PermissionException("COPY FROM is disabled by configuration");
	}
	BoundStatement result;
	result.types = {LogicalType::BIGINT};
	result.names = {"Count"};

	D_ASSERT(!stmt.info->table.empty());
	// COPY FROM a file
	// generate an insert statement for the the to-be-inserted table
	InsertStatement insert;
	insert.table = stmt.info->table;
	insert.schema = stmt.info->schema;
	insert.columns = stmt.info->select_list;

	// bind the insert statement to the base table
	auto insert_statement = Bind(insert);
	D_ASSERT(insert_statement.plan->type == LogicalOperatorType::LOGICAL_INSERT);

	auto &bound_insert = (LogicalInsert &)*insert_statement.plan;

	// lookup the format in the catalog
	auto &catalog = Catalog::GetCatalog(context);
	auto copy_function = catalog.GetEntry<CopyFunctionCatalogEntry>(context, DEFAULT_SCHEMA, stmt.info->format);
	if (!copy_function->function.copy_from_bind) {
		throw NotImplementedException("COPY FROM is not supported for FORMAT \"%s\"", stmt.info->format);
	}
	// lookup the table to copy into
	auto table = Catalog::GetCatalog(context).GetEntry<TableCatalogEntry>(context, stmt.info->schema, stmt.info->table);
	vector<string> expected_names;
	if (!bound_insert.column_index_map.empty()) {
		expected_names.resize(bound_insert.expected_types.size());
		for (idx_t i = 0; i < table->columns.size(); i++) {
			if (bound_insert.column_index_map[i] != DConstants::INVALID_INDEX) {
				expected_names[bound_insert.column_index_map[i]] = table->columns[i].Name();
			}
		}
	} else {
		expected_names.reserve(bound_insert.expected_types.size());
		for (idx_t i = 0; i < table->columns.size(); i++) {
			expected_names.push_back(table->columns[i].Name());
		}
	}

	auto function_data =
	    copy_function->function.copy_from_bind(context, *stmt.info, expected_names, bound_insert.expected_types);
	auto get = make_unique<LogicalGet>(0, copy_function->function.copy_from_function, move(function_data),
	                                   bound_insert.expected_types, expected_names);
	for (idx_t i = 0; i < bound_insert.expected_types.size(); i++) {
		get->column_ids.push_back(i);
	}
	insert_statement.plan->children.push_back(move(get));
	result.plan = move(insert_statement.plan);
	return result;
}

BoundStatement Binder::Bind(CopyStatement &stmt) {
	if (!stmt.info->is_from && !stmt.select_statement) {
		// copy table into file without a query
		// generate SELECT * FROM table;
		auto ref = make_unique<BaseTableRef>();
		ref->schema_name = stmt.info->schema;
		ref->table_name = stmt.info->table;

		auto statement = make_unique<SelectNode>();
		statement->from_table = move(ref);
		if (!stmt.info->select_list.empty()) {
			for (auto &name : stmt.info->select_list) {
				statement->select_list.push_back(make_unique<ColumnRefExpression>(name));
			}
		} else {
			statement->select_list.push_back(make_unique<StarExpression>());
		}
		stmt.select_statement = move(statement);
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

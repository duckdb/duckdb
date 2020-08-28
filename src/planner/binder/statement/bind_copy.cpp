#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/planner/operator/logical_copy_from_file.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

#include <algorithm>

namespace duckdb {
using namespace std;

BoundStatement Binder::BindCopyTo(CopyStatement &stmt) {
	// COPY TO a file
	if (!context.db.config.enable_copy) {
		throw Exception("COPY TO is disabled by configuration");
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

	auto function_data =
	    copy_function->function.copy_to_bind(context, *stmt.info, select_node.names, select_node.types);
	// now create the copy information
	auto copy = make_unique<LogicalCopyToFile>(copy_function->function, move(function_data));
	copy->AddChild(move(select_node.plan));

	result.plan = move(copy);

	return result;
}

BoundStatement Binder::BindCopyFrom(CopyStatement &stmt) {
	if (!context.db.config.enable_copy) {
		throw Exception("COPY FROM is disabled by configuration");
	}
	BoundStatement result;
	result.types = {LogicalType::BIGINT};
	result.names = {"Count"};

	assert(!stmt.info->table.empty());
	// COPY FROM a file
	// generate an insert statement for the the to-be-inserted table
	InsertStatement insert;
	insert.table = stmt.info->table;
	insert.schema = stmt.info->schema;
	insert.columns = stmt.info->select_list;

	// bind the insert statement to the base table
	auto insert_statement = Bind(insert);
	assert(insert_statement.plan->type == LogicalOperatorType::INSERT);

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
	if (bound_insert.column_index_map.size() > 0) {
		expected_names.resize(bound_insert.expected_types.size());
		for (idx_t i = 0; i < table->columns.size(); i++) {
			if (bound_insert.column_index_map[i] != INVALID_INDEX) {
				expected_names[bound_insert.column_index_map[i]] = table->columns[i].name;
			}
		}
	} else {
		expected_names.reserve(bound_insert.expected_types.size());
		for (idx_t i = 0; i < table->columns.size(); i++) {
			expected_names.push_back(table->columns[i].name);
		}
	}

	auto function_data =
	    copy_function->function.copy_from_bind(context, *stmt.info, expected_names, bound_insert.expected_types);

	// now create the copy statement and set it as a child of the insert statement
	auto copy =
	    make_unique<LogicalCopyFromFile>(0, copy_function->function, move(function_data), bound_insert.expected_types);
	insert_statement.plan->children.push_back(move(copy));
	result.plan = move(insert_statement.plan);
	return result;
}

BoundStatement Binder::Bind(CopyStatement &stmt) {
	if (stmt.select_statement) {
		return BindCopyTo(stmt);
	} else {
		return BindCopyFrom(stmt);
	}
}

} // namespace duckdb

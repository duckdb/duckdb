#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/planner/operator/logical_copy_from_file.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"

#include <algorithm>

using namespace duckdb;
using namespace std;

BoundStatement Binder::BindCopyTo(CopyStatement &stmt) {
	// COPY TO a file
	BoundStatement result;
	result.types = {SQLType::BIGINT};
	result.names = {"Count"};

	// bind the select statement
	auto select_node = Bind(*stmt.select_statement);

	// TODO catalog lookup for copy_to function
	auto &catalog = Catalog::GetCatalog(context);
	auto copy_function = catalog.GetEntry<CopyFunctionCatalogEntry>(context, stmt.info->schema, stmt.info->format);

	unique_ptr<FunctionData> function_data =
	    copy_function->function.copy_to_bind(context, *stmt.info, select_node.names, select_node.types);

	// now create the copy information
	auto copy = make_unique<LogicalCopyToFile>(copy_function->function, move(function_data));
	copy->AddChild(move(select_node.plan));

	result.plan = move(copy);

	return result;
}

BoundStatement Binder::BindCopyFrom(CopyStatement &stmt) {
	BoundStatement result;
	result.types = {SQLType::BIGINT};
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

	// auto table = Catalog::GetCatalog(context).GetEntry<TableCatalogEntry>(context, stmt.info->schema,
	// stmt.info->table); set all columns to false

	// now create the copy statement and set it as a child of the insert statement
	auto copy = make_unique<LogicalCopyFromFile>(0, move(stmt.info), bound_insert.expected_types);
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

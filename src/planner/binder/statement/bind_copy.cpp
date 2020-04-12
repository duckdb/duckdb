#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/planner/operator/logical_copy_from_file.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"

using namespace duckdb;
using namespace std;

BoundStatement Binder::BindCopyTo(CopyStatement &stmt) {
	// COPY TO a file
	BoundStatement result;
	result.types = {SQLType::BIGINT};
	result.names = {"Count"};

	// bind the select statement
	auto select_node = Bind(*stmt.select_statement);

	auto &names = select_node.names;
	auto &quote_list = stmt.info->force_quote_list;

	// set all columns to false
	for (idx_t i = 0; i < names.size(); i++) {
		stmt.info->force_quote.push_back(stmt.info->quote_all);
	}

	if (!quote_list.empty()) {
		// validate force_quote_list entries
		for (const auto &column : quote_list) {
			auto it = find(names.begin(), names.end(), column);
			if (it != names.end()) {
				stmt.info->force_quote[distance(names.begin(), it)] = true;
			} else {
				throw BinderException("Column %s in FORCE_QUOTE is not used in COPY", column.c_str());
			}
		}
	}
	// now create the copy information
	auto copy = make_unique<LogicalCopyToFile>(move(stmt.info));
	copy->AddChild(move(select_node.plan));
	copy->names = select_node.names;
	copy->sql_types = select_node.types;
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

	auto table = Catalog::GetCatalog(context).GetEntry<TableCatalogEntry>(context, stmt.info->schema, stmt.info->table);
	// set all columns to false
	idx_t column_count = stmt.info->select_list.empty() ? table->columns.size() : stmt.info->select_list.size();
	stmt.info->force_not_null.resize(column_count, false);

	// transform column names of force_not_null_list into force_not_null booleans
	if (!stmt.info->force_not_null_list.empty()) {
		// validate force_not_null_list entries
		for (const auto &column : stmt.info->force_not_null_list) {
			auto entry = table->name_map.find(column);
			if (entry == table->name_map.end()) {
				throw BinderException("Column %s not found in table %s", column.c_str(), table->name.c_str());
			}
			if (bound_insert.column_index_map.size() > 0) {
				auto it =
				    find(bound_insert.column_index_map.begin(), bound_insert.column_index_map.end(), entry->second);
				if (it != bound_insert.column_index_map.end()) {
					stmt.info->force_not_null[entry->second] = true;
				} else {
					throw BinderException("Column %s in FORCE_NOT_NULL is not used in COPY", column.c_str());
				}
			} else {
				stmt.info->force_not_null[entry->second] = true;
			}
		}
	}
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

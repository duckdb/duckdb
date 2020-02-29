#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/planner/statement/bound_copy_statement.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundSQLStatement> Binder::Bind(CopyStatement &stmt) {
	auto result = make_unique<BoundCopyStatement>();

	if (stmt.select_statement) {
		// COPY TO a file
		result->select_statement = Bind(*stmt.select_statement);
		result->names = {"Count"};
		result->sql_types = {SQLType::BIGINT};

		auto names = result->select_statement->names;
		auto quote_list = stmt.info->force_quote_list;

		// set all columns to false
		for (index_t i = 0; i < names.size(); i++) {
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

	} else {
		assert(!stmt.info->table.empty());
		// COPY FROM a file
		// generate an insert statement for the the to-be-inserted table
		InsertStatement insert;
		insert.table = stmt.info->table;
		insert.schema = stmt.info->schema;
		insert.columns = stmt.info->select_list;

		// bind the insert statement to the base table
		result->bound_insert = Bind(insert);

		auto &bound_insert = (BoundInsertStatement &)*result->bound_insert;
		// get the set of expected columns from the insert statement; these types will be parsed from the CSV
		result->sql_types = bound_insert.expected_types;

		auto table =
		    Catalog::GetCatalog(context).GetEntry<TableCatalogEntry>(context, stmt.info->schema, stmt.info->table);
		// set all columns to false
		index_t column_count = stmt.info->select_list.empty() ? table->columns.size() : stmt.info->select_list.size();
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
	}
	result->info = move(stmt.info);
	return move(result);
}

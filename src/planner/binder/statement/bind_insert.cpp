#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/insert_binder.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression_binder/returning_binder.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/expression_binder/update_binder.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/planner/expression/bound_default_expression.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"

namespace duckdb {

static void CheckInsertColumnCountMismatch(int64_t expected_columns, int64_t result_columns, bool columns_provided,
                                           const char *tname) {
	if (result_columns != expected_columns) {
		string msg = StringUtil::Format(!columns_provided ? "table %s has %lld columns but %lld values were supplied"
		                                                  : "Column name/value mismatch for insert on %s: "
		                                                    "expected %lld columns but %lld values were supplied",
		                                tname, expected_columns, result_columns);
		throw BinderException(msg);
	}
}

unique_ptr<LogicalProjection> Binder::BindOnConflictClause(unique_ptr<LogicalOperator> &root,
                                                           unique_ptr<LogicalInsert> &insert, TableCatalogEntry *table,
                                                           InsertStatement &stmt) {
	D_ASSERT(root->type == LogicalOperatorType::LOGICAL_INSERT);
	if (!stmt.on_conflict_info) {
		insert->action_type = OnConflictAction::THROW;
		return nullptr;
	}
	auto &on_conflict = *stmt.on_conflict_info;
	insert->action_type = on_conflict.action_type;

	// Bind the indexed columns
	if (!on_conflict.constraint_name.empty()) {
		// Bind the ON CONFLICT ON CONSTRAINT <constraint name>
		insert->constraint_name = on_conflict.constraint_name;
		// FIXME: do we need to grab a lock on the indexes here?
		auto &catalog = Catalog::GetCatalog(context);
		auto catalog_entry =
		    catalog.GetEntry<IndexCatalogEntry>(context, insert->table->schema->name, insert->constraint_name, true);
		if (!catalog_entry) {
			throw BinderException("No INDEX by the name '%s' exists in the schema '%s'", insert->constraint_name,
			                      insert->table->schema->name);
		}
		// Verify that this index is part of the table
		auto &found_index = catalog_entry->index;
		bool index_located = false;
		for (auto &index : table->storage->info->indexes.Indexes()) {
			if (index.get() == found_index) {
				index_located = true;
				break;
			}
		}
		if (!index_located) {
			throw BinderException("INDEX '%s' is not part of the table '%s'", insert->constraint_name, table->name);
		}
		// Lastly, verify that at least one of the columns of the table is referenced by this Index
		bool index_references_table = false;
		auto &indexed_columns = found_index->column_id_set;
		for (auto &column : table->columns.Physical()) {
			if (indexed_columns.count(column.Physical().index)) {
				index_references_table = true;
				break;
			}
		}
		if (!index_references_table) {
			// The index does not reference this table at all, do we just turn it into a DO THROW
			// or should we throw because the conflict target makes no sense?
			throw BinderException("The specified INDEX does not apply to this table");
		}
	} else if (!on_conflict.indexed_columns.empty()) {
		// Bind the ON CONFLICT (<columns>)

		// create a mapping of (list index) -> (column index)x
		case_insensitive_map_t<idx_t> specified_columns;
		for (idx_t i = 0; i < on_conflict.indexed_columns.size(); i++) {
			specified_columns[on_conflict.indexed_columns[i]] = i;
			auto column_index = table->GetColumnIndex(on_conflict.indexed_columns[i]);
			if (column_index.index == COLUMN_IDENTIFIER_ROW_ID) {
				throw BinderException("Cannot specify ROWID as ON CONFLICT target");
			}
			auto &col = table->columns.GetColumn(column_index);
			if (col.Generated()) {
				throw BinderException("Cannot specify a generated column as ON CONFLICT target");
			}
		}
		for (auto &col : table->columns.Physical()) {
			auto entry = specified_columns.find(col.Name());
			if (entry != specified_columns.end()) {
				// column was specified, set to the index
				insert->on_conflict_filter.push_back(entry->second);
			}
		}
		auto &indexes = table->storage->info->indexes;
		bool index_references_columns;
		indexes.Scan([&](Index &index) {
			if (index.IsUnique()) {
				return false;
			}
			auto &column_ids = index.column_id_set;
			for (auto &column_id : insert->on_conflict_filter) {
				if (column_ids.count(column_id)) {
					// We got a hit
					index_references_columns = true;
					return true;
				}
			}
			return false;
		});
		if (!index_references_columns) {
			// Same as before, this is essentially a no-op, turning this into a DO THROW instead
			// But since this makes no logical sense, it's probably better to throw
			throw BinderException(
			    "The specified columns as conflict target are not referenced by a UNIQUE/PRIMARY KEY CONSTRAINT/INDEX");
		}
	} else {
		// The ON CONFLICT applies to every UNIQUE index on the table

		// We check if there are any indexes on the table, without it the ON CONFLICT clause
		// makes no sense, so we can transform this into a DO THROW (so we can avoid doing a bunch of extra wasted work)
		// or just throw a binder exception because this query should logically not have ON CONFLICT
		bool index_references_table = false;
		auto &indexes = table->storage->info->indexes;
		indexes.Scan([&](Index &index) {
			if (!index.IsUnique()) {
				return false;
			}
			auto &indexed_columns = index.column_id_set;
			for (auto &column : table->columns.Physical()) {
				if (indexed_columns.count(column.Physical().index)) {
					index_references_table = true;
					return true;
				}
			}
			return false;
		});
		if (!index_references_table) {
			throw BinderException(
			    "There are no UNIQUE/PRIMARY KEY Indexes that refer to this table, ON CONFLICT is a no-op");
		}
	}

	if (insert->action_type != OnConflictAction::UPDATE) {
		return nullptr;
	}
	D_ASSERT(on_conflict.set_info);
	auto &set_info = *on_conflict.set_info;
	D_ASSERT(!set_info.columns.empty());
	D_ASSERT(set_info.columns.size() == set_info.expressions.size());

	// Bind the SET columns and expressions
	auto proj_index = GenerateTableIndex();
	vector<unique_ptr<Expression>> projection_expressions;

	if (set_info.condition) {
		// Bind the SET .. WHERE clause
		WhereBinder where_binder(*this, context);
		auto condition = where_binder.Bind(set_info.condition);

		PlanSubqueries(&condition, &root);
		auto filter = make_unique<LogicalFilter>(move(condition));
		filter->AddChild(move(root));
		root = move(filter);
	}

	vector<PhysicalIndex> set_columns;
	return BindUpdateSet(insert.get(), root, set_info, table, set_columns);
}

BoundStatement Binder::Bind(InsertStatement &stmt) {
	BoundStatement result;
	result.names = {"Count"};
	result.types = {LogicalType::BIGINT};

	// Fetch the table to insert into
	auto table = Catalog::GetCatalog(context).GetEntry<TableCatalogEntry>(context, stmt.schema, stmt.table);
	D_ASSERT(table);
	if (!table->temporary) {
		// inserting into a non-temporary table: alters underlying database
		properties.read_only = false;
	}

	auto insert = make_unique<LogicalInsert>(table, GenerateTableIndex());
	// Add CTEs as bindable
	AddCTEMap(stmt.cte_map);

	vector<LogicalIndex> named_column_map;
	if (!stmt.columns.empty()) {
		// insertion statement specifies column list

		// create a mapping of (list index) -> (column index)
		case_insensitive_map_t<idx_t> column_name_map;
		for (idx_t i = 0; i < stmt.columns.size(); i++) {
			column_name_map[stmt.columns[i]] = i;
			auto column_index = table->GetColumnIndex(stmt.columns[i]);
			if (column_index.index == COLUMN_IDENTIFIER_ROW_ID) {
				throw BinderException("Cannot explicitly insert values into rowid column");
			}
			auto &col = table->columns.GetColumn(column_index);
			if (col.Generated()) {
				throw BinderException("Cannot insert into a generated column");
			}
			insert->expected_types.push_back(col.Type());
			named_column_map.push_back(column_index);
		}
		for (auto &col : table->columns.Physical()) {
			auto entry = column_name_map.find(col.Name());
			if (entry == column_name_map.end()) {
				// column not specified, set index to DConstants::INVALID_INDEX
				insert->column_index_map.push_back(DConstants::INVALID_INDEX);
			} else {
				// column was specified, set to the index
				insert->column_index_map.push_back(entry->second);
			}
		}
	} else {
		// No columns specified, assume insertion into all columns
		// Intentionally don't populate 'column_index_map' as an indication of this
		for (auto &col : table->columns.Physical()) {
			named_column_map.push_back(col.Logical());
			insert->expected_types.push_back(col.Type());
		}
	}

	// Bind the default values
	BindDefaultValues(table->columns, insert->bound_defaults);
	if (!stmt.select_statement) {
		result.plan = move(insert);
		return result;
	}

	// Exclude the generated columns from this amount
	idx_t expected_columns = stmt.columns.empty() ? table->columns.PhysicalColumnCount() : stmt.columns.size();

	// special case: check if we are inserting from a VALUES statement
	auto values_list = stmt.GetValuesList();
	if (values_list) {
		auto &expr_list = (ExpressionListRef &)*values_list;
		expr_list.expected_types.resize(expected_columns);
		expr_list.expected_names.resize(expected_columns);

		D_ASSERT(expr_list.values.size() > 0);
		CheckInsertColumnCountMismatch(expected_columns, expr_list.values[0].size(), !stmt.columns.empty(),
		                               table->name.c_str());

		// VALUES list!
		for (idx_t col_idx = 0; col_idx < expected_columns; col_idx++) {
			D_ASSERT(named_column_map.size() >= col_idx);
			auto &table_col_idx = named_column_map[col_idx];

			// set the expected types as the types for the INSERT statement
			auto &column = table->columns.GetColumn(table_col_idx);
			expr_list.expected_types[col_idx] = column.Type();
			expr_list.expected_names[col_idx] = column.Name();

			// now replace any DEFAULT values with the corresponding default expression
			for (idx_t list_idx = 0; list_idx < expr_list.values.size(); list_idx++) {
				if (expr_list.values[list_idx][col_idx]->type == ExpressionType::VALUE_DEFAULT) {
					// DEFAULT value! replace the entry
					if (column.DefaultValue()) {
						expr_list.values[list_idx][col_idx] = column.DefaultValue()->Copy();
					} else {
						expr_list.values[list_idx][col_idx] = make_unique<ConstantExpression>(Value(column.Type()));
					}
				}
			}
		}
	}

	// parse select statement and add to logical plan
	auto select_binder = Binder::CreateBinder(context, this);
	auto root_select = select_binder->Bind(*stmt.select_statement);
	MoveCorrelatedExpressions(*select_binder);

	CheckInsertColumnCountMismatch(expected_columns, root_select.types.size(), !stmt.columns.empty(),
	                               table->name.c_str());

	auto root = CastLogicalOperatorToTypes(root_select.types, insert->expected_types, move(root_select.plan));

	auto root = unique_ptr_cast<LogicalInsert, LogicalOperator>(move(insert));
	auto proj = BindOnConflictClause(root, insert, table, stmt);
	if (proj) {
		// We have a DO UPDATE on conflict action, created a projection and moved root into it
		insert->AddChild(move(proj));
	} else {
		insert->AddChild(move(root));
	}

	if (!stmt.returning_list.empty()) {
		insert->return_chunk = true;
		result.types.clear();
		result.names.clear();
		auto insert_table_index = GenerateTableIndex();
		insert->table_index = insert_table_index;
		unique_ptr<LogicalOperator> index_as_logicaloperator = move(insert);

		return BindReturning(move(stmt.returning_list), table, insert_table_index, move(index_as_logicaloperator),
		                     move(result));
	}

	D_ASSERT(result.types.size() == result.names.size());
	result.plan = move(insert);
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::CHANGED_ROWS;
	return result;
}

} // namespace duckdb

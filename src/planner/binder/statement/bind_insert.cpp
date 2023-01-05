#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/insert_binder.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
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
#include "duckdb/planner/bound_tableref.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/planner/tableref/bound_dummytableref.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

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

void QualifyColumnReferences(ParsedExpression &expr, const string &table_name) {
	// To avoid ambiguity with 'excluded', we explicitly qualify all column references
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &column_ref = (ColumnRefExpression &)expr;
		if (column_ref.IsQualified()) {
			return;
		}
		column_ref.SetTableName(table_name);
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](unique_ptr<ParsedExpression> &child) { QualifyColumnReferences(*child, table_name); });
}

// FIXME: this is dumb, doesn't work and is all-in-all just an attempt at a bandaid fix
void ReplaceColumnBindings(Expression &expr, idx_t source, idx_t dest) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_columnref = (BoundColumnRefExpression &)expr;
		if (bound_columnref.binding.table_index == source) {
			bound_columnref.binding.table_index = dest;
		}
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](unique_ptr<Expression> &child) { ReplaceColumnBindings(*child, source, dest); });
}

void Binder::BindDoUpdateSetExpressions(LogicalInsert *insert, UpdateSetInfo &set_info, TableCatalogEntry *table,
                                        vector<PhysicalIndex> &columns) {

	// add the 'excluded' dummy table binding
	AddTableName("excluded");
	// add a bind context entry for it
	auto excluded_index = GenerateTableIndex();
	insert->excluded_table_index = excluded_index;
	auto table_column_names = table->columns.GetColumnNames();
	auto table_column_types = table->columns.GetColumnTypes();
	bind_context.AddGenericBinding(excluded_index, "excluded", table_column_names, table_column_types);

	D_ASSERT(insert->children.size() == 1);
	D_ASSERT(insert->children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION);
	auto projection_index = insert->children[0]->GetTableIndex()[0];

	vector<column_t> logical_column_ids;
	vector<string> column_names;
	D_ASSERT(set_info.columns.size() == set_info.expressions.size());

	string unused;
	auto original_binding = bind_context.GetBinding(table->name, unused);
	D_ASSERT(original_binding);

	auto table_index = original_binding->index;

	for (idx_t i = 0; i < set_info.columns.size(); i++) {
		auto &colname = set_info.columns[i];
		auto &expr = set_info.expressions[i];
		if (!table->ColumnExists(colname)) {
			throw BinderException("Referenced update column %s not found in table!", colname);
		}
		auto &column = table->GetColumn(colname);
		if (column.Generated()) {
			throw BinderException("Cant update column \"%s\" because it is a generated column!", column.Name());
		}
		if (std::find(columns.begin(), columns.end(), column.Physical()) != columns.end()) {
			throw BinderException("Multiple assignments to same column \"%s\"", colname);
		}
		columns.push_back(column.Physical());
		logical_column_ids.push_back(column.Oid());
		column_names.push_back(colname);
		if (expr->type == ExpressionType::VALUE_DEFAULT) {
			insert->expressions.push_back(make_unique<BoundDefaultExpression>(column.Type()));
		} else {
			UpdateBinder binder(*this, context);
			binder.target_type = column.Type();

			// Avoid ambiguity issues
			QualifyColumnReferences(*expr, table->name);

			auto bound_expr = binder.Bind(expr);
			D_ASSERT(bound_expr);
			if (bound_expr->expression_class == ExpressionClass::BOUND_SUBQUERY) {
				throw BinderException("Expression in the DO UPDATE SET clause can not be a subquery");
			}

			// Change the non-excluded column references to refer to the projection index
			ReplaceColumnBindings(*bound_expr, table_index, projection_index);

			insert->expressions.push_back(move(bound_expr));
		}
	}

	// Verify that none of the columns that are targeted with a SET expression are indexed on

	unordered_set<column_t> indexed_columns;
	auto &indexes = table->storage->info->indexes.Indexes();
	for (auto &index : indexes) {
		for (auto &column_id : index->column_id_set) {
			indexed_columns.insert(column_id);
		}
	}

	for (idx_t i = 0; i < logical_column_ids.size(); i++) {
		auto &column = logical_column_ids[i];
		if (indexed_columns.count(column)) {
			throw BinderException("Can not assign to column '%s' because an Index exists on it", column_names[i]);
		}
	}
}

void Binder::BindOnConflictClause(unique_ptr<LogicalInsert> &insert, TableCatalogEntry *table, InsertStatement &stmt) {
	if (!stmt.on_conflict_info) {
		insert->action_type = OnConflictAction::THROW;
		return;
	}
	D_ASSERT(stmt.table_ref->type == TableReferenceType::BASE_TABLE);

	// visit the table reference
	auto bound_table = Bind(*stmt.table_ref);
	if (bound_table->type != TableReferenceType::BASE_TABLE) {
		throw BinderException("Can only update base table!");
	}

	auto &bound_base_tableref = (BoundBaseTableRef &)*bound_table;

	auto &on_conflict = *stmt.on_conflict_info;
	insert->action_type = on_conflict.action_type;

	// Bind the indexed columns
	if (!on_conflict.constraint_name.empty()) {
		// Bind the ON CONFLICT ON CONSTRAINT <constraint name>
		insert->constraint_name = on_conflict.constraint_name;
		// FIXME: do we need to grab a lock on the indexes here?
		auto &catalog = Catalog::GetCatalog(context, stmt.catalog);
		auto catalog_entry = (IndexCatalogEntry *)catalog.GetEntry(
		    context, CatalogType::INDEX_ENTRY, insert->table->schema->name, insert->constraint_name, true);
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
				insert->on_conflict_filter.push_back(col.Oid());
			}
		}
		// TODO: verify that no columns that are indexed on appear as target of a SET expression (<indexed_on_column> =
		// <expression>)
		auto &indexes = table->storage->info->indexes;
		bool index_references_columns = false;
		indexes.Scan([&](Index &index) {
			if (!index.IsUnique()) {
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
		return;
	}
	D_ASSERT(on_conflict.set_info);
	auto &set_info = *on_conflict.set_info;
	D_ASSERT(!set_info.columns.empty());
	D_ASSERT(set_info.columns.size() == set_info.expressions.size());

	// if (on_conflict.condition) {
	//	// Bind the ON CONFLICT ... WHERE clause
	//	WhereBinder where_binder(*this, context);
	//	auto condition = where_binder.Bind(on_conflict.condition);
	//	if (condition && condition->expression_class == ExpressionClass::BOUND_SUBQUERY) {
	//		throw BinderException("conflict_target WHERE clause can not be a subquery");
	//	}
	//	insert->on_conflict_condition = move(condition);
	// }

	// if (set_info.condition) {
	//	// Bind the SET ... WHERE clause
	//	WhereBinder where_binder(*this, context);
	//	auto condition = where_binder.Bind(set_info.condition);
	//	if (condition && condition->expression_class == ExpressionClass::BOUND_SUBQUERY) {
	//		throw BinderException("conflict_target WHERE clause can not be a subquery");
	//	}
	//	insert->do_update_condition = move(condition);
	// }

	// Instead of this, it should probably be a DummyTableRef
	// so we can resolve the Bind for 'excluded' and create proper BoundReferenceExpressions for it
	vector<PhysicalIndex> set_columns;
	BindDoUpdateSetExpressions(insert.get(), set_info, table, set_columns);
}

BoundStatement Binder::Bind(InsertStatement &stmt) {
	BoundStatement result;
	result.names = {"Count"};
	result.types = {LogicalType::BIGINT};

	BindSchemaOrCatalog(stmt.catalog, stmt.schema);
	auto table = Catalog::GetEntry<TableCatalogEntry>(context, stmt.catalog, stmt.schema, stmt.table);
	D_ASSERT(table);
	if (!table->temporary) {
		// inserting into a non-temporary table: alters underlying database
		properties.modified_databases.insert(table->catalog->GetName());
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
	if (stmt.on_conflict_info) {
		ExpressionBinder expr_binder(*select_binder, context);
		if (stmt.on_conflict_info->condition) {
			insert->on_conflict_condition = expr_binder.Bind(stmt.on_conflict_info->condition);
		}
		if (stmt.on_conflict_info->set_info && stmt.on_conflict_info->set_info->condition) {
			insert->do_update_condition = expr_binder.Bind(stmt.on_conflict_info->set_info->condition);
		}
	}
	MoveCorrelatedExpressions(*select_binder);

	CheckInsertColumnCountMismatch(expected_columns, root_select.types.size(), !stmt.columns.empty(),
	                               table->name.c_str());

	// FIXME: this creates a projection, with a child LogicalExpressionGet
	auto root = CastLogicalOperatorToTypes(root_select.types, insert->expected_types, move(root_select.plan));
	insert->AddChild(move(root));
	BindOnConflictClause(insert, table, stmt);

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

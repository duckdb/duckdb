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
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression_binder/returning_binder.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/expression_binder/update_binder.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/planner/expression/bound_default_expression.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/bound_tableref.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/planner/tableref/bound_dummytableref.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/storage/table_storage_info.hpp"

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

unique_ptr<ParsedExpression> ExpandDefaultExpression(const ColumnDefinition &column) {
	if (column.DefaultValue()) {
		return column.DefaultValue()->Copy();
	} else {
		return make_uniq<ConstantExpression>(Value(column.Type()));
	}
}

void ReplaceDefaultExpression(unique_ptr<ParsedExpression> &expr, const ColumnDefinition &column) {
	D_ASSERT(expr->type == ExpressionType::VALUE_DEFAULT);
	expr = ExpandDefaultExpression(column);
}

void QualifyColumnReferences(unique_ptr<ParsedExpression> &expr, const string &table_name) {
	// To avoid ambiguity with 'excluded', we explicitly qualify all column references
	if (expr->type == ExpressionType::COLUMN_REF) {
		auto &column_ref = expr->Cast<ColumnRefExpression>();
		if (column_ref.IsQualified()) {
			return;
		}
		auto column_name = column_ref.GetColumnName();
		expr = make_uniq<ColumnRefExpression>(column_name, table_name);
	}
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { QualifyColumnReferences(child, table_name); });
}

// Replace binding.table_index with 'dest' if it's 'source'
void ReplaceColumnBindings(Expression &expr, idx_t source, idx_t dest) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_columnref = expr.Cast<BoundColumnRefExpression>();
		if (bound_columnref.binding.table_index == source) {
			bound_columnref.binding.table_index = dest;
		}
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](unique_ptr<Expression> &child) { ReplaceColumnBindings(*child, source, dest); });
}

void Binder::BindDoUpdateSetExpressions(const string &table_alias, LogicalInsert &insert, UpdateSetInfo &set_info,
                                        TableCatalogEntry &table, TableStorageInfo &storage_info) {
	D_ASSERT(insert.children.size() == 1);
	D_ASSERT(insert.children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION);

	vector<column_t> logical_column_ids;
	vector<string> column_names;
	D_ASSERT(set_info.columns.size() == set_info.expressions.size());

	for (idx_t i = 0; i < set_info.columns.size(); i++) {
		auto &colname = set_info.columns[i];
		auto &expr = set_info.expressions[i];
		if (!table.ColumnExists(colname)) {
			throw BinderException("Referenced update column %s not found in table!", colname);
		}
		auto &column = table.GetColumn(colname);
		if (column.Generated()) {
			throw BinderException("Cant update column \"%s\" because it is a generated column!", column.Name());
		}
		if (std::find(insert.set_columns.begin(), insert.set_columns.end(), column.Physical()) !=
		    insert.set_columns.end()) {
			throw BinderException("Multiple assignments to same column \"%s\"", colname);
		}
		insert.set_columns.push_back(column.Physical());
		logical_column_ids.push_back(column.Oid());
		insert.set_types.push_back(column.Type());
		column_names.push_back(colname);
		if (expr->type == ExpressionType::VALUE_DEFAULT) {
			expr = ExpandDefaultExpression(column);
		}
		UpdateBinder binder(*this, context);
		binder.target_type = column.Type();

		// Avoid ambiguity issues
		QualifyColumnReferences(expr, table_alias);

		auto bound_expr = binder.Bind(expr);
		D_ASSERT(bound_expr);
		if (bound_expr->expression_class == ExpressionClass::BOUND_SUBQUERY) {
			throw BinderException("Expression in the DO UPDATE SET clause can not be a subquery");
		}

		insert.expressions.push_back(std::move(bound_expr));
	}

	// Figure out which columns are indexed on
	unordered_set<column_t> indexed_columns;
	for (auto &index : storage_info.index_info) {
		for (auto &column_id : index.column_set) {
			indexed_columns.insert(column_id);
		}
	}

	// Verify that none of the columns that are targeted with a SET expression are indexed on
	for (idx_t i = 0; i < logical_column_ids.size(); i++) {
		auto &column = logical_column_ids[i];
		if (indexed_columns.count(column)) {
			throw BinderException("Can not assign to column '%s' because it has a UNIQUE/PRIMARY KEY constraint",
			                      column_names[i]);
		}
	}
}

unique_ptr<UpdateSetInfo> CreateSetInfoForReplace(TableCatalogEntry &table, InsertStatement &insert,
                                                  TableStorageInfo &storage_info) {
	auto set_info = make_uniq<UpdateSetInfo>();

	auto &columns = set_info->columns;
	// Figure out which columns are indexed on

	unordered_set<column_t> indexed_columns;
	for (auto &index : storage_info.index_info) {
		for (auto &column_id : index.column_set) {
			indexed_columns.insert(column_id);
		}
	}

	auto &column_list = table.GetColumns();
	if (insert.columns.empty()) {
		for (auto &column : column_list.Physical()) {
			auto &name = column.Name();
			// FIXME: can these column names be aliased somehow?
			if (indexed_columns.count(column.Oid())) {
				continue;
			}
			columns.push_back(name);
		}
	} else {
		// a list of columns was explicitly supplied, only update those
		for (auto &name : insert.columns) {
			auto &column = column_list.GetColumn(name);
			if (indexed_columns.count(column.Oid())) {
				continue;
			}
			columns.push_back(name);
		}
	}

	// Create 'excluded' qualified column references of these columns
	for (auto &column : columns) {
		set_info->expressions.push_back(make_uniq<ColumnRefExpression>(column, "excluded"));
	}

	return set_info;
}

void Binder::BindOnConflictClause(LogicalInsert &insert, TableCatalogEntry &table, InsertStatement &stmt) {
	if (!stmt.on_conflict_info) {
		insert.action_type = OnConflictAction::THROW;
		return;
	}
	D_ASSERT(stmt.table_ref->type == TableReferenceType::BASE_TABLE);

	// visit the table reference
	auto bound_table = Bind(*stmt.table_ref);
	if (bound_table->type != TableReferenceType::BASE_TABLE) {
		throw BinderException("Can only update base table!");
	}

	auto &table_ref = stmt.table_ref->Cast<BaseTableRef>();
	const string &table_alias = !table_ref.alias.empty() ? table_ref.alias : table_ref.table_name;

	auto &on_conflict = *stmt.on_conflict_info;
	D_ASSERT(on_conflict.action_type != OnConflictAction::THROW);
	insert.action_type = on_conflict.action_type;

	// obtain the table storage info
	auto storage_info = table.GetStorageInfo(context);

	auto &columns = table.GetColumns();
	if (!on_conflict.indexed_columns.empty()) {
		// Bind the ON CONFLICT (<columns>)

		// create a mapping of (list index) -> (column index)
		case_insensitive_map_t<idx_t> specified_columns;
		for (idx_t i = 0; i < on_conflict.indexed_columns.size(); i++) {
			specified_columns[on_conflict.indexed_columns[i]] = i;
			auto column_index = table.GetColumnIndex(on_conflict.indexed_columns[i]);
			if (column_index.index == COLUMN_IDENTIFIER_ROW_ID) {
				throw BinderException("Cannot specify ROWID as ON CONFLICT target");
			}
			auto &col = columns.GetColumn(column_index);
			if (col.Generated()) {
				throw BinderException("Cannot specify a generated column as ON CONFLICT target");
			}
		}
		for (auto &col : columns.Physical()) {
			auto entry = specified_columns.find(col.Name());
			if (entry != specified_columns.end()) {
				// column was specified, set to the index
				insert.on_conflict_filter.insert(col.Oid());
			}
		}
		bool index_references_columns = false;
		for (auto &index : storage_info.index_info) {
			if (!index.is_unique) {
				continue;
			}
			bool index_matches = insert.on_conflict_filter == index.column_set;
			if (index_matches) {
				index_references_columns = true;
				break;
			}
		}
		if (!index_references_columns) {
			// Same as before, this is essentially a no-op, turning this into a DO THROW instead
			// But since this makes no logical sense, it's probably better to throw an error
			throw BinderException(
			    "The specified columns as conflict target are not referenced by a UNIQUE/PRIMARY KEY CONSTRAINT");
		}
	} else {
		// When omitting the conflict target, the ON CONFLICT applies to every UNIQUE/PRIMARY KEY on the table

		// We check if there are any constraints on the table, if there aren't we throw an error.
		idx_t found_matching_indexes = 0;
		for (auto &index : storage_info.index_info) {
			if (!index.is_unique) {
				continue;
			}
			// does this work with multi-column indexes?
			auto &indexed_columns = index.column_set;
			for (auto &column : table.GetColumns().Physical()) {
				if (indexed_columns.count(column.Physical().index)) {
					found_matching_indexes++;
				}
			}
		}
		if (!found_matching_indexes) {
			throw BinderException(
			    "There are no UNIQUE/PRIMARY KEY Indexes that refer to this table, ON CONFLICT is a no-op");
		}
		if (insert.action_type != OnConflictAction::NOTHING && found_matching_indexes != 1) {
			// When no conflict target is provided, and the action type is UPDATE,
			// we only allow the operation when only a single Index exists
			throw BinderException("Conflict target has to be provided for a DO UPDATE operation when the table has "
			                      "multiple UNIQUE/PRIMARY KEY constraints");
		}
	}

	// add the 'excluded' dummy table binding
	AddTableName("excluded");
	// add a bind context entry for it
	auto excluded_index = GenerateTableIndex();
	insert.excluded_table_index = excluded_index;
	auto table_column_names = columns.GetColumnNames();
	auto table_column_types = columns.GetColumnTypes();
	bind_context.AddGenericBinding(excluded_index, "excluded", table_column_names, table_column_types);

	if (on_conflict.condition) {
		// Avoid ambiguity between <table_name> binding and 'excluded'
		QualifyColumnReferences(on_conflict.condition, table_alias);
		// Bind the ON CONFLICT ... WHERE clause
		WhereBinder where_binder(*this, context);
		auto condition = where_binder.Bind(on_conflict.condition);
		if (condition && condition->expression_class == ExpressionClass::BOUND_SUBQUERY) {
			throw BinderException("conflict_target WHERE clause can not be a subquery");
		}
		insert.on_conflict_condition = std::move(condition);
	}

	auto bindings = insert.children[0]->GetColumnBindings();
	idx_t projection_index = DConstants::INVALID_INDEX;
	vector<unique_ptr<LogicalOperator>> *insert_child_operators;
	insert_child_operators = &insert.children;
	while (projection_index == DConstants::INVALID_INDEX) {
		if (insert_child_operators->empty()) {
			// No further children to visit
			break;
		}
		D_ASSERT(insert_child_operators->size() >= 1);
		auto &current_child = (*insert_child_operators)[0];
		auto table_indices = current_child->GetTableIndex();
		if (table_indices.empty()) {
			// This operator does not have a table index to refer to, we have to visit its children
			insert_child_operators = &current_child->children;
			continue;
		}
		projection_index = table_indices[0];
	}
	if (projection_index == DConstants::INVALID_INDEX) {
		throw InternalException("Could not locate a table_index from the children of the insert");
	}

	string unused;
	auto original_binding = bind_context.GetBinding(table_alias, unused);
	D_ASSERT(original_binding);

	auto table_index = original_binding->index;

	// Replace any column bindings to refer to the projection table_index, rather than the source table
	if (insert.on_conflict_condition) {
		ReplaceColumnBindings(*insert.on_conflict_condition, table_index, projection_index);
	}

	if (insert.action_type == OnConflictAction::REPLACE) {
		D_ASSERT(on_conflict.set_info == nullptr);
		on_conflict.set_info = CreateSetInfoForReplace(table, stmt, storage_info);
		insert.action_type = OnConflictAction::UPDATE;
	}
	if (on_conflict.set_info && on_conflict.set_info->columns.empty()) {
		// if we are doing INSERT OR REPLACE on a table with no columns outside of the primary key column
		// convert to INSERT OR IGNORE
		insert.action_type = OnConflictAction::NOTHING;
	}
	if (insert.action_type == OnConflictAction::NOTHING) {
		if (!insert.on_conflict_condition) {
			return;
		}
		// Get the column_ids we need to fetch later on from the conflicting tuples
		// of the original table, to execute the expressions
		D_ASSERT(original_binding->binding_type == BindingType::TABLE);
		auto &table_binding = original_binding->Cast<TableBinding>();
		insert.columns_to_fetch = table_binding.GetBoundColumnIds();
		return;
	}

	D_ASSERT(on_conflict.set_info);
	auto &set_info = *on_conflict.set_info;
	D_ASSERT(set_info.columns.size() == set_info.expressions.size());

	if (set_info.condition) {
		// Avoid ambiguity between <table_name> binding and 'excluded'
		QualifyColumnReferences(set_info.condition, table_alias);
		// Bind the SET ... WHERE clause
		WhereBinder where_binder(*this, context);
		auto condition = where_binder.Bind(set_info.condition);
		if (condition && condition->expression_class == ExpressionClass::BOUND_SUBQUERY) {
			throw BinderException("conflict_target WHERE clause can not be a subquery");
		}
		insert.do_update_condition = std::move(condition);
	}

	BindDoUpdateSetExpressions(table_alias, insert, set_info, table, storage_info);

	// Get the column_ids we need to fetch later on from the conflicting tuples
	// of the original table, to execute the expressions
	D_ASSERT(original_binding->binding_type == BindingType::TABLE);
	auto &table_binding = original_binding->Cast<TableBinding>();
	insert.columns_to_fetch = table_binding.GetBoundColumnIds();

	// Replace the column bindings to refer to the child operator
	for (auto &expr : insert.expressions) {
		// Change the non-excluded column references to refer to the projection index
		ReplaceColumnBindings(*expr, table_index, projection_index);
	}
	// Do the same for the (optional) DO UPDATE condition
	if (insert.do_update_condition) {
		ReplaceColumnBindings(*insert.do_update_condition, table_index, projection_index);
	}
}

BoundStatement Binder::Bind(InsertStatement &stmt) {
	BoundStatement result;
	result.names = {"Count"};
	result.types = {LogicalType::BIGINT};

	BindSchemaOrCatalog(stmt.catalog, stmt.schema);
	auto &table = Catalog::GetEntry<TableCatalogEntry>(context, stmt.catalog, stmt.schema, stmt.table);
	if (!table.temporary) {
		// inserting into a non-temporary table: alters underlying database
		properties.modified_databases.insert(table.catalog.GetName());
	}

	auto insert = make_uniq<LogicalInsert>(table, GenerateTableIndex());
	// Add CTEs as bindable
	AddCTEMap(stmt.cte_map);

	auto values_list = stmt.GetValuesList();

	// bind the root select node (if any)
	BoundStatement root_select;
	if (stmt.column_order == InsertColumnOrder::INSERT_BY_NAME) {
		if (values_list) {
			throw BinderException("INSERT BY NAME can only be used when inserting from a SELECT statement");
		}
		if (!stmt.columns.empty()) {
			throw BinderException("INSERT BY NAME cannot be combined with an explicit column list");
		}
		D_ASSERT(stmt.select_statement);
		// INSERT BY NAME - generate the columns from the names of the SELECT statement
		auto select_binder = Binder::CreateBinder(context, this);
		root_select = select_binder->Bind(*stmt.select_statement);
		MoveCorrelatedExpressions(*select_binder);

		stmt.columns = root_select.names;
	}

	vector<LogicalIndex> named_column_map;
	if (!stmt.columns.empty() || stmt.default_values) {
		// insertion statement specifies column list

		// create a mapping of (list index) -> (column index)
		case_insensitive_map_t<idx_t> column_name_map;
		for (idx_t i = 0; i < stmt.columns.size(); i++) {
			auto entry = column_name_map.insert(make_pair(stmt.columns[i], i));
			if (!entry.second) {
				throw BinderException("Duplicate column name \"%s\" in INSERT", stmt.columns[i]);
			}
			column_name_map[stmt.columns[i]] = i;
			auto column_index = table.GetColumnIndex(stmt.columns[i]);
			if (column_index.index == COLUMN_IDENTIFIER_ROW_ID) {
				throw BinderException("Cannot explicitly insert values into rowid column");
			}
			auto &col = table.GetColumn(column_index);
			if (col.Generated()) {
				throw BinderException("Cannot insert into a generated column");
			}
			insert->expected_types.push_back(col.Type());
			named_column_map.push_back(column_index);
		}
		for (auto &col : table.GetColumns().Physical()) {
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
		// insert by position and no columns specified - insertion into all columns of the table
		// intentionally don't populate 'column_index_map' as an indication of this
		for (auto &col : table.GetColumns().Physical()) {
			named_column_map.push_back(col.Logical());
			insert->expected_types.push_back(col.Type());
		}
	}

	// bind the default values
	BindDefaultValues(table.GetColumns(), insert->bound_defaults);
	if (!stmt.select_statement && !stmt.default_values) {
		result.plan = std::move(insert);
		return result;
	}
	// Exclude the generated columns from this amount
	idx_t expected_columns = stmt.columns.empty() ? table.GetColumns().PhysicalColumnCount() : stmt.columns.size();

	// special case: check if we are inserting from a VALUES statement
	if (values_list) {
		auto &expr_list = values_list->Cast<ExpressionListRef>();
		expr_list.expected_types.resize(expected_columns);
		expr_list.expected_names.resize(expected_columns);

		D_ASSERT(expr_list.values.size() > 0);
		CheckInsertColumnCountMismatch(expected_columns, expr_list.values[0].size(), !stmt.columns.empty(),
		                               table.name.c_str());

		// VALUES list!
		for (idx_t col_idx = 0; col_idx < expected_columns; col_idx++) {
			D_ASSERT(named_column_map.size() >= col_idx);
			auto &table_col_idx = named_column_map[col_idx];

			// set the expected types as the types for the INSERT statement
			auto &column = table.GetColumn(table_col_idx);
			expr_list.expected_types[col_idx] = column.Type();
			expr_list.expected_names[col_idx] = column.Name();

			// now replace any DEFAULT values with the corresponding default expression
			for (idx_t list_idx = 0; list_idx < expr_list.values.size(); list_idx++) {
				if (expr_list.values[list_idx][col_idx]->type == ExpressionType::VALUE_DEFAULT) {
					// DEFAULT value! replace the entry
					ReplaceDefaultExpression(expr_list.values[list_idx][col_idx], column);
				}
			}
		}
	}

	// parse select statement and add to logical plan
	unique_ptr<LogicalOperator> root;
	if (stmt.select_statement) {
		if (stmt.column_order == InsertColumnOrder::INSERT_BY_POSITION) {
			auto select_binder = Binder::CreateBinder(context, this);
			root_select = select_binder->Bind(*stmt.select_statement);
			MoveCorrelatedExpressions(*select_binder);
		}
		// inserting from a select - check if the column count matches
		CheckInsertColumnCountMismatch(expected_columns, root_select.types.size(), !stmt.columns.empty(),
		                               table.name.c_str());

		root = CastLogicalOperatorToTypes(root_select.types, insert->expected_types, std::move(root_select.plan));
	} else {
		root = make_uniq<LogicalDummyScan>(GenerateTableIndex());
	}
	insert->AddChild(std::move(root));

	BindOnConflictClause(*insert, table, stmt);

	if (!stmt.returning_list.empty()) {
		insert->return_chunk = true;
		result.types.clear();
		result.names.clear();
		auto insert_table_index = GenerateTableIndex();
		insert->table_index = insert_table_index;
		unique_ptr<LogicalOperator> index_as_logicaloperator = std::move(insert);

		return BindReturning(std::move(stmt.returning_list), table, stmt.table_ref ? stmt.table_ref->alias : string(),
		                     insert_table_index, std::move(index_as_logicaloperator), std::move(result));
	}

	D_ASSERT(result.types.size() == result.names.size());
	result.plan = std::move(insert);
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::CHANGED_ROWS;
	return result;
}

} // namespace duckdb

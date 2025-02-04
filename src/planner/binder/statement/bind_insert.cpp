#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
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
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/expression_binder/update_binder.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/planner/expression/bound_default_expression.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/bound_tableref.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/planner/tableref/bound_dummytableref.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

namespace duckdb {

static void CheckInsertColumnCountMismatch(idx_t expected_columns, idx_t result_columns, bool columns_provided,
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
	if (column.HasDefaultValue()) {
		return column.DefaultValue().Copy();
	} else {
		return make_uniq<ConstantExpression>(Value(column.Type()));
	}
}

void ReplaceDefaultExpression(unique_ptr<ParsedExpression> &expr, const ColumnDefinition &column) {
	D_ASSERT(expr->GetExpressionType() == ExpressionType::VALUE_DEFAULT);
	expr = ExpandDefaultExpression(column);
}

void ExpressionBinder::DoUpdateSetQualifyInLambda(FunctionExpression &function, const string &table_name,
                                                  vector<unordered_set<string>> &lambda_params) {

	for (auto &child : function.children) {
		if (child->GetExpressionClass() != ExpressionClass::LAMBDA) {
			DoUpdateSetQualify(child, table_name, lambda_params);
			continue;
		}

		// Special-handling for LHS lambda parameters.
		// We do not qualify them, and we add them to the lambda_params vector.
		auto &lambda_expr = child->Cast<LambdaExpression>();
		string error_message;
		auto column_ref_expressions = lambda_expr.ExtractColumnRefExpressions(error_message);

		if (!error_message.empty()) {
			// Possibly a JSON function, qualify both LHS and RHS.
			ParsedExpressionIterator::EnumerateChildren(*lambda_expr.lhs, [&](unique_ptr<ParsedExpression> &child) {
				DoUpdateSetQualify(child, table_name, lambda_params);
			});
			ParsedExpressionIterator::EnumerateChildren(*lambda_expr.expr, [&](unique_ptr<ParsedExpression> &child) {
				DoUpdateSetQualify(child, table_name, lambda_params);
			});
			continue;
		}

		// Push the lambda parameter names of this level.
		lambda_params.emplace_back();
		for (const auto &column_ref_expr : column_ref_expressions) {
			const auto &column_ref = column_ref_expr.get().Cast<ColumnRefExpression>();
			lambda_params.back().emplace(column_ref.GetName());
		}

		// Only qualify in the RHS of the expression.
		ParsedExpressionIterator::EnumerateChildren(*lambda_expr.expr, [&](unique_ptr<ParsedExpression> &child) {
			DoUpdateSetQualify(child, table_name, lambda_params);
		});

		lambda_params.pop_back();
	}
}

void ExpressionBinder::DoUpdateSetQualify(unique_ptr<ParsedExpression> &expr, const string &table_name,
                                          vector<unordered_set<string>> &lambda_params) {

	// We avoid ambiguity with EXCLUDED columns by qualifying all column references.
	switch (expr->GetExpressionClass()) {
	case ExpressionClass::COLUMN_REF: {
		auto &col_ref = expr->Cast<ColumnRefExpression>();
		if (col_ref.IsQualified()) {
			return;
		}

		// Don't qualify lambda parameters.
		if (LambdaExpression::IsLambdaParameter(lambda_params, col_ref.GetName())) {
			return;
		}

		// Qualify the column reference.
		expr = make_uniq<ColumnRefExpression>(col_ref.GetColumnName(), table_name);
		return;
	}
	case ExpressionClass::FUNCTION: {
		// Special-handling for lambdas, which are inside function expressions.
		auto &function = expr->Cast<FunctionExpression>();
		if (function.IsLambdaFunction()) {
			return DoUpdateSetQualifyInLambda(function, table_name, lambda_params);
		}
		break;
	}
	case ExpressionClass::SUBQUERY: {
		throw BinderException("DO UPDATE SET clause cannot contain a subquery");
	}
	default:
		break;
	}

	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { DoUpdateSetQualify(child, table_name, lambda_params); });
}

// Replace binding.table_index with 'dest' if it's 'source'
void ReplaceColumnBindings(Expression &expr, idx_t source, idx_t dest) {
	if (expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
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

		if (!column.Type().SupportsRegularUpdate()) {
			insert.update_is_del_and_insert = true;
		}

		insert.set_columns.push_back(column.Physical());
		logical_column_ids.push_back(column.Oid());
		insert.set_types.push_back(column.Type());
		column_names.push_back(colname);
		if (expr->GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
			expr = ExpandDefaultExpression(column);
		}

		// Qualify and bind the ON CONFLICT DO UPDATE SET expression.
		UpdateBinder update_binder(*this, context);
		update_binder.target_type = column.Type();

		// Avoid ambiguity between existing table columns and EXCLUDED columns.
		vector<unordered_set<string>> lambda_params;
		update_binder.DoUpdateSetQualify(expr, table_alias, lambda_params);

		auto bound_expr = update_binder.Bind(expr);
		D_ASSERT(bound_expr);
		insert.expressions.push_back(std::move(bound_expr));
	}

	// Figure out which columns are indexed on
	unordered_set<column_t> indexed_columns;
	for (auto &index : storage_info.index_info) {
		for (auto &column_id : index.column_set) {
			indexed_columns.insert(column_id);
		}
	}

	// If any column targeted by a SET expression has an index, then
	// we need to rewrite this to an DELETE + INSERT.
	for (idx_t i = 0; i < logical_column_ids.size(); i++) {
		auto &column = logical_column_ids[i];
		if (indexed_columns.count(column)) {
			insert.update_is_del_and_insert = true;
			break;
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

vector<column_t> GetColumnsToFetch(const TableBinding &binding) {
	auto &bound_columns = binding.GetBoundColumnIds();
	vector<column_t> result;
	for (auto &col : bound_columns) {
		result.push_back(col.GetPrimaryIndex());
	}
	return result;
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
				insert.on_conflict_filter.insert(col.Physical().index);
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
			throw BinderException("The specified columns as conflict target are not referenced by a UNIQUE/PRIMARY KEY "
			                      "CONSTRAINT or INDEX");
		}
	} else {
		// When omitting the conflict target, the ON CONFLICT applies to every UNIQUE/PRIMARY KEY on the table

		// We check if there are any constraints on the table, if there aren't we throw an error.
		idx_t found_matching_indexes = 0;
		for (auto &index : storage_info.index_info) {
			if (!index.is_unique) {
				continue;
			}
			auto &indexed_columns = index.column_set;
			bool matches = false;
			for (auto &column : table.GetColumns().Physical()) {
				if (indexed_columns.count(column.Physical().index)) {
					matches = true;
					break;
				}
			}
			found_matching_indexes += matches;
		}

		if (!found_matching_indexes) {
			throw BinderException(
			    "There are no UNIQUE/PRIMARY KEY Indexes that refer to this table, ON CONFLICT is a no-op");
		} else if (found_matching_indexes != 1) {
			if (insert.action_type != OnConflictAction::NOTHING) {
				// When no conflict target is provided, and the action type is UPDATE,
				// we only allow the operation when only a single Index exists
				throw BinderException("Conflict target has to be provided for a DO UPDATE operation when the table has "
				                      "multiple UNIQUE/PRIMARY KEY constraints");
			}
		}
	}

	// add the 'excluded' dummy table binding
	AddTableName("excluded");
	// add a bind context entry for it
	auto excluded_index = GenerateTableIndex();
	insert.excluded_table_index = excluded_index;
	vector<string> table_column_names;
	vector<LogicalType> table_column_types;
	for (auto &col : columns.Physical()) {
		table_column_names.push_back(col.Name());
		table_column_types.push_back(col.Type());
	}
	bind_context.AddGenericBinding(excluded_index, "excluded", table_column_names, table_column_types);

	if (on_conflict.condition) {
		WhereBinder where_binder(*this, context);

		// Avoid ambiguity between existing table columns and EXCLUDED columns.
		vector<unordered_set<string>> lambda_params;
		where_binder.DoUpdateSetQualify(on_conflict.condition, table_alias, lambda_params);

		// Bind the ON CONFLICT ... WHERE clause.
		auto condition = where_binder.Bind(on_conflict.condition);
		insert.on_conflict_condition = std::move(condition);
	}

	optional_idx projection_index;
	reference<vector<unique_ptr<LogicalOperator>>> insert_child_operators = insert.children;
	while (!projection_index.IsValid()) {
		if (insert_child_operators.get().empty()) {
			// No further children to visit
			break;
		}
		auto &current_child = insert_child_operators.get()[0];
		auto table_indices = current_child->GetTableIndex();
		if (table_indices.empty()) {
			// This operator does not have a table index to refer to, we have to visit its children
			insert_child_operators = current_child->children;
			continue;
		}
		projection_index = table_indices[0];
	}
	if (!projection_index.IsValid()) {
		throw InternalException("Could not locate a table_index from the children of the insert");
	}

	ErrorData unused;
	auto original_binding = bind_context.GetBinding(table_alias, unused);
	D_ASSERT(original_binding && !unused.HasError());

	auto table_index = original_binding->index;

	// Replace any column bindings to refer to the projection table_index, rather than the source table
	if (insert.on_conflict_condition) {
		ReplaceColumnBindings(*insert.on_conflict_condition, table_index, projection_index.GetIndex());
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
		insert.columns_to_fetch = GetColumnsToFetch(table_binding);
		return;
	}

	D_ASSERT(on_conflict.set_info);
	auto &set_info = *on_conflict.set_info;
	D_ASSERT(set_info.columns.size() == set_info.expressions.size());

	if (set_info.condition) {
		WhereBinder where_binder(*this, context);

		// Avoid ambiguity between existing table columns and EXCLUDED columns.
		vector<unordered_set<string>> lambda_params;
		where_binder.DoUpdateSetQualify(set_info.condition, table_alias, lambda_params);

		// Bind the SET ... WHERE clause.
		auto condition = where_binder.Bind(set_info.condition);
		insert.do_update_condition = std::move(condition);
	}

	BindDoUpdateSetExpressions(table_alias, insert, set_info, table, storage_info);

	// Get the column_ids we need to fetch later on from the conflicting tuples
	// of the original table, to execute the expressions
	D_ASSERT(original_binding->binding_type == BindingType::TABLE);
	auto &table_binding = original_binding->Cast<TableBinding>();
	insert.columns_to_fetch = GetColumnsToFetch(table_binding);

	// Replace the column bindings to refer to the child operator
	for (auto &expr : insert.expressions) {
		// Change the non-excluded column references to refer to the projection index
		ReplaceColumnBindings(*expr, table_index, projection_index.GetIndex());
	}
	// Do the same for the (optional) DO UPDATE condition
	if (insert.do_update_condition) {
		ReplaceColumnBindings(*insert.do_update_condition, table_index, projection_index.GetIndex());
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
		auto &properties = GetStatementProperties();
		properties.RegisterDBModify(table.catalog, context);
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
		if (stmt.default_values) {
			throw BinderException("INSERT BY NAME cannot be combined with with DEFAULT VALUES");
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
	auto &catalog_name = table.ParentCatalog().GetName();
	auto &schema_name = table.ParentSchema().name;
	BindDefaultValues(table.GetColumns(), insert->bound_defaults, catalog_name, schema_name);
	insert->bound_constraints = BindConstraints(table);
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

		D_ASSERT(!expr_list.values.empty());
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
				if (expr_list.values[list_idx][col_idx]->GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
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

	auto &properties = GetStatementProperties();
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::CHANGED_ROWS;
	return result;
}

} // namespace duckdb

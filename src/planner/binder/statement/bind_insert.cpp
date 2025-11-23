#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"
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
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

namespace duckdb {

void Binder::CheckInsertColumnCountMismatch(idx_t expected_columns, idx_t result_columns, bool columns_provided,
                                            const string &tname) {
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

void Binder::TryReplaceDefaultExpression(unique_ptr<ParsedExpression> &expr, const ColumnDefinition &column) {
	if (expr->GetExpressionType() != ExpressionType::VALUE_DEFAULT) {
		return;
	}
	expr = ExpandDefaultExpression(column);
}

void Binder::ExpandDefaultInValuesList(InsertStatement &stmt, TableCatalogEntry &table,
                                       optional_ptr<ExpressionListRef> values_list,
                                       const vector<LogicalIndex> &named_column_map) {
	if (!values_list) {
		return;
	}
	idx_t expected_columns = stmt.columns.empty() ? table.GetColumns().PhysicalColumnCount() : stmt.columns.size();

	// special case: check if we are inserting from a VALUES statement
	if (values_list) {
		auto &expr_list = values_list->Cast<ExpressionListRef>();
		expr_list.expected_types.resize(expected_columns);
		expr_list.expected_names.resize(expected_columns);

		D_ASSERT(!expr_list.values.empty());
		CheckInsertColumnCountMismatch(expected_columns, expr_list.values[0].size(), !stmt.columns.empty(), table.name);

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
				TryReplaceDefaultExpression(expr_list.values[list_idx][col_idx], column);
			}
		}
	}
}

void DoUpdateSetQualify(unique_ptr<ParsedExpression> &expr, const string &table_name,
                        vector<unordered_set<string>> &lambda_params);

void DoUpdateSetQualifyInLambda(FunctionExpression &function, const string &table_name,
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

void DoUpdateSetQualify(unique_ptr<ParsedExpression> &expr, const string &table_name,
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

void Binder::BindInsertColumnList(TableCatalogEntry &table, vector<string> &columns, bool default_values,
                                  vector<LogicalIndex> &named_column_map, vector<LogicalType> &expected_types,
                                  IndexVector<idx_t, PhysicalIndex> &column_index_map) {
	if (!columns.empty() || default_values) {
		// insertion statement specifies column list

		// create a mapping of (list index) -> (column index)
		case_insensitive_map_t<idx_t> column_name_map;
		for (idx_t i = 0; i < columns.size(); i++) {
			auto entry = column_name_map.insert(make_pair(columns[i], i));
			if (!entry.second) {
				throw BinderException("Duplicate column name \"%s\" in INSERT", columns[i]);
			}
			auto column_index = table.GetColumnIndex(columns[i]);
			if (column_index.index == COLUMN_IDENTIFIER_ROW_ID) {
				throw BinderException("Cannot explicitly insert values into rowid column");
			}
			auto &col = table.GetColumn(column_index);
			if (col.Generated()) {
				throw BinderException("Cannot insert into a generated column");
			}
			expected_types.push_back(col.Type());
			named_column_map.push_back(column_index);
		}
		for (auto &col : table.GetColumns().Physical()) {
			auto entry = column_name_map.find(col.Name());
			if (entry == column_name_map.end()) {
				// column not specified, set index to DConstants::INVALID_INDEX
				column_index_map.push_back(DConstants::INVALID_INDEX);
			} else {
				// column was specified, set to the index
				column_index_map.push_back(entry->second);
			}
		}
	} else {
		// insert by position and no columns specified - insertion into all columns of the table
		// intentionally don't populate 'column_index_map' as an indication of this
		for (auto &col : table.GetColumns().Physical()) {
			named_column_map.push_back(col.Logical());
			expected_types.push_back(col.Type());
		}
	}
}

unique_ptr<MergeIntoStatement> Binder::GenerateMergeInto(InsertStatement &stmt, TableCatalogEntry &table) {
	D_ASSERT(stmt.on_conflict_info);

	auto &on_conflict_info = *stmt.on_conflict_info;
	auto merge_into = make_uniq<MergeIntoStatement>();
	// set up the target table
	string table_name = !stmt.table_ref->alias.empty() ? stmt.table_ref->alias : stmt.table;
	merge_into->target = std::move(stmt.table_ref);

	auto storage_info = table.GetStorageInfo(context);
	auto &columns = table.GetColumns();
	// set up the columns on which to join
	vector<vector<string>> all_distinct_on_columns;
	if (on_conflict_info.indexed_columns.empty()) {
		// When omitting the conflict target, we derive the join columns from the primary key/unique constraints
		// traverse the primary key/unique constraints

		vector<unique_ptr<ParsedExpression>> join_conditions;
		// We check if there are any constraints on the table, if there aren't we throw an error.
		idx_t found_matching_indexes = 0;
		for (auto &index : storage_info.index_info) {
			if (!index.is_unique) {
				continue;
			}

			vector<unique_ptr<ParsedExpression>> and_children;
			auto &indexed_columns = index.column_set;
			vector<string> distinct_on_columns;
			for (auto &column : columns.Physical()) {
				if (!indexed_columns.count(column.Physical().index)) {
					continue;
				}
				auto lhs = make_uniq<ColumnRefExpression>(column.Name(), table_name);
				auto rhs = make_uniq<ColumnRefExpression>(column.Name(), "excluded");
				auto new_condition =
				    make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(lhs), std::move(rhs));
				and_children.push_back(std::move(new_condition));
				distinct_on_columns.push_back(column.Name());
			}
			all_distinct_on_columns.push_back(std::move(distinct_on_columns));
			if (and_children.empty()) {
				continue;
			}
			unique_ptr<ParsedExpression> condition;
			if (and_children.size() == 1) {
				condition = std::move(and_children[0]);
			} else {
				// AND together
				condition = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(and_children));
			}
			join_conditions.push_back(std::move(condition));
			found_matching_indexes++;
		}
		unique_ptr<ParsedExpression> join_condition;
		if (join_conditions.size() == 1) {
			join_condition = std::move(join_conditions[0]);
		} else {
			// OR together
			join_condition =
			    make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(join_conditions));
		}
		merge_into->join_condition = std::move(join_condition);

		if (!found_matching_indexes) {
			throw BinderException("There are no UNIQUE/PRIMARY KEY constraints that refer to this table, specify ON "
			                      "CONFLICT columns manually");
		} else if (found_matching_indexes != 1) {
			if (on_conflict_info.action_type != OnConflictAction::NOTHING) {
				// When no conflict target is provided, and the action type is UPDATE,
				// we only allow the operation when only a single Index exists
				throw BinderException("Conflict target has to be provided for a DO UPDATE operation when the table has "
				                      "multiple UNIQUE/PRIMARY KEY constraints");
			}
		}
	} else {
		// when on conflict columns are explicitly provided - use them directly
		// first figure out if there is an index on the columns or not
		case_insensitive_map_t<idx_t> specified_columns;
		for (idx_t i = 0; i < on_conflict_info.indexed_columns.size(); i++) {
			specified_columns[on_conflict_info.indexed_columns[i]] = i;
			auto column_index = table.GetColumnIndex(on_conflict_info.indexed_columns[i]);
			if (column_index.index == COLUMN_IDENTIFIER_ROW_ID) {
				throw BinderException("Cannot specify ROWID as ON CONFLICT target");
			}
			auto &col = columns.GetColumn(column_index);
			if (col.Generated()) {
				throw BinderException("Cannot specify a generated column as ON CONFLICT target");
			}
		}
		unordered_set<column_t> on_conflict_filter;
		for (auto &col : columns.Physical()) {
			auto entry = specified_columns.find(col.Name());
			if (entry != specified_columns.end()) {
				// column was specified, set to the index
				on_conflict_filter.insert(col.Physical().index);
			}
		}
		bool index_references_columns = false;
		for (auto &index : storage_info.index_info) {
			if (!index.is_unique) {
				continue;
			}
			bool index_matches = on_conflict_filter == index.column_set;
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
		all_distinct_on_columns.push_back(on_conflict_info.indexed_columns);
		merge_into->using_columns = std::move(on_conflict_info.indexed_columns);
	}

	// expand any default values
	auto values_list = stmt.GetValuesList();
	if (values_list) {
		vector<LogicalIndex> named_column_map;
		if (stmt.columns.empty()) {
			for (auto &col : table.GetColumns().Physical()) {
				named_column_map.push_back(col.Logical());
			}
		} else {
			for (auto &col_name : stmt.columns) {
				auto &col = table.GetColumn(col_name);
				named_column_map.push_back(col.Logical());
			}
		}
		ExpandDefaultInValuesList(stmt, table, values_list, named_column_map);
	}
	// set up the data source
	unique_ptr<TableRef> source;
	if (stmt.select_statement) {
		source = make_uniq<SubqueryRef>(std::move(stmt.select_statement), "excluded");
	} else {
		source = make_uniq<EmptyTableRef>();
	}
	if (stmt.column_order == InsertColumnOrder::INSERT_BY_POSITION) {
		// if we are inserting by position add the columns of the target table as an alias to the source
		if (!stmt.columns.empty() || stmt.default_values) {
			// we are not emitting all columns - set the column set as the set of aliases
			source->column_name_alias = stmt.columns;

			// now push another subquery that adds the default columns
			auto select_stmt = make_uniq<SelectStatement>();
			auto select_node = make_uniq<SelectNode>();
			unordered_set<string> set_columns;
			for (auto &set_col : stmt.columns) {
				set_columns.insert(set_col);
			}

			for (auto &column : columns.Physical()) {
				auto &name = column.Name();
				unique_ptr<ParsedExpression> expr;
				if (set_columns.find(name) == set_columns.end()) {
					// column is not specified - at the default value
					if (column.HasDefaultValue()) {
						expr = column.DefaultValue().Copy();
					} else {
						expr = make_uniq<ConstantExpression>(Value(column.Type()));
					}
				} else {
					// column is specified - add a reference to it
					expr = make_uniq<ColumnRefExpression>(name);
				}
				select_node->select_list.push_back(std::move(expr));
			}
			select_node->from_table = std::move(source);
			select_stmt->node = std::move(select_node);

			source = make_uniq<SubqueryRef>(std::move(select_stmt), "excluded");
		}
		// push all columns of the table as an alias
		for (auto &column : columns.Physical()) {
			source->column_name_alias.push_back(column.Name());
		}
	}
	// push DISTINCT ON(unique_columns)
	for (auto &distinct_on_columns : all_distinct_on_columns) {
		auto distinct_stmt = make_uniq<SelectStatement>();
		auto select_node = make_uniq<SelectNode>();
		auto distinct = make_uniq<DistinctModifier>();
		for (auto &col : distinct_on_columns) {
			distinct->distinct_on_targets.push_back(make_uniq<ColumnRefExpression>(col));
		}
		select_node->modifiers.push_back(std::move(distinct));
		select_node->select_list.push_back(make_uniq<StarExpression>());
		select_node->from_table = std::move(source);
		distinct_stmt->node = std::move(select_node);
		source = make_uniq<SubqueryRef>(std::move(distinct_stmt), "excluded");
	}

	merge_into->source = std::move(source);

	if (on_conflict_info.action_type == OnConflictAction::REPLACE) {
		D_ASSERT(!on_conflict_info.set_info);
		on_conflict_info.set_info = CreateSetInfoForReplace(table, stmt, storage_info);
		on_conflict_info.action_type = OnConflictAction::UPDATE;
	}
	// now set up the merge actions
	// first set up the base (insert) action when not matched
	auto insert_action = make_uniq<MergeIntoAction>();
	insert_action->action_type = MergeActionType::MERGE_INSERT;
	insert_action->column_order = stmt.column_order;

	merge_into->actions[MergeActionCondition::WHEN_NOT_MATCHED_BY_TARGET].push_back(std::move(insert_action));

	if (on_conflict_info.condition) {
		throw BinderException("ON CONFLICT WHERE clause is only supported in DO UPDATE SET ... WHERE ...\nThe WHERE "
		                      "clause after the conflict columns is used for partial indexes which are not supported.");
	}
	if (on_conflict_info.action_type == OnConflictAction::UPDATE) {
		// when doing UPDATE set up the when matched action
		auto update_action = make_uniq<MergeIntoAction>();
		update_action->action_type = MergeActionType::MERGE_UPDATE;
		for (auto &col : on_conflict_info.set_info->expressions) {
			vector<unordered_set<string>> lambda_params;
			DoUpdateSetQualify(col, table_name, lambda_params);
		}
		if (on_conflict_info.set_info->condition) {
			vector<unordered_set<string>> lambda_params;
			DoUpdateSetQualify(on_conflict_info.set_info->condition, table_name, lambda_params);
			update_action->condition = std::move(on_conflict_info.set_info->condition);
		}
		update_action->update_info = std::move(on_conflict_info.set_info);

		merge_into->actions[MergeActionCondition::WHEN_MATCHED].push_back(std::move(update_action));
	}

	// move over extra properties
	merge_into->cte_map = std::move(stmt.cte_map);
	merge_into->returning_list = std::move(stmt.returning_list);
	return merge_into;
}

BoundStatement Binder::Bind(InsertStatement &stmt) {
	BoundStatement result;
	result.names = {"Count"};
	result.types = {LogicalType::BIGINT};

	BindSchemaOrCatalog(stmt.catalog, stmt.schema);
	auto &table = Catalog::GetEntry<TableCatalogEntry>(context, stmt.catalog, stmt.schema, stmt.table);
	if (stmt.on_conflict_info) {
		// generate a MERGE INTO statement and bind it instead
		auto merge_into = GenerateMergeInto(stmt, table);
		return Bind(*merge_into);
	}
	if (!table.temporary) {
		// inserting into a non-temporary table: alters underlying database
		auto &properties = GetStatementProperties();
		properties.RegisterDBModify(table.catalog, context);
	}

	auto insert = make_uniq<LogicalInsert>(table, GenerateTableIndex());

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
	BindInsertColumnList(table, stmt.columns, stmt.default_values, named_column_map, insert->expected_types,
	                     insert->column_index_map);

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
	ExpandDefaultInValuesList(stmt, table, values_list, named_column_map);

	// parse select statement and add to logical plan
	unique_ptr<LogicalOperator> root;
	if (stmt.select_statement) {
		if (stmt.column_order == InsertColumnOrder::INSERT_BY_POSITION) {
			auto select_binder = Binder::CreateBinder(context, this);
			root_select = select_binder->Bind(*stmt.select_statement);
			MoveCorrelatedExpressions(*select_binder);
		}
		// inserting from a select - check if the column count matches
		CheckInsertColumnCountMismatch(expected_columns, root_select.types.size(), !stmt.columns.empty(), table.name);

		root = CastLogicalOperatorToTypes(root_select.types, insert->expected_types, std::move(root_select.plan));
	} else {
		root = make_uniq<LogicalDummyScan>(GenerateTableIndex());
	}

	insert->AddChild(std::move(root));
	if (!stmt.returning_list.empty()) {
		insert->return_chunk = true;
		auto insert_table_index = GenerateTableIndex();
		insert->table_index = insert_table_index;
		unique_ptr<LogicalOperator> index_as_logicaloperator = std::move(insert);

		return BindReturning(std::move(stmt.returning_list), table, stmt.table_ref ? stmt.table_ref->alias : string(),
		                     insert_table_index, std::move(index_as_logicaloperator));
	}

	D_ASSERT(result.types.size() == result.names.size());
	result.plan = std::move(insert);

	auto &properties = GetStatementProperties();
	properties.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	properties.return_type = StatementReturnType::CHANGED_ROWS;
	return result;
}

} // namespace duckdb

#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"
#include "duckdb/planner/tableref/bound_joinref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_binder/insert_binder.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/expression/default_expression.hpp"
#include "duckdb/parser/tableref/bound_ref_wrapper.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/expression_binder/projection_binder.hpp"
#include <algorithm>

namespace duckdb {

vector<unique_ptr<ParsedExpression>> GenerateColumnReferences(Binder &binder, const vector<BindingAlias> &aliases,
                                                              const vector<string> &names) {
	vector<unique_ptr<ParsedExpression>> result;
	D_ASSERT(aliases.size() == names.size());

	for (idx_t c = 0; c < aliases.size(); c++) {
		auto colref = binder.bind_context.CreateColumnReference(aliases[c], names[c],
		                                                        ColumnBindType::DO_NOT_EXPAND_GENERATED_COLUMNS);
		result.push_back(std::move(colref));
	}
	return result;
}

unique_ptr<BoundMergeIntoAction> Binder::BindMergeAction(LogicalMergeInto &merge_into, TableCatalogEntry &table,
                                                         LogicalGet &get, idx_t proj_index,
                                                         vector<unique_ptr<Expression>> &expressions,
                                                         unique_ptr<LogicalOperator> &root, MergeIntoAction &action,
                                                         const vector<BindingAlias> &source_aliases,
                                                         const vector<string> &source_names) {
	auto result = make_uniq<BoundMergeIntoAction>();
	result->action_type = action.action_type;
	if (action.condition) {
		if (action.condition->HasSubquery()) {
			// if we have a subquery we need to execute the condition outside of the MERGE INTO statement
			WhereBinder where_binder(*this, context);
			auto cond = where_binder.Bind(action.condition);
			PlanSubqueries(cond, root);
			result->condition =
			    make_uniq<BoundColumnRefExpression>(cond->return_type, ColumnBinding(proj_index, expressions.size()));
			expressions.push_back(std::move(cond));
		} else {
			ProjectionBinder proj_binder(*this, context, proj_index, expressions, "WHERE clause");
			proj_binder.target_type = LogicalType::BOOLEAN;
			auto cond = proj_binder.Bind(action.condition);
			result->condition = std::move(cond);
		}
	}
	switch (action.action_type) {
	case MergeActionType::MERGE_UPDATE: {
		if (!action.update_info) {
			// empty update list - generate it
			action.update_info = make_uniq<UpdateSetInfo>();
			if (action.column_order == InsertColumnOrder::INSERT_BY_NAME) {
				// UPDATE BY NAME - get the name list from the source binder
				action.update_info->columns = source_names;
			} else {
				// UPDATE BY POSITION - get the name list from the table
				for (auto &col : table.GetColumns().Physical()) {
					action.update_info->columns.push_back(col.Name());
				}
			}
			if (source_names.size() != action.update_info->columns.size()) {
				throw BinderException(
				    "Data provided for UPDATE did not match column count in table - expected %d columns but got %d",
				    action.update_info->columns.size(), source_names.size());
			}
			action.update_info->expressions = GenerateColumnReferences(*this, source_aliases, source_names);
		}
		BindUpdateSet(proj_index, root, *action.update_info, table, result->columns, result->expressions, expressions);

		// bind any additional columns that need to be bound for update constraints
		// FIXME: this is pretty hacky
		// construct a dummy projection and update
		LogicalProjection proj(proj_index, std::move(expressions));
		LogicalUpdate update(table);
		update.return_chunk = merge_into.return_chunk;
		update.columns = std::move(result->columns);
		update.expressions = std::move(result->expressions);
		update.bound_defaults = std::move(merge_into.bound_defaults);
		update.bound_constraints = std::move(merge_into.bound_constraints);
		update.update_is_del_and_insert = false;

		// call BindUpdateConstraints
		table.BindUpdateConstraints(*this, get, proj, update, context);

		// move all moved values back
		merge_into.bound_defaults = std::move(update.bound_defaults);
		merge_into.bound_constraints = std::move(update.bound_constraints);
		expressions = std::move(proj.expressions);
		result->columns = std::move(update.columns);
		result->expressions = std::move(update.expressions);
		result->update_is_del_and_insert = update.update_is_del_and_insert;
		break;
	}
	case MergeActionType::MERGE_INSERT: {
		if (action.column_order == InsertColumnOrder::INSERT_BY_NAME) {
			// INSERT BY NAME - get the name list from the source binder and push it into the table
			if (!action.insert_columns.empty()) {
				throw InternalException("INSERT BY NAME cannot be combined with a column list");
			}
			action.insert_columns = source_names;
		}
		vector<LogicalIndex> named_column_map;
		vector<LogicalType> expected_types;
		BindInsertColumnList(table, action.insert_columns, action.default_values, named_column_map, expected_types,
		                     result->column_index_map);

		vector<unique_ptr<Expression>> insert_expressions;
		if (!action.default_values && action.expressions.empty()) {
			// no expressions: *
			// expand source bindings
			action.expressions = GenerateColumnReferences(*this, source_aliases, source_names);
		}
		CheckInsertColumnCountMismatch(expected_types.size(), action.expressions.size(), !action.insert_columns.empty(),
		                               table.name);
		// explicit expressions - plan them
		for (idx_t i = 0; i < action.expressions.size(); i++) {
			auto &column = table.GetColumns().GetColumn(named_column_map[i]);

			InsertBinder insert_binder(*this, context);
			insert_binder.target_type = column.Type();

			TryReplaceDefaultExpression(action.expressions[i], column);
			auto insert_expr = insert_binder.Bind(action.expressions[i]);

			PlanSubqueries(insert_expr, root);
			insert_expressions.push_back(std::move(insert_expr));
		}

		for (auto &insert_expr : insert_expressions) {
			result->expressions.push_back(make_uniq<BoundColumnRefExpression>(
			    insert_expr->return_type, ColumnBinding(proj_index, expressions.size())));
			expressions.push_back(std::move(insert_expr));
		}
		break;
	}
	case MergeActionType::MERGE_ERROR: {
		// bind the error message (if any)
		for (auto &expr : action.expressions) {
			ProjectionBinder proj_binder(*this, context, proj_index, expressions, "Error Message");
			proj_binder.target_type = LogicalType::VARCHAR;
			auto error_msg = proj_binder.Bind(expr);
			result->expressions.push_back(std::move(error_msg));
		}
		break;
	}
	case MergeActionType::MERGE_DELETE:
	case MergeActionType::MERGE_DO_NOTHING:
		// DELETE / DO NOTHING have nothing extra to bind
		break;
	default:
		throw InternalException("Unsupported merge action type");
	}
	return result;
}

void RewriteMergeBindings(unique_ptr<Expression> &expr, const vector<ColumnBinding> &source_bindings,
                          idx_t new_table_index) {
	ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
	    expr, [&](BoundColumnRefExpression &bound_colref, unique_ptr<Expression> &expr) {
		    for (idx_t i = 0; i < source_bindings.size(); i++) {
			    if (bound_colref.binding == source_bindings[i]) {
				    bound_colref.binding.table_index = new_table_index;
				    bound_colref.binding.column_index = i;
			    }
		    }
	    });
}

void RewriteMergeBindings(LogicalOperator &op, const vector<ColumnBinding> &source_bindings, idx_t new_table_index) {
	LogicalOperatorVisitor::EnumerateExpressions(
	    op, [&](unique_ptr<Expression> *child) { RewriteMergeBindings(*child, source_bindings, new_table_index); });
}

LogicalGet &ExtractLogicalGet(LogicalOperator &op) {
	reference<LogicalOperator> current_op(op);
	while (current_op.get().type == LogicalOperatorType::LOGICAL_FILTER) {
		current_op = *current_op.get().children[0];
	}
	if (current_op.get().type != LogicalOperatorType::LOGICAL_GET) {
		throw InvalidInputException("BindMerge - expected to find an operator of type LOGICAL_GET but got %s",
		                            op.ToString());
	}
	return current_op.get().Cast<LogicalGet>();
}

void CheckMergeAction(MergeActionCondition condition, MergeActionType action_type) {
	if (condition == MergeActionCondition::WHEN_NOT_MATCHED_BY_TARGET) {
		switch (action_type) {
		case MergeActionType::MERGE_UPDATE:
		case MergeActionType::MERGE_DELETE:
			throw ParserException("WHEN NOT MATCHED (BY TARGET) cannot be combined with UPDATE or DELETE actions - as "
			                      "there is no corresponding row in the target to update or delete.\nDid you mean to "
			                      "use WHEN MATCHED or WHEN NOT MATCHED BY SOURCE?");
		default:
			break;
		}
	}
}

BoundStatement Binder::Bind(MergeIntoStatement &stmt) {
	// bind the target table
	auto target_binder = Binder::CreateBinder(context, this);
	string table_alias = stmt.target->alias;
	auto bound_table = target_binder->Bind(*stmt.target);
	if (bound_table.plan->type != LogicalOperatorType::LOGICAL_GET) {
		throw BinderException("Can only merge into base tables!");
	}
	auto table_ptr = bound_table.plan->Cast<LogicalGet>().GetTable();
	if (!table_ptr) {
		throw BinderException("Can only merge into base tables!");
	}
	auto &table = *table_ptr;
	if (!table.temporary) {
		// update of persistent table: not read only!
		auto &properties = GetStatementProperties();
		properties.RegisterDBModify(table.catalog, context);
	}

	// bind the source
	auto source_binder = Binder::CreateBinder(context, this);
	auto source_binding = source_binder->Bind(*stmt.source);

	// get the source names/types
	vector<BindingAlias> source_aliases;
	vector<string> source_names;
	for (auto &binding_entry : source_binder->bind_context.GetBindingsList()) {
		auto &binding = *binding_entry;
		auto &column_names = binding.GetColumnNames();
		for (idx_t c = 0; c < column_names.size(); c++) {
			source_aliases.push_back(binding.GetBindingAlias());
			source_names.push_back(column_names[c]);
		}
	}

	// bind the join between the source and target
	// our conditions determine the join type we need
	// if we have WHEN NOT MATCHED BY SOURCE we need all source rows -> RIGHT join
	// if we have WHEN NOT MATCHED BY TARGET we need all target rows -> LEFT join
	// if we have both                                               -> FULL join
	// if we only have WHEN MATCHED we only need matches             -> INNER join
	JoinRef join;
	auto has_not_matched_by_source = stmt.actions.count(MergeActionCondition::WHEN_NOT_MATCHED_BY_SOURCE) > 0;
	auto has_not_matched_by_target = stmt.actions.count(MergeActionCondition::WHEN_NOT_MATCHED_BY_TARGET) > 0;
	if (has_not_matched_by_source && has_not_matched_by_target) {
		join.type = JoinType::OUTER;
	} else if (has_not_matched_by_source) {
		join.type = JoinType::RIGHT;
	} else if (has_not_matched_by_target) {
		join.type = JoinType::LEFT;
	} else {
		join.type = JoinType::INNER;
	}
	join.left = make_uniq<BoundRefWrapper>(std::move(source_binding), std::move(source_binder));
	join.right = make_uniq<BoundRefWrapper>(std::move(bound_table), std::move(target_binder));
	if (stmt.join_condition) {
		join.condition = std::move(stmt.join_condition);
	} else {
		join.using_columns = std::move(stmt.using_columns);
	}
	auto bound_join_node = Bind(join);

	auto root = std::move(bound_join_node.plan);
	auto join_ref = reference<LogicalOperator>(*root);
	while (join_ref.get().children.size() == 1) {
		join_ref = *join_ref.get().children[0];
	}
	if (join_ref.get().children.size() != 2) {
		throw NotImplementedException("Expected a join after binding a join operator - but got a %s",
		                              join_ref.get().type);
	}
	// kind of hacky, CreatePlan turns a RIGHT join into a LEFT join so the children get reversed from what we need
	bool inverted = join.type == JoinType::RIGHT;
	auto &source = join_ref.get().children[inverted ? 1 : 0];
	auto &get = ExtractLogicalGet(*join_ref.get().children[inverted ? 0 : 1]);

	auto merge_into = make_uniq<LogicalMergeInto>(table);
	merge_into->table_index = GenerateTableIndex();
	if (!stmt.returning_list.empty()) {
		merge_into->return_chunk = true;
	}

	// bind table constraints/default values in case these are referenced
	auto &catalog_name = table.ParentCatalog().GetName();
	auto &schema_name = table.ParentSchema().name;
	BindDefaultValues(table.GetColumns(), merge_into->bound_defaults, catalog_name, schema_name);

	merge_into->bound_constraints = BindConstraints(table);

	// bind the merge actions
	auto proj_index = GenerateTableIndex();
	vector<unique_ptr<Expression>> projection_expressions;

	for (auto &entry : stmt.actions) {
		vector<unique_ptr<BoundMergeIntoAction>> bound_actions;
		for (auto &action : entry.second) {
			CheckMergeAction(entry.first, action->action_type);
			bound_actions.push_back(BindMergeAction(*merge_into, table, get, proj_index, projection_expressions, root,
			                                        *action, source_aliases, source_names));
		}
		merge_into->actions.emplace(entry.first, std::move(bound_actions));
	}

	if (has_not_matched_by_source) {
		// if we have "has_not_matched_by_source" we need to push an extra marker into the source
		// this marker tells us if we have found a source match or not
		auto new_proj_index = GenerateTableIndex();

		source->ResolveOperatorTypes();
		auto source_bindings = source->GetColumnBindings();
		vector<unique_ptr<Expression>> select_list;
		for (idx_t c = 0; c < source_bindings.size(); c++) {
			select_list.push_back(make_uniq<BoundColumnRefExpression>(source->types[c], source_bindings[c]));
		}

		// insert the source marker
		auto marker = make_uniq<BoundConstantExpression>(Value::INTEGER(42));
		marker->alias = "source_marker";
		ColumnBinding source_marker;
		source_marker = ColumnBinding(new_proj_index, select_list.size());
		select_list.push_back(std::move(marker));

		// construct the new projection
		auto proj = make_uniq<LogicalProjection>(new_proj_index, std::move(select_list));
		proj->children.push_back(std::move(source));
		source = std::move(proj);

		// rewrite "column_bindings" in the join to refer to the new projection we have just pushed
		for (auto &expr : projection_expressions) {
			RewriteMergeBindings(expr, source_bindings, new_proj_index);
		}
		RewriteMergeBindings(*root, source_bindings, new_proj_index);
		RewriteMergeBindings(*merge_into, source_bindings, new_proj_index);

		// push a reference
		merge_into->source_marker = projection_expressions.size();
		auto marker_ref = make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, source_marker);
		marker_ref->alias = "source_marker";
		projection_expressions.push_back(std::move(marker_ref));
	}

	merge_into->row_id_start = projection_expressions.size();
	// finally bind the row id column and add them to the projection list
	BindRowIdColumns(table, get, projection_expressions);

	auto proj = make_uniq<LogicalProjection>(proj_index, std::move(projection_expressions));
	proj->AddChild(std::move(root));

	merge_into->AddChild(std::move(proj));

	if (!stmt.returning_list.empty()) {
		auto merge_table_index = merge_into->table_index;
		unique_ptr<LogicalOperator> index_as_logicaloperator = std::move(merge_into);

		// add the merge_action virtual column
		virtual_column_map_t virtual_columns;
		virtual_columns.insert(make_pair(VIRTUAL_COLUMN_START, TableColumn("merge_action", LogicalType::VARCHAR)));
		return BindReturning(std::move(stmt.returning_list), table, table_alias, merge_table_index,
		                     std::move(index_as_logicaloperator), std::move(virtual_columns));
	}

	BoundStatement result;
	result.plan = std::move(merge_into);
	result.names = {"Count"};
	result.types = {LogicalType::BIGINT};

	auto &properties = GetStatementProperties();
	properties.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	properties.return_type = StatementReturnType::CHANGED_ROWS;
	return result;
}

} // namespace duckdb

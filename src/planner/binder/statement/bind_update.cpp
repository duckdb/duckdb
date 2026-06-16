#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_joinref.hpp"
#include "duckdb/planner/constraints/bound_check_constraint.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_default_expression.hpp"
#include "duckdb/planner/expression_binder/update_binder.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/storage/data_table.hpp"

#include <algorithm>

namespace duckdb {

void Binder::BindUpdateSet(TableIndex proj_index, unique_ptr<LogicalOperator> &root, UpdateSetInfo &set_info,
                           TableCatalogEntry &table, vector<PhysicalIndex> &columns,
                           vector<unique_ptr<Expression>> &update_expressions,
                           vector<unique_ptr<Expression>> &projection_expressions, bool prioritize_table_when_binding) {
	D_ASSERT(set_info.columns.size() == set_info.expressions.size());

	Binder *expr_binder_ptr = this;
	shared_ptr<Binder> binder_with_search_path;

	if (prioritize_table_when_binding) {
		binder_with_search_path =
		    CreateBinderWithSearchPath(table.ParentCatalog().GetName(), table.ParentSchema().name);
		expr_binder_ptr = binder_with_search_path.get();
	}

	auto &all_columns = table.GetColumns();

	// Bind one column assignment and add it to the UPDATE projection. Shared by
	// the explicit SET list and the stored-generated recompute below so the two
	// paths stay in lockstep.
	auto append_assignment = [&](const ColumnDefinition &column, unique_ptr<ParsedExpression> &expr) {
		columns.push_back(column.Physical());
		if (expr->GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
			update_expressions.push_back(make_uniq<BoundDefaultExpression>(column.Type()));
			return;
		}
		UpdateBinder binder(*expr_binder_ptr, context);
		binder.target_type = column.Type();
		auto bound_expr = binder.Bind(expr);
		PlanSubqueries(bound_expr, root);
		auto bound_type = bound_expr->GetReturnType();
		auto expr_index = ColumnBinding::PushExpression(projection_expressions, std::move(bound_expr));
		update_expressions.push_back(
		    make_uniq<BoundColumnRefExpression>(bound_type, ColumnBinding(proj_index, expr_index)));
	};

	// Stored generated columns are recomputed below when an UPDATE assigns a
	// column they derive from; with none present, skip all of that work.
	auto logical_columns = all_columns.Logical();
	bool has_stored_generated =
	    std::any_of(logical_columns.begin(), logical_columns.end(),
	                [](const ColumnDefinition &col) { return col.Category() == TableColumnType::GENERATED_STORED; });

	// Snapshot the parsed SET expressions before the bind loop consumes them.
	case_insensitive_map_t<unique_ptr<ParsedExpression>> parsed_set_exprs;
	if (has_stored_generated) {
		parsed_set_exprs.reserve(set_info.columns.size());
		for (idx_t i = 0; i < set_info.columns.size(); i++) {
			parsed_set_exprs[set_info.columns[i]] = set_info.expressions[i]->Copy();
		}
	}

	for (idx_t i = 0; i < set_info.columns.size(); i++) {
		auto &colname = set_info.columns[i];
		auto &expr = set_info.expressions[i];
		if (!table.ColumnExists(colname)) {
			vector<string> column_names;
			for (auto &col : all_columns.Physical()) {
				column_names.push_back(col.Name());
			}
			auto candidates = StringUtil::CandidatesErrorMessage(column_names, colname, "Did you mean");
			throw BinderException("Referenced update column %s not found in table!\n%s", colname, candidates);
		}
		auto &column = table.GetColumn(colname);
		if (column.Generated()) {
			throw BinderException("Cant update column \"%s\" because it is a generated column!", column.Name());
		}
		if (std::find(columns.begin(), columns.end(), column.Physical()) != columns.end()) {
			throw BinderException("Multiple assignments to same column \"%s\"", colname);
		}
		append_assignment(column, expr);
	}

	if (!has_stored_generated) {
		return;
	}
	// Recompute each stored generated column whose value depends on an assigned
	// column. Generated-column references are expanded inline -- the same
	// recursion the binder uses for INSERT defaults and SELECT -- and assigned
	// columns are substituted with their new value, so chained and virtual
	// dependencies resolve without an explicit ordering pass.
	for (auto &column : all_columns.Logical()) {
		if (column.Category() != TableColumnType::GENERATED_STORED) {
			continue;
		}
		auto recompute = column.GeneratedExpression().Copy();
		bool depends_on_update = false;
		auto expand = [&](this auto &self, unique_ptr<ParsedExpression> &e) -> void {
			if (e->GetExpressionClass() != ExpressionClass::COLUMN_REF) {
				ParsedExpressionIterator::EnumerateChildren(*e,
				                                            [&](unique_ptr<ParsedExpression> &child) { self(child); });
				return;
			}
			const auto &name = e->Cast<ColumnRefExpression>().GetColumnName();
			if (auto entry = parsed_set_exprs.find(name); entry != parsed_set_exprs.end()) {
				// Assigned column: substitute its new value (DEFAULT means the column's
				// default expression, or NULL when it has none). Its own refs read the
				// old row, so this is not expanded further.
				depends_on_update = true;
				if (entry->second->GetExpressionType() != ExpressionType::VALUE_DEFAULT) {
					e = entry->second->Copy();
				} else {
					auto &base_col = table.GetColumn(name);
					e = base_col.HasDefaultValue() ? base_col.DefaultValue().Copy()
					                               : make_uniq<ConstantExpression>(Value(base_col.Type()));
				}
				return;
			}
			if (table.ColumnExists(name) && table.GetColumn(name).Generated()) {
				// Generated dependency: inline its expression and keep expanding.
				e = table.GetColumn(name).GeneratedExpression().Copy();
				self(e);
			}
		};
		expand(recompute);
		if (depends_on_update) {
			append_assignment(column, recompute);
		}
	}
}

// This creates a LogicalProjection and moves 'root' into it as a child
// unless there are no expressions to project, in which case it just returns 'root'
unique_ptr<LogicalOperator> Binder::BindUpdateSet(LogicalOperator &op, unique_ptr<LogicalOperator> root,
                                                  UpdateSetInfo &set_info, TableCatalogEntry &table,
                                                  vector<PhysicalIndex> &columns, bool prioritize_table_when_binding) {
	auto proj_index = GenerateTableIndex();

	vector<unique_ptr<Expression>> projection_expressions;
	BindUpdateSet(proj_index, root, set_info, table, columns, op.expressions, projection_expressions,
	              prioritize_table_when_binding);
	if (op.type != LogicalOperatorType::LOGICAL_UPDATE && projection_expressions.empty()) {
		return root;
	}
	// now create the projection
	auto proj = make_uniq<LogicalProjection>(proj_index, std::move(projection_expressions));
	proj->AddChild(std::move(root));
	return unique_ptr_cast<LogicalProjection, LogicalOperator>(std::move(proj));
}

void Binder::BindRowIdColumns(TableCatalogEntry &table, LogicalGet &get, vector<unique_ptr<Expression>> &expressions) {
	auto row_id_columns = table.GetRowIdColumns();
	auto virtual_columns = table.GetVirtualColumns();
	auto &column_ids = get.GetColumnIds();
	for (auto &row_id_column : row_id_columns) {
		auto row_id_entry = virtual_columns.find(row_id_column);
		if (row_id_entry == virtual_columns.end()) {
			throw InternalException(
			    "BindRowIdColumns could not find the row id column in the virtual columns list of the table");
		}
		// check if this column has already been projected
		idx_t column_idx;
		for (column_idx = 0; column_idx < column_ids.size(); ++column_idx) {
			if (column_ids[column_idx].GetPrimaryIndex() == row_id_column) {
				// it has! avoid projecting it again
				break;
			}
		}
		auto row_id_expr = make_uniq<BoundColumnRefExpression>(
		    row_id_entry->second.type, ColumnBinding(get.table_index, ProjectionIndex(column_idx)));
		row_id_expr->SetAlias(row_id_entry->second.name);
		expressions.push_back(std::move(row_id_expr));
		if (column_idx == column_ids.size()) {
			get.AddColumnId(row_id_column);
		}
	}
}

BoundStatement Binder::Bind(UpdateStatement &stmt) {
	return Bind(*stmt.node);
}

BoundStatement Binder::BindNode(UpdateQueryNode &node) {
	unique_ptr<LogicalOperator> root;

	// visit the table reference
	auto bound_table = Bind(*node.table);
	if (bound_table.plan->type != LogicalOperatorType::LOGICAL_GET) {
		throw BinderException("Can only update base table");
	}
	auto &bound_table_get = bound_table.plan->Cast<LogicalGet>();
	auto table_ptr = bound_table_get.GetTable();
	if (!table_ptr) {
		throw BinderException("Can only update base table");
	}
	if (node.table->type == TableReferenceType::BASE_TABLE) {
		// A catalog may delegate the scan of its table to a storage table in
		// another catalog; the update targets the entry the name resolves to.
		auto &target_ref = node.table->Cast<BaseTableRef>();
		EntryLookupInfo table_lookup(CatalogType::TABLE_ENTRY, target_ref.table_name);
		auto resolved = Catalog::GetEntry(context, target_ref.catalog_name, target_ref.schema_name, table_lookup,
		                                  OnEntryNotFound::RETURN_NULL);
		if (resolved && resolved->type == CatalogType::TABLE_ENTRY) {
			table_ptr = &resolved->Cast<TableCatalogEntry>();
		}
	}
	auto &table = *table_ptr;

	if (auto expanded = TryExpandAfterTriggers(node, node.returning_list, table, TriggerEventType::UPDATE_EVENT)) {
		return std::move(*expanded);
	}

	optional_ptr<LogicalGet> get;
	if (node.from_table) {
		auto from_binder = Binder::CreateBinder(context, this);
		BoundJoinRef bound_crossproduct(JoinRefType::CROSS);
		bound_crossproduct.left = std::move(bound_table);
		bound_crossproduct.right = from_binder->Bind(*node.from_table);
		root = CreatePlan(bound_crossproduct);
		get = &root->children[0]->Cast<LogicalGet>();
		bind_context.AddContext(std::move(from_binder->bind_context));
	} else {
		root = std::move(bound_table.plan);
		get = &root->Cast<LogicalGet>();
	}

	if (!table.temporary) {
		// update of persistent table: not read only!
		auto &properties = GetStatementProperties();
		properties.RegisterDBModify(table.GetStorageCatalog(context), context, DatabaseModificationType::UPDATE_DATA);
	}
	auto update = make_uniq<LogicalUpdate>(table);

	// set return_chunk boolean early because it needs uses update_is_del_and_insert logic
	if (!node.returning_list.empty()) {
		update->return_chunk = true;
	}
	// bind the default values
	auto &catalog_name = table.ParentCatalog().GetName();
	auto &schema_name = table.ParentSchema().name;
	BindDefaultValues(table.GetColumns(), update->bound_defaults, catalog_name, schema_name);
	update->bound_constraints = BindConstraints(table);

	// project any additional columns required for the condition/expressions
	if (node.set_info->condition) {
		WhereBinder binder(*this, context);
		auto condition = binder.Bind(node.set_info->condition);

		PlanSubqueries(condition, root);
		auto filter = make_uniq<LogicalFilter>(std::move(condition));
		filter->AddChild(std::move(root));
		root = std::move(filter);
	}

	D_ASSERT(node.set_info);
	D_ASSERT(node.set_info->columns.size() == node.set_info->expressions.size());

	auto proj_tmp = BindUpdateSet(*update, std::move(root), *node.set_info, table, update->columns,
	                              node.prioritize_table_when_binding);
	D_ASSERT(proj_tmp->type == LogicalOperatorType::LOGICAL_PROJECTION);
	auto proj = unique_ptr_cast<LogicalOperator, LogicalProjection>(std::move(proj_tmp));

	// bind any extra columns necessary for CHECK constraints or indexes;
	// storage-derived decisions (index updates force delete+insert) come
	// from the scan-bound table when the catalog delegates storage
	auto storage_table = get->GetTable();
	if (storage_table && storage_table.get() != &table) {
		storage_table->BindUpdateConstraints(*this, *get, *proj, *update, context);
	} else {
		table.BindUpdateConstraints(*this, *get, *proj, *update, context);
	}

	// finally bind the row id column and add them to the projection list
	BindRowIdColumns(table, *get, proj->expressions);

	// set the projection as child of the update node and finalize the result
	update->AddChild(std::move(proj));

	auto update_table_index = GenerateTableIndex();
	update->table_index = update_table_index;
	if (!node.returning_list.empty()) {
		unique_ptr<LogicalOperator> update_as_logicaloperator = std::move(update);

		return BindReturning(std::move(node.returning_list), table, node.table->alias, update_table_index,
		                     std::move(update_as_logicaloperator));
	}

	BoundStatement result;
	result.names = {"Count"};
	result.types = {LogicalType::BIGINT};
	result.plan = std::move(update);

	auto &properties = GetStatementProperties();
	properties.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	properties.return_type = StatementReturnType::CHANGED_ROWS;
	return result;
}

} // namespace duckdb

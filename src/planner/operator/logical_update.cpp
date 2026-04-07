#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

LogicalUpdate::LogicalUpdate(TableCatalogEntry &table)
    : LogicalOperator(LogicalOperatorType::LOGICAL_UPDATE), table(table), table_index(0), return_chunk(false) {
}

LogicalUpdate::LogicalUpdate(ClientContext &context, const unique_ptr<CreateInfo> &table_info)
    : LogicalOperator(LogicalOperatorType::LOGICAL_UPDATE),
      table(Catalog::GetEntry<TableCatalogEntry>(context, table_info->catalog, table_info->schema,
                                                 table_info->Cast<CreateTableInfo>().table)) {
	auto binder = Binder::CreateBinder(context);
	bound_constraints = binder->BindConstraints(table);
}

idx_t LogicalUpdate::EstimateCardinality(ClientContext &context) {
	return return_chunk ? LogicalOperator::EstimateCardinality(context) : 1;
}

vector<ColumnBinding> LogicalUpdate::GetColumnBindings() {
	if (return_chunk) {
		return GenerateColumnBindings(table_index, table.GetTypes().size());
	}
	return {ColumnBinding(0, 0)};
}

void LogicalUpdate::ResolveTypes() {
	if (return_chunk) {
		types = table.GetTypes();
	} else {
		types.emplace_back(LogicalType::BIGINT);
	}
}

string LogicalUpdate::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

void LogicalUpdate::RewriteInPlaceUpdates(LogicalOperator &update_op) {
	auto &update = update_op.Cast<LogicalUpdate>();

	if (update.update_is_del_and_insert) {
		return;
	}
	auto needs_reinsert = false;
	for (auto &col_idx : update.columns) {
		auto &column = update.table.GetColumns().GetColumn(col_idx);
		if (!column.Type().SupportsRegularUpdate()) {
			needs_reinsert = true;
			break;
		}
	}
	if (!needs_reinsert) {
		return;
	}

	// Okay, we are reading an old plan version that has in-place updates for a type that no longer supports it.
	// We need to convert the update into a delete + insert.

	// The UPDATE operator always expects the rowid column to be the last.
	auto rowid_binding = update_op.children.back()->GetColumnBindings().back();

	// We're looking for the GET operator that produces this rowid, and therefore produce this binding.
	// There might be projections in betweeen though, so we need to traverse through them.
	auto target_binding = rowid_binding;
	vector<reference<unique_ptr<LogicalOperator>>> stack;

	// Collect all projections along the way, as well as the GET operator, so that we can update them later.
	vector<reference<unique_ptr<LogicalOperator>>> projections;
	unique_ptr<LogicalOperator> *get_op = nullptr;

	stack.emplace_back(update_op.children.back());

	auto is_done = false;

	while (!stack.empty() && !is_done) {
		auto &current = stack.back().get();
		stack.pop_back();

		switch (current->type) {
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			// Does this projection produce the target binding?
			auto &proj = current->Cast<LogicalProjection>();
			if (target_binding.table_index == proj.table_index) {
				// This is the projection we're looking for!
				projections.emplace_back(current);

				// Update the target binding.
				target_binding =
				    proj.expressions[target_binding.column_index]->Cast<BoundColumnRefExpression>().binding;

				// Traverse the child.
				stack.push_back(proj.children.back());
			}
		} break;
		case LogicalOperatorType::LOGICAL_GET: {
			// Is this the GET at the root of our projection chain?

			auto &get = current->Cast<LogicalGet>();
			if (target_binding.table_index == get.table_index) {
				// We found the GET operator we're looking for, we can stop traversing.
				is_done = true;
				get_op = &current;
				break;
			}
		} break;
		default: {
			// Otherwise, this is some random operator. Traverse all children!
			for (auto &child : current->children) {
				stack.emplace_back(child);
			}
		}
		}
	}

	// We should have found the GET operator by now, if not, this is an error in the plan.
	if (!is_done || !get_op) {
		throw InternalException("Could not find the expected GET operator in the LogicalUpdate operator children");
	}

	auto &get = get_op->get()->Cast<LogicalGet>();

	// Now, we need to update the GET operator to include ALL the other columns.
	physical_index_set_t all_columns;
	for (auto &column : update.table.GetColumns().Physical()) {
		all_columns.insert(column.Physical());
	}

	idx_t found_column_count = 0;
	physical_index_set_t found_columns;
	for (idx_t i = 0; i < update.columns.size(); i++) {
		if (all_columns.find(update.columns[i]) != all_columns.end()) {
			// this column is referenced already
			found_column_count++;
			found_columns.insert(update.columns[i]);
		}
	}

	if (found_column_count != all_columns.size()) {
		for (auto &physical_id : all_columns) {
			if (found_columns.find(physical_id) != found_columns.end()) {
				// column is already projected
				continue;
			}

			auto &column = update.table.GetColumns().GetColumn(physical_id);

			// Add it to the GET operator
			get.AddColumnId(column.Logical().index);

			// Now, for each projection on the path from the GET to the UPDATE, we need to add this column as well.
			// We do this in backwards order, always adding a column reference to the previously added column,
			// so that we end up with a chain of column references that all point to the newly added column in the GET.

			auto prev_col_idx = get.GetColumnIds().size() - 1;
			auto prev_tbl_idx = get.GetTableIndex().back();

			for (int64_t i = UnsafeNumericCast<int64_t>(projections.size()) - 1; i >= 0; i--) {
				auto &proj = projections[UnsafeNumericCast<idx_t>(i)].get()->Cast<LogicalProjection>();

				if (i == 0) {
					// This is the last projection, push the new columns next-to-last so that rowid remains last
					proj.expressions.insert(
					    proj.expressions.end() - 1,
					    make_uniq<BoundColumnRefExpression>(column.Type(), ColumnBinding(prev_tbl_idx, prev_col_idx)));

					prev_col_idx = proj.expressions.size() - 2;
					prev_tbl_idx = proj.table_index;
				} else {
					proj.expressions.push_back(
					    make_uniq<BoundColumnRefExpression>(column.Type(), ColumnBinding(prev_tbl_idx, prev_col_idx)));

					prev_col_idx = proj.expressions.size() - 1;
					prev_tbl_idx = proj.table_index;
				}
			}

			// Finally, add the column to the UPDATE operator too
			update.columns.push_back(physical_id);
			update.expressions.push_back(
			    make_uniq<BoundColumnRefExpression>(column.Type(), ColumnBinding(prev_tbl_idx, prev_col_idx)));
		}
	}

	update.update_is_del_and_insert = true;
	update.ResolveOperatorTypes();
}

} // namespace duckdb

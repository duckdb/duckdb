#include "duckdb/optimizer/remove_unused_columns.hpp"

#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/parser/parsed_data/vacuum_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"

namespace duckdb {

void RemoveUnusedColumns::ReplaceBinding(ColumnBinding current_binding, ColumnBinding new_binding) {
	auto colrefs = column_references.find(current_binding);
	if (colrefs != column_references.end()) {
		for (auto &colref : colrefs->second) {
			D_ASSERT(colref->binding == current_binding);
			colref->binding = new_binding;
		}
	}
}

template <class T>
void RemoveUnusedColumns::ClearUnusedExpressions(vector<T> &list, idx_t table_idx, bool replace) {
	idx_t offset = 0;
	for (idx_t col_idx = 0; col_idx < list.size(); col_idx++) {
		auto current_binding = ColumnBinding(table_idx, col_idx + offset);
		auto entry = column_references.find(current_binding);
		if (entry == column_references.end()) {
			// this entry is not referred to, erase it from the set of expressions
			list.erase_at(col_idx);
			offset++;
			col_idx--;
		} else if (offset > 0 && replace) {
			// column is used but the ColumnBinding has changed because of removed columns
			ReplaceBinding(current_binding, ColumnBinding(table_idx, col_idx));
		}
	}
}

void RemoveUnusedColumns::VisitOperator(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		// aggregate
		if (!everything_referenced) {
			// FIXME: groups that are not referenced need to stay -> but they don't need to be scanned and output!
			auto &aggr = op.Cast<LogicalAggregate>();
			ClearUnusedExpressions(aggr.expressions, aggr.aggregate_index);
			if (aggr.expressions.empty() && aggr.groups.empty()) {
				// removed all expressions from the aggregate: push a COUNT(*)
				auto count_star_fun = CountStarFun::GetFunction();
				FunctionBinder function_binder(context);
				aggr.expressions.push_back(
				    function_binder.BindAggregateFunction(count_star_fun, {}, nullptr, AggregateType::NON_DISTINCT));
			}
		}

		// then recurse into the children of the aggregate
		RemoveUnusedColumns remove(binder, context);
		remove.VisitOperatorExpressions(op);
		remove.VisitOperator(*op.children[0]);
		return;
	}
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		if (!everything_referenced) {
			auto &comp_join = op.Cast<LogicalComparisonJoin>();

			if (comp_join.join_type != JoinType::INNER) {
				break;
			}
			// for inner joins with equality predicates in the form of (X=Y)
			// we can replace any references to the RHS (Y) to references to the LHS (X)
			// this reduces the amount of columns we need to extract from the join hash table
			for (auto &cond : comp_join.conditions) {
				if (cond.comparison == ExpressionType::COMPARE_EQUAL) {
					if (cond.left->expression_class == ExpressionClass::BOUND_COLUMN_REF &&
					    cond.right->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
						// comparison join between two bound column refs
						// we can replace any reference to the RHS (build-side) with a reference to the LHS (probe-side)
						auto &lhs_col = cond.left->Cast<BoundColumnRefExpression>();
						auto &rhs_col = cond.right->Cast<BoundColumnRefExpression>();
						// if there are any columns that refer to the RHS,
						auto colrefs = column_references.find(rhs_col.binding);
						if (colrefs != column_references.end()) {
							for (auto &entry : colrefs->second) {
								entry->binding = lhs_col.binding;
								column_references[lhs_col.binding].push_back(entry);
							}
							column_references.erase(rhs_col.binding);
						}
					}
				}
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
		break;
	case LogicalOperatorType::LOGICAL_UNION: {
		auto &setop = op.Cast<LogicalSetOperation>();
		if (setop.setop_all && !everything_referenced) {
			// for UNION we can remove unreferenced columns if union all is used
			// it's possible not all columns are referenced, but unreferenced columns in the union can
			// still have an affect on the result of the union
			vector<idx_t> entries;
			for (idx_t i = 0; i < setop.column_count; i++) {
				entries.push_back(i);
			}
			ClearUnusedExpressions(entries, setop.table_index);
			if (entries.size() < setop.column_count) {
				if (entries.empty()) {
					// no columns referenced: this happens in the case of a COUNT(*)
					// extract the first column
					entries.push_back(0);
				}
				// columns were cleared
				setop.column_count = entries.size();

				for (idx_t child_idx = 0; child_idx < op.children.size(); child_idx++) {
					RemoveUnusedColumns remove(binder, context, true);
					auto &child = op.children[child_idx];

					// we push a projection under this child that references the required columns of the union
					child->ResolveOperatorTypes();
					auto bindings = child->GetColumnBindings();
					vector<unique_ptr<Expression>> expressions;
					expressions.reserve(entries.size());
					for (auto &column_idx : entries) {
						expressions.push_back(
						    make_uniq<BoundColumnRefExpression>(child->types[column_idx], bindings[column_idx]));
					}
					auto new_projection =
					    make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(expressions));
					new_projection->children.push_back(std::move(child));
					op.children[child_idx] = std::move(new_projection);

					remove.VisitOperator(*op.children[child_idx]);
				}
				return;
			}
		}
		for (auto &child : op.children) {
			RemoveUnusedColumns remove(binder, context, true);
			remove.VisitOperator(*child);
		}
		return;
	}
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT: {
		// for INTERSECT/EXCEPT operations we can't remove anything, just recursively visit the children
		for (auto &child : op.children) {
			RemoveUnusedColumns remove(binder, context, true);
			remove.VisitOperator(*child);
		}
		return;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		if (!everything_referenced) {
			auto &proj = op.Cast<LogicalProjection>();
			ClearUnusedExpressions(proj.expressions, proj.table_index);

			if (proj.expressions.empty()) {
				// nothing references the projected expressions
				// this happens in the case of e.g. EXISTS(SELECT * FROM ...)
				// in this case we only need to project a single constant
				proj.expressions.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(42)));
			}
		}
		// then recurse into the children of this projection
		RemoveUnusedColumns remove(binder, context);
		remove.VisitOperatorExpressions(op);
		remove.VisitOperator(*op.children[0]);
		return;
	}
	case LogicalOperatorType::LOGICAL_INSERT:
	case LogicalOperatorType::LOGICAL_UPDATE:
	case LogicalOperatorType::LOGICAL_DELETE: {
		//! When RETURNING is used, a PROJECTION is the top level operator for INSERTS, UPDATES, and DELETES
		//! We still need to project all values from these operators so the projection
		//! on top of them can select from only the table values being inserted.
		//! TODO: Push down the projections from the returning statement
		//! TODO: Be careful because you might be adding expressions when a user returns *
		RemoveUnusedColumns remove(binder, context, true);
		remove.VisitOperatorExpressions(op);
		remove.VisitOperator(*op.children[0]);
		return;
	}
	case LogicalOperatorType::LOGICAL_GET:
		LogicalOperatorVisitor::VisitOperatorExpressions(op);
		if (!everything_referenced) {
			auto &get = op.Cast<LogicalGet>();
			if (!get.function.projection_pushdown) {
				return;
			}

			// Create "selection vector" of all column ids
			vector<idx_t> proj_sel;
			for (idx_t col_idx = 0; col_idx < get.column_ids.size(); col_idx++) {
				proj_sel.push_back(col_idx);
			}
			// Create a copy that we can use to match ids later
			auto col_sel = proj_sel;
			// Clear unused ids, exclude filter columns that are projected out immediately
			ClearUnusedExpressions(proj_sel, get.table_index, false);

			// for every table filter, push a column binding into the column references map to prevent the column from
			// being projected out
			for (auto &filter : get.table_filters.filters) {
				optional_idx index;
				for (idx_t i = 0; i < get.column_ids.size(); i++) {
					if (get.column_ids[i] == filter.first) {
						index = i;
						break;
					}
				}
				if (!index.IsValid()) {
					throw InternalException("Could not find column index for table filter");
				}
				ColumnBinding filter_binding(get.table_index, index.GetIndex());
				if (column_references.find(filter_binding) == column_references.end()) {
					column_references.insert(make_pair(filter_binding, vector<BoundColumnRefExpression *>()));
				}
			}

			// Clear unused ids, include filter columns that are projected out immediately
			ClearUnusedExpressions(col_sel, get.table_index);

			// Now set the column ids in the LogicalGet using the "selection vector"
			vector<column_t> column_ids;
			column_ids.reserve(col_sel.size());
			for (auto col_sel_idx : col_sel) {
				column_ids.push_back(get.column_ids[col_sel_idx]);
			}
			get.column_ids = std::move(column_ids);

			if (get.function.filter_prune) {
				// Now set the projection cols by matching the "selection vector" that excludes filter columns
				// with the "selection vector" that includes filter columns
				idx_t col_idx = 0;
				for (auto proj_sel_idx : proj_sel) {
					for (; col_idx < col_sel.size(); col_idx++) {
						if (proj_sel_idx == col_sel[col_idx]) {
							get.projection_ids.push_back(col_idx);
							break;
						}
					}
				}
			}

			if (get.column_ids.empty()) {
				// this generally means we are only interested in whether or not anything exists in the table (e.g.
				// EXISTS(SELECT * FROM tbl)) in this case, we just scan the row identifier column as it means we do not
				// need to read any of the columns
				get.column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);
			}
		}
		return;
	case LogicalOperatorType::LOGICAL_FILTER: {
		auto &filter = op.Cast<LogicalFilter>();
		if (!filter.projection_map.empty()) {
			// if we have any entries in the filter projection map don't prune any columns
			// FIXME: we can do something more clever here
			everything_referenced = true;
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		auto &distinct = op.Cast<LogicalDistinct>();
		if (distinct.distinct_type == DistinctType::DISTINCT_ON) {
			// distinct type references columns that need to be distinct on, so no
			// need to implicity reference everything.
			break;
		}
		// distinct, all projected columns are used for the DISTINCT computation
		// mark all columns as used and continue to the children
		// FIXME: DISTINCT with expression list does not implicitly reference everything
		everything_referenced = true;
		break;
	}
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE: {
		everything_referenced = true;
		break;
	}
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE: {
		everything_referenced = true;
		break;
	}
	case LogicalOperatorType::LOGICAL_CTE_REF: {
		everything_referenced = true;
		break;
	}
	case LogicalOperatorType::LOGICAL_PIVOT: {
		everything_referenced = true;
		break;
	}
	default:
		break;
	}
	LogicalOperatorVisitor::VisitOperatorExpressions(op);
	LogicalOperatorVisitor::VisitOperatorChildren(op);

	if (op.type == LogicalOperatorType::LOGICAL_ASOF_JOIN || op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN ||
	    op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &comp_join = op.Cast<LogicalComparisonJoin>();
		// after removing duplicate columns we may have duplicate join conditions (if the join graph is cyclical)
		vector<JoinCondition> unique_conditions;
		for (auto &cond : comp_join.conditions) {
			bool found = false;
			for (auto &unique_cond : unique_conditions) {
				if (cond.comparison == unique_cond.comparison && cond.left->Equals(*unique_cond.left) &&
				    cond.right->Equals(*unique_cond.right)) {
					found = true;
					break;
				}
			}
			if (!found) {
				unique_conditions.push_back(std::move(cond));
			}
		}
		comp_join.conditions = std::move(unique_conditions);
	}
}

unique_ptr<Expression> RemoveUnusedColumns::VisitReplace(BoundColumnRefExpression &expr,
                                                         unique_ptr<Expression> *expr_ptr) {
	// add a column reference
	column_references[expr.binding].push_back(&expr);
	return nullptr;
}

unique_ptr<Expression> RemoveUnusedColumns::VisitReplace(BoundReferenceExpression &expr,
                                                         unique_ptr<Expression> *expr_ptr) {
	// BoundReferenceExpression should not be used here yet, they only belong in the physical plan
	throw InternalException("BoundReferenceExpression should not be used here yet!");
}

} // namespace duckdb

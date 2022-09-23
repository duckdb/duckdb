#include "duckdb/optimizer/remove_unused_columns.hpp"

#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/parser/parsed_data/vacuum_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
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
void RemoveUnusedColumns::ClearUnusedExpressions(vector<T> &list, idx_t table_idx) {
	idx_t offset = 0;
	for (idx_t col_idx = 0; col_idx < list.size(); col_idx++) {
		auto current_binding = ColumnBinding(table_idx, col_idx + offset);
		auto entry = column_references.find(current_binding);
		if (entry == column_references.end()) {
			// this entry is not referred to, erase it from the set of expressions
			list.erase(list.begin() + col_idx);
			offset++;
			col_idx--;
		} else if (offset > 0) {
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
			auto &aggr = (LogicalAggregate &)op;
			ClearUnusedExpressions(aggr.expressions, aggr.aggregate_index);
			if (aggr.expressions.empty() && aggr.groups.empty()) {
				// removed all expressions from the aggregate: push a COUNT(*)
				auto count_star_fun = CountStarFun::GetFunction();
				aggr.expressions.push_back(
				    AggregateFunction::BindAggregateFunction(context, count_star_fun, {}, nullptr, false));
			}
		}

		// then recurse into the children of the aggregate
		RemoveUnusedColumns remove(binder, context);
		remove.VisitOperatorExpressions(op);
		remove.VisitOperator(*op.children[0]);
		return;
	}
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		if (!everything_referenced) {
			auto &comp_join = (LogicalComparisonJoin &)op;

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
						auto &lhs_col = (BoundColumnRefExpression &)*cond.left;
						auto &rhs_col = (BoundColumnRefExpression &)*cond.right;
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
	case LogicalOperatorType::LOGICAL_UNION:
		if (!everything_referenced) {
			// for UNION we can remove unreferenced columns as long as everything_referenced is false (i.e. we
			// encounter a UNION node that is not preceded by a DISTINCT)
			// this happens when UNION ALL is used
			auto &setop = (LogicalSetOperation &)op;
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
						    make_unique<BoundColumnRefExpression>(child->types[column_idx], bindings[column_idx]));
					}
					auto new_projection =
					    make_unique<LogicalProjection>(binder.GenerateTableIndex(), move(expressions));
					new_projection->children.push_back(move(child));
					op.children[child_idx] = move(new_projection);

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
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
		// for INTERSECT/EXCEPT operations we can't remove anything, just recursively visit the children
		for (auto &child : op.children) {
			RemoveUnusedColumns remove(binder, context, true);
			remove.VisitOperator(*child);
		}
		return;
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		if (!everything_referenced) {
			auto &proj = (LogicalProjection &)op;
			ClearUnusedExpressions(proj.expressions, proj.table_index);

			if (proj.expressions.empty()) {
				// nothing references the projected expressions
				// this happens in the case of e.g. EXISTS(SELECT * FROM ...)
				// in this case we only need to project a single constant
				proj.expressions.push_back(make_unique<BoundConstantExpression>(Value::INTEGER(42)));
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
			auto &get = (LogicalGet &)op;
			// for every table filter, push a column binding into the column references map to prevent the column from
			// being projected out
			for (auto &filter : get.table_filters.filters) {
				idx_t index = DConstants::INVALID_INDEX;
				for (idx_t i = 0; i < get.column_ids.size(); i++) {
					if (get.column_ids[i] == filter.first) {
						index = i;
						break;
					}
				}
				if (index == DConstants::INVALID_INDEX) {
					throw InternalException("Could not find column index for table filter");
				}
				ColumnBinding filter_binding(get.table_index, index);
				if (column_references.find(filter_binding) == column_references.end()) {
					column_references.insert(make_pair(filter_binding, vector<BoundColumnRefExpression *>()));
				}
			}
			// table scan: figure out which columns are referenced
			ClearUnusedExpressions(get.column_ids, get.table_index);

			if (get.column_ids.empty()) {
				// this generally means we are only interested in whether or not anything exists in the table (e.g.
				// EXISTS(SELECT * FROM tbl)) in this case, we just scan the row identifier column as it means we do not
				// need to read any of the columns
				get.column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);
			}
		}
		return;
	case LogicalOperatorType::LOGICAL_DISTINCT: {
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
	case LogicalOperatorType::LOGICAL_CTE_REF: {
		everything_referenced = true;
		break;
	}
	default:
		break;
	}
	LogicalOperatorVisitor::VisitOperatorExpressions(op);
	LogicalOperatorVisitor::VisitOperatorChildren(op);
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

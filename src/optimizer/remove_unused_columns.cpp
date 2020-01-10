#include "duckdb/optimizer/remove_unused_columns.hpp"

#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/column_binding_map.hpp"

#include "duckdb/function/aggregate/distributive_functions.hpp"

#include "duckdb/planner/expression_iterator.hpp"

using namespace duckdb;
using namespace std;

RemoveUnusedColumns::~RemoveUnusedColumns() {
	for(auto &remap_entry : remap) {
		PerformBindingReplacement(remap_entry.first, remap_entry.second);
	}
}

void RemoveUnusedColumns::PerformBindingReplacement(ColumnBinding current_binding, ColumnBinding new_binding) {
	auto colrefs = column_references.find(current_binding);
	if (colrefs != column_references.end()) {
		for(auto &colref : colrefs->second) {
			assert(colref->binding == current_binding);
			colref->binding = new_binding;
		}
	}
}

void RemoveUnusedColumns::ReplaceBinding(ColumnBinding current_binding, ColumnBinding new_binding) {
	// add the entry to the remap set
	remap[current_binding] = new_binding;
}

template<class T>
void RemoveUnusedColumns::ClearUnusedExpressions(vector<T> &list, index_t table_idx) {
	index_t offset = 0;
	for(index_t col_idx = 0; col_idx < list.size(); col_idx++) {
		auto current_binding = ColumnBinding(table_idx, col_idx + offset);
		auto entry = column_references.find(current_binding);
		if (entry == column_references.end()) {
			// this entry is not referred to, erase it from the set of expresisons
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
	case LogicalOperatorType::AGGREGATE_AND_GROUP_BY: {
		// aggregate
		if (!everything_referenced) {
			// FIXME: groups that are not referenced need to stay -> but they don't need to be scanned and output!
			auto &aggr = (LogicalAggregate&) op;
			ClearUnusedExpressions(aggr.expressions, aggr.aggregate_index);

			if (aggr.expressions.size() == 0 && aggr.groups.size() == 0) {
				// removed all expressions from the aggregate: push a COUNT(*)
				aggr.expressions.push_back(make_unique<BoundAggregateExpression>(TypeId::BIGINT, CountStarFun::GetFunction(), false));
			}
		}

		// then recurse into the children of the aggregate
		RemoveUnusedColumns remove;
		remove.VisitOperatorExpressions(op);
		remove.VisitOperator(*op.children[0]);
		return;
	}
	case LogicalOperatorType::DELIM_JOIN:
	case LogicalOperatorType::COMPARISON_JOIN: {
		if (!everything_referenced) {
			auto &comp_join = (LogicalComparisonJoin&) op;

			// vector<index_t> projection_map_left;
			// vector<index_t> projection_map_right;
			// check for comparison joins; for any comparison
			if (comp_join.join_type == JoinType::INNER) {
				// for inner joins
				for(auto &cond : comp_join.conditions) {
					if (cond.comparison == ExpressionType::COMPARE_EQUAL &&
						cond.left->expression_class == ExpressionClass::BOUND_COLUMN_REF &&
						cond.right->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
						// comparison join between two bound column refs
						// we can replace any reference to the RHS (build-side) with a reference to the LHS (probe-side)
						auto &lhs_col = (BoundColumnRefExpression &) *cond.left;
						auto &rhs_col = (BoundColumnRefExpression &) *cond.right;
						// if there are any columns that refer to the RHS,
						auto colrefs = column_references.find(rhs_col.binding);
						if (colrefs != column_references.end()) {
							for(auto &entry : colrefs->second) {
								entry->binding = lhs_col.binding;
								column_references[lhs_col.binding].push_back(entry);
							}
							column_references.erase(rhs_col.binding);
						}
					}
				}
			}


			// // first check which column bindings are required after the join
			// auto column_bindings = op.GetColumnBindings();
			// column_binding_set_t unused_bindings;
			// for(auto &column_binding : column_bindings) {
			// 	auto entry = column_references.find(column_binding);
			// 	if (entry == column_references.end()) {
			// 		// column is unused, add to set of unused column bindings
			// 		unused_bindings.insert(column_binding);
			// 	}
			// }
			// // now visit the join expressions and remove any columns that are not used at all
			// LogicalOperatorVisitor::VisitOperatorExpressions(op);
			// LogicalOperatorVisitor::VisitOperatorChildren(op);
			// // after that, figure out if there are columns that were ONLY used in the filter
			// // these can be projected out during the filter
			// vector<index_t> projection_map;
			// column_bindings = op.GetColumnBindings();
			// for(index_t i = 0; i < column_bindings.size(); i++) {
			// 	auto entry = original_bindings.find(column_bindings[i]);
			// 	auto binding = entry == original_bindings.end() ? column_bindings[i] : entry->second;
			// 	if (unused_bindings.find(binding) == unused_bindings.end()) {
			// 		projection_map.push_back(i);
			// 	}
			// }
			// if (projection_map.size() == column_bindings.size()) {
			// 	projection_map.clear();
			// }
			// return;
		}
		break;
	}
	case LogicalOperatorType::ANY_JOIN:
		break;
	case LogicalOperatorType::UNION:
	case LogicalOperatorType::EXCEPT:
	case LogicalOperatorType::INTERSECT:
		// for set operations we don't remove anything, just recursively visit the children
		// FIXME: for UNION ALL we can remove unreferenced columns (i.e. we encounter a UNION node that is not preceded by a DISTINCT)
		for(auto &child : op.children) {
			RemoveUnusedColumns remove(true);
			remove.VisitOperator(*child);
		}
		return;
	case LogicalOperatorType::PROJECTION: {
		if (!everything_referenced) {
			auto &proj = (LogicalProjection&) op;
			ClearUnusedExpressions(proj.expressions, proj.table_index);

			if (proj.expressions.size() == 0) {
				// nothing references the projected expressions
				// this happens in the case of e.g. EXISTS(SELECT * FROM ...)
				// in this case we only need to project a single constant
				proj.expressions.push_back(make_unique<BoundConstantExpression>(Value::INTEGER(42)));
			}
		}
		// then recurse into the children of this projection
		RemoveUnusedColumns remove;
		remove.VisitOperatorExpressions(op);
		remove.VisitOperator(*op.children[0]);
		return;
	}
	case LogicalOperatorType::GET:
		if (!everything_referenced) {
			auto &get = (LogicalGet&) op;
			// table scan: figure out which columns are referenced
			ClearUnusedExpressions(get.column_ids, get.table_index);

			if (get.column_ids.size() == 0) {
				// this generally means we are only interested in whether or not anything exists in the table (e.g. EXISTS(SELECT * FROM tbl))
				// in this case, we just scan the row identifier column as it means we do not need to read any of the columns
				get.column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);
			}
		}
		return;
	case LogicalOperatorType::DISTINCT: {
		// distinct, all projected columns are used for the DISTINCT computation
		// mark all columns as used and continue to the children
		// FIXME: DISTINCT with expression list does not implicitly reference everything
		everything_referenced = true;
		break;
	}
	case LogicalOperatorType::FILTER: {
		auto &filter = (LogicalFilter&) op;
		if (!everything_referenced) {
			// filter
			// get a list of the bound column references that are used in the filter
			// first empty the set of column references
			auto current_references = move(column_references);
			// then visit the expressions of the filter operator
			LogicalOperatorVisitor::VisitOperatorExpressions(op);
			// now iterate over the references found in the filter
			vector<BoundColumnRefExpression*> unused_binding_expressions;
			for(auto &entry : column_references) {
				auto binding = entry.first;
				if (current_references.find(binding) == current_references.end()) {
					// this reference was used in the filter, but is not used AFTER the filter
					// store a reference to the bound column ref expression
					unused_binding_expressions.push_back(entry.second[0]);
				}
				// now move the bound column references back to the main set
				for(auto expr : entry.second) {
					current_references[binding].push_back(expr);
				}
			}
			// now we have a list of column references that are only used in the filter (unused_bindings)
			// move on and visit the children of the logical filter
			column_references = move(current_references);
			LogicalOperatorVisitor::VisitOperatorChildren(op);
			if (unused_binding_expressions.size() > 0) {
				// if there were unused binding expressions, check if we can project anything out of the filter
				// first create a set of bindings that are unused after the filter
				column_binding_set_t unused_bindings;
				for(auto expr : unused_binding_expressions) {
					unused_bindings.insert(expr->binding);
				}
				// now iterate over the result bindings of the chld of the filter
				auto column_bindings = op.children[0]->GetColumnBindings();
				for(index_t i = 0; i < column_bindings.size(); i++) {
					// if this binding does not belong to the unused column bindings, add it to the projection map
					if (unused_bindings.find(column_bindings[i]) == unused_bindings.end()) {
						filter.projection_map.push_back(i);
					}
				}
				if (filter.projection_map.size() == column_bindings.size()) {
					filter.projection_map.clear();
				}
			}
			return;
		}
		break;
	}
	default:
		break;
	}
	LogicalOperatorVisitor::VisitOperatorExpressions(op);
	LogicalOperatorVisitor::VisitOperatorChildren(op);
}


unique_ptr<Expression> RemoveUnusedColumns::VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) {
	// add a column reference
	column_references[expr.binding].push_back(&expr);
	return nullptr;
}

unique_ptr<Expression> RemoveUnusedColumns::VisitReplace(BoundReferenceExpression &expr, unique_ptr<Expression> *expr_ptr) {
	// BoundReferenceExpression should not be used here yet, they only belong in the physical plan
	throw InternalException("BoundReferenceExpression should not be used here yet!");
}

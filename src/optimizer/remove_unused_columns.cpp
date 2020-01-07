#include "duckdb/optimizer/remove_unused_columns.hpp"

#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/column_binding_map.hpp"

#include "duckdb/function/aggregate/distributive_functions.hpp"

using namespace duckdb;
using namespace std;

// static void UpdateColumnReferences(index_t original, index_t new_mapping, vector<BoundColumnRefExpression*> &references) {
// 	// this entry was referred to, but entries before it have been deleted
// 	// alter the column index in the BoundColumnRef expressions referring to the projection
// 	if (original != new_mapping) {
// 		for(auto &expr : references) {
// 			if (!expr) {
// 				continue;
// 			}
// 			assert(expr->binding.column_index == original);
// 			expr->binding.column_index = new_mapping;
// 		}
// 	}
// }

RemoveUnusedColumns::~RemoveUnusedColumns() {
	for(auto &remap_entry : remap) {
		auto colrefs = column_references.find(remap_entry.first);
		if (colrefs != column_references.end()) {
			for(auto &colref : colrefs->second) {
				assert(colref->binding == remap_entry.first);
				colref->binding = remap_entry.second;
			}
		}
	}
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
			// add the entry to the remap set
			auto new_binding = ColumnBinding(table_idx, col_idx);
			remap[current_binding] = new_binding;
			original_bindings[new_binding] = current_binding;
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
	case LogicalOperatorType::ANY_JOIN:
	case LogicalOperatorType::COMPARISON_JOIN: {
	 	// join
	  	// FIXME: remove columns that are only used in the join itself
		break;
	}
	case LogicalOperatorType::DELIM_JOIN: {
		break;
	}
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
		everything_referenced = true;
		break;
	}
	case LogicalOperatorType::FILTER: {
		auto &filter = (LogicalFilter&) op;
		// filter
		// first check which column bindings are required after the filter
		auto column_bindings = op.GetColumnBindings();
		column_binding_set_t unused_bindings;
		for(auto &column_binding : column_bindings) {
			auto entry = column_references.find(column_binding);
			if (entry == column_references.end()) {
				// column is unused, add to set of unused column bindings
				unused_bindings.insert(column_binding);
			}
		}
		// now visit the filter expressions and remove any columns that are not used at all
		LogicalOperatorVisitor::VisitOperatorExpressions(op);
		LogicalOperatorVisitor::VisitOperatorChildren(op);
		// after that, figure out if there are columns that were ONLY used in the filter
		// these can be projected out during the filter
		column_bindings = op.GetColumnBindings();
		for(index_t i = 0; i < column_bindings.size(); i++) {
			auto entry = original_bindings.find(column_bindings[i]);
			auto binding = entry == original_bindings.end() ? column_bindings[i] : entry->second;
			if (unused_bindings.find(binding) == unused_bindings.end()) {
				filter.projection_map.push_back(i);
			}
		}
		if (filter.projection_map.size() == column_bindings.size()) {
			filter.projection_map.clear();
		}
		return;
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

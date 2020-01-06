#include "duckdb/optimizer/remove_unused_columns.hpp"

#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/table_binding_resolver.hpp"

#include "duckdb/function/aggregate/distributive_functions.hpp"

using namespace duckdb;
using namespace std;

static void UpdateColumnReferences(index_t original, index_t new_mapping, vector<BoundColumnRefExpression*> &references) {
	// this entry was referred to, but entries before it have been deleted
	// alter the column index in the BoundColumnRef expressions referring to the projection
	if (original != new_mapping) {
		for(auto &expr : references) {
			if (!expr) {
				continue;
			}
			assert(expr->binding.column_index == original);
			expr->binding.column_index = new_mapping;
		}
	}
}

void RemoveUnusedColumns::ClearExpressions(LogicalOperator &op, unordered_map<index_t, vector<BoundColumnRefExpression*>> &ref_map) {
	index_t offset = 0;
	for(index_t col_idx = 0; col_idx < op.expressions.size(); col_idx++) {
		auto entry = ref_map.find(col_idx + offset);
		if (entry == ref_map.end()) {
			// this entry is not referred to, erase it from the set of expresisons
			op.expressions.erase(op.expressions.begin() + col_idx);
			offset++;
			col_idx--;
		} else {
			// this entry was referred to, update any column references
			UpdateColumnReferences(col_idx + offset, col_idx, entry->second);
		}
	}
}

void RemoveUnusedColumns::VisitOperator(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::AGGREGATE_AND_GROUP_BY: {
		// aggregate
		if (!everything_referenced) {
			auto &aggr = (LogicalAggregate&) op;
			// FIXME: groups that are not referenced need to stay -> but they don't need to be scanned and output!
			// remove any aggregates that are not referenced
			auto references = column_references.find(aggr.aggregate_index);
			if (references == column_references.end()) {
				// nothing references the aggregates
				// in this case we can just clear all the aggregates (if there are any)
				aggr.expressions.clear();
			} else {
				// there are references to aggregates, however, potentially not all aggregates are referenced
				// clear any unreferenced aggregates
				auto &ref_map = references->second;
				ClearExpressions(aggr, ref_map);
			}
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
	  	// FIXME: remove columns that are only used in the join during the projection phase
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
			// remove any children that are not referenced from this projection
			auto references = column_references.find(proj.table_index);
			if (references == column_references.end()) {
				// nothing references the projected expressions
				// this happens in the case of e.g. EXISTS(SELECT * FROM ...)
				// in this case we only need to project a single constant
				proj.expressions.clear();
				proj.expressions.push_back(make_unique<BoundConstantExpression>(Value::INTEGER(1)));
			} else {
				// we have references to the projection: check if all projected columns are referred to
				auto &ref_map = references->second;
				ClearExpressions(proj, ref_map);
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
			auto references = column_references.find(get.table_index);
			if (references == column_references.end()) {
				// nothing references the table scan
				// this generally means we are only interested in whether or not anything exists in the table (e.g. EXISTS(SELECT * FROM tbl))
				// clear all references and only scan the row id
				get.column_ids.clear();
				get.column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);
			} else {
				// we have references to the table: erase any column ids that are not used
				auto &ref_map = references->second;
				index_t offset = 0;
				for(index_t col_idx = 0; col_idx < get.column_ids.size(); col_idx++) {
					auto entry = ref_map.find(col_idx + offset);
					if (entry == ref_map.end()) {
						// this entry is not referred to, erase it from the set of expresisons
						get.column_ids.erase(get.column_ids.begin() + col_idx);
						offset++;
						col_idx--;
					} else {
						UpdateColumnReferences(col_idx + offset, col_idx, entry->second);
					}
				}
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
		// filter
		// FIXME: remove any columns that are only used in the filter
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
	column_references[expr.binding.table_index][expr.binding.column_index].push_back(&expr);
	return nullptr;
}

unique_ptr<Expression> RemoveUnusedColumns::VisitReplace(BoundReferenceExpression &expr, unique_ptr<Expression> *expr_ptr) {
	// BoundReferenceExpression should not be used here yet, they only belong in the physical plan
	throw NotImplementedException("FIXME: BoundReferenceExpression should not be used here yet!");
}

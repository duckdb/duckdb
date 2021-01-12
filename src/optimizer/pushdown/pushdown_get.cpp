#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {
using Filter = FilterPushdown::Filter;

void get_equality_constant_filters(Expression *filter, unordered_map<idx_t, unique_ptr<Expression>> &cons_eq_filters) {
	switch (filter->type) {
	case ExpressionType::CONJUNCTION_OR: {
		auto &or_exp = (BoundConjunctionExpression &)*filter;
		for (auto &child : or_exp.children) {
			get_equality_constant_filters(child.get(), cons_eq_filters);
		}
		break;
	}
	case ExpressionType::CONJUNCTION_AND: {
		auto &and_exp = (BoundConjunctionExpression &)*filter;
		for (auto &child : and_exp.children) {
			get_equality_constant_filters(child.get(), cons_eq_filters);
		}
		break;
	}
	case ExpressionType::COMPARE_EQUAL: {
		auto &comp_exp = (BoundComparisonExpression &)*filter;
		if ((comp_exp.left->expression_class == ExpressionClass::BOUND_COLUMN_REF &&
		     comp_exp.right->expression_class == ExpressionClass::BOUND_CONSTANT)) {
			auto &column_ref = (BoundColumnRefExpression &)*comp_exp.left;
			cons_eq_filters[column_ref.binding.column_index] = comp_exp.right->Copy();
		}
		break;
	}
	default:
		break;
	}
}

void replace_column_constants(Expression *filter, unordered_map<idx_t, unique_ptr<Expression>> &cons_eq_filters) {
	switch (filter->type) {
	case ExpressionType::CONJUNCTION_OR: {
		auto &or_exp = (BoundConjunctionExpression &)*filter;
		for (auto &child : or_exp.children) {
			get_equality_constant_filters(child.get(), cons_eq_filters);
		}
		break;
	}
	case ExpressionType::CONJUNCTION_AND: {
		auto &and_exp = (BoundConjunctionExpression &)*filter;
		for (auto &child : and_exp.children) {
			get_equality_constant_filters(child.get(), cons_eq_filters);
		}
		break;
	}
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHAN: {
		auto &comp_exp = (BoundComparisonExpression &)*filter;
		if (comp_exp.left->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
			if (comp_exp.right->expression_class == ExpressionClass::BOUND_CAST) {
				auto &cast = (BoundCastExpression &)*comp_exp.right;
				if (cast.child->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
					auto &column_ref = (BoundColumnRefExpression &)*cast.child;
					auto it = cons_eq_filters.find(column_ref.binding.column_index);
					if (it != cons_eq_filters.end()) {
						//! We can replace it.
						comp_exp.right = it->second->Copy();
					}
				}
			} else if (comp_exp.right->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
				auto &column_ref = (BoundColumnRefExpression &)*comp_exp.right;
				auto it = cons_eq_filters.find(column_ref.binding.column_index);
				if (it != cons_eq_filters.end()) {
					//! We can replace it.
					comp_exp.right = it->second->Copy();
				}
			}
		}
		break;
	}
	default:
		break;
	}
}

void rewrite_transitive_filters(vector<unique_ptr<Filter>> &filters) {
	unordered_map<idx_t, unique_ptr<Expression>> cons_eq_filters;
	//! First we collect all filters that are equality with constants
	for (auto &filter : filters) {
		get_equality_constant_filters(filter->filter.get(), cons_eq_filters);
	}
	//! Now we try to replace column references for constants
	for (auto &filter : filters) {
		replace_column_constants(filter->filter.get(), cons_eq_filters);
	}
}
unique_ptr<LogicalOperator> FilterPushdown::PushdownGet(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_GET);
	auto &get = (LogicalGet &)*op;
	rewrite_transitive_filters(filters);
	// first push down arbitrary filters
	if (get.function.pushdown_complex_filter) {
		// for the remaining filters, check if we can push any of them into the scan as well
		vector<unique_ptr<Expression>> expressions;
		for (auto &filter : filters) {
			expressions.push_back(move(filter->filter));
		}
		filters.clear();

		get.function.pushdown_complex_filter(optimizer.context, get, get.bind_data.get(), expressions);

		if (expressions.empty()) {
			return op;
		}
		// re-generate the filters
		for (auto &expr : expressions) {
			auto f = make_unique<Filter>();
			f->filter = move(expr);
			f->ExtractBindings();
			filters.push_back(move(f));
		}
	}
	if (!get.table_filters.empty() || !get.function.filter_pushdown) {
		// the table function does not support filter pushdown: push a LogicalFilter on top
		return FinishPushdown(move(op));
	}
	PushFilters();

	//! We generate the table filters that will be executed during the table scan
	//! Right now this only executes simple AND filters
	get.table_filters = combiner.GenerateTableScanFilters(get.column_ids);

	//! For more complex filters if all filters to a column are constants we generate a min max boundary used to check
	//! the zonemaps and as a pre-filtering step.
	get.zonemap_checks = combiner.GenerateZonemapChecks(get.column_ids, get.table_filters);

	for (auto &f : get.table_filters) {
		f.column_index = get.column_ids[f.column_index];
	}

	GenerateFilters();

	//! Now we try to pushdown the remaining filters to perform zonemap checking
	return FinishPushdown(move(op));
}

} // namespace duckdb

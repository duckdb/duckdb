#include "duckdb/optimizer/filter_combiner.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/operator/subtract.hpp"

namespace duckdb {

using ExpressionValueInformation = FilterCombiner::ExpressionValueInformation;

ValueComparisonResult CompareValueInformation(ExpressionValueInformation &left, ExpressionValueInformation &right);

FilterCombiner::FilterCombiner(ClientContext &context) : context(context) {
}

FilterCombiner::FilterCombiner(Optimizer &optimizer) : FilterCombiner(optimizer.context) {
}

Expression &FilterCombiner::GetNode(Expression &expr) {
	auto entry = stored_expressions.find(expr);
	if (entry != stored_expressions.end()) {
		// expression already exists: return a reference to the stored expression
		return *entry->second;
	}
	// expression does not exist yet: create a copy and store it
	auto copy = expr.Copy();
	auto &copy_ref = *copy;
	D_ASSERT(stored_expressions.find(copy_ref) == stored_expressions.end());
	stored_expressions[copy_ref] = std::move(copy);
	return copy_ref;
}

idx_t FilterCombiner::GetEquivalenceSet(Expression &expr) {
	D_ASSERT(stored_expressions.find(expr) != stored_expressions.end());
	D_ASSERT(stored_expressions.find(expr)->second.get() == &expr);
	auto entry = equivalence_set_map.find(expr);
	if (entry == equivalence_set_map.end()) {
		idx_t index = set_index++;
		equivalence_set_map[expr] = index;
		equivalence_map[index].push_back(expr);
		constant_values.insert(make_pair(index, vector<ExpressionValueInformation>()));
		return index;
	} else {
		return entry->second;
	}
}

FilterResult FilterCombiner::AddConstantComparison(vector<ExpressionValueInformation> &info_list,
                                                   ExpressionValueInformation info) {
	if (info.constant.IsNull()) {
		return FilterResult::UNSATISFIABLE;
	}
	for (idx_t i = 0; i < info_list.size(); i++) {
		auto comparison = CompareValueInformation(info_list[i], info);
		switch (comparison) {
		case ValueComparisonResult::PRUNE_LEFT:
			// prune the entry from the info list
			info_list.erase_at(i);
			i--;
			break;
		case ValueComparisonResult::PRUNE_RIGHT:
			// prune the current info
			return FilterResult::SUCCESS;
		case ValueComparisonResult::UNSATISFIABLE_CONDITION:
			// combination of filters is unsatisfiable: prune the entire branch
			info_list.push_back(info);
			return FilterResult::UNSATISFIABLE;
		default:
			// prune nothing, move to the next condition
			break;
		}
	}
	// finally add the entry to the list
	info_list.push_back(info);
	return FilterResult::SUCCESS;
}

FilterResult FilterCombiner::AddFilter(unique_ptr<Expression> expr) {
	//	LookUpConjunctions(expr.get());
	// try to push the filter into the combiner
	auto result = AddFilter(*expr);
	if (result == FilterResult::UNSUPPORTED) {
		// unsupported filter, push into remaining filters
		remaining_filters.push_back(std::move(expr));
		return FilterResult::SUCCESS;
	}
	return result;
}

void FilterCombiner::GenerateFilters(const std::function<void(unique_ptr<Expression> filter)> &callback) {
	// first loop over the remaining filters
	for (auto &filter : remaining_filters) {
		callback(std::move(filter));
	}
	remaining_filters.clear();
	// now loop over the equivalence sets
	for (auto &entry : equivalence_map) {
		auto equivalence_set = entry.first;
		auto &entries = entry.second;
		auto &constant_list = constant_values.find(equivalence_set)->second;
		// for each entry generate an equality expression comparing to each other
		for (idx_t i = 0; i < entries.size(); i++) {
			for (idx_t k = i + 1; k < entries.size(); k++) {
				auto comparison = make_uniq<BoundComparisonExpression>(
				    ExpressionType::COMPARE_EQUAL, entries[i].get().Copy(), entries[k].get().Copy());
				callback(std::move(comparison));
			}
			// for each entry also create a comparison with each constant
			auto lower_index = optional_idx::Invalid();
			auto upper_index = optional_idx::Invalid();
			bool lower_inclusive = false;
			bool upper_inclusive = false;
			for (idx_t k = 0; k < constant_list.size(); k++) {
				auto &info = constant_list[k];
				if (info.comparison_type == ExpressionType::COMPARE_GREATERTHAN ||
				    info.comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
					lower_index = k;
					lower_inclusive = info.comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO;
				} else if (info.comparison_type == ExpressionType::COMPARE_LESSTHAN ||
				           info.comparison_type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
					upper_index = k;
					upper_inclusive = info.comparison_type == ExpressionType::COMPARE_LESSTHANOREQUALTO;
				} else {
					auto constant = make_uniq<BoundConstantExpression>(info.constant);
					auto comparison = make_uniq<BoundComparisonExpression>(
					    info.comparison_type, entries[i].get().Copy(), std::move(constant));
					callback(std::move(comparison));
				}
			}
			if (lower_index.IsValid() && upper_index.IsValid()) {
				// found both lower and upper index, create a BETWEEN expression
				auto lower_constant =
				    make_uniq<BoundConstantExpression>(constant_list[lower_index.GetIndex()].constant);
				auto upper_constant =
				    make_uniq<BoundConstantExpression>(constant_list[upper_index.GetIndex()].constant);
				auto between =
				    make_uniq<BoundBetweenExpression>(entries[i].get().Copy(), std::move(lower_constant),
				                                      std::move(upper_constant), lower_inclusive, upper_inclusive);
				callback(std::move(between));
			} else if (lower_index.IsValid()) {
				// only lower index found, create simple comparison expression
				auto constant = make_uniq<BoundConstantExpression>(constant_list[lower_index.GetIndex()].constant);
				auto comparison =
				    make_uniq<BoundComparisonExpression>(constant_list[lower_index.GetIndex()].comparison_type,
				                                         entries[i].get().Copy(), std::move(constant));
				callback(std::move(comparison));
			} else if (upper_index.IsValid()) {
				// only upper index found, create simple comparison expression
				auto constant = make_uniq<BoundConstantExpression>(constant_list[upper_index.GetIndex()].constant);
				auto comparison =
				    make_uniq<BoundComparisonExpression>(constant_list[upper_index.GetIndex()].comparison_type,
				                                         entries[i].get().Copy(), std::move(constant));
				callback(std::move(comparison));
			}
		}
	}
	stored_expressions.clear();
	equivalence_set_map.clear();
	constant_values.clear();
	equivalence_map.clear();
}

bool FilterCombiner::HasFilters() {
	bool has_filters = false;
	GenerateFilters([&](unique_ptr<Expression> child) { has_filters = true; });
	return has_filters;
}

// Try to extract a column index from a bound column ref expression, or a column ref recursively nested
// inside of a struct_extract call. If the expression is not a column ref (or nested column ref), return false.
static bool TryGetBoundColumnIndex(const vector<ColumnIndex> &column_ids, const Expression &expr, ColumnIndex &result) {
	switch (expr.GetExpressionType()) {
	case ExpressionType::BOUND_COLUMN_REF: {
		auto &ref = expr.Cast<BoundColumnRefExpression>();
		result = column_ids[ref.binding.column_index];
		return true;
	}
	case ExpressionType::BOUND_FUNCTION: {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (func.function.name == "struct_extract" || func.function.name == "struct_extract_at") {
			auto &child_expr = func.children[0];
			return TryGetBoundColumnIndex(column_ids, *child_expr, result);
		}
		return false;
	}
	default:
		return false;
	}
}

// Try to push down a filter into a expression by recursively wrapping any nested expressions in StructFilters.
// If the expression is not a struct_extract, return the inner_filter unchanged.
static unique_ptr<TableFilter> PushDownFilterIntoExpr(const Expression &expr, unique_ptr<TableFilter> inner_filter) {
	if (expr.GetExpressionType() == ExpressionType::BOUND_FUNCTION) {
		auto &func = expr.Cast<BoundFunctionExpression>();
		auto &child_expr = func.children[0];
		auto child_value = func.children[1]->Cast<BoundConstantExpression>().value;
		if (func.function.name == "struct_extract") {
			string child_name = child_value.GetValue<string>();
			auto child_index = StructType::GetChildIndexUnsafe(func.children[0]->return_type, child_name);
			inner_filter = make_uniq<StructFilter>(child_index, child_name, std::move(inner_filter));
			return PushDownFilterIntoExpr(*child_expr, std::move(inner_filter));
		} else if (func.function.name == "struct_extract_at") {
			inner_filter = make_uniq<StructFilter>(child_value.GetValue<idx_t>() - 1, "", std::move(inner_filter));
			return PushDownFilterIntoExpr(*child_expr, std::move(inner_filter));
		}
	}
	return inner_filter;
}

bool FilterCombiner::ContainsNull(vector<Value> &in_list) {
	for (idx_t i = 0; i < in_list.size(); i++) {
		if (in_list[i].IsNull()) {
			return true;
		}
	}
	return false;
}

bool FilterCombiner::IsDenseRange(vector<Value> &in_list) {
	if (in_list.empty()) {
		return true;
	}
	if (!in_list[0].type().IsIntegral()) {
		return false;
	}
	// sort the input list
	sort(in_list.begin(), in_list.end());

	// check if the gap between each value is exactly one
	hugeint_t prev_value = in_list[0].GetValue<hugeint_t>();
	for (idx_t i = 1; i < in_list.size(); i++) {
		hugeint_t current_value = in_list[i].GetValue<hugeint_t>();
		hugeint_t diff;
		if (!TrySubtractOperator::Operation(current_value, prev_value, diff)) {
			// if subtract would overflow then it's certainly not 1
			return false;
		}
		if (diff != 1) {
			// gap is not 1 - this is not a dense range
			return false;
		}
		prev_value = current_value;
	}
	// dense range
	return true;
}

static bool SupportedFilterComparison(ExpressionType expression_type) {
	switch (expression_type) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_NOTEQUAL:
		return true;
	default:
		return false;
	}
}

bool TypeSupportsConstantFilter(const LogicalType &type) {
	if (TypeIsNumeric(type.InternalType())) {
		return true;
	}
	if (type.InternalType() == PhysicalType::VARCHAR || type.InternalType() == PhysicalType::BOOL) {
		return true;
	}
	return false;
}

FilterPushdownResult FilterCombiner::TryPushdownConstantFilter(TableFilterSet &table_filters,
                                                               const vector<ColumnIndex> &column_ids,
                                                               column_t column_id,
                                                               vector<ExpressionValueInformation> &info_list) {
	if (info_list.empty()) {
		// no constants - already removed
		return FilterPushdownResult::NO_PUSHDOWN;
	}
	auto filter_exp = equivalence_map.end();
	auto &constant_info = info_list[0];
	// check if the type is supported
	if (!TypeSupportsConstantFilter(constant_info.constant.type())) {
		// not supported
		return FilterPushdownResult::NO_PUSHDOWN;
	}
	if (!SupportedFilterComparison(constant_info.comparison_type)) {
		return FilterPushdownResult::NO_PUSHDOWN;
	}
	//! Here we check if these filters are column references
	filter_exp = equivalence_map.find(column_id);

	if (filter_exp->second.size() != 1) {
		return FilterPushdownResult::NO_PUSHDOWN;
	}

	auto &expr = filter_exp->second[0];
	auto equiv_set = filter_exp->first;

	// Try to get the column index, either from bound column ref, or a column ref nested inside of a
	// struct_extract call
	ColumnIndex column_index;
	if (!TryGetBoundColumnIndex(column_ids, expr, column_index)) {
		return FilterPushdownResult::NO_PUSHDOWN;
	}

	auto &constant_list = constant_values.find(equiv_set)->second;
	for (auto &constant_cmp : constant_list) {
		auto constant_filter = make_uniq<ConstantFilter>(constant_cmp.comparison_type, constant_cmp.constant);
		table_filters.PushFilter(column_index, PushDownFilterIntoExpr(expr, std::move(constant_filter)));
	}
	equivalence_map.erase(filter_exp);
	return FilterPushdownResult::PUSHED_DOWN_FULLY;
}

FilterPushdownResult FilterCombiner::TryPushdownPrefixFilter(TableFilterSet &table_filters,
                                                             const vector<ColumnIndex> &column_ids, Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return FilterPushdownResult::NO_PUSHDOWN;
	}
	auto &func = expr.Cast<BoundFunctionExpression>();
	if (func.function.name != "prefix") {
		return FilterPushdownResult::NO_PUSHDOWN;
	}
	if (func.children[0]->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
	    func.children[1]->GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
		// we need prefix(col, 'literal') in order to push this down
		return FilterPushdownResult::NO_PUSHDOWN;
	}
	auto &column_ref = func.children[0]->Cast<BoundColumnRefExpression>();
	auto &constant_value_expr = func.children[1]->Cast<BoundConstantExpression>();
	auto prefix_string = StringValue::Get(constant_value_expr.value);
	if (prefix_string.empty()) {
		// empty prefix - skip
		return FilterPushdownResult::NO_PUSHDOWN;
	}
	auto &column_index = column_ids[column_ref.binding.column_index];
	//! Replace prefix with a set of comparisons
	auto lower_bound = make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, Value(prefix_string));
	prefix_string[prefix_string.size() - 1]++;
	auto upper_bound = make_uniq<ConstantFilter>(ExpressionType::COMPARE_LESSTHAN, Value(prefix_string));
	table_filters.PushFilter(column_index, std::move(lower_bound));
	table_filters.PushFilter(column_index, std::move(upper_bound));
	return FilterPushdownResult::PUSHED_DOWN_FULLY;
}

FilterPushdownResult FilterCombiner::TryPushdownLikeFilter(TableFilterSet &table_filters,
                                                           const vector<ColumnIndex> &column_ids, Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return FilterPushdownResult::NO_PUSHDOWN;
	}
	auto &func = expr.Cast<BoundFunctionExpression>();
	if (func.function.name != "~~") {
		return FilterPushdownResult::NO_PUSHDOWN;
	}
	if (func.children[0]->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
	    func.children[1]->GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
		// we need col LIKE 'literal' in order to generate extra filters
		return FilterPushdownResult::NO_PUSHDOWN;
	}

	//! This is a like function.
	auto &column_ref = func.children[0]->Cast<BoundColumnRefExpression>();
	auto &constant_value_expr = func.children[1]->Cast<BoundConstantExpression>();
	auto &column_index = column_ids[column_ref.binding.column_index];
	// constant value expr can sometimes be null. if so, push is not null filter, which will
	// make the filter unsatisfiable and return no results.
	if (constant_value_expr.value.IsNull()) {
		auto is_not_null = make_uniq<IsNotNullFilter>();
		table_filters.PushFilter(column_index, std::move(is_not_null));
		return FilterPushdownResult::PUSHED_DOWN_FULLY;
	}
	auto &like_string = StringValue::Get(constant_value_expr.value);
	if (like_string[0] == '%' || like_string[0] == '_') {
		//! If the like starts with a special character we have no fixed prefix so nothing to pushdown
		return FilterPushdownResult::NO_PUSHDOWN;
	}
	string prefix;
	bool equality = true;
	for (char const &c : like_string) {
		if (c == '%' || c == '_') {
			equality = false;
			break;
		}
		prefix += c;
	}
	if (equality) {
		//! If the LIKE has no special characters we can turn it into an equality and push that down
		auto equal_filter = make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL, Value(prefix));
		table_filters.PushFilter(column_index, std::move(equal_filter));
		return FilterPushdownResult::PUSHED_DOWN_FULLY;
	}

	//! We have a prefix - we can push down the prefix using a bound (x >= PREFIX AND x <= prefix + 1)
	// Note that we still need to execute the LIKE filter
	auto lower_bound = make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, Value(prefix));
	prefix[prefix.size() - 1]++;
	auto upper_bound = make_uniq<ConstantFilter>(ExpressionType::COMPARE_LESSTHAN, Value(prefix));
	table_filters.PushFilter(column_index, std::move(lower_bound));
	table_filters.PushFilter(column_index, std::move(upper_bound));
	return FilterPushdownResult::PUSHED_DOWN_PARTIALLY;
}

FilterPushdownResult FilterCombiner::TryPushdownInFilter(TableFilterSet &table_filters,
                                                         const vector<ColumnIndex> &column_ids, Expression &expr) {
	if (expr.GetExpressionType() != ExpressionType::COMPARE_IN) {
		return FilterPushdownResult::NO_PUSHDOWN;
	}
	auto &func = expr.Cast<BoundOperatorExpression>();
	D_ASSERT(func.children.size() > 1);
	if (func.children[0]->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		// we need col IN (...) to be able to push this down
		return FilterPushdownResult::NO_PUSHDOWN;
	}
	auto &column_ref = func.children[0]->Cast<BoundColumnRefExpression>();
	auto &column_index = column_ids[column_ref.binding.column_index];
	//! check if all children are const expr
	bool children_constant = true;
	for (size_t i {1}; i < func.children.size(); i++) {
		if (func.children[i]->GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
			children_constant = false;
			break;
		}
		auto &const_value_expr = func.children[i]->Cast<BoundConstantExpression>();
		if (const_value_expr.value.IsNull()) {
			// cannot simplify NULL values
			children_constant = false;
			break;
		}
	}
	if (!children_constant) {
		// all children must be constant
		return FilterPushdownResult::NO_PUSHDOWN;
	}
	auto &fst_const_value_expr = func.children[1]->Cast<BoundConstantExpression>();
	auto &type = fst_const_value_expr.value.type();

	if (func.children.size() == 2 && TypeSupportsConstantFilter(type)) {
		// col IN (literal) is equivalent to an equality comparison - push that down
		auto bound_eq_comparison = make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL, fst_const_value_expr.value);
		table_filters.PushFilter(column_index, std::move(bound_eq_comparison));
		return FilterPushdownResult::PUSHED_DOWN_FULLY;
	}

	//! Check if values are consecutive, if yes transform them to >= <= (only for integers)
	// e.g. if we have x IN (1, 2, 3, 4, 5) we transform this into x >= 1 AND x <= 5
	vector<Value> in_list;
	for (idx_t i = 1; i < func.children.size(); i++) {
		auto &const_value_expr = func.children[i]->Cast<BoundConstantExpression>();
		D_ASSERT(!const_value_expr.value.IsNull());
		in_list.push_back(const_value_expr.value);
	}
	if (type.IsIntegral() && IsDenseRange(in_list)) {
		// dense range! turn this into x >= min AND x <= max
		// IsDenseRange sorts in_list, so the front element is the min and the back element is the max
		auto lower_bound =
		    make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, std::move(in_list.front()));
		auto upper_bound =
		    make_uniq<ConstantFilter>(ExpressionType::COMPARE_LESSTHANOREQUALTO, std::move(in_list.back()));
		table_filters.PushFilter(column_index, std::move(lower_bound));
		table_filters.PushFilter(column_index, std::move(upper_bound));
		return FilterPushdownResult::PUSHED_DOWN_FULLY;
	}
	// if this is not a dense range we can push an optional filter for zone-map pruning
	auto optional_filter = make_uniq<OptionalFilter>();
	auto in_filter = make_uniq<InFilter>(std::move(in_list));
	optional_filter->child_filter = std::move(in_filter);
	table_filters.PushFilter(column_index, std::move(optional_filter));
	return FilterPushdownResult::PUSHED_DOWN_PARTIALLY;
}

FilterPushdownResult FilterCombiner::TryPushdownOrClause(TableFilterSet &table_filters,
                                                         const vector<ColumnIndex> &column_ids, Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_CONJUNCTION) {
		return FilterPushdownResult::NO_PUSHDOWN;
	}
	auto &conj = expr.Cast<BoundConjunctionExpression>();
	if (conj.GetExpressionType() != ExpressionType::CONJUNCTION_OR) {
		return FilterPushdownResult::NO_PUSHDOWN;
	}
	auto conj_filter = make_uniq<ConjunctionOrFilter>();
	if (conj.children.empty()) {
		return FilterPushdownResult::NO_PUSHDOWN;
	}
	idx_t column_id = 0;
	for (idx_t i = 0; i < conj.children.size(); i++) {
		auto &child = conj.children[i];
		if (child->GetExpressionClass() != ExpressionClass::BOUND_COMPARISON) {
			return FilterPushdownResult::NO_PUSHDOWN;
		}
		optional_ptr<BoundColumnRefExpression> column_ref;
		optional_ptr<BoundConstantExpression> const_val;
		auto &comp = child->Cast<BoundComparisonExpression>();
		bool invert = false;
		if (comp.left->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
		    comp.right->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			column_ref = comp.left->Cast<BoundColumnRefExpression>();
			const_val = comp.right->Cast<BoundConstantExpression>();
		} else if (comp.left->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
		           comp.right->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			column_ref = comp.right->Cast<BoundColumnRefExpression>();
			const_val = comp.left->Cast<BoundConstantExpression>();
			invert = true;
		} else {
			// child of OR filter is not simple so we do not push the or filter down at all
			return FilterPushdownResult::NO_PUSHDOWN;
		}

		if (i == 0) {
			auto &col_id = column_ids[column_ref->binding.column_index];
			column_id = col_id.GetPrimaryIndex();
		} else if (column_id != column_ids[column_ref->binding.column_index].GetPrimaryIndex()) {
			return FilterPushdownResult::NO_PUSHDOWN;
		}

		auto comparison_type = invert ? FlipComparisonExpression(comp.GetExpressionType()) : comp.GetExpressionType();
		if (const_val->value.IsNull()) {
			switch (comparison_type) {
			case ExpressionType::COMPARE_DISTINCT_FROM: {
				auto null_filter = make_uniq<IsNotNullFilter>();
				conj_filter->child_filters.push_back(std::move(null_filter));
				break;
			}
			case ExpressionType::COMPARE_NOT_DISTINCT_FROM: {
				auto null_filter = make_uniq<IsNullFilter>();
				conj_filter->child_filters.push_back(std::move(null_filter));
				break;
			}
			default:
				// if any other comparison type (i.e EQUAL, NOT_EQUAL) do not push a table filter - this is a nop
				// since x = NULL is always falsey, and this is a chain of OR conditions, we can just ignore it
				break;
			}
		} else {
			auto const_filter = make_uniq<ConstantFilter>(comparison_type, const_val->value);
			conj_filter->child_filters.push_back(std::move(const_filter));
		}
	}
	auto optional_filter = make_uniq<OptionalFilter>();
	optional_filter->child_filter = std::move(conj_filter);
	table_filters.PushFilter(ColumnIndex(column_id), std::move(optional_filter));
	return FilterPushdownResult::PUSHED_DOWN_PARTIALLY;
}

FilterPushdownResult FilterCombiner::TryPushdownExpression(TableFilterSet &table_filters,
                                                           const vector<ColumnIndex> &column_ids, Expression &expr) {
	auto pushdown_result = TryPushdownPrefixFilter(table_filters, column_ids, expr);
	if (pushdown_result != FilterPushdownResult::NO_PUSHDOWN) {
		return pushdown_result;
	}
	pushdown_result = TryPushdownLikeFilter(table_filters, column_ids, expr);
	if (pushdown_result != FilterPushdownResult::NO_PUSHDOWN) {
		return pushdown_result;
	}
	pushdown_result = TryPushdownInFilter(table_filters, column_ids, expr);
	if (pushdown_result != FilterPushdownResult::NO_PUSHDOWN) {
		return pushdown_result;
	}
	pushdown_result = TryPushdownOrClause(table_filters, column_ids, expr);
	if (pushdown_result != FilterPushdownResult::NO_PUSHDOWN) {
		return pushdown_result;
	}
	return FilterPushdownResult::NO_PUSHDOWN;
}

TableFilterSet FilterCombiner::GenerateTableScanFilters(const vector<ColumnIndex> &column_ids) {
	TableFilterSet table_filters;
	//! First, we figure the filters that have constant expressions that we can push down to the table scan
	for (auto &constant_value : constant_values) {
		TryPushdownConstantFilter(table_filters, column_ids, constant_value.first, constant_value.second);
	}
	//! Here we look for LIKE or IN filters
	for (idx_t rem_fil_idx = 0; rem_fil_idx < remaining_filters.size(); rem_fil_idx++) {
		auto &remaining_filter = remaining_filters[rem_fil_idx];
		auto pushdown_result = TryPushdownExpression(table_filters, column_ids, *remaining_filter);
		if (pushdown_result == FilterPushdownResult::PUSHED_DOWN_FULLY) {
			// the filter has been pushed down entirely - we can prune it
			remaining_filters.erase_at(rem_fil_idx--);
		}
	}
	return table_filters;
}

static bool IsGreaterThan(ExpressionType type) {
	return type == ExpressionType::COMPARE_GREATERTHAN || type == ExpressionType::COMPARE_GREATERTHANOREQUALTO;
}

static bool IsLessThan(ExpressionType type) {
	return type == ExpressionType::COMPARE_LESSTHAN || type == ExpressionType::COMPARE_LESSTHANOREQUALTO;
}

FilterResult FilterCombiner::AddBoundComparisonFilter(Expression &expr) {
	auto &comparison = expr.Cast<BoundComparisonExpression>();
	if (!SupportedFilterComparison(comparison.GetExpressionType())) {
		// only support [>, >=, <, <=, ==, !=] expressions
		return FilterResult::UNSUPPORTED;
	}
	// check if one of the sides is a scalar value
	bool left_is_scalar = comparison.left->IsFoldable();
	bool right_is_scalar = comparison.right->IsFoldable();
	if (left_is_scalar || right_is_scalar) {
		// comparison with scalar
		auto &node = GetNode(left_is_scalar ? *comparison.right : *comparison.left);
		idx_t equivalence_set = GetEquivalenceSet(node);
		auto &scalar = left_is_scalar ? comparison.left : comparison.right;
		Value constant_value;
		if (!ExpressionExecutor::TryEvaluateScalar(context, *scalar, constant_value)) {
			return FilterResult::UNSUPPORTED;
		}
		if (constant_value.IsNull()) {
			// comparisons with null are always null (i.e. will never result in rows)
			return FilterResult::UNSATISFIABLE;
		}

		// create the ExpressionValueInformation
		ExpressionValueInformation info;
		info.comparison_type =
		    left_is_scalar ? FlipComparisonExpression(comparison.GetExpressionType()) : comparison.GetExpressionType();
		info.constant = constant_value;

		// get the current bucket of constant values
		D_ASSERT(constant_values.find(equivalence_set) != constant_values.end());
		auto &info_list = constant_values.find(equivalence_set)->second;
		if (node.return_type != info.constant.type()) {
			return FilterResult::UNSUPPORTED;
		}
		// check the existing constant comparisons to see if we can do any pruning
		auto ret = AddConstantComparison(info_list, info);

		auto &non_scalar = left_is_scalar ? *comparison.right : *comparison.left;
		auto transitive_filter = FindTransitiveFilter(non_scalar);
		if (transitive_filter != nullptr) {
			// try to add transitive filters
			auto transitive_result = AddTransitiveFilters(transitive_filter->Cast<BoundComparisonExpression>());
			if (transitive_result == FilterResult::UNSUPPORTED) {
				// in case of unsuccessful re-add filter into remaining ones
				remaining_filters.push_back(std::move(transitive_filter));
			}
			if (transitive_result == FilterResult::UNSATISFIABLE) {
				// in case transitive filter is unsatisfiable - abort filter pushdown
				return FilterResult::UNSATISFIABLE;
			}
		}
		return ret;
	} else {
		// comparison between two non-scalars
		// only handle comparisons for now
		if (expr.GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
			return FilterResult::UNSUPPORTED;
		}
		// get the LHS and RHS nodes
		auto &left_node = GetNode(*comparison.left);
		auto &right_node = GetNode(*comparison.right);
		if (left_node.Equals(right_node)) {
			return FilterResult::UNSUPPORTED;
		}
		// get the equivalence sets of the LHS and RHS
		auto left_equivalence_set = GetEquivalenceSet(left_node);
		auto right_equivalence_set = GetEquivalenceSet(right_node);
		if (left_equivalence_set == right_equivalence_set) {
			// this equality filter already exists, prune it
			return FilterResult::SUCCESS;
		}
		// add the right bucket into the left bucket
		D_ASSERT(equivalence_map.find(left_equivalence_set) != equivalence_map.end());
		D_ASSERT(equivalence_map.find(right_equivalence_set) != equivalence_map.end());

		auto &left_bucket = equivalence_map.find(left_equivalence_set)->second;
		auto &right_bucket = equivalence_map.find(right_equivalence_set)->second;
		for (auto &right_expr : right_bucket) {
			// rewrite the equivalence set mapping for this node
			equivalence_set_map[right_expr] = left_equivalence_set;
			// add the node to the left bucket
			left_bucket.push_back(right_expr);
		}
		// now add all constant values from the right bucket to the left bucket
		D_ASSERT(constant_values.find(left_equivalence_set) != constant_values.end());
		D_ASSERT(constant_values.find(right_equivalence_set) != constant_values.end());
		auto &left_constant_bucket = constant_values.find(left_equivalence_set)->second;
		auto &right_constant_bucket = constant_values.find(right_equivalence_set)->second;
		for (auto &right_constant : right_constant_bucket) {
			if (AddConstantComparison(left_constant_bucket, right_constant) == FilterResult::UNSATISFIABLE) {
				return FilterResult::UNSATISFIABLE;
			}
		}
	}
	return FilterResult::SUCCESS;
}

FilterResult FilterCombiner::AddFilter(Expression &expr) {
	if (expr.HasParameter()) {
		return FilterResult::UNSUPPORTED;
	}
	if (expr.IsFoldable()) {
		// scalar condition, evaluate it
		Value result;
		if (!ExpressionExecutor::TryEvaluateScalar(context, expr, result)) {
			return FilterResult::UNSUPPORTED;
		}
		result = result.DefaultCastAs(LogicalType::BOOLEAN);
		// check if the filter passes
		if (result.IsNull() || !BooleanValue::Get(result)) {
			// the filter does not pass the scalar test, create an empty result
			return FilterResult::UNSATISFIABLE;
		} else {
			// the filter passes the scalar test, just remove the condition
			return FilterResult::SUCCESS;
		}
	}
	D_ASSERT(!expr.IsFoldable());
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_BETWEEN) {
		auto &comparison = expr.Cast<BoundBetweenExpression>();
		//! check if one of the sides is a scalar value
		bool lower_is_scalar = comparison.lower->IsFoldable();
		bool upper_is_scalar = comparison.upper->IsFoldable();
		if (lower_is_scalar || upper_is_scalar) {
			//! comparison with scalar - break apart
			auto &node = GetNode(*comparison.input);
			idx_t equivalence_set = GetEquivalenceSet(node);
			auto result = FilterResult::UNSATISFIABLE;

			if (lower_is_scalar) {
				auto scalar = comparison.lower.get();
				Value constant_value;
				if (!ExpressionExecutor::TryEvaluateScalar(context, *scalar, constant_value)) {
					return FilterResult::UNSUPPORTED;
				}

				// create the ExpressionValueInformation
				ExpressionValueInformation info;
				if (comparison.lower_inclusive) {
					info.comparison_type = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
				} else {
					info.comparison_type = ExpressionType::COMPARE_GREATERTHAN;
				}
				info.constant = constant_value;

				// get the current bucket of constant values
				D_ASSERT(constant_values.find(equivalence_set) != constant_values.end());
				auto &info_list = constant_values.find(equivalence_set)->second;
				// check the existing constant comparisons to see if we can do any pruning
				result = AddConstantComparison(info_list, info);
			} else {
				D_ASSERT(upper_is_scalar);
				const auto type = comparison.upper_inclusive ? ExpressionType::COMPARE_LESSTHANOREQUALTO
				                                             : ExpressionType::COMPARE_LESSTHAN;
				auto left = comparison.lower->Copy();
				auto right = comparison.input->Copy();
				auto lower_comp = make_uniq<BoundComparisonExpression>(type, std::move(left), std::move(right));
				result = AddBoundComparisonFilter(*lower_comp);
			}

			//	 Stop if we failed
			if (result != FilterResult::SUCCESS) {
				return result;
			}

			if (upper_is_scalar) {
				auto scalar = comparison.upper.get();
				Value constant_value;
				if (!ExpressionExecutor::TryEvaluateScalar(context, *scalar, constant_value)) {
					return FilterResult::UNSUPPORTED;
				}

				// create the ExpressionValueInformation
				ExpressionValueInformation info;
				if (comparison.upper_inclusive) {
					info.comparison_type = ExpressionType::COMPARE_LESSTHANOREQUALTO;
				} else {
					info.comparison_type = ExpressionType::COMPARE_LESSTHAN;
				}
				info.constant = constant_value;

				// get the current bucket of constant values
				D_ASSERT(constant_values.find(equivalence_set) != constant_values.end());
				// check the existing constant comparisons to see if we can do any pruning
				result = AddConstantComparison(constant_values.find(equivalence_set)->second, info);
			} else {
				D_ASSERT(lower_is_scalar);
				const auto type = comparison.upper_inclusive ? ExpressionType::COMPARE_LESSTHANOREQUALTO
				                                             : ExpressionType::COMPARE_LESSTHAN;
				auto left = comparison.input->Copy();
				auto right = comparison.upper->Copy();
				auto upper_comp = make_uniq<BoundComparisonExpression>(type, std::move(left), std::move(right));
				result = AddBoundComparisonFilter(*upper_comp);
			}

			return result;
		}
	} else if (expr.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
		return AddBoundComparisonFilter(expr);
	}
	// only comparisons supported for now
	return FilterResult::UNSUPPORTED;
}

/*
 * Create and add new transitive filters from a two non-scalar filter such as j > i, j >= i, j < i, and j <= i
 * It's missing to create another method to add transitive filters from scalar filters, e.g, i > 10
 */
FilterResult FilterCombiner::AddTransitiveFilters(BoundComparisonExpression &comparison, bool is_root) {
	if (!IsGreaterThan(comparison.GetExpressionType()) && !IsLessThan(comparison.GetExpressionType())) {
		return FilterResult::UNSUPPORTED;
	}
	// get the LHS and RHS nodes
	auto &left_node = GetNode(*comparison.left);
	reference<Expression> right_node = GetNode(*comparison.right);
	// In case with filters like CAST(i) = j and i = 5 we replace the COLUMN_REF i with the constant 5
	do {
		if (right_node.get().GetExpressionType() != ExpressionType::OPERATOR_CAST) {
			break;
		}
		auto &bound_cast_expr = right_node.get().Cast<BoundCastExpression>();
		if (bound_cast_expr.child->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			break;
		}
		auto &col_ref = bound_cast_expr.child->Cast<BoundColumnRefExpression>();
		for (auto &stored_exp : stored_expressions) {
			reference<Expression> expr = stored_exp.first;
			if (expr.get().GetExpressionType() == ExpressionType::OPERATOR_CAST) {
				expr = *(right_node.get().Cast<BoundCastExpression>().child);
			}
			if (expr.get().GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
				continue;
			}
			auto &st_col_ref = expr.get().Cast<BoundColumnRefExpression>();
			if (st_col_ref.binding != col_ref.binding) {
				continue;
			}
			if (bound_cast_expr.return_type != stored_exp.second->return_type) {
				continue;
			}
			bound_cast_expr.child = stored_exp.second->Copy();
			right_node = GetNode(*bound_cast_expr.child);
			break;
		}
	} while (false);

	if (left_node.Equals(right_node)) {
		return FilterResult::UNSUPPORTED;
	}
	// get the equivalence sets of the LHS and RHS
	idx_t left_equivalence_set = GetEquivalenceSet(left_node);
	idx_t right_equivalence_set = GetEquivalenceSet(right_node);
	if (left_equivalence_set == right_equivalence_set) {
		// this equality filter already exists, prune it
		return FilterResult::SUCCESS;
	}

	vector<ExpressionValueInformation> &left_constants = constant_values.find(left_equivalence_set)->second;
	vector<ExpressionValueInformation> &right_constants = constant_values.find(right_equivalence_set)->second;
	bool is_successful = false;
	bool is_inserted = false;
	// read every constant filters already inserted for the right scalar variable
	// and see if we can create new transitive filters, e.g., there is already a filter i > 10,
	// suppose that we have now the j >= i, then we can infer a new filter j > 10
	for (const auto &right_constant : right_constants) {
		ExpressionValueInformation info;
		info.constant = right_constant.constant;
		// there is already an equality filter, e.g., i = 10
		if (right_constant.comparison_type == ExpressionType::COMPARE_EQUAL) {
			// create filter j [>, >=, <, <=] 10
			// suppose the new comparison is j >= i and we have already a filter i = 10,
			// then we create a new filter j >= 10
			// and the filter j >= i can be pruned by not adding it into the remaining filters
			info.comparison_type = comparison.GetExpressionType();
		} else if ((comparison.GetExpressionType() == ExpressionType::COMPARE_GREATERTHANOREQUALTO &&
		            IsGreaterThan(right_constant.comparison_type)) ||
		           (comparison.GetExpressionType() == ExpressionType::COMPARE_LESSTHANOREQUALTO &&
		            IsLessThan(right_constant.comparison_type))) {
			// filters (j >= i AND i [>, >=] 10) OR (j <= i AND i [<, <=] 10)
			// create filter j [>, >=] 10 and add the filter j [>=, <=] i into the remaining filters
			info.comparison_type = right_constant.comparison_type; // create filter j [>, >=, <, <=] 10
			if (!is_inserted) {
				// Add the filter j >= i in the remaing filters
				auto filter = make_uniq<BoundComparisonExpression>(comparison.GetExpressionType(),
				                                                   comparison.left->Copy(), comparison.right->Copy());
				remaining_filters.push_back(std::move(filter));
				is_inserted = true;
			}
		} else if ((comparison.GetExpressionType() == ExpressionType::COMPARE_GREATERTHAN &&
		            IsGreaterThan(right_constant.comparison_type)) ||
		           (comparison.GetExpressionType() == ExpressionType::COMPARE_LESSTHAN &&
		            IsLessThan(right_constant.comparison_type))) {
			// filters (j > i AND i [>, >=] 10) OR j < i AND i [<, <=] 10
			// create filter j [>, <] 10 and add the filter j [>, <] i into the remaining filters
			// the comparisons j > i and j < i are more restrictive
			info.comparison_type = comparison.GetExpressionType();
			if (!is_inserted) {
				// Add the filter j [>, <] i
				auto filter = make_uniq<BoundComparisonExpression>(comparison.GetExpressionType(),
				                                                   comparison.left->Copy(), comparison.right->Copy());
				remaining_filters.push_back(std::move(filter));
				is_inserted = true;
			}
		} else {
			// we cannot add a new filter
			continue;
		}
		// Add the new filer into the left set
		if (AddConstantComparison(left_constants, info) == FilterResult::UNSATISFIABLE) {
			return FilterResult::UNSATISFIABLE;
		}
		is_successful = true;
	}
	if (is_successful) {
		if (is_root) {
			// now check for remaining transitive filters from the left column
			auto transitive_filter = FindTransitiveFilter(*comparison.left);
			if (transitive_filter != nullptr) {
				// try to add transitive filters
				auto &transitive_cast = transitive_filter->Cast<BoundComparisonExpression>();
				auto transitive_result = AddTransitiveFilters(transitive_cast, false);
				if (transitive_result == FilterResult::UNSUPPORTED) {
					// in case of unsuccessful re-add filter into remaining ones
					remaining_filters.push_back(std::move(transitive_filter));
				}
				if (transitive_result == FilterResult::UNSATISFIABLE) {
					// while adding transitive filters we discovered the filter is unsatisfisable - we can prune
					return FilterResult::UNSATISFIABLE;
				}
			}
		}
		return FilterResult::SUCCESS;
	}

	return FilterResult::UNSUPPORTED;
}

/*
 * Find a transitive filter already inserted into the remaining filters
 * Check for a match between the right column of bound comparisons and the expression,
 * then removes the bound comparison from the remaining filters and returns it
 */
unique_ptr<Expression> FilterCombiner::FindTransitiveFilter(Expression &expr) {
	// We only check for bound column ref
	if (expr.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return nullptr;
	}
	for (idx_t i = 0; i < remaining_filters.size(); i++) {
		if (remaining_filters[i]->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
			auto &comparison = remaining_filters[i]->Cast<BoundComparisonExpression>();
			if (expr.Equals(*comparison.right) && comparison.GetExpressionType() != ExpressionType::COMPARE_NOTEQUAL) {
				auto filter = std::move(remaining_filters[i]);
				remaining_filters.erase_at(i);
				return filter;
			}
		}
	}
	return nullptr;
}

ValueComparisonResult InvertValueComparisonResult(ValueComparisonResult result) {
	if (result == ValueComparisonResult::PRUNE_RIGHT) {
		return ValueComparisonResult::PRUNE_LEFT;
	}
	if (result == ValueComparisonResult::PRUNE_LEFT) {
		return ValueComparisonResult::PRUNE_RIGHT;
	}
	return result;
}

ValueComparisonResult CompareValueInformation(ExpressionValueInformation &left, ExpressionValueInformation &right) {
	if (left.comparison_type == ExpressionType::COMPARE_EQUAL) {
		// left is COMPARE_EQUAL, we can either
		// (1) prune the right side or
		// (2) return UNSATISFIABLE
		bool prune_right_side = false;
		switch (right.comparison_type) {
		case ExpressionType::COMPARE_LESSTHAN:
			prune_right_side = left.constant < right.constant;
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			prune_right_side = left.constant <= right.constant;
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			prune_right_side = left.constant > right.constant;
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			prune_right_side = left.constant >= right.constant;
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
			prune_right_side = left.constant != right.constant;
			break;
		default:
			D_ASSERT(right.comparison_type == ExpressionType::COMPARE_EQUAL);
			prune_right_side = left.constant == right.constant;
			break;
		}
		if (prune_right_side) {
			return ValueComparisonResult::PRUNE_RIGHT;
		} else {
			return ValueComparisonResult::UNSATISFIABLE_CONDITION;
		}
	} else if (right.comparison_type == ExpressionType::COMPARE_EQUAL) {
		// right is COMPARE_EQUAL
		return InvertValueComparisonResult(CompareValueInformation(right, left));
	} else if (left.comparison_type == ExpressionType::COMPARE_NOTEQUAL) {
		// left is COMPARE_NOTEQUAL, we can either
		// (1) prune the left side or
		// (2) not prune anything
		bool prune_left_side = false;
		switch (right.comparison_type) {
		case ExpressionType::COMPARE_LESSTHAN:
			prune_left_side = left.constant >= right.constant;
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			prune_left_side = left.constant > right.constant;
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			prune_left_side = left.constant <= right.constant;
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			prune_left_side = left.constant < right.constant;
			break;
		default:
			D_ASSERT(right.comparison_type == ExpressionType::COMPARE_NOTEQUAL);
			prune_left_side = left.constant == right.constant;
			break;
		}
		if (prune_left_side) {
			return ValueComparisonResult::PRUNE_LEFT;
		} else {
			return ValueComparisonResult::PRUNE_NOTHING;
		}
	} else if (right.comparison_type == ExpressionType::COMPARE_NOTEQUAL) {
		return InvertValueComparisonResult(CompareValueInformation(right, left));
	} else if (IsGreaterThan(left.comparison_type) && IsGreaterThan(right.comparison_type)) {
		// both comparisons are [>], we can either
		// (1) prune the left side or
		// (2) prune the right side
		if (left.constant > right.constant) {
			// left constant is more selective, prune right
			return ValueComparisonResult::PRUNE_RIGHT;
		} else if (left.constant < right.constant) {
			// right constant is more selective, prune left
			return ValueComparisonResult::PRUNE_LEFT;
		} else {
			// constants are equivalent
			// however we can still have the scenario where one is [>=] and the other is [>]
			// we want to prune the [>=] because [>] is more selective
			// if left is [>=] we prune the left, else we prune the right
			if (left.comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
				return ValueComparisonResult::PRUNE_LEFT;
			} else {
				return ValueComparisonResult::PRUNE_RIGHT;
			}
		}
	} else if (IsLessThan(left.comparison_type) && IsLessThan(right.comparison_type)) {
		// both comparisons are [<], we can either
		// (1) prune the left side or
		// (2) prune the right side
		if (left.constant < right.constant) {
			// left constant is more selective, prune right
			return ValueComparisonResult::PRUNE_RIGHT;
		} else if (left.constant > right.constant) {
			// right constant is more selective, prune left
			return ValueComparisonResult::PRUNE_LEFT;
		} else {
			// constants are equivalent
			// however we can still have the scenario where one is [<=] and the other is [<]
			// we want to prune the [<=] because [<] is more selective
			// if left is [<=] we prune the left, else we prune the right
			if (left.comparison_type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
				return ValueComparisonResult::PRUNE_LEFT;
			} else {
				return ValueComparisonResult::PRUNE_RIGHT;
			}
		}
	} else if (IsLessThan(left.comparison_type)) {
		D_ASSERT(IsGreaterThan(right.comparison_type));
		// left is [<] and right is [>], in this case we can either
		// (1) prune nothing or
		// (2) return UNSATISFIABLE
		// the SMALLER THAN constant has to be greater than the BIGGER THAN constant
		if (left.constant >= right.constant) {
			return ValueComparisonResult::PRUNE_NOTHING;
		} else {
			return ValueComparisonResult::UNSATISFIABLE_CONDITION;
		}
	} else {
		// left is [>] and right is [<] or [!=]
		D_ASSERT(IsLessThan(right.comparison_type) && IsGreaterThan(left.comparison_type));
		return InvertValueComparisonResult(CompareValueInformation(right, left));
	}
}

} // namespace duckdb

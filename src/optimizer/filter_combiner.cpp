#include "duckdb/optimizer/filter_combiner.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression.hpp"
using namespace duckdb;
using namespace std;

using ExpressionValueInformation = FilterCombiner::ExpressionValueInformation;

ValueComparisonResult CompareValueInformation(ExpressionValueInformation &left, ExpressionValueInformation &right);

Expression *FilterCombiner::GetNode(Expression *expr) {
	auto entry = stored_expressions.find(expr);
	if (entry != stored_expressions.end()) {
		// expression already exists: return a reference to the stored expression
		return entry->second.get();
	}
	// expression does not exist yet: create a copy and store it
	auto copy = expr->Copy();
	auto pointer_copy = copy.get();
	stored_expressions.insert(make_pair(pointer_copy, move(copy)));
	return pointer_copy;
}

idx_t FilterCombiner::GetEquivalenceSet(Expression *expr) {
	assert(stored_expressions.find(expr) != stored_expressions.end());
	assert(stored_expressions.find(expr)->second.get() == expr);

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
	for (idx_t i = 0; i < info_list.size(); i++) {
		auto comparison = CompareValueInformation(info_list[i], info);
		switch (comparison) {
		case ValueComparisonResult::PRUNE_LEFT:
			// prune the entry from the info list
			info_list.erase(info_list.begin() + i);
			i--;
			break;
		case ValueComparisonResult::PRUNE_RIGHT:
			// prune the current info
			return FilterResult::SUCCESS;
		case ValueComparisonResult::UNSATISFIABLE_CONDITION:
			// combination of filters is unsatisfiable: prune the entire branch
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
	// try to push the filter into the combiner
	auto result = AddFilter(expr.get());
	if (result == FilterResult::UNSUPPORTED) {
		// unsupported filter, push into remaining filters
		remaining_filters.push_back(move(expr));
		return FilterResult::SUCCESS;
	}
	return result;
}

void FilterCombiner::GenerateFilters(std::function<void(unique_ptr<Expression> filter)> callback) {
	// first loop over the remaining filters
	for (auto &filter : remaining_filters) {
		callback(move(filter));
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
				auto comparison = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL,
				                                                         entries[i]->Copy(), entries[k]->Copy());
				callback(move(comparison));
			}
			// for each entry also create a comparison with each constant
			int lower_index = -1, upper_index = -1;
			bool lower_inclusive, upper_inclusive;
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
					auto constant = make_unique<BoundConstantExpression>(info.constant);
					auto comparison = make_unique<BoundComparisonExpression>(info.comparison_type, entries[i]->Copy(),
					                                                         move(constant));
					callback(move(comparison));
				}
			}
			if (lower_index >= 0 && upper_index >= 0) {
				// found both lower and upper index, create a BETWEEN expression
				auto lower_constant = make_unique<BoundConstantExpression>(constant_list[lower_index].constant);
				auto upper_constant = make_unique<BoundConstantExpression>(constant_list[upper_index].constant);
				auto between = make_unique<BoundBetweenExpression>(
				    entries[i]->Copy(), move(lower_constant), move(upper_constant), lower_inclusive, upper_inclusive);
				callback(move(between));
			} else if (lower_index >= 0) {
				// only lower index found, create simple comparison expression
				auto constant = make_unique<BoundConstantExpression>(constant_list[lower_index].constant);
				auto comparison = make_unique<BoundComparisonExpression>(constant_list[lower_index].comparison_type,
				                                                         entries[i]->Copy(), move(constant));
				callback(move(comparison));
			} else if (upper_index >= 0) {
				// only upper index found, create simple comparison expression
				auto constant = make_unique<BoundConstantExpression>(constant_list[upper_index].constant);
				auto comparison = make_unique<BoundComparisonExpression>(constant_list[upper_index].comparison_type,
				                                                         entries[i]->Copy(), move(constant));
				callback(move(comparison));
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

vector<TableFilter>
FilterCombiner::GenerateTableScanFilters(std::function<void(unique_ptr<Expression> filter)> callback,
                                         vector<idx_t> &column_ids) {
	vector<TableFilter> tableFilters;
	//! First, we figure the filters that have constant expressions that we can push down to the table scan
	for (auto &constant_value : constant_values) {
		if (constant_value.second.size() > 0) {
			//			for (idx_t i = 0; i < constant_value.second.size(); ++i) {
			auto filter_exp = equivalence_map.end();
			if ((constant_value.second[0].comparison_type == ExpressionType::COMPARE_EQUAL ||
			     constant_value.second[0].comparison_type == ExpressionType::COMPARE_GREATERTHAN ||
			     constant_value.second[0].comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
			     constant_value.second[0].comparison_type == ExpressionType::COMPARE_LESSTHAN ||
			     constant_value.second[0].comparison_type == ExpressionType::COMPARE_LESSTHANOREQUALTO) &&
			    (TypeIsNumeric(constant_value.second[0].constant.type) ||
			     constant_value.second[0].constant.type == TypeId::VARCHAR)) {
				//! Here we check if these filters are column references
				filter_exp = equivalence_map.find(constant_value.first);
				if (filter_exp->second.size() == 1 && filter_exp->second[0]->type == ExpressionType::BOUND_COLUMN_REF) {
					auto filter_col_exp = static_cast<BoundColumnRefExpression *>(filter_exp->second[0]);
					if (column_ids[filter_col_exp->binding.column_index] == COLUMN_IDENTIFIER_ROW_ID) {
						break;
					}
					auto equivalence_set = filter_exp->first;
					auto &entries = filter_exp->second;
					auto &constant_list = constant_values.find(equivalence_set)->second;
					// for each entry generate an equality expression comparing to each other
					for (idx_t i = 0; i < entries.size(); i++) {
						for (idx_t k = i + 1; k < entries.size(); k++) {
							auto comparison = make_unique<BoundComparisonExpression>(
							    ExpressionType::COMPARE_EQUAL, entries[i]->Copy(), entries[k]->Copy());
							callback(move(comparison));
						}
						// for each entry also create a comparison with each constant
						int lower_index = -1, upper_index = -1;
						for (idx_t k = 0; k < constant_list.size(); k++) {
							tableFilters.push_back(TableFilter(constant_value.second[k].constant,
							                                   constant_value.second[k].comparison_type,
							                                   filter_col_exp->binding.column_index));
							auto &info = constant_list[k];
							if (info.comparison_type == ExpressionType::COMPARE_GREATERTHAN ||
							    info.comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
								lower_index = k;

							} else if (info.comparison_type == ExpressionType::COMPARE_LESSTHAN ||
							           info.comparison_type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
								upper_index = k;
							} else {
								auto constant = make_unique<BoundConstantExpression>(info.constant);
								auto comparison = make_unique<BoundComparisonExpression>(
								    info.comparison_type, entries[i]->Copy(), move(constant));
								callback(move(comparison));
							}
						}
						if (lower_index >= 0) {
							// only lower index found, create simple comparison expression
							auto constant = make_unique<BoundConstantExpression>(constant_list[lower_index].constant);
							auto comparison = make_unique<BoundComparisonExpression>(
							    constant_list[lower_index].comparison_type, entries[i]->Copy(), move(constant));
							callback(move(comparison));
						}
						if (upper_index >= 0) {
							// only upper index found, create simple comparison expression
							auto constant = make_unique<BoundConstantExpression>(constant_list[upper_index].constant);
							auto comparison = make_unique<BoundComparisonExpression>(
							    constant_list[upper_index].comparison_type, entries[i]->Copy(), move(constant));
							callback(move(comparison));
						}
					}
					equivalence_map.erase(filter_exp);
				}
			}
		}
	}
	//! Here we look for LIKE filters with a prefix to use them to skip partitions
	for (auto &remaining_filter : remaining_filters) {
		if (remaining_filter->expression_class == ExpressionClass::BOUND_FUNCTION) {
			auto &func = (BoundFunctionExpression &)*remaining_filter;
			if (func.function.name == "prefix" &&
			    func.children[0]->expression_class == ExpressionClass::BOUND_COLUMN_REF &&
			    func.children[1]->type == ExpressionType::VALUE_CONSTANT) {
				//! This is a like function.
				auto &column_ref = (BoundColumnRefExpression &)*func.children[0].get();
				auto &constant_value_expr = (BoundConstantExpression &)*func.children[1].get();
				string like_string = constant_value_expr.value.str_value;
				if (like_string.empty()) {
					continue;
				}
				auto const_value = constant_value_expr.value.Copy();
				const_value.str_value = like_string;
				//! Here the like must be transformed to a BOUND COMPARISON geq le
				tableFilters.push_back(TableFilter(const_value, ExpressionType::COMPARE_GREATERTHANOREQUALTO,
				                                   column_ref.binding.column_index));
				const_value.str_value[const_value.str_value.size() - 1]++;
				tableFilters.push_back(
				    TableFilter(const_value, ExpressionType::COMPARE_LESSTHAN, column_ref.binding.column_index));
			}
			if (func.function.name == "~~" && func.children[0]->expression_class == ExpressionClass::BOUND_COLUMN_REF &&
			    func.children[1]->type == ExpressionType::VALUE_CONSTANT) {
				//! This is a like function.
				auto &column_ref = (BoundColumnRefExpression &)*func.children[0].get();
				auto &constant_value_expr = (BoundConstantExpression &)*func.children[1].get();
				string like_string = constant_value_expr.value.str_value;
				auto const_value = constant_value_expr.value.Copy();
				if (like_string[0] == '%' || like_string[0] == '_') {
					//! We have no prefix so nothing to pushdown
					break;
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
				const_value.str_value = prefix;
				if (equality) {
					//! Here the like can be transformed to an equality query
					tableFilters.push_back(
					    TableFilter(const_value, ExpressionType::COMPARE_EQUAL, column_ref.binding.column_index));
				} else {
					//! Here the like must be transformed to a BOUND COMPARISON geq le
					tableFilters.push_back(TableFilter(const_value, ExpressionType::COMPARE_GREATERTHANOREQUALTO,
					                                   column_ref.binding.column_index));
					const_value.str_value[const_value.str_value.size() - 1]++;
					tableFilters.push_back(
					    TableFilter(const_value, ExpressionType::COMPARE_LESSTHAN, column_ref.binding.column_index));
				}
			}
		}
	}

	return tableFilters;
}

FilterResult FilterCombiner::AddFilter(Expression *expr) {
	if (expr->HasParameter()) {
		return FilterResult::UNSUPPORTED;
	}
	if (expr->IsFoldable()) {
		// scalar condition, evaluate it
		auto result = ExpressionExecutor::EvaluateScalar(*expr).CastAs(TypeId::BOOL);
		// check if the filter passes
		if (result.is_null || !result.value_.boolean) {
			// the filter does not pass the scalar test, create an empty result
			return FilterResult::UNSATISFIABLE;
		} else {
			// the filter passes the scalar test, just remove the condition
			return FilterResult::SUCCESS;
		}
	}
	assert(!expr->IsFoldable());
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_BETWEEN) {
		auto &comparison = (BoundBetweenExpression &)*expr;
		//! check if one of the sides is a scalar value
		bool left_is_scalar = comparison.lower->IsFoldable();
		bool right_is_scalar = comparison.upper->IsFoldable();
		if (left_is_scalar || right_is_scalar) {
			//! comparison with scalar
			auto node = GetNode(comparison.input.get());
			idx_t equivalence_set = GetEquivalenceSet(node);
			auto scalar = comparison.lower.get();
			auto constant_value = ExpressionExecutor::EvaluateScalar(*scalar);

			// create the ExpressionValueInformation
			ExpressionValueInformation info;
			if (comparison.lower_inclusive) {
				info.comparison_type = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
			} else {
				info.comparison_type = ExpressionType::COMPARE_GREATERTHAN;
			}
			info.constant = constant_value;

			// get the current bucket of constant values
			assert(constant_values.find(equivalence_set) != constant_values.end());
			auto &info_list = constant_values.find(equivalence_set)->second;
			// check the existing constant comparisons to see if we can do any pruning
			AddConstantComparison(info_list, info);
			scalar = comparison.upper.get();
			constant_value = ExpressionExecutor::EvaluateScalar(*scalar);

			// create the ExpressionValueInformation
			if (comparison.upper_inclusive) {
				info.comparison_type = ExpressionType::COMPARE_LESSTHANOREQUALTO;
			} else {
				info.comparison_type = ExpressionType::COMPARE_LESSTHAN;
			}
			info.constant = constant_value;

			// get the current bucket of constant values
			assert(constant_values.find(equivalence_set) != constant_values.end());
			// check the existing constant comparisons to see if we can do any pruning
			return AddConstantComparison(constant_values.find(equivalence_set)->second, info);
		}
	} else if (expr->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
		auto &comparison = (BoundComparisonExpression &)*expr;
		if (comparison.type != ExpressionType::COMPARE_LESSTHAN &&
		    comparison.type != ExpressionType::COMPARE_LESSTHANOREQUALTO &&
		    comparison.type != ExpressionType::COMPARE_GREATERTHAN &&
		    comparison.type != ExpressionType::COMPARE_GREATERTHANOREQUALTO &&
		    comparison.type != ExpressionType::COMPARE_EQUAL && comparison.type != ExpressionType::COMPARE_NOTEQUAL) {
			// only support [>, >=, <, <=, ==] expressions
			return FilterResult::UNSUPPORTED;
		}
		// check if one of the sides is a scalar value
		bool left_is_scalar = comparison.left->IsFoldable();
		bool right_is_scalar = comparison.right->IsFoldable();
		if (left_is_scalar || right_is_scalar) {
			// comparison with scalar
			auto node = GetNode(left_is_scalar ? comparison.right.get() : comparison.left.get());
			idx_t equivalence_set = GetEquivalenceSet(node);
			auto scalar = left_is_scalar ? comparison.left.get() : comparison.right.get();
			auto constant_value = ExpressionExecutor::EvaluateScalar(*scalar);

			// create the ExpressionValueInformation
			ExpressionValueInformation info;
			info.comparison_type = left_is_scalar ? FlipComparisionExpression(comparison.type) : comparison.type;
			info.constant = constant_value;

			// get the current bucket of constant values
			assert(constant_values.find(equivalence_set) != constant_values.end());
			auto &info_list = constant_values.find(equivalence_set)->second;
			// check the existing constant comparisons to see if we can do any pruning
			return AddConstantComparison(info_list, info);
		} else {
			// comparison between two non-scalars
			// only handle comparisons for now
			if (expr->type != ExpressionType::COMPARE_EQUAL) {
				return FilterResult::UNSUPPORTED;
			}
			// get the LHS and RHS nodes
			auto left_node = GetNode(comparison.left.get());
			auto right_node = GetNode(comparison.right.get());
			if (BaseExpression::Equals(left_node, right_node)) {
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
			assert(equivalence_map.find(left_equivalence_set) != equivalence_map.end());
			assert(equivalence_map.find(right_equivalence_set) != equivalence_map.end());

			auto &left_bucket = equivalence_map.find(left_equivalence_set)->second;
			auto &right_bucket = equivalence_map.find(right_equivalence_set)->second;
			for (idx_t i = 0; i < right_bucket.size(); i++) {
				// rewrite the equivalence set mapping for this node
				equivalence_set_map[right_bucket[i]] = left_equivalence_set;
				// add the node to the left bucket
				left_bucket.push_back(right_bucket[i]);
			}
			// now add all constant values from the right bucket to the left bucket
			assert(constant_values.find(left_equivalence_set) != constant_values.end());
			assert(constant_values.find(right_equivalence_set) != constant_values.end());
			auto &left_constant_bucket = constant_values.find(left_equivalence_set)->second;
			auto &right_constant_bucket = constant_values.find(right_equivalence_set)->second;
			for (idx_t i = 0; i < right_constant_bucket.size(); i++) {
				if (AddConstantComparison(left_constant_bucket, right_constant_bucket[i]) ==
				    FilterResult::UNSATISFIABLE) {
					return FilterResult::UNSATISFIABLE;
				}
			}
		}
		return FilterResult::SUCCESS;
	}
	// only comparisons supported for now
	return FilterResult::UNSUPPORTED;
}

static bool IsGreaterThan(ExpressionType type) {
	return type == ExpressionType::COMPARE_GREATERTHAN || type == ExpressionType::COMPARE_GREATERTHANOREQUALTO;
}

static bool IsLessThan(ExpressionType type) {
	return type == ExpressionType::COMPARE_LESSTHAN || type == ExpressionType::COMPARE_LESSTHANOREQUALTO;
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
			assert(right.comparison_type == ExpressionType::COMPARE_EQUAL);
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
			assert(right.comparison_type == ExpressionType::COMPARE_NOTEQUAL);
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
		assert(IsGreaterThan(right.comparison_type));
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
		assert(IsLessThan(right.comparison_type) && IsGreaterThan(left.comparison_type));
		return InvertValueComparisonResult(CompareValueInformation(right, left));
	}
}

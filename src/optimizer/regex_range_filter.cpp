#include <utf8proc_wrapper.hpp>
#include "duckdb/optimizer/regex_range_filter.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"

#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "re2/re2.h"

using namespace duckdb;
using namespace std;

string like_to_regex(string like) {
	string regex;
	bool start_wildcard = like.at(0) == '%';
	bool end_wildcard = like.at(like.size() - 1) == '%';
	if (start_wildcard) {
		regex += ".*?";
		regex += like.substr(1, like.size() - 1);
	} else {
		regex += like.substr(0, like.size());
	}
	if (end_wildcard) {
		regex.pop_back();
		regex += ".*?";
	}
//	regex += "$";
	return regex;
}
unique_ptr<LogicalOperator> RegexRangeFilter::Rewrite(unique_ptr<LogicalOperator> op) {

	for (idx_t child_idx = 0; child_idx < op->children.size(); child_idx++) {
		op->children[child_idx] = Rewrite(move(op->children[child_idx]));
	}

	if (op->type != LogicalOperatorType::FILTER) {
		return op;
	}

	auto new_filter = make_unique<LogicalFilter>();

	for (auto &expr : op->expressions) {
		if (expr->type == ExpressionType::BOUND_FUNCTION) {
			auto &func = (BoundFunctionExpression &)*expr.get();
//			if (func.function.name == "~~") {
//				//! this is a like function. Need to replace it to a regex
//				assert(func.children[1]);
//				assert(func.children[1]->type == ExpressionType::VALUE_CONSTANT);
//				auto &constant_value = (BoundConstantExpression &)*func.children[1].get();
//				constant_value.value.str_value = like_to_regex(constant_value.value.str_value);
//				func.function.name = "regexp_matches";
//				RE2::Options options;
//				options.set_log_errors(false);
//				auto re = make_unique<RE2>(constant_value.value.str_value, options);
//				if (!re->ok()) {
//					throw Exception(re->error());
//				}
//
//				string range_min, range_max;
//				auto range_success = re->PossibleMatchRange(&range_min, &range_max, 1000);
//				// range_min and range_max might produce non-valid UTF8 strings, e.g. in the case of 'a.*'
//				// in this case we don't push a range filter
//				if (range_success && (Utf8Proc::Analyze(range_min) == UnicodeType::INVALID ||
//				                      Utf8Proc::Analyze(range_max) == UnicodeType::INVALID)) {
//					continue;
//				}
//				if (range_success) {
//					//                    auto regex_op =  make_unique<RegexpMatchesBindData>(move(re), range_min,
//					//                    range_max, range_success); auto &info = (RegexpMatchesBindData
//					//                    &)*regex_op->bind_info;
//					auto filter_left = make_unique<BoundComparisonExpression>(
//					    ExpressionType::COMPARE_GREATERTHANOREQUALTO, func.children[0]->Copy(),
//					    make_unique<BoundConstantExpression>(Value(range_min)));
//					auto filter_right = make_unique<BoundComparisonExpression>(
//					    ExpressionType::COMPARE_LESSTHANOREQUALTO, func.children[0]->Copy(),
//					    make_unique<BoundConstantExpression>(Value(range_max)));
//					auto filter_expr = make_unique<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND,
//					                                                           move(filter_left), move(filter_right));
//
//					new_filter->expressions.push_back(move(filter_expr));
//					continue;
//				}
//			}
			if (func.function.name != "regexp_matches" || func.children.size() != 2) {
				continue;
			}
			auto &info = (RegexpMatchesBindData &)*func.bind_info;
			if (!info.range_success) {
				continue;
			}
			auto filter_left = make_unique<BoundComparisonExpression>(
			    ExpressionType::COMPARE_GREATERTHANOREQUALTO, func.children[0]->Copy(),
			    make_unique<BoundConstantExpression>(Value(info.range_min)));
			auto filter_right = make_unique<BoundComparisonExpression>(
			    ExpressionType::COMPARE_LESSTHANOREQUALTO, func.children[0]->Copy(),
			    make_unique<BoundConstantExpression>(Value(info.range_max)));
			auto filter_expr = make_unique<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND,
			                                                           move(filter_left), move(filter_right));

			new_filter->expressions.push_back(move(filter_expr));
		}
	}

	if (new_filter->expressions.size() > 0) {
		new_filter->children = move(op->children);
		op->children.clear();
		op->children.push_back(move(new_filter));
	}

	return op;
}

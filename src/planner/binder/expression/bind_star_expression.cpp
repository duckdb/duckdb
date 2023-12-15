#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/expression_binder/table_function_binder.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "re2/re2.h"

namespace duckdb {

string GetColumnsStringValue(ParsedExpression &expr) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		return colref.GetColumnName();
	} else {
		return expr.ToString();
	}
}

bool Binder::FindStarExpression(unique_ptr<ParsedExpression> &expr, StarExpression **star, bool is_root,
                                bool in_columns) {
	bool has_star = false;
	if (expr->GetExpressionClass() == ExpressionClass::STAR) {
		auto &current_star = expr->Cast<StarExpression>();
		if (!current_star.columns) {
			if (is_root) {
				*star = &current_star;
				return true;
			}
			if (!in_columns) {
				throw BinderException(
				    "STAR expression is only allowed as the root element of an expression. Use COLUMNS(*) instead.");
			}
			// star expression inside a COLUMNS - convert to a constant list
			if (!current_star.replace_list.empty()) {
				throw BinderException(
				    "STAR expression with REPLACE list is only allowed as the root element of COLUMNS");
			}
			vector<unique_ptr<ParsedExpression>> star_list;
			bind_context.GenerateAllColumnExpressions(current_star, star_list);

			vector<Value> values;
			values.reserve(star_list.size());
			for (auto &expr : star_list) {
				values.emplace_back(GetColumnsStringValue(*expr));
			}
			D_ASSERT(!values.empty());

			expr = make_uniq<ConstantExpression>(Value::LIST(LogicalType::VARCHAR, values));
			return true;
		}
		if (in_columns) {
			throw BinderException("COLUMNS expression is not allowed inside another COLUMNS expression");
		}
		in_columns = true;
		if (*star) {
			// we can have multiple
			if (!(*star)->Equals(current_star)) {
				throw BinderException(
				    FormatError(*expr, "Multiple different STAR/COLUMNS in the same expression are not supported"));
			}
			return true;
		}
		*star = &current_star;
		has_star = true;
	}
	ParsedExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<ParsedExpression> &child_expr) {
		if (FindStarExpression(child_expr, star, false, in_columns)) {
			has_star = true;
		}
	});
	return has_star;
}

void Binder::ReplaceStarExpression(unique_ptr<ParsedExpression> &expr, unique_ptr<ParsedExpression> &replacement) {
	D_ASSERT(expr);
	if (expr->GetExpressionClass() == ExpressionClass::STAR) {
		D_ASSERT(replacement);
		expr = replacement->Copy();
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child_expr) { ReplaceStarExpression(child_expr, replacement); });
}

void Binder::ExpandStarExpression(unique_ptr<ParsedExpression> expr,
                                  vector<unique_ptr<ParsedExpression>> &new_select_list) {
	StarExpression *star = nullptr;
	if (!FindStarExpression(expr, &star, true, false)) {
		// no star expression: add it as-is
		D_ASSERT(!star);
		new_select_list.push_back(std::move(expr));
		return;
	}
	D_ASSERT(star);
	vector<unique_ptr<ParsedExpression>> star_list;
	// we have star expressions! expand the list of star expressions
	bind_context.GenerateAllColumnExpressions(*star, star_list);

	if (star->expr) {
		// COLUMNS with an expression
		// two options:
		// VARCHAR parameter <- this is a regular expression
		// LIST of VARCHAR parameters <- this is a set of columns
		TableFunctionBinder binder(*this, context);
		auto child = star->expr->Copy();
		auto result = binder.Bind(child);
		if (!result->IsFoldable()) {
			// cannot resolve parameters here
			if (star->expr->HasParameter()) {
				throw ParameterNotResolvedException();
			} else {
				throw BinderException("Unsupported expression in COLUMNS");
			}
		}
		auto val = ExpressionExecutor::EvaluateScalar(context, *result);
		if (val.type().id() == LogicalTypeId::VARCHAR) {
			// regex
			if (val.IsNull()) {
				throw BinderException("COLUMNS does not support NULL as regex argument");
			}
			auto &regex_str = StringValue::Get(val);
			duckdb_re2::RE2 regex(regex_str);
			if (!regex.error().empty()) {
				auto err = StringUtil::Format("Failed to compile regex \"%s\": %s", regex_str, regex.error());
				throw BinderException(FormatError(*star, err));
			}
			vector<unique_ptr<ParsedExpression>> new_list;
			for (idx_t i = 0; i < star_list.size(); i++) {
				auto &colref = star_list[i]->Cast<ColumnRefExpression>();
				if (!RE2::PartialMatch(colref.GetColumnName(), regex)) {
					continue;
				}
				new_list.push_back(std::move(star_list[i]));
			}
			if (new_list.empty()) {
				auto err = StringUtil::Format("No matching columns found that match regex \"%s\"", regex_str);
				throw BinderException(FormatError(*star, err));
			}
			star_list = std::move(new_list);
		} else if (val.type().id() == LogicalTypeId::LIST &&
		           ListType::GetChildType(val.type()).id() == LogicalTypeId::VARCHAR) {
			// list of varchar columns
			if (val.IsNull() || ListValue::GetChildren(val).empty()) {
				auto err =
				    StringUtil::Format("Star expression \"%s\" resulted in an empty set of columns", star->ToString());
				throw BinderException(FormatError(*star, err));
			}
			auto &children = ListValue::GetChildren(val);
			vector<unique_ptr<ParsedExpression>> new_list;
			// scan the list for all selected columns and construct a lookup table
			case_insensitive_map_t<bool> selected_set;
			for (auto &child : children) {
				selected_set.insert(make_pair(StringValue::Get(child), false));
			}
			// now check the list of all possible expressions and select which ones make it in
			for (auto &expr : star_list) {
				auto str = GetColumnsStringValue(*expr);
				auto entry = selected_set.find(str);
				if (entry != selected_set.end()) {
					new_list.push_back(std::move(expr));
					entry->second = true;
				}
			}
			// check if all expressions found a match
			for (auto &entry : selected_set) {
				if (!entry.second) {
					throw BinderException("Column \"%s\" was selected but was not found in the FROM clause",
					                      entry.first);
				}
			}
			star_list = std::move(new_list);
		} else {
			throw BinderException(FormatError(
			    *star, "COLUMNS expects either a VARCHAR argument (regex) or a LIST of VARCHAR (list of columns)"));
		}
	}

	// now perform the replacement
	for (idx_t i = 0; i < star_list.size(); i++) {
		auto new_expr = expr->Copy();
		ReplaceStarExpression(new_expr, star_list[i]);
		new_select_list.push_back(std::move(new_expr));
	}
}

void Binder::ExpandStarExpressions(vector<unique_ptr<ParsedExpression>> &select_list,
                                   vector<unique_ptr<ParsedExpression>> &new_select_list) {
	for (auto &select_element : select_list) {
		ExpandStarExpression(std::move(select_element), new_select_list);
	}
}

} // namespace duckdb

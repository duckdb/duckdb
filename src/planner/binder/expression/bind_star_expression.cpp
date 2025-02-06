#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/planner/expression_binder/table_function_binder.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/regexp.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

namespace duckdb {

string GetColumnsStringValue(ParsedExpression &expr) {
	if (expr.GetExpressionType() == ExpressionType::COLUMN_REF) {
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
		if (StarExpression::IsStar(*expr)) {
			if (is_root) {
				D_ASSERT(!in_columns);
				// At the root level
				*star = &current_star;
				return true;
			}

			if (!in_columns) {
				// '*' can only appear inside COLUMNS or at the root level
				throw BinderException(
				    "STAR expression is only allowed as the root element of an expression. Use COLUMNS(*) instead.");
			}

			if (!current_star.replace_list.empty()) {
				// '*' inside COLUMNS can not have a REPLACE list
				throw BinderException(
				    "STAR expression with REPLACE list is only allowed as the root element of COLUMNS");
			}
			if (!current_star.rename_list.empty()) {
				// '*' inside COLUMNS can not have a REPLACE list
				throw BinderException(
				    "STAR expression with RENAME list is only allowed as the root element of COLUMNS");
			}

			// '*' expression inside a COLUMNS - convert to a constant list of strings (column names)
			vector<unique_ptr<ParsedExpression>> star_list;
			bind_context.GenerateAllColumnExpressions(current_star, star_list);

			vector<Value> values;
			values.reserve(star_list.size());
			for (auto &element : star_list) {
				values.emplace_back(GetColumnsStringValue(*element));
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
				throw BinderException(*expr,
				                      "Multiple different STAR/COLUMNS in the same expression are not supported");
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
	if (StarExpression::IsColumns(*expr) || StarExpression::IsStar(*expr)) {
		D_ASSERT(replacement);
		auto alias = expr->GetAlias();
		expr = replacement->Copy();
		if (!alias.empty()) {
			expr->SetAlias(std::move(alias));
		}
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child_expr) { ReplaceStarExpression(child_expr, replacement); });
}

static string ReplaceColumnsAlias(const string &alias, const string &column_name, optional_ptr<duckdb_re2::RE2> regex) {
	string result;
	result.reserve(alias.size());
	for (idx_t c = 0; c < alias.size(); c++) {
		if (alias[c] == '\\') {
			c++;
			if (c >= alias.size()) {
				throw BinderException("Unterminated backslash in COLUMNS(*) \"%s\" alias. Backslashes must either be "
				                      "escaped or followed by a number",
				                      alias);
			}
			if (alias[c] == '\\') {
				result += "\\";
				continue;
			}
			if (alias[c] < '0' || alias[c] > '9') {
				throw BinderException("Invalid backslash code in COLUMNS(*) \"%s\" alias. Backslashes must either be "
				                      "escaped or followed by a number",
				                      alias);
			}
			if (alias[c] == '0') {
				result += column_name;
			} else if (!regex) {
				throw BinderException(
				    "Only the backslash escape code \\0 can be used when no regex is supplied to COLUMNS(*)");
			} else {
				string extracted;
				RE2::Extract(column_name, *regex, "\\" + alias.substr(c, 1), &extracted);
				result += extracted;
			}
		} else {
			result += alias[c];
		}
	}
	return result;
}

void TryTransformStarLike(unique_ptr<ParsedExpression> &root) {
	// detect "* LIKE [literal]" and similar expressions
	if (root->GetExpressionClass() != ExpressionClass::FUNCTION) {
		return;
	}
	auto &function = root->Cast<FunctionExpression>();
	if (function.children.size() < 2 || function.children.size() > 3) {
		return;
	}
	auto &left = function.children[0];
	// expression must have a star on the LHS, and a literal on the RHS
	if (left->GetExpressionClass() != ExpressionClass::STAR) {
		return;
	}
	auto &star = left->Cast<StarExpression>();
	if (star.columns) {
		// COLUMNS(*) has different semantics
		return;
	}
	unordered_set<string> supported_ops {"~~",
	                                     "!~~",
	                                     "~~~",
	                                     "!~~~",
	                                     "~~*",
	                                     "!~~*",
	                                     "regexp_full_match",
	                                     "not_like_escape",
	                                     "ilike_escape",
	                                     "not_ilike_escape",
	                                     "like_escape"};
	if (supported_ops.count(function.function_name) == 0) {
		// unsupported op for * expression
		throw BinderException(*root, "Function \"%s\" cannot be applied to a star expression", function.function_name);
	}
	auto &right = function.children[1];
	if (right->GetExpressionClass() != ExpressionClass::CONSTANT) {
		throw BinderException(*root, "Pattern applied to a star expression must be a constant");
	}
	if (!star.rename_list.empty()) {
		throw BinderException(*root, "Rename list cannot be combined with a filtering operation");
	}
	if (!star.replace_list.empty()) {
		throw BinderException(*root, "Replace list cannot be combined with a filtering operation");
	}
	auto original_alias = root->GetAlias();
	auto star_expr = std::move(left);
	unique_ptr<ParsedExpression> child_expr;
	if (function.function_name == "regexp_full_match" && star.exclude_list.empty()) {
		// * SIMILAR TO '[regex]' is equivalent to COLUMNS('[regex]') so we can just move the expression directly
		child_expr = std::move(right);
	} else {
		// for other expressions -> generate a columns expression
		// "* LIKE '%literal%'
		// -> COLUMNS(list_filter(*, x -> x LIKE '%literal%'))
		auto lhs = make_uniq<ColumnRefExpression>("__lambda_col");
		function.children[0] = lhs->Copy();

		auto lambda = make_uniq<LambdaExpression>(std::move(lhs), std::move(root));
		vector<unique_ptr<ParsedExpression>> filter_children;
		filter_children.push_back(std::move(star_expr));
		filter_children.push_back(std::move(lambda));
		auto list_filter = make_uniq<FunctionExpression>("list_filter", std::move(filter_children));
		child_expr = std::move(list_filter);
	}

	auto columns_expr = make_uniq<StarExpression>();
	columns_expr->columns = true;
	columns_expr->expr = std::move(child_expr);
	columns_expr->SetAlias(std::move(original_alias));
	root = std::move(columns_expr);
}

optional_ptr<ParsedExpression> Binder::GetResolvedColumnExpression(ParsedExpression &root_expr) {
	optional_ptr<ParsedExpression> expr = &root_expr;
	while (expr) {
		if (expr->GetExpressionType() == ExpressionType::COLUMN_REF) {
			break;
		}
		if (expr->GetExpressionType() == ExpressionType::OPERATOR_COALESCE) {
			expr = expr->Cast<OperatorExpression>().children[0].get();
		} else {
			// unknown expression
			return nullptr;
		}
	}
	return expr;
}

void Binder::ExpandStarExpression(unique_ptr<ParsedExpression> expr,
                                  vector<unique_ptr<ParsedExpression>> &new_select_list) {
	TryTransformStarLike(expr);

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

	unique_ptr<duckdb_re2::RE2> regex;
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
			regex = make_uniq<duckdb_re2::RE2>(regex_str);
			if (!regex->error().empty()) {
				auto err = StringUtil::Format("Failed to compile regex \"%s\": %s", regex_str, regex->error());
				throw BinderException(*star, err);
			}
			vector<unique_ptr<ParsedExpression>> new_list;
			for (idx_t i = 0; i < star_list.size(); i++) {
				auto child_expr = GetResolvedColumnExpression(*star_list[i]);
				if (!child_expr) {
					continue;
				}
				auto &colref = child_expr->Cast<ColumnRefExpression>();
				if (!RE2::PartialMatch(colref.GetColumnName(), *regex)) {
					continue;
				}
				new_list.push_back(std::move(star_list[i]));
			}
			if (new_list.empty()) {
				auto err = StringUtil::Format("No matching columns found that match regex \"%s\"", regex_str);
				throw BinderException(*star, err);
			}
			star_list = std::move(new_list);
		} else if (val.type().id() == LogicalTypeId::LIST &&
		           ListType::GetChildType(val.type()).id() == LogicalTypeId::VARCHAR) {
			// list of varchar columns
			if (val.IsNull() || ListValue::GetChildren(val).empty()) {
				auto err =
				    StringUtil::Format("Star expression \"%s\" resulted in an empty set of columns", star->ToString());
				throw BinderException(*star, err);
			}
			auto &children = ListValue::GetChildren(val);
			vector<unique_ptr<ParsedExpression>> new_list;
			// scan the list for all selected columns and construct a lookup table
			case_insensitive_map_t<bool> selected_set;
			for (auto &child : children) {
				if (child.IsNull()) {
					throw BinderException(*star, "Columns expression does not support NULL input parameters");
				}
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
			throw BinderException(
			    *star, "COLUMNS expects either a VARCHAR argument (regex) or a LIST of VARCHAR (list of columns)");
		}
	}

	// now perform the replacement
	if (StarExpression::IsColumnsUnpacked(*star)) {
		if (StarExpression::IsColumnsUnpacked(*expr)) {
			throw BinderException("*COLUMNS not allowed at the root level, use COLUMNS instead");
		}
		ReplaceUnpackedStarExpression(expr, star_list);
		new_select_list.push_back(std::move(expr));
		return;
	}
	for (idx_t i = 0; i < star_list.size(); i++) {
		auto new_expr = expr->Copy();
		ReplaceStarExpression(new_expr, star_list[i]);
		if (StarExpression::IsColumns(*star)) {
			auto expr = GetResolvedColumnExpression(*star_list[i]);
			if (expr) {
				auto &colref = expr->Cast<ColumnRefExpression>();
				if (new_expr->GetAlias().empty()) {
					new_expr->SetAlias(colref.GetColumnName());
				} else {
					new_expr->SetAlias(ReplaceColumnsAlias(new_expr->GetAlias(), colref.GetColumnName(), regex.get()));
				}
			}
		}
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

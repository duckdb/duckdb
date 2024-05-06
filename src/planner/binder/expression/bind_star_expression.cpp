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
			// This is a regular (non-COLUMNS) * expression.
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
			throw BinderException("(*)COLUMNS expression is not allowed inside another (*)COLUMNS expression");
		}
		in_columns = true;

		if (*star) {
			// we can have multiple
			if (!(*star)->Equals(current_star)) {
				throw BinderException(
				    *expr, "Multiple different STAR/COLUMNS/*COLUMNS in the same expression are not supported");
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
		auto alias = expr->alias;
		expr = replacement->Copy();
		if (!alias.empty()) {
			expr->alias = std::move(alias);
		}
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child_expr) { ReplaceStarExpression(child_expr, replacement); });
}

void Binder::ReplaceUnpackedStarExpression(unique_ptr<ParsedExpression> &expr,
                                           vector<unique_ptr<ParsedExpression>> &replacements,
                                           ColumnUnpackResult &parent) {
	D_ASSERT(expr);
	if (expr->GetExpressionClass() == ExpressionClass::STAR) {
		// Let the parent know that the child was replaced with an unpacked COLUMNS expression
		D_ASSERT(!replacements.empty());
		auto original_alias = expr->alias;
		// Replace the current expression with the expressions of the unpacked COLUMNS expression
		vector<unique_ptr<ParsedExpression>> new_children;
		for (auto &replacement : replacements) {
			D_ASSERT(replacement);
			auto new_child = replacement->Copy();
			if (!original_alias.empty()) {
				new_child->alias = original_alias;
			}
			new_children.push_back(std::move(new_child));
		}
		parent.AddChild(std::move(new_children));
		return;
	}
	ColumnUnpackResult children;
	// Visit the children of this expression, collecting the unpacked expressions
	ParsedExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<ParsedExpression> &child_expr) {
		ReplaceUnpackedStarExpression(child_expr, replacements, children);
	});
	if (children.AnyChildUnpacked()) {
		auto expression_class = expr->GetExpressionClass();
		switch (expression_class) {
		case ExpressionClass::FUNCTION: {
			auto &function_expr = expr->Cast<FunctionExpression>();

			// Replace children
			vector<unique_ptr<ParsedExpression>> new_children;
			for (auto &unused : function_expr.children) {
				(void)unused;
				auto child_expressions = children.GetChild();
				for (auto &child : child_expressions) {
					new_children.push_back(std::move(child));
				}
			}
			function_expr.children = std::move(new_children);

			// Replace FILTER
			if (function_expr.filter) {
				auto child_expressions = children.GetChild();
				if (child_expressions.size() != 1) {
					throw NotImplementedException("*COLUMNS(...) is not supported in the filter expression");
				}
				function_expr.filter = std::move(child_expressions[0]);
			}

			// Replace ORDER_BY
			if (function_expr.order_bys) {
				vector<unique_ptr<ParsedExpression>> new_orders;
				for (auto &order : function_expr.order_bys->orders) {
					(void)order;
					auto child_expressions = children.GetChild();
					for (auto &child : child_expressions) {
						new_orders.push_back(std::move(child));
					}
				}
				if (new_orders.size() != function_expr.order_bys->orders.size()) {
					throw NotImplementedException("*COLUMNS(...) is not supported in the order expression");
				}
				for (idx_t i = 0; i < new_orders.size(); i++) {
					auto &new_order = new_orders[i];
					function_expr.order_bys->orders[i].expression = std::move(new_order);
				}
			}
			break;
		}
		case ExpressionClass::OPERATOR: {
			auto &operator_expr = expr->Cast<OperatorExpression>();

			// Replace children
			vector<unique_ptr<ParsedExpression>> new_children;
			for (auto &child : operator_expr.children) {
				(void)child;
				auto child_expressions = children.GetChild();
				for (auto &child : child_expressions) {
					new_children.push_back(std::move(child));
				}
			}
			operator_expr.children = std::move(new_children);
			break;
		}
		default: {
			throw BinderException("Unpacked columns (*COLUMNS(...)) are not allowed in this expression");
		}
		}
	}
	// This child was not replaced, copy it
	parent.AddChild(expr->Copy());
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
				auto &colref = star_list[i]->Cast<ColumnRefExpression>();
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
	if (star->unpacked) {
		D_ASSERT(star->columns);
		ColumnUnpackResult children;
		ReplaceUnpackedStarExpression(expr, star_list, children);
		if (children.AnyChildUnpacked()) {
			throw BinderException("*COLUMNS not allowed at the root level, use COLUMNS instead");
		}
		auto unpacked_expressions = children.GetChild();
		for (auto &unpacked_expr : unpacked_expressions) {
			new_select_list.push_back(std::move(unpacked_expr));
		}
		return;
	}
	for (idx_t i = 0; i < star_list.size(); i++) {
		auto new_expr = expr->Copy();
		ReplaceStarExpression(new_expr, star_list[i]);
		if (star->columns) {
			optional_ptr<ParsedExpression> expr = star_list[i].get();
			while (expr) {
				if (expr->type == ExpressionType::COLUMN_REF) {
					break;
				}
				if (expr->type == ExpressionType::OPERATOR_COALESCE) {
					expr = expr->Cast<OperatorExpression>().children[0].get();
				} else {
					// unknown expression
					expr = nullptr;
				}
			}
			if (expr) {
				auto &colref = expr->Cast<ColumnRefExpression>();
				if (new_expr->alias.empty()) {
					new_expr->alias = colref.GetColumnName();
				} else {
					new_expr->alias = ReplaceColumnsAlias(new_expr->alias, colref.GetColumnName(), regex.get());
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

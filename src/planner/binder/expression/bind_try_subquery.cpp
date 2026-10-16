#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

static unique_ptr<ParsedExpression> WrapInErrorCapture(ExpressionType type, unique_ptr<ParsedExpression> child) {
	auto result = make_uniq<OperatorExpression>(type);
	result->SetQueryLocation(child->GetQueryLocation());
	result->GetChildrenMutable().push_back(std::move(child));
	return std::move(result);
}

static bool ContainsSubquery(const ParsedExpression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		return true;
	}
	bool result = false;
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { result = result || ContainsSubquery(child); });
	return result;
}

void ExpressionBinder::RewriteTrySubqueryExpression(unique_ptr<ParsedExpression> &expr,
                                                    vector<unique_ptr<ParsedExpression>> &gates, bool &has_aggregate) {
	switch (expr->GetExpressionClass()) {
	case ExpressionClass::SUBQUERY:
		// a nested subquery is not a maskable position, errors inside of it propagate
		return;
	case ExpressionClass::STAR:
		throw BinderException(*expr, "TRY over a subquery with a star expression in its select list is not supported");
	case ExpressionClass::WINDOW:
		throw BinderException(*expr, "TRY over a subquery with a window function in its select list is not supported");
	case ExpressionClass::FUNCTION: {
		auto &function = expr->Cast<FunctionExpression>();
		auto function_name = StringUtil::Lower(function.FunctionName().GetIdentifierName());
		if (function_name == "unnest" || function_name == "unlist") {
			throw BinderException(function, "TRY over a subquery with UNNEST in its select list is not supported");
		}
		auto &func = BindFunction(function);
		switch (func.type) {
		case CatalogType::MACRO_ENTRY:
			// a macro can expand to an aggregate, so the select list cannot be classified before expansion
			throw BinderException(function, "TRY over a subquery with a macro in its select list is not supported");
		case CatalogType::WINDOW_FUNCTION_ENTRY:
			throw BinderException(function,
			                      "TRY over a subquery with a window function in its select list is not supported");
		case CatalogType::AGGREGATE_FUNCTION_ENTRY: {
			has_aggregate = true;
			if (function.Filter()) {
				throw BinderException(function,
				                      "TRY over a subquery with an aggregate FILTER clause is not supported");
			}
			if (function.OrderBy() && !function.OrderBy()->orders.empty()) {
				throw BinderException(function, "TRY over a subquery with an ordered aggregate is not supported");
			}
			if (function.ExportState()) {
				throw BinderException(function,
				                      "TRY over a subquery with an exported aggregate state is not supported");
			}
			for (auto &argument : function.GetArgumentsMutable()) {
				auto &arg = argument.GetExpressionMutable();
				if (ContainsSubquery(*arg)) {
					throw BinderException(
					    function, "TRY over a subquery with a subquery in an aggregate argument is not supported");
				}
				gates.push_back(WrapInErrorCapture(ExpressionType::OPERATOR_HAS_ERROR, arg->Copy()));
				arg = WrapInErrorCapture(ExpressionType::OPERATOR_TRY, std::move(arg));
			}
			// the TRY wrappers capture every error inside the arguments, nothing is left to rewrite below them
			return;
		}
		default:
			break;
		}
		break;
	}
	default:
		break;
	}
	ParsedExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<ParsedExpression> &child) {
		RewriteTrySubqueryExpression(child, gates, has_aggregate);
	});
}

//! TRY over a scalar subquery is rewritten before the subquery is bound and flattened. In the plain projection
//! case, TRY moves onto the projected expression, which yields a NULL result under the one-row contract of a
//! scalar subquery. In the aggregate case, the projected expression is gated on HAS_ERROR probes of the
//! aggregate inputs, so that an input error anywhere turns the aggregated result into NULL instead of a
//! partially aggregated value. Errors raised outside these positions, such as in the WHERE clause, in the
//! ORDER BY or by the one-row cardinality check, propagate deliberately.
BindResult ExpressionBinder::BindTrySubquery(OperatorExpression &op, idx_t depth) {
	D_ASSERT(op.GetChildrenMutable().size() == 1);
	auto &subquery_expr = op.GetChildrenMutable()[0]->Cast<SubqueryExpression>();
	D_ASSERT(subquery_expr.GetSubqueryType() == SubqueryType::SCALAR);
	auto &node = *subquery_expr.SubqueryMutable()->node;
	if (node.type != QueryNodeType::SELECT_NODE) {
		throw BinderException(op, "TRY over a subquery is only supported for plain SELECT subqueries");
	}
	auto &select_node = node.Cast<SelectNode>();
	if (select_node.select_list.size() != 1) {
		throw BinderException(op, "Subquery returns %zu columns - expected %d", select_node.select_list.size(), 1);
	}
	auto &select_item = select_node.select_list[0];
	auto alias = select_item->GetAlias();

	vector<unique_ptr<ParsedExpression>> gates;
	bool has_aggregate = false;
	RewriteTrySubqueryExpression(select_item, gates, has_aggregate);
	if (!has_aggregate) {
		select_item = WrapInErrorCapture(ExpressionType::OPERATOR_TRY, std::move(select_item));
	} else if (!gates.empty()) {
		auto gate = std::move(gates[0]);
		for (idx_t gate_idx = 1; gate_idx < gates.size(); gate_idx++) {
			gate = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(gate),
			                                        std::move(gates[gate_idx]));
		}
		vector<unique_ptr<ParsedExpression>> gate_children;
		gate_children.push_back(std::move(gate));
		auto gate_aggregate = make_uniq<FunctionExpression>(
		    QualifiedName(Identifier::SystemCatalog(), Identifier::DefaultSchema(), Identifier("bool_or")),
		    std::move(gate_children));
		gate_aggregate->SetQueryLocation(op.GetQueryLocation());
		auto case_expr = make_uniq<CaseExpression>();
		CaseCheck check;
		check.when_expr = std::move(gate_aggregate);
		check.then_expr = make_uniq<ConstantExpression>(Literal::Null());
		case_expr->CaseChecksMutable().push_back(std::move(check));
		case_expr->ElseMutable() = std::move(select_item);
		select_item = std::move(case_expr);
	}
	if (!alias.empty()) {
		select_item->SetAlias(alias);
	}

	// the TRY is consumed by the rewrite, bind the result as an ordinary scalar subquery
	const auto was_inside_try = inside_try;
	inside_try = false;
	auto result = BindExpression(subquery_expr, depth);
	inside_try = was_inside_try;
	return result;
}

} // namespace duckdb

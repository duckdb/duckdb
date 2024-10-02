#include "duckdb/parser/expression/list.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/positional_reference_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {

void RemoveOrderQualificationRecursive(unique_ptr<ParsedExpression> &expr) {
	if (expr->GetExpressionType() == ExpressionType::COLUMN_REF) {
		auto &col_ref = expr->Cast<ColumnRefExpression>();
		auto &col_names = col_ref.column_names;
		if (col_names.size() > 1) {
			col_names = vector<string> {col_names.back()};
		}
	} else {
		ParsedExpressionIterator::EnumerateChildren(
		    *expr, [](unique_ptr<ParsedExpression> &child) { RemoveOrderQualificationRecursive(child); });
	}
}

unique_ptr<ParsedExpression> Transformer::TransformSubquery(duckdb_libpgquery::PGSubLink &root) {
	auto subquery_expr = make_uniq<SubqueryExpression>();

	subquery_expr->subquery = TransformSelectStmt(*root.subselect);
	SetQueryLocation(*subquery_expr, root.location);
	D_ASSERT(subquery_expr->subquery);
	D_ASSERT(!subquery_expr->subquery->node->GetSelectList().empty());

	switch (root.subLinkType) {
	case duckdb_libpgquery::PG_EXISTS_SUBLINK: {
		subquery_expr->subquery_type = SubqueryType::EXISTS;
		break;
	}
	case duckdb_libpgquery::PG_ANY_SUBLINK:
	case duckdb_libpgquery::PG_ALL_SUBLINK: {
		// comparison with ANY() or ALL()
		subquery_expr->subquery_type = SubqueryType::ANY;
		subquery_expr->child = TransformExpression(root.testexpr);
		// get the operator name
		if (!root.operName) {
			// simple IN
			subquery_expr->comparison_type = ExpressionType::COMPARE_EQUAL;
		} else {
			auto operator_name =
			    string((PGPointerCast<duckdb_libpgquery::PGValue>(root.operName->head->data.ptr_value))->val.str);
			subquery_expr->comparison_type = OperatorToExpressionType(operator_name);
		}
		if (subquery_expr->comparison_type != ExpressionType::COMPARE_EQUAL &&
		    subquery_expr->comparison_type != ExpressionType::COMPARE_NOTEQUAL &&
		    subquery_expr->comparison_type != ExpressionType::COMPARE_GREATERTHAN &&
		    subquery_expr->comparison_type != ExpressionType::COMPARE_GREATERTHANOREQUALTO &&
		    subquery_expr->comparison_type != ExpressionType::COMPARE_LESSTHAN &&
		    subquery_expr->comparison_type != ExpressionType::COMPARE_LESSTHANOREQUALTO) {
			throw ParserException("ANY and ALL operators require one of =,<>,>,<,>=,<= comparisons!");
		}
		if (root.subLinkType == duckdb_libpgquery::PG_ALL_SUBLINK) {
			// ALL sublink is equivalent to NOT(ANY) with inverted comparison
			// e.g. [= ALL()] is equivalent to [NOT(<> ANY())]
			// first invert the comparison type
			subquery_expr->comparison_type = NegateComparisonExpression(subquery_expr->comparison_type);
			return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(subquery_expr));
		}
		break;
	}
	case duckdb_libpgquery::PG_EXPR_SUBLINK: {
		// return a single scalar value from the subquery
		// no child expression to compare to
		subquery_expr->subquery_type = SubqueryType::SCALAR;
		break;
	}
	case duckdb_libpgquery::PG_ARRAY_SUBLINK: {
		// ARRAY expression
		// wrap subquery into
		// "SELECT CASE WHEN ARRAY_AGG(COLUMNS(*)) IS NULL THEN [] ELSE ARRAY_AGG(COLUMNS(*)) END FROM (...) tbl"
		auto select_node = make_uniq<SelectNode>();

		// COLUMNS(*)
		auto columns_star = make_uniq<StarExpression>();
		columns_star->columns = true;

		// ARRAY_AGG(COLUMNS(*))
		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(std::move(columns_star));
		auto aggr = make_uniq<FunctionExpression>("array_agg", std::move(children));
		// push ORDER BY modifiers into the array_agg
		for (auto &modifier : subquery_expr->subquery->node->modifiers) {
			if (modifier->type == ResultModifierType::ORDER_MODIFIER) {
				aggr->order_bys = unique_ptr_cast<ResultModifier, OrderModifier>(modifier->Copy());
				break;
			}
		}
		// transform constants (e.g. ORDER BY 1) into positional references (ORDER BY #1)
		if (aggr->order_bys) {
			for (auto &order : aggr->order_bys->orders) {
				if (order.expression->type == ExpressionType::VALUE_CONSTANT) {
					auto &constant_expr = order.expression->Cast<ConstantExpression>();
					Value bigint_value;
					string error;
					if (constant_expr.value.DefaultTryCastAs(LogicalType::BIGINT, bigint_value, &error)) {
						int64_t order_index = BigIntValue::Get(bigint_value);
						idx_t positional_index = order_index < 0 ? NumericLimits<idx_t>::Maximum() : idx_t(order_index);
						order.expression = make_uniq<PositionalReferenceExpression>(positional_index);
					}
				} else {
					RemoveOrderQualificationRecursive(order.expression);
				}
			}
		}
		// ARRAY_AGG(COLUMNS(*)) IS NULL
		auto agg_is_null = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_IS_NULL, aggr->Copy());
		// empty list
		vector<unique_ptr<ParsedExpression>> list_children;
		auto empty_list = make_uniq<FunctionExpression>("list_value", std::move(list_children));
		// CASE
		auto case_expr = make_uniq<CaseExpression>();
		CaseCheck check;
		check.when_expr = std::move(agg_is_null);
		check.then_expr = std::move(empty_list);
		case_expr->case_checks.push_back(std::move(check));
		case_expr->else_expr = std::move(aggr);

		select_node->select_list.push_back(std::move(case_expr));

		// FROM (...) tbl
		auto child_subquery = make_uniq<SubqueryRef>(std::move(subquery_expr->subquery));
		select_node->from_table = std::move(child_subquery);

		auto new_subquery = make_uniq<SelectStatement>();
		new_subquery->node = std::move(select_node);
		subquery_expr->subquery = std::move(new_subquery);

		subquery_expr->subquery_type = SubqueryType::SCALAR;
		break;
	}
	default:
		throw NotImplementedException("Subquery of type %d not implemented\n", (int)root.subLinkType);
	}
	return std::move(subquery_expr);
}

} // namespace duckdb

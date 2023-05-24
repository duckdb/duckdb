#include "duckdb/parser/expression/list.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformSubquery(duckdb_libpgquery::PGSubLink &root) {
	auto subquery_expr = make_uniq<SubqueryExpression>();

	subquery_expr->subquery = TransformSelect(root.subselect);
	D_ASSERT(subquery_expr->subquery);
	D_ASSERT(subquery_expr->subquery->node->GetSelectList().size() > 0);

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
		auto subquery_table_alias = "__subquery";
		auto subquery_column_alias = "__arr_element";

		// ARRAY expression
		// wrap subquery into "SELECT CASE WHEN ARRAY_AGG(i) IS NULL THEN [] ELSE ARRAY_AGG(i) END FROM (...) tbl(i)"
		auto select_node = make_uniq<SelectNode>();

		// ARRAY_AGG(i)
		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(
		    make_uniq_base<ParsedExpression, ColumnRefExpression>(subquery_column_alias, subquery_table_alias));
		auto aggr = make_uniq<FunctionExpression>("array_agg", std::move(children));
		// ARRAY_AGG(i) IS NULL
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

		// FROM (...) tbl(i)
		auto child_subquery = make_uniq<SubqueryRef>(std::move(subquery_expr->subquery), subquery_table_alias);
		child_subquery->column_name_alias.emplace_back(subquery_column_alias);
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
	subquery_expr->query_location = root.location;
	return std::move(subquery_expr);
}

} // namespace duckdb

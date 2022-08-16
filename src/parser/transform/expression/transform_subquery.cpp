#include "duckdb/parser/expression/list.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformSubquery(duckdb_libpgquery::PGSubLink *root) {
	D_ASSERT(root);
	auto subquery_expr = make_unique<SubqueryExpression>();

	subquery_expr->subquery = TransformSelect(root->subselect);
	D_ASSERT(subquery_expr->subquery);
	D_ASSERT(subquery_expr->subquery->node->GetSelectList().size() > 0);

	switch (root->subLinkType) {
	case duckdb_libpgquery::PG_EXISTS_SUBLINK: {
		subquery_expr->subquery_type = SubqueryType::EXISTS;
		break;
	}
	case duckdb_libpgquery::PG_ANY_SUBLINK:
	case duckdb_libpgquery::PG_ALL_SUBLINK: {
		// comparison with ANY() or ALL()
		subquery_expr->subquery_type = SubqueryType::ANY;
		subquery_expr->child = TransformExpression(root->testexpr);
		// get the operator name
		if (!root->operName) {
			// simple IN
			subquery_expr->comparison_type = ExpressionType::COMPARE_EQUAL;
		} else {
			auto operator_name =
			    string((reinterpret_cast<duckdb_libpgquery::PGValue *>(root->operName->head->data.ptr_value))->val.str);
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
		if (root->subLinkType == duckdb_libpgquery::PG_ALL_SUBLINK) {
			// ALL sublink is equivalent to NOT(ANY) with inverted comparison
			// e.g. [= ALL()] is equivalent to [NOT(<> ANY())]
			// first invert the comparison type
			subquery_expr->comparison_type = NegateComparisionExpression(subquery_expr->comparison_type);
			return make_unique<OperatorExpression>(ExpressionType::OPERATOR_NOT, move(subquery_expr));
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
		auto select_node = make_unique<SelectNode>();

		// ARRAY_AGG(i)
		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(
		    make_unique_base<ParsedExpression, ColumnRefExpression>(subquery_column_alias, subquery_table_alias));
		auto aggr = make_unique<FunctionExpression>("array_agg", move(children));
		// ARRAY_AGG(i) IS NULL
		auto agg_is_null = make_unique<OperatorExpression>(ExpressionType::OPERATOR_IS_NULL, aggr->Copy());
		// empty list
		vector<unique_ptr<ParsedExpression>> list_children;
		auto empty_list = make_unique<FunctionExpression>("list_value", move(list_children));
		// CASE
		auto case_expr = make_unique<CaseExpression>();
		CaseCheck check;
		check.when_expr = move(agg_is_null);
		check.then_expr = move(empty_list);
		case_expr->case_checks.push_back(move(check));
		case_expr->else_expr = move(aggr);

		select_node->select_list.push_back(move(case_expr));

		// FROM (...) tbl(i)
		auto child_subquery = make_unique<SubqueryRef>(move(subquery_expr->subquery), subquery_table_alias);
		child_subquery->column_name_alias.emplace_back(subquery_column_alias);
		select_node->from_table = move(child_subquery);

		auto new_subquery = make_unique<SelectStatement>();
		new_subquery->node = move(select_node);
		subquery_expr->subquery = move(new_subquery);

		subquery_expr->subquery_type = SubqueryType::SCALAR;
		break;
	}
	default:
		throw NotImplementedException("Subquery of type %d not implemented\n", (int)root->subLinkType);
	}
	subquery_expr->query_location = root->location;
	return move(subquery_expr);
}

} // namespace duckdb

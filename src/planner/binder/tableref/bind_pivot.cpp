#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

namespace duckdb {

unique_ptr<BoundTableRef> Binder::Bind(PivotRef &ref) {
	//SELECT 'AverageCost', AVG(CASE WHEN DaysToManufacture=0 THEN StandardCost ELSE NULL END) AS "0", AVG(CASE WHEN DaysToManufacture=1 THEN StandardCost ELSE NULL END) AS "1" FROM Product;
	auto select_node = make_unique<SelectNode>();
	if (!ref.source) {
		throw InternalException("Pivot without a source!?");
	}
	select_node->from_table = std::move(ref.source);
	for(auto &pivot : ref.pivots) {
		for(auto &val : pivot.values) {
			for(auto &aggr : ref.aggregates) {
				if (aggr->type != ExpressionType::FUNCTION) {
					throw BinderException(FormatError(*aggr, "Pivot expression must be an aggregate"));
				}
				auto copy = aggr->Copy();
				auto &function = (FunctionExpression &) *copy;
				if (function.children.size() != 1) {
					throw BinderException(FormatError(*aggr, "Pivot expression must have a single argument"));
				}

				auto column_ref = make_unique<ColumnRefExpression>(pivot.name);
				auto constant_value = make_unique<ConstantExpression>(Value(val));

				auto case_expr = make_unique<CaseExpression>();
				auto comp_expr = make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(column_ref),
																   std::move(constant_value));

				CaseCheck check;
				check.when_expr = std::move(comp_expr);
				check.then_expr = std::move(function.children[0]);
				case_expr->case_checks.push_back(std::move(check));
				case_expr->else_expr = make_unique<ConstantExpression>(Value());

				function.children[0] = std::move(case_expr);
				function.alias = val;
				select_node->select_list.push_back(std::move(copy));
			}
		}
	}

	auto select = make_unique<SelectStatement>();
	select->node = std::move(select_node);
	auto result = make_unique<SubqueryRef>(std::move(select), ref.alias);
	return Bind(*result);
}

} // namespace duckdb

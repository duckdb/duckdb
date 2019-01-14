#include "parser/query_node.hpp"
#include "parser/expression/bound_expression.hpp"
#include "parser/expression/constant_expression.hpp"
#include "planner/binder.hpp"

using namespace duckdb;
using namespace std;

void Binder::BindOrderBy(OrderByDescription &orderby, vector<unique_ptr<Expression>>& select_list, size_t max_index) {
	for (auto &order : orderby.orders) {
		VisitExpression(&order.expression);
		if (order.expression->type == ExpressionType::VALUE_CONSTANT) {
			auto &constant = (ConstantExpression&) *order.expression;
			if (TypeIsIntegral(constant.value.type)) {
				// this ORDER BY expression refers to a column in the select
				// clause by index e.g. ORDER BY 1 assign the type of the SELECT
				// clause
				auto index = constant.value.GetNumericValue();
				if (index < 1 || index > max_index) {
					throw BinderException("ORDER term out of range - should be between 1 and %d",
											(int)select_list.size());
				}
				order.expression = make_unique<BoundExpression>(select_list[index - 1]->return_type, index - 1);
			}
		}
		order.expression->ResolveType();
	}
}

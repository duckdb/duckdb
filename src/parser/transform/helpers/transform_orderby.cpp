
#include "parser/expression/columnref_expression.hpp"
#include "parser/expression/constant_expression.hpp"
#include "parser/statement/select_statement.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

bool Transformer::TransformOrderBy(List *order, OrderByDescription &result) {
	if (!order) {
		return false;
	}

	for (auto node = order->head; node != nullptr; node = node->next) {
		Node *temp = reinterpret_cast<Node *>(node->data.ptr_value);
		if (temp->type == T_SortBy) {
			OrderByNode ordernode;
			SortBy *sort = reinterpret_cast<SortBy *>(temp);
			Node *target = sort->node;
			if (sort->sortby_dir == SORTBY_ASC ||
			    sort->sortby_dir == SORTBY_DEFAULT) {
				ordernode.type = OrderType::ASCENDING;
			} else if (sort->sortby_dir == SORTBY_DESC) {
				ordernode.type = OrderType::DESCENDING;
			} else {
				throw NotImplementedException("Unimplemented order by type");
			}
			ordernode.expression = TransformExpression(target);
			if (ordernode.expression->type == ExpressionType::VALUE_CONSTANT) {
				auto constant = reinterpret_cast<ConstantExpression *>(
				    ordernode.expression.get());
				if (TypeIsIntegral(constant->value.type)) {
					ordernode.expression = make_unique<ColumnRefExpression>(
					    TypeId::INVALID, constant->value.GetNumericValue());
				} else {
					// order by non-integral constant, continue
					continue;
				}
			}
			result.orders.push_back(
			    OrderByNode(ordernode.type, move(ordernode.expression)));
		} else {
			throw NotImplementedException("ORDER BY list member type %d\n",
			                              temp->type);
		}
	}
	return true;
}

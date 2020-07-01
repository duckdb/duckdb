#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

bool Transformer::TransformOrderBy(PGList *order, vector<OrderByNode> &result) {
	if (!order) {
		return false;
	}

	for (auto node = order->head; node != nullptr; node = node->next) {
		auto temp = reinterpret_cast<PGNode *>(node->data.ptr_value);
		if (temp->type == T_PGSortBy) {
			OrderType type;
			OrderByNullType null_order;
			auto sort = reinterpret_cast<PGSortBy *>(temp);
			auto target = sort->node;
			if (sort->sortby_dir == PG_SORTBY_DEFAULT) {
				type = OrderType::ORDER_DEFAULT;
			} else if (sort->sortby_dir == PG_SORTBY_ASC) {
				type = OrderType::ASCENDING;
			} else if (sort->sortby_dir == PG_SORTBY_DESC) {
				type = OrderType::DESCENDING;
			} else {
				throw NotImplementedException("Unimplemented order by type");
			}
			if (sort->sortby_nulls == PG_SORTBY_NULLS_DEFAULT) {
				null_order = OrderByNullType::ORDER_DEFAULT;
			} else if (sort->sortby_nulls == PG_SORTBY_NULLS_FIRST) {
				null_order = OrderByNullType::NULLS_FIRST;
			} else if (sort->sortby_nulls == PG_SORTBY_NULLS_LAST) {
				null_order = OrderByNullType::NULLS_LAST;
			} else {
				throw NotImplementedException("Unimplemented order by type");
			}
			auto order_expression = TransformExpression(target);
			result.push_back(OrderByNode(type, null_order, move(order_expression)));
		} else {
			throw NotImplementedException("ORDER BY list member type %d\n", temp->type);
		}
	}
	return true;
}

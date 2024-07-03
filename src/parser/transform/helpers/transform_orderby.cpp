#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

bool Transformer::TransformOrderBy(duckdb_libpgquery::PGList *order, vector<OrderByNode> &result) {
	if (!order) {
		return false;
	}

	for (auto node = order->head; node != nullptr; node = node->next) {
		auto temp_node = PGPointerCast<duckdb_libpgquery::PGNode>(node->data.ptr_value);
		if (temp_node->type != duckdb_libpgquery::T_PGSortBy) {
			throw NotImplementedException("ORDER BY list member type %d\n", temp_node->type);
		}

		OrderType type;
		OrderByNullType null_order;
		auto sort = PGCast<duckdb_libpgquery::PGSortBy>(*temp_node.get());
		auto target = sort.node;

		if (sort.sortby_dir == duckdb_libpgquery::PG_SORTBY_DEFAULT) {
			type = OrderType::ORDER_DEFAULT;
		} else if (sort.sortby_dir == duckdb_libpgquery::PG_SORTBY_ASC) {
			type = OrderType::ASCENDING;
		} else if (sort.sortby_dir == duckdb_libpgquery::PG_SORTBY_DESC) {
			type = OrderType::DESCENDING;
		} else {
			throw NotImplementedException("Unimplemented order by type");
		}

		if (sort.sortby_nulls == duckdb_libpgquery::PG_SORTBY_NULLS_DEFAULT) {
			null_order = OrderByNullType::ORDER_DEFAULT;
		} else if (sort.sortby_nulls == duckdb_libpgquery::PG_SORTBY_NULLS_FIRST) {
			null_order = OrderByNullType::NULLS_FIRST;
		} else if (sort.sortby_nulls == duckdb_libpgquery::PG_SORTBY_NULLS_LAST) {
			null_order = OrderByNullType::NULLS_LAST;
		} else {
			throw NotImplementedException("Unimplemented order by type");
		}

		auto order_expression = TransformExpression(target);
		result.emplace_back(type, null_order, std::move(order_expression));
	}
	return true;
}

} // namespace duckdb

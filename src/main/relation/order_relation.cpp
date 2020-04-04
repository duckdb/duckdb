#include "duckdb/main/relation/order_relation.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/expression_binder/relation_binder.hpp"
#include "duckdb/planner/bound_query_node.hpp"

namespace duckdb {

OrderRelation::OrderRelation(shared_ptr<Relation> child_p, vector<OrderByNode> orders) :
	Relation(child_p->context, RelationType::ORDER), orders(move(orders)), child(move(child_p)) {
	// bind the expressions
	vector<ColumnDefinition> dummy_columns;
	context.TryBindRelation(*this, dummy_columns);
}

BoundStatement OrderRelation::Bind(Binder &binder) {
	auto result = child->Bind(binder);

	// now bind the expressions
	RelationBinder expr_binder(binder, context, "ORDER");
	vector<BoundOrderByNode> bound_orders;
	for(idx_t i = 0; i < orders.size(); i++) {
		auto expr = orders[i].expression->Copy();

		SQLType result_type;
		auto bound_expr = expr_binder.Bind(expr, &result_type);

		BoundOrderByNode node;
		node.expression = move(bound_expr);
		node.type = orders[i].type;
		bound_orders.push_back(move(node));
	}

	// create the logical projection node
	auto order = make_unique<LogicalOrder>(move(bound_orders));
	order->AddChild(move(result.plan));
	result.plan = move(order);
	return result;
}

const vector<ColumnDefinition> &OrderRelation::Columns() {
	return child->Columns();
}

string OrderRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Order [";
	for(idx_t i = 0; i < orders.size(); i++) {
		if (i != 0) {
			str += ", ";
		}
		str += orders[i].expression->ToString() + (orders[i].type == OrderType::ASCENDING ? " ASC" : " DESC");
	}
	str += "]\n";
	return str + child->ToString(depth + 1);;
}

}
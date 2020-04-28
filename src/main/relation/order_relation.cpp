#include "duckdb/main/relation/order_relation.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

OrderRelation::OrderRelation(shared_ptr<Relation> child_p, vector<OrderByNode> orders)
    : Relation(child_p->context, RelationType::ORDER_RELATION), orders(move(orders)), child(move(child_p)) {
	// bind the expressions
	vector<ColumnDefinition> dummy_columns;
	context.TryBindRelation(*this, dummy_columns);
}

unique_ptr<QueryNode> OrderRelation::GetQueryNode() {
	auto child_node = child->GetQueryNode();
	auto order_node = make_unique<OrderModifier>();
	for (idx_t i = 0; i < orders.size(); i++) {
		OrderByNode node;
		node.expression = orders[i].expression->Copy();
		node.type = orders[i].type;
		order_node->orders.push_back(move(node));
	}
	child_node->modifiers.push_back(move(order_node));
	return child_node;
}

string OrderRelation::GetAlias() {
	return child->GetAlias();
}

const vector<ColumnDefinition> &OrderRelation::Columns() {
	return child->Columns();
}

string OrderRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Order [";
	for (idx_t i = 0; i < orders.size(); i++) {
		if (i != 0) {
			str += ", ";
		}
		str += orders[i].expression->ToString() + (orders[i].type == OrderType::ASCENDING ? " ASC" : " DESC");
	}
	str += "]\n";
	return str + child->ToString(depth + 1);
}

} // namespace duckdb

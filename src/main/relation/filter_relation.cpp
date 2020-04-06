#include "duckdb/main/relation/filter_relation.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

FilterRelation::FilterRelation(shared_ptr<Relation> child_p, unique_ptr<ParsedExpression> condition_p) :
	Relation(child_p->context, RelationType::FILTER), condition(move(condition_p)), child(move(child_p)) {
	vector<ColumnDefinition> dummy_columns;
	context.TryBindRelation(*this, dummy_columns);
}

unique_ptr<QueryNode> FilterRelation::GetQueryNode() {
	auto child_node = child->GetQueryNode();
	auto filter_node = make_unique<FilterModifier>();
	filter_node->filter = condition->Copy();
	child_node->modifiers.push_back(move(filter_node));
	return child_node;
}

string FilterRelation::GetAlias() {
	return child->GetAlias();
}

const vector<ColumnDefinition> &FilterRelation::Columns() {
	return child->Columns();
}

string FilterRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Filter [" + condition->ToString() + "]\n";
	return str + child->ToString(depth + 1);;
}

}
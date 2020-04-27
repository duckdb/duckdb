#include "duckdb/main/relation/join_relation.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/joinref.hpp"

namespace duckdb {

JoinRelation::JoinRelation(shared_ptr<Relation> left_p, shared_ptr<Relation> right_p,
                           unique_ptr<ParsedExpression> condition_p, JoinType type)
    : Relation(left_p->context, RelationType::JOIN_RELATION), left(move(left_p)), right(move(right_p)),
      condition(move(condition_p)), join_type(type) {
	if (&left->context != &right->context) {
		throw Exception("Cannot combine LEFT and RIGHT relations of different connections!");
	}
	context.TryBindRelation(*this, this->columns);
}

JoinRelation::JoinRelation(shared_ptr<Relation> left_p, shared_ptr<Relation> right_p, vector<string> using_columns_p,
                           JoinType type)
    : Relation(left_p->context, RelationType::JOIN_RELATION), left(move(left_p)), right(move(right_p)),
      using_columns(move(using_columns_p)), join_type(type) {
	if (&left->context != &right->context) {
		throw Exception("Cannot combine LEFT and RIGHT relations of different connections!");
	}
	context.TryBindRelation(*this, this->columns);
}

unique_ptr<QueryNode> JoinRelation::GetQueryNode() {
	auto result = make_unique<SelectNode>();
	result->select_list.push_back(make_unique<StarExpression>());
	result->from_table = GetTableRef();
	return move(result);
}

unique_ptr<TableRef> JoinRelation::GetTableRef() {
	auto join_ref = make_unique<JoinRef>();
	join_ref->left = left->GetTableRef();
	join_ref->right = right->GetTableRef();
	if (condition) {
		join_ref->condition = condition->Copy();
	}
	join_ref->using_columns = using_columns;
	join_ref->type = join_type;
	return move(join_ref);
}

const vector<ColumnDefinition> &JoinRelation::Columns() {
	return this->columns;
}

string JoinRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth);
	str = "Join";
	return str + "\n" + left->ToString(depth + 1) + right->ToString(depth + 1);
}

} // namespace duckdb

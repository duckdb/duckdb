#include "duckdb/main/relation/join_relation.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/main/relation/projection_relation.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"

namespace duckdb {

JoinRelation::JoinRelation(shared_ptr<Relation> left_p, shared_ptr<Relation> right_p,
                           unique_ptr<ParsedExpression> condition_p, JoinType type)
    : Relation(left_p->context, RelationType::JOIN_RELATION), left(std::move(left_p)), right(std::move(right_p)),
      condition(std::move(condition_p)), join_type(type) {
	if (left->context.GetContext() != right->context.GetContext()) {
		throw Exception("Cannot combine LEFT and RIGHT relations of different connections!");
	}
	context.GetContext()->TryBindRelation(*this, this->columns);
}

JoinRelation::JoinRelation(shared_ptr<Relation> left_p, shared_ptr<Relation> right_p, vector<string> using_columns_p,
                           JoinType type)
    : Relation(left_p->context, RelationType::JOIN_RELATION), left(std::move(left_p)), right(std::move(right_p)),
      using_columns(std::move(using_columns_p)), join_type(type) {
	if (left->context.GetContext() != right->context.GetContext()) {
		throw Exception("Cannot combine LEFT and RIGHT relations of different connections!");
	}
	context.GetContext()->TryBindRelation(*this, this->columns);
}

JoinRelation::JoinRelation(shared_ptr<Relation> left_p, shared_ptr<Relation> left_expr, shared_ptr<Relation> right_proj,
                           JoinType type)
    : Relation(left_p->context, RelationType::JOIN_RELATION), left(std::move(left_p)), right(std::move(right_proj)),
      left_expr(std::move(left_expr)), using_columns(), join_type(type) {
	if (join_type != JoinType::ANTI && join_type != JoinType::SEMI) {
		throw Exception("Must pass conditions for join of type " + JoinTypeToString(join_type));
	}
	if (left->context.GetContext() != right->context.GetContext()) {
		throw Exception("Cannot combine LEFT and RIGHT relations of different connections!");
	}
	if (right->type != RelationType::PROJECTION_RELATION) {
		throw Exception(JoinTypeToString(join_type) + " requires a projection for the right relation. Received " +
		                RelationTypeToString(right->type));
	}
	condition = nullptr;
	context.GetContext()->TryBindRelation(*this, this->columns);
}

unique_ptr<QueryNode> JoinRelation::GetQueryNode() {
	auto result = make_unique<SelectNode>();
	result->select_list.push_back(make_unique<StarExpression>());
	result->from_table = GetTableRef();
	return std::move(result);
}

unique_ptr<TableRef> JoinRelation::GetTableRef() {
	auto join_ref = make_unique<JoinRef>(JoinRefType::REGULAR);
	join_ref->left = left->GetTableRef();
	join_ref->right = right->GetTableRef();
	if (condition) {
		join_ref->condition = condition->Copy();
	}
	join_ref->using_columns = using_columns;
	join_ref->type = join_type;
	return std::move(join_ref);
}

const vector<ColumnDefinition> &JoinRelation::Columns() {
	return this->columns;
}

string JoinRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth);
	str += "Join " + JoinTypeToString(join_type);
	if (condition) {
		str += " " + condition->GetName();
	}

	return str + "\n" + left->ToString(depth + 1) + "\n" + right->ToString(depth + 1);
}

} // namespace duckdb

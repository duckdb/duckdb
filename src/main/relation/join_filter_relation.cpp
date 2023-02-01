#include "duckdb/main/relation/join_filter_relation.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/main/relation/projection_relation.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"

namespace duckdb {

JoinFilterRelation::JoinFilterRelation(shared_ptr<Relation> left_p, shared_ptr<Relation> left_expr,
                                       shared_ptr<Relation> right_p, JoinType type)
    : Relation(left_p->context, RelationType::JOIN_RELATION), left(std::move(left_p)), right(std::move(right_p)),
      left_expr(std::move(left_expr)), join_type(type) {
	if (join_type != JoinType::ANTI && join_type != JoinType::SEMI) {
		throw Exception("join type is not 'semi' or 'anti'. Received " + JoinTypeToString(join_type));
	}
	if (left->context.GetContext() != right->context.GetContext()) {
		throw Exception("Cannot combine LEFT and RIGHT relations of different connections!");
	}
	if (right->type != RelationType::PROJECTION_RELATION) {
		throw Exception(JoinTypeToString(join_type) + " requires a projection for the right relation. Received " +
		                RelationTypeToString(right->type));
	}
	context.GetContext()->TryBindRelation(*this, this->columns);
}

unique_ptr<QueryNode> JoinFilterRelation::GetQueryNode() {
	auto result = make_unique<SelectNode>();
	result->select_list.push_back(make_unique<StarExpression>());
	result->from_table = left->GetTableRef();
	D_ASSERT(right->type == RelationType::PROJECTION_RELATION);
	D_ASSERT(left_expr->type == RelationType::PROJECTION_RELATION);
	auto right_projection = std::dynamic_pointer_cast<ProjectionRelation>(right);
	auto left_projection = std::dynamic_pointer_cast<ProjectionRelation>(left_expr);
	if (right_projection->expressions.size() != left_projection->expressions.size()) {
		throw Exception(JoinTypeToString(join_type) +
		                " JOIN requires projections to have the same number of expressions");
	}
	auto where_child = make_unique<SubqueryExpression>();
	auto select_statement = make_unique<SelectStatement>();
	select_statement->node = right->GetQueryNode();
	where_child->subquery = std::move(select_statement);
	where_child->subquery_type = SubqueryType::ANY;
	if (left_projection->expressions.size() > 1) {
		throw Exception("Cannot project more than one expression from left.");
	}
	where_child->child = left_projection->expressions.at(0)->Copy();
	where_child->comparison_type = ExpressionType::COMPARE_EQUAL;
	// wrap anti joins in extra operator_not expression
	if (join_type == JoinType::ANTI) {
		auto where_clause = make_unique<OperatorExpression>(ExpressionType::OPERATOR_NOT);
		where_clause->children.push_back(std::move(where_child));
		result->where_clause = std::move(where_clause);
	} else {
		result->where_clause = std::move(where_child);
	}
	return result;
}

unique_ptr<TableRef> JoinFilterRelation::GetTableRef() {
	// only return left table ref, as no right values are returned
	// as part of an anti or semi join
	auto result = left->GetTableRef();
	return result;
}

const vector<ColumnDefinition> &JoinFilterRelation::Columns() {
	return this->columns;
}

string JoinFilterRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth);
	str += "SELECT " + left->ToString(depth + 1);
	str += " WHERE ";
	str += " " + left_expr->ToString(depth + 1);
	if (join_type == JoinType::ANTI) {
		str += " NOT ";
	}
	str += " IN ( " + right->ToString(depth + 1);
	return str;
}

} // namespace duckdb

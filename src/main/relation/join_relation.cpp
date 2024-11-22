#include "duckdb/main/relation/join_relation.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/main/client_context_wrapper.hpp"

namespace duckdb {

JoinRelation::JoinRelation(shared_ptr<Relation> left_p, shared_ptr<Relation> right_p,
                           unique_ptr<ParsedExpression> condition_p, JoinType type, JoinRefType join_ref_type)
    : Relation(left_p->context, RelationType::JOIN_RELATION), left(std::move(left_p)), right(std::move(right_p)),
      condition(std::move(condition_p)), join_type(type), join_ref_type(join_ref_type) {
	if (left->context->GetContext() != right->context->GetContext()) {
		throw InvalidInputException("Cannot combine LEFT and RIGHT relations of different connections!");
	}
	TryBindRelation(columns);
}

JoinRelation::JoinRelation(shared_ptr<Relation> left_p, shared_ptr<Relation> right_p, vector<string> using_columns_p,
                           JoinType type, JoinRefType join_ref_type)
    : Relation(left_p->context, RelationType::JOIN_RELATION), left(std::move(left_p)), right(std::move(right_p)),
      using_columns(std::move(using_columns_p)), join_type(type), join_ref_type(join_ref_type) {
	if (left->context->GetContext() != right->context->GetContext()) {
		throw InvalidInputException("Cannot combine LEFT and RIGHT relations of different connections!");
	}
	TryBindRelation(columns);
}

unique_ptr<QueryNode> JoinRelation::GetQueryNode() {
	auto result = make_uniq<SelectNode>();
	result->select_list.push_back(make_uniq<StarExpression>());
	result->from_table = GetTableRef();
	return std::move(result);
}

unique_ptr<TableRef> JoinRelation::GetTableRef() {
	auto join_ref = make_uniq<JoinRef>(join_ref_type);
	join_ref->left = left->GetTableRef();
	join_ref->right = right->GetTableRef();
	if (condition) {
		join_ref->condition = condition->Copy();
	}
	join_ref->using_columns = using_columns;
	join_ref->type = join_type;
	join_ref->delim_flipped = delim_flipped;
	for (auto &col : duplicate_eliminated_columns) {
		join_ref->duplicate_eliminated_columns.emplace_back(col->Copy());
	}
	return std::move(join_ref);
}

const vector<ColumnDefinition> &JoinRelation::Columns() {
	return this->columns;
}

string JoinRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth);
	str += "Join " + EnumUtil::ToString(join_ref_type) + " " + EnumUtil::ToString(join_type);
	if (condition) {
		str += " " + condition->GetName();
	}

	return str + "\n" + left->ToString(depth + 1) + "\n" + right->ToString(depth + 1);
}

} // namespace duckdb

#include "duckdb/main/relation/limit_relation.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

LimitRelation::LimitRelation(shared_ptr<Relation> child_p, int64_t limit, int64_t offset) :
	Relation(child_p->context, RelationType::PROJECTION), limit(limit), offset(offset), child(move(child_p)) {
}

unique_ptr<QueryNode> LimitRelation::GetQueryNode() {
	auto child_node = child->GetQueryNode();

	auto result = make_unique<SelectNode>();
	result->select_list.push_back(make_unique<StarExpression>());
	result->from_table = make_unique<SubqueryRef>(move(child_node), child->GetAlias());
	result->limit = make_unique<ConstantExpression>(SQLType::BIGINT, Value::BIGINT(limit));
	if (offset > 0) {
		result->offset = make_unique<ConstantExpression>(SQLType::BIGINT, Value::BIGINT(offset));
	}
	return move(result);
}

const vector<ColumnDefinition> &LimitRelation::Columns() {
	return child->Columns();
}

string LimitRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Limit " + std::to_string(limit);
	if (offset > 0) {
		str += " Offset " + std::to_string(offset);
	}
	str += "\n";
	return str + child->ToString(depth + 1);;
}

}
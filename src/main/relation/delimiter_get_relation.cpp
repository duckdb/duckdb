#include "duckdb/main/relation/delimiter_get_relation.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/joinref.hpp"

namespace duckdb {

DelimiterGetRelation::DelimiterGetRelation(vector<LogicalType> chunk_types_p)
    : Relation(left_p->context, RelationType::CROSS_PRODUCT_RELATION), chunk_types(std::move(chunk_types_p)){
	context.GetContext()->TryBindRelation(*this, this->columns);
}

unique_ptr<QueryNode> DelimiterGetRelation::GetQueryNode() {
	auto result = make_uniq<SelectNode>();
	result->select_list.push_back(make_uniq<StarExpression>());
	result->from_table = GetTableRef();
	return std::move(result);
}

unique_ptr<TableRef> DelimiterGetRelation::GetTableRef() {
	auto cross_product_ref = make_uniq<JoinRef>(ref_type);
	cross_product_ref->left = left->GetTableRef();
	cross_product_ref->right = right->GetTableRef();
	return std::move(cross_product_ref);
}

const vector<ColumnDefinition> &DelimiterGetRelation::Columns() {
	return this->columns;
}

string DelimiterGetRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth);
	str = "DelimiterGet";
	return str;
}

} // namespace duckdb

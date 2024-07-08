#include "duckdb/main/relation/delimiter_get_relation.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/delimgetref.hpp"

namespace duckdb {

DelimiterGetRelation::DelimiterGetRelation(const shared_ptr<ClientContext> &context, vector<LogicalType> chunk_types_p)
    : Relation(context, RelationType::DELIMITER_GET_RELATION), chunk_types(std::move(chunk_types_p)) {
	context->TryBindRelation(*this, this->columns);
}

unique_ptr<QueryNode> DelimiterGetRelation::GetQueryNode() {
	auto result = make_uniq<SelectNode>();
	result->select_list.push_back(make_uniq<StarExpression>());
	result->from_table = GetTableRef();
	return std::move(result);
}

unique_ptr<TableRef> DelimiterGetRelation::GetTableRef() {
	auto delim_get_ref = make_uniq<DelimGetRef>();
	return std::move(delim_get_ref);
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

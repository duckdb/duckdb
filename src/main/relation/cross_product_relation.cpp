#include "duckdb/main/relation/cross_product_relation.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/crossproductref.hpp"

namespace duckdb {

CrossProductRelation::CrossProductRelation(shared_ptr<Relation> left_p, shared_ptr<Relation> right_p)
    : Relation(left_p->context, RelationType::CROSS_PRODUCT_RELATION), left(move(left_p)), right(move(right_p)) {
	if (left->context.GetContext() != right->context.GetContext()) {
		throw Exception("Cannot combine LEFT and RIGHT relations of different connections!");
	}
	context.GetContext()->TryBindRelation(*this, this->columns);
}

unique_ptr<QueryNode> CrossProductRelation::GetQueryNode() {
	auto result = make_unique<SelectNode>();
	result->select_list.push_back(make_unique<StarExpression>());
	result->from_table = GetTableRef();
	return move(result);
}

unique_ptr<TableRef> CrossProductRelation::GetTableRef() {
	auto cross_product_ref = make_unique<CrossProductRef>();
	cross_product_ref->left = left->GetTableRef();
	cross_product_ref->right = right->GetTableRef();
	return move(cross_product_ref);
}

const vector<ColumnDefinition> &CrossProductRelation::Columns() {
	return this->columns;
}

string CrossProductRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth);
	str = "Cross Product";
	return str + "\n" + left->ToString(depth + 1) + right->ToString(depth + 1);
}

} // namespace duckdb

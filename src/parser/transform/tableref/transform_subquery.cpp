#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<TableRef> Transformer::TransformRangeSubselect(PGRangeSubselect *root) {
	Transformer subquery_transformer(this);
	auto subquery = subquery_transformer.TransformSelectNode((PGSelectStmt *)root->subquery);
	if (!subquery) {
		return nullptr;
	}
	auto alias = TransformAlias(root->alias);
	auto result = make_unique<SubqueryRef>(move(subquery), alias);
	if (root->alias->colnames) {
		for (auto node = root->alias->colnames->head; node != nullptr; node = node->next) {
			result->column_name_alias.push_back(reinterpret_cast<PGValue *>(node->data.ptr_value)->val.str);
		}
	}
	return move(result);
}

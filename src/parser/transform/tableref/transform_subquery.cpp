#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {
using namespace std;
using namespace duckdb_libpgquery;

unique_ptr<TableRef> Transformer::TransformRangeSubselect(PGRangeSubselect *root) {
	Transformer subquery_transformer(this);
	auto subquery = subquery_transformer.TransformSelectNode((PGSelectStmt *)root->subquery);
	if (!subquery) {
		return nullptr;
	}
	auto result = make_unique<SubqueryRef>(move(subquery));
	result->alias = TransformAlias(root->alias, result->column_name_alias);
	return move(result);
}

} // namespace duckdb

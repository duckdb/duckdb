#include "duckdb/common/exception.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<TableRef> Transformer::TransformPivot(duckdb_libpgquery::PGPivotExpr *root) {
	auto result = make_unique<PivotRef>();
	result->source = TransformTableRefNode(root->source);
	TransformExpressionList(*root->aggrs, result->aggregates);
	PivotColumn col;
	col.name = root->aliasname;
	col.values = TransformStringList(root->colnames);
	result->pivots.push_back(std::move(col));
	result->alias = root->alias ? root->alias->aliasname : string();
	return std::move(result);
}

} // namespace duckdb

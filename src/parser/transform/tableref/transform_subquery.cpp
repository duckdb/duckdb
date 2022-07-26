#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<TableRef> Transformer::TransformRangeSubselect(duckdb_libpgquery::PGRangeSubselect *root) {
	Transformer subquery_transformer(this);
	auto subquery = subquery_transformer.TransformSelect(root->subquery);
	if (!subquery) {
		return nullptr;
	}
	if (root->lateral) {
		throw NotImplementedException("LATERAL not implemented");
	}
	auto result = make_unique<SubqueryRef>(move(subquery));
	result->alias = TransformAlias(root->alias, result->column_name_alias);
	if (root->sample) {
		result->sample = TransformSampleOptions(root->sample);
	}
	return move(result);
}

} // namespace duckdb

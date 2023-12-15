#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<TableRef> Transformer::TransformRangeSubselect(duckdb_libpgquery::PGRangeSubselect &root) {
	Transformer subquery_transformer(*this);
	auto subquery = subquery_transformer.TransformSelect(root.subquery);
	if (!subquery) {
		return nullptr;
	}
	auto result = make_uniq<SubqueryRef>(std::move(subquery));
	result->alias = TransformAlias(root.alias, result->column_name_alias);
	if (root.sample) {
		result->sample = TransformSampleOptions(root.sample);
	}
	return std::move(result);
}

} // namespace duckdb

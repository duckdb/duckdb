#include <string>
#include <utility>

#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/parser/parsed_data/sample_options.hpp"
#include "duckdb/parser/tableref.hpp"
#include "nodes/parsenodes.hpp"

namespace duckdb {

unique_ptr<TableRef> Transformer::TransformRangeSubselect(duckdb_libpgquery::PGRangeSubselect &root) {
	Transformer subquery_transformer(*this);
	auto subquery = subquery_transformer.TransformSelectStmt(*root.subquery);
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

#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/tableref/column_data_ref.hpp"
#include "duckdb/planner/tableref/bound_column_data_ref.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"

namespace duckdb {

unique_ptr<BoundTableRef> Binder::Bind(ColumnDataRef &ref) {
	auto types = ref.collection->Types();
	auto result = make_uniq<BoundColumnDataRef>(std::move(ref.collection));
	result->bind_index = GenerateTableIndex();
	bind_context.AddGenericBinding(result->bind_index, ref.alias, ref.expected_names, types);
	return unique_ptr_cast<BoundColumnDataRef, BoundTableRef>(std::move(result));
}

} // namespace duckdb

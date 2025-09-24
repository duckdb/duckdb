#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/tableref/column_data_ref.hpp"
#include "duckdb/planner/tableref/bound_column_data_ref.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"

namespace duckdb {

unique_ptr<BoundTableRef> Binder::Bind(ColumnDataRef &ref) {
	auto types = ref.Collection()->Types();
	auto result = make_uniq<BoundColumnDataRef>(std::move(ref.Collection()));
	result->bind_index = GenerateTableIndex();
	for (idx_t i = ref.expected_names.size(); i < types.size(); i++) {
		ref.expected_names.push_back("col" + to_string(i + 1));
	}
	bind_context.AddGenericBinding(result->bind_index, ref.alias, ref.expected_names, types);
	return unique_ptr_cast<BoundColumnDataRef, BoundTableRef>(std::move(result));
}

} // namespace duckdb

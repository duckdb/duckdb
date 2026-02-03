#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/tableref/column_data_ref.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"

namespace duckdb {

BoundStatement Binder::Bind(ColumnDataRef &ref) {
	auto &collection = *ref.collection;
	auto types = collection.Types();

	BoundStatement result;
	result.names = std::move(ref.expected_names);
	for (idx_t i = result.names.size(); i < types.size(); i++) {
		result.names.push_back("col" + to_string(i + 1));
	}
	result.types = types;
	auto bind_index = GenerateTableIndex();
	bind_context.AddGenericBinding(bind_index, ref.alias, result.names, types);

	result.plan =
	    make_uniq_base<LogicalOperator, LogicalColumnDataGet>(bind_index, std::move(types), std::move(ref.collection));
	return result;
}

} // namespace duckdb

#include <string>
#include <utility>

#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/tableref/column_data_ref.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/optionally_owned_ptr.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/bind_context.hpp"
#include "duckdb/planner/bound_statement.hpp"
#include "duckdb/planner/logical_operator.hpp"

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

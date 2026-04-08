#include <string>
#include <utility>

#include "duckdb/parser/tableref/delimgetref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/table_index.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/bind_context.hpp"
#include "duckdb/planner/bound_statement.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

BoundStatement Binder::Bind(DelimGetRef &ref) {
	// Have to add bindings
	auto tbl_idx = GenerateTableIndex();
	string internal_name = "__internal_delim_get_ref_" + std::to_string(tbl_idx.index);

	BoundStatement result;
	result.types = std::move(ref.types);
	result.names = std::move(ref.internal_aliases);
	result.plan = make_uniq<LogicalDelimGet>(tbl_idx, result.types);

	bind_context.AddGenericBinding(tbl_idx, internal_name, result.names, result.types);
	return result;
}

} // namespace duckdb

#include "duckdb/parser/tableref/delimgetref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_delimgetref.hpp"

namespace duckdb {

unique_ptr<BoundTableRef> Binder::Bind(DelimGetRef &ref) {
	// Have to add bindings
	idx_t tbl_idx = GenerateTableIndex();
	string internal_name = "__internal_delim_get_ref_" + std::to_string(tbl_idx);
	bind_context.AddGenericBinding(tbl_idx, internal_name, ref.internal_aliases, ref.types);

	return make_uniq<BoundDelimGetRef>(tbl_idx, ref.types);
}

} // namespace duckdb

#include "duckdb/parser/tableref/delimgetref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_delimgetref.hpp"

namespace duckdb {

unique_ptr<BoundTableRef> Binder::Bind(DelimGetRef &ref) {
	// Have to add bindings
	idx_t tbl_idx = GenerateTableIndex();
	string fake_name = "table_" + std::to_string(tbl_idx);
	bind_context.AddGenericBinding(tbl_idx, fake_name, ref.fake_aliases, ref.types);
	return make_uniq<BoundDelimGetRef>(GenerateTableIndex());
}

} // namespace duckdb

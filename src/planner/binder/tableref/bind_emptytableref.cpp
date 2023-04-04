#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_dummytableref.hpp"

namespace duckdb {

unique_ptr<BoundTableRef> Binder::Bind(EmptyTableRef &ref) {
	return make_uniq<BoundEmptyTableRef>(GenerateTableIndex());
}

} // namespace duckdb

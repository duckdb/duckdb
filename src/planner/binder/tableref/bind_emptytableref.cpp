#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"

namespace duckdb {

BoundStatement Binder::Bind(EmptyTableRef &ref) {
	BoundStatement result;
	result.plan = make_uniq<LogicalDummyScan>(GenerateTableIndex());
	return result;
}

} // namespace duckdb

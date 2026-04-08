#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/planner/bound_statement.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class EmptyTableRef;

BoundStatement Binder::Bind(EmptyTableRef &ref) {
	BoundStatement result;
	result.plan = make_uniq<LogicalDummyScan>(GenerateTableIndex());
	return result;
}

} // namespace duckdb

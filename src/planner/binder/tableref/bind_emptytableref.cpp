#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

using namespace std;

namespace duckdb {

unique_ptr<LogicalOperator> Binder::Bind(EmptyTableRef &ref) {
	return make_unique<LogicalGet>(GenerateTableIndex());
}

}

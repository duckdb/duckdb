#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_dummytableref.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundTableRef> Binder::Bind(EmptyTableRef &ref) {
	return make_unique<BoundEmptyTableRef>(GenerateTableIndex());
}

#include "parser/tableref/dummytableref.hpp"
#include "planner/binder.hpp"
#include "planner/tableref/bound_dummytableref.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundTableRef> Binder::Bind(DummyTableRef &ref) {
	return make_unique<BoundDummyTableRef>(GenerateTableIndex());
}

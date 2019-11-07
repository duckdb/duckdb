#include "duckdb/parser/tableref/crossproductref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_crossproductref.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundTableRef> Binder::Bind(CrossProductRef &ref) {
	auto result = make_unique<BoundCrossProductRef>();
	result->left = Bind(*ref.left);
	result->right = Bind(*ref.right);
	return move(result);
}

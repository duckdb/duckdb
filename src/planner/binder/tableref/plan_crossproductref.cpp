#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/tableref/bound_crossproductref.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundCrossProductRef &expr) {
	auto cross_product = make_unique<LogicalCrossProduct>();

	auto left = CreatePlan(*expr.left);
	auto right = CreatePlan(*expr.right);

	cross_product->AddChild(move(left));
	cross_product->AddChild(move(right));

	return move(cross_product);
}

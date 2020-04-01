#include "duckdb/parser/tableref/crossproductref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"

using namespace std;

namespace duckdb {

unique_ptr<LogicalOperator> Binder::Bind(CrossProductRef &ref) {
	auto cross_product = make_unique<LogicalCrossProduct>();

	auto left = Bind(*ref.left);
	auto right = Bind(*ref.right);

	cross_product->AddChild(move(left));
	cross_product->AddChild(move(right));

	return move(cross_product);
}

}

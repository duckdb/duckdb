#include "planner/binder.hpp"
#include "parser/tableref/joinref.hpp"
#include "planner/tableref/bound_joinrefref.hpp"
#include "planner/expression_binder/where_binder.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundTableRef> Binder::Bind(JoinRef &ref) {
	auto result = make_unique<BoundJoinRef>();
	result->type = ref.type;
	result->left = Bind(*ref.left);
	result->right = Bind(*ref.right);
	
	WhereBinder binder(*this, context);
	result->condition = binder.BindAndResolveType(*ref.condition);
	return move(result);
}

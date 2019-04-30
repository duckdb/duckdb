#include "parser/tableref/joinref.hpp"
#include "planner/binder.hpp"
#include "planner/expression_binder/where_binder.hpp"
#include "planner/tableref/bound_joinref.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundTableRef> Binder::Bind(JoinRef &ref) {
	auto result = make_unique<BoundJoinRef>();
	result->type = ref.type;
	result->left = Bind(*ref.left);
	result->right = Bind(*ref.right);

	for (auto &hidden_column : ref.hidden_columns) {
		bind_context.hidden_columns.insert(hidden_column);
	}

	WhereBinder binder(*this, context);
	result->condition = binder.Bind(ref.condition);
	return move(result);
}

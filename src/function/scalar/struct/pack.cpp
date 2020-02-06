#include "duckdb/function/scalar/struct_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"

#include <set>

using namespace std;

namespace duckdb {

static void struct_pack_fun(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (StructPackBindData &)*func_expr.bind_info;

	// this should never happen if the binder below is sane
	assert(input.column_count == info.stype.child_type.size());

	for (size_t i = 0; i < input.column_count; i++) {
		// same holds for this
		assert(input.data[i].type == GetInternalType(info.stype.child_type[i].second));
		auto new_child = make_unique<Vector>();
		new_child->Reference(input.data[i]);
		result.AddChild(move(new_child), info.stype.child_type[i].first);
	}
	result.count = input.data[0].count;
}

static unique_ptr<FunctionData> struct_pack_bind(BoundFunctionExpression &expr, ClientContext &context) {
	SQLType stype (SQLTypeId::STRUCT);
	set<string> name_collision_set;

	// collect names and deconflict, construct return type
	assert(expr.arguments.size() == expr.children.size());
	for (index_t i = 0; i < expr.children.size(); i++) {
		auto &child = expr.children[i];
		if (name_collision_set.find(child->alias) != name_collision_set.end()) {
			throw Exception("Duplicate struct entry name");
		}
		stype.child_type.push_back(make_pair(child->alias, expr.arguments[i]));
	}

	// this is more for completeness reasons
	expr.function.return_type = stype;
	return make_unique<StructPackBindData>(stype);
}


void StructPackFun::RegisterFunction(BuiltinFunctions &set) {
	// the arguments and return types are actually set in the binder function
	ScalarFunction fun("struct_pack", {}, SQLType::STRUCT, struct_pack_fun, false, struct_pack_bind);
	fun.varargs = SQLType::ANY;
	set.AddFunction(fun);
}

} // namespace duckdb

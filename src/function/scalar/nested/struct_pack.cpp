#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

#include <set>

using namespace std;

namespace duckdb {

static void struct_pack_fun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (VariableReturnBindData &)*func_expr.bind_info;

	// this should never happen if the binder below is sane
	assert(args.column_count() == info.stype.child_type.size());

	bool all_const = true;
	for (size_t i = 0; i < args.column_count(); i++) {
		if (args.data[i].vector_type != VectorType::CONSTANT_VECTOR) {
			all_const = false;
		}
		// same holds for this
		assert(args.data[i].type == GetInternalType(info.stype.child_type[i].second));
		auto new_child = make_unique<Vector>();
		new_child->Reference(args.data[i]);
		StructVector::AddEntry(result, info.stype.child_type[i].first, move(new_child));
	}
	result.vector_type = all_const ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR;

	result.Verify(args.size());
}

static unique_ptr<FunctionData> struct_pack_bind(BoundFunctionExpression &expr, ClientContext &context) {
	SQLType stype(SQLTypeId::STRUCT);
	set<string> name_collision_set;

	// collect names and deconflict, construct return type
	assert(expr.arguments.size() == expr.children.size());

	if (expr.arguments.size() == 0) {
		throw Exception("Can't pack nothing into a struct");
	}
	for (idx_t i = 0; i < expr.children.size(); i++) {
		auto &child = expr.children[i];
		if (child->alias.size() == 0) {
			throw Exception("Need named argument for struct pack, e.g. STRUCT_PACK(a := b)");
		}
		if (name_collision_set.find(child->alias) != name_collision_set.end()) {
			throw Exception("Duplicate struct entry name");
		}
		name_collision_set.insert(child->alias);
		stype.child_type.push_back(make_pair(child->alias, expr.arguments[i]));
	}

	// this is more for completeness reasons
	expr.sql_return_type = stype;
	return make_unique<VariableReturnBindData>(stype);
}

void StructPackFun::RegisterFunction(BuiltinFunctions &set) {
	// the arguments and return types are actually set in the binder function
	ScalarFunction fun("struct_pack", {}, SQLType::STRUCT, struct_pack_fun, false, struct_pack_bind);
	fun.varargs = SQLType::ANY;
	set.AddFunction(fun);
}

} // namespace duckdb

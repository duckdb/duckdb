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
	assert(args.column_count() == info.stype.child_types().size());

	bool all_const = true;
	for (size_t i = 0; i < args.column_count(); i++) {
		if (args.data[i].vector_type != VectorType::CONSTANT_VECTOR) {
			all_const = false;
		}
		// same holds for this
		assert(args.data[i].type == info.stype.child_types()[i].second);
		auto new_child = make_unique<Vector>();
		new_child->Reference(args.data[i]);
		StructVector::AddEntry(result, info.stype.child_types()[i].first, move(new_child));
	}
	result.vector_type = all_const ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR;

	result.Verify(args.size());
}

static unique_ptr<FunctionData> struct_pack_bind(ClientContext &context, ScalarFunction &bound_function, vector<unique_ptr<Expression>> &arguments) {
	set<string> name_collision_set;

	// collect names and deconflict, construct return type
	if (arguments.size() == 0) {
		throw Exception("Can't pack nothing into a struct");
	}
	child_list_t<LogicalType> struct_children;
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto &child = arguments[i];
		if (child->alias.size() == 0) {
			throw Exception("Need named argument for struct pack, e.g. STRUCT_PACK(a := b)");
		}
		if (name_collision_set.find(child->alias) != name_collision_set.end()) {
			throw Exception("Duplicate struct entry name");
		}
		name_collision_set.insert(child->alias);
		struct_children.push_back(make_pair(child->alias, arguments[i]->return_type));
	}

	// this is more for completeness reasons
	bound_function.return_type = LogicalType(LogicalTypeId::STRUCT, move(struct_children));
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

void StructPackFun::RegisterFunction(BuiltinFunctions &set) {
	// the arguments and return types are actually set in the binder function
	ScalarFunction fun("struct_pack", {}, LogicalType::STRUCT, struct_pack_fun, false, struct_pack_bind);
	fun.varargs = LogicalType::ANY;
	set.AddFunction(fun);
}

} // namespace duckdb

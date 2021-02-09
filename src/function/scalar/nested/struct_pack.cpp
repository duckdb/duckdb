#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

static void StructPackFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (VariableReturnBindData &)*func_expr.bind_info;

	// this should never happen if the binder below is sane
	D_ASSERT(args.ColumnCount() == info.stype.child_types().size());

	bool all_const = true;
	for (size_t i = 0; i < args.ColumnCount(); i++) {
		if (args.data[i].buffer->vector_type != VectorType::CONSTANT_VECTOR) {
			all_const = false;
		}
		// same holds for this
		D_ASSERT(args.data[i].buffer->type == info.stype.child_types()[i].second);
		auto new_child = make_unique<Vector>(info.stype.child_types()[i].second);
		new_child->Reference(args.data[i]);
		StructVector::AddEntry(result, info.stype.child_types()[i].first, move(new_child));
	}
	result.buffer->vector_type = all_const ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR;

	result.Verify(args.size());
}

static unique_ptr<FunctionData> StructPackBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	unordered_set<string> name_collision_set;

	// collect names and deconflict, construct return type
	if (arguments.empty()) {
		throw Exception("Can't pack nothing into a struct");
	}
	child_list_t<LogicalType> struct_children;
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto &child = arguments[i];
		if (child->alias.empty() && bound_function.name == "struct_pack") {
			throw Exception("Need named argument for struct pack, e.g. STRUCT_PACK(a := b)");
		}
		if (child->alias.empty() && bound_function.name == "row") {
			child->alias = "v" + std::to_string(i + 1);
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
	ScalarFunction fun("struct_pack", {}, LogicalType::STRUCT, StructPackFunction, false, StructPackBind);
	fun.varargs = LogicalType::ANY;
	set.AddFunction(fun);
	fun.name = "row";
	set.AddFunction(fun);
}

} // namespace duckdb

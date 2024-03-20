#include "duckdb/core_functions/scalar/struct_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

static void StructPackFunction(DataChunk &args, ExpressionState &state, Vector &result) {
#ifdef DEBUG
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<VariableReturnBindData>();
	// this should never happen if the binder below is sane
	D_ASSERT(args.ColumnCount() == StructType::GetChildTypes(info.stype).size());
#endif
	bool all_const = true;
	auto &child_entries = StructVector::GetEntries(result);
	for (size_t i = 0; i < args.ColumnCount(); i++) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			all_const = false;
		}
		// same holds for this
		child_entries[i]->Reference(args.data[i]);
	}
	result.SetVectorType(all_const ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);

	result.Verify(args.size());
}

template <bool IS_STRUCT_PACK>
static unique_ptr<FunctionData> StructPackBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	case_insensitive_set_t name_collision_set;

	// collect names and deconflict, construct return type
	if (arguments.empty()) {
		throw InvalidInputException("Can't pack nothing into a struct");
	}
	child_list_t<LogicalType> struct_children;
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto &child = arguments[i];
		string alias;
		if (IS_STRUCT_PACK) {
			if (child->alias.empty()) {
				throw BinderException("Need named argument for struct pack, e.g. STRUCT_PACK(a := b)");
			}
			alias = child->alias;
			if (name_collision_set.find(alias) != name_collision_set.end()) {
				throw BinderException("Duplicate struct entry name \"%s\"", alias);
			}
			name_collision_set.insert(alias);
		}
		struct_children.push_back(make_pair(alias, arguments[i]->return_type));
	}

	// this is more for completeness reasons
	bound_function.return_type = LogicalType::STRUCT(struct_children);
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

unique_ptr<BaseStatistics> StructPackStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	auto struct_stats = StructStats::CreateUnknown(expr.return_type);
	for (idx_t i = 0; i < child_stats.size(); i++) {
		StructStats::SetChildStats(struct_stats, i, child_stats[i]);
	}
	return struct_stats.ToUnique();
}

template <bool IS_STRUCT_PACK>
ScalarFunction GetStructPackFunction() {
	ScalarFunction fun(IS_STRUCT_PACK ? "struct_pack" : "row", {}, LogicalTypeId::STRUCT, StructPackFunction,
	                   StructPackBind<IS_STRUCT_PACK>, nullptr, StructPackStats);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	fun.serialize = VariableReturnBindData::Serialize;
	fun.deserialize = VariableReturnBindData::Deserialize;
	return fun;
}

ScalarFunction StructPackFun::GetFunction() {
	return GetStructPackFunction<true>();
}

ScalarFunction RowFun::GetFunction() {
	return GetStructPackFunction<false>();
}

} // namespace duckdb

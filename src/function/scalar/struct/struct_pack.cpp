#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar/struct_functions.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/planner/expression/bound_argument_pack.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"

namespace duckdb {

// row(*args) collects its arguments positionally into an unnamed TUPLE, struct_pack(**fields) collects them by name
// into a STRUCT - either way the pack is the function's only argument.

static void StructPackFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = ArgumentPack::GetInput(args.data[0]);
#ifdef DEBUG
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.BindInfo()->Cast<VariableReturnBindData>();
	// this should never happen if the binder below is sane
	D_ASSERT(input.size() == StructType::GetChildTypes(info.stype).size());
#endif
	if (input.empty()) {
		// empty struct: no children to reference, the value is a single non-null constant
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, false);
		return;
	}
	bool all_const = true;
	auto &child_entries = StructVector::GetEntries(result);
	idx_t children_size = 0;
	for (idx_t i = 0; i < input.size(); i++) {
		if (input[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			all_const = false;
		}
		// same holds for this
		child_entries[i].Reference(input[i]);
		children_size = MaxValue<idx_t>(children_size, child_entries[i].size());
	}
	// set only the struct buffer's type/size - do not propagate to children
	// since children reference external vectors (args) that may have incompatible buffer types.
	// match the parent size to the (already-set) child vector size, not to the chunk cardinality - those can
	// differ when the caller is collapsing all-constant inputs to a single argument row.
	result.BufferMutable().SetVectorTypeOnly(all_const ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);
	result.BufferMutable().SetVectorSizeOnly(children_size);
	result.Verify();
}

template <bool IS_STRUCT_PACK>
static unique_ptr<FunctionData> StructPackBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &pack_type = input.GetArguments()[0]->GetReturnType();

	// the pack already collected the arguments under the names they were passed with (the binder rejects duplicate
	// names), so the struct is just the pack without its alias
	// note: zero arguments is allowed, producing an empty struct
	child_list_t<LogicalType> struct_children = ArgumentPack::GetTypes(pack_type);

	// this is more for completeness reasons
	// row() produces an unnamed TUPLE, struct_pack() produces a named STRUCT
	if (IS_STRUCT_PACK) {
		bound_function.SetReturnType(LogicalType::STRUCT(std::move(struct_children)));
	} else {
		bound_function.SetReturnType(LogicalType::TUPLE(std::move(struct_children)));
	}
	return make_uniq<VariableReturnBindData>(bound_function.GetReturnType());
}

template <bool IS_STRUCT_PACK>
static unique_ptr<BaseStatistics> StructPackStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &pack_stats = input.child_stats[0];
	auto &expr = input.expr;
	if (pack_stats.GetStatsType() != StatisticsType::STRUCT_STATS) {
		return nullptr;
	}
	auto struct_stats = StructStats::CreateUnknown(expr.GetReturnType());
	for (idx_t i = 0; i < StructType::GetChildCount(expr.GetReturnType()); i++) {
		StructStats::SetChildStats(struct_stats, i, StructStats::GetChildStats(pack_stats, i));
	}
	return struct_stats.ToUnique();
}

template <bool IS_STRUCT_PACK>
static ScalarFunction GetStructPackFunction() {
	FunctionSignature signature;
	if (IS_STRUCT_PACK) {
		signature.AddVarKeywordParameter("fields", LogicalType::ANY);
	} else {
		signature.AddVarPositionalParameter("args", LogicalType::ANY);
	}
	signature.SetReturnType(IS_STRUCT_PACK ? LogicalTypeId::STRUCT : LogicalTypeId::TUPLE);

	ScalarFunction fun(IS_STRUCT_PACK ? "struct_pack" : "row", std::move(signature));
	fun.SetFunctionCallback(StructPackFunction);
	fun.SetBindCallback(StructPackBind<IS_STRUCT_PACK>);
	fun.SetStatisticsCallback(StructPackStats<IS_STRUCT_PACK>);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	fun.SetSerializeCallback(VariableReturnBindData::Serialize);
	fun.SetDeserializeCallback(VariableReturnBindData::Deserialize);
	return fun;
}

ScalarFunction StructPackFun::GetFunction() {
	return GetStructPackFunction<true>();
}

ScalarFunction RowFun::GetFunction() {
	return GetStructPackFunction<false>();
}

} // namespace duckdb

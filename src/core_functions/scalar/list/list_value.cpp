#include "duckdb/core_functions/scalar/list_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

static void ListValueFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	auto &child_type = ListType::GetChildType(result.GetType());

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::FLAT_VECTOR);
		}
	}

	auto result_data = FlatVector::GetData<list_entry_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i].offset = ListVector::GetListSize(result);
		for (idx_t col_idx = 0; col_idx < args.ColumnCount(); col_idx++) {
			auto val = args.GetValue(col_idx, i).DefaultCastAs(child_type);
			ListVector::PushBack(result, val);
		}
		result_data[i].length = args.ColumnCount();
	}
	result.Verify(args.size());
}

static unique_ptr<FunctionData> ListValueBind(ClientContext &context, ScalarFunction &bound_function,
                                              vector<unique_ptr<Expression>> &arguments) {
	// collect names and deconflict, construct return type
	LogicalType child_type = arguments.empty() ? LogicalType::SQLNULL : arguments[0]->return_type;
	for (idx_t i = 1; i < arguments.size(); i++) {
		child_type = LogicalType::MaxLogicalType(child_type, arguments[i]->return_type);
	}

	// this is more for completeness reasons
	bound_function.varargs = child_type;
	bound_function.return_type = LogicalType::LIST(child_type);
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

unique_ptr<BaseStatistics> ListValueStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	auto list_stats = ListStats::CreateEmpty(expr.return_type);
	auto &list_child_stats = ListStats::GetChildStats(list_stats);
	for (idx_t i = 0; i < child_stats.size(); i++) {
		list_child_stats.Merge(child_stats[i]);
	}
	return list_stats.ToUnique();
}

ScalarFunction ListValueFun::GetFunction() {
	// the arguments and return types are actually set in the binder function
	ScalarFunction fun("list_value", {}, LogicalTypeId::LIST, ListValueFunction, ListValueBind, nullptr,
	                   ListValueStats);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

} // namespace duckdb

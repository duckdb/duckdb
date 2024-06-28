#include "duckdb/core_functions/scalar/list_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/parser/query_error_context.hpp"

namespace duckdb {

struct ListValueAssign {
	template <class T>
	static T Assign(const T &input, Vector &result) {
		return input;
	}
};

struct ListValueStringAssign {
	template <class T>
	static T Assign(const T &input, Vector &result) {
		return StringVector::AddStringOrBlob(result, input);
	}
};

template <class T, class OP = ListValueAssign>
static void TemplatedListValueFunction(DataChunk &args, Vector &result) {
	idx_t list_size = args.ColumnCount();
	ListVector::Reserve(result, args.size() * list_size);
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	auto &list_child = ListVector::GetEntry(result);
	auto child_data = FlatVector::GetData<T>(list_child);
	auto &child_validity = FlatVector::Validity(list_child);

	auto unified_format = args.ToUnifiedFormat();
	for (idx_t r = 0; r < args.size(); r++) {
		for (idx_t c = 0; c < list_size; c++) {
			auto input_idx = unified_format[c].sel->get_index(r);
			auto result_idx = r * list_size + c;
			auto input_data = UnifiedVectorFormat::GetData<T>(unified_format[c]);
			if (unified_format[c].validity.RowIsValid(input_idx)) {
				child_data[result_idx] = OP::template Assign<T>(input_data[input_idx], list_child);
			} else {
				child_validity.SetInvalid(result_idx);
			}
		}
		result_data[r].offset = r * list_size;
		result_data[r].length = list_size;
	}
	ListVector::SetListSize(result, args.size() * list_size);
}

static void TemplatedListValueFunctionFallback(DataChunk &args, Vector &result) {
	auto &child_type = ListType::GetChildType(result.GetType());
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i].offset = ListVector::GetListSize(result);
		for (idx_t col_idx = 0; col_idx < args.ColumnCount(); col_idx++) {
			auto val = args.GetValue(col_idx, i).DefaultCastAs(child_type);
			ListVector::PushBack(result, val);
		}
		result_data[i].length = args.ColumnCount();
	}
}

static void ListValueFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::FLAT_VECTOR);
		}
	}
	if (args.ColumnCount() == 0) {
		// no columns - early out - result is a constant empty list
		auto result_data = FlatVector::GetData<list_entry_t>(result);
		result_data[0].length = 0;
		result_data[0].offset = 0;
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		return;
	}
	auto &result_type = ListVector::GetEntry(result).GetType();
	switch (result_type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedListValueFunction<int8_t>(args, result);
		break;
	case PhysicalType::INT16:
		TemplatedListValueFunction<int16_t>(args, result);
		break;
	case PhysicalType::INT32:
		TemplatedListValueFunction<int32_t>(args, result);
		break;
	case PhysicalType::INT64:
		TemplatedListValueFunction<int64_t>(args, result);
		break;
	case PhysicalType::UINT8:
		TemplatedListValueFunction<uint8_t>(args, result);
		break;
	case PhysicalType::UINT16:
		TemplatedListValueFunction<uint16_t>(args, result);
		break;
	case PhysicalType::UINT32:
		TemplatedListValueFunction<uint32_t>(args, result);
		break;
	case PhysicalType::UINT64:
		TemplatedListValueFunction<uint64_t>(args, result);
		break;
	case PhysicalType::INT128:
		TemplatedListValueFunction<hugeint_t>(args, result);
		break;
	case PhysicalType::UINT128:
		TemplatedListValueFunction<uhugeint_t>(args, result);
		break;
	case PhysicalType::FLOAT:
		TemplatedListValueFunction<float>(args, result);
		break;
	case PhysicalType::DOUBLE:
		TemplatedListValueFunction<double>(args, result);
		break;
	case PhysicalType::INTERVAL:
		TemplatedListValueFunction<interval_t>(args, result);
		break;
	case PhysicalType::VARCHAR:
		TemplatedListValueFunction<string_t, ListValueStringAssign>(args, result);
		break;
	default: {
		TemplatedListValueFunctionFallback(args, result);
		break;
	}
	}
}

template <bool IS_UNPIVOT = false>
static unique_ptr<FunctionData> ListValueBind(ClientContext &context, ScalarFunction &bound_function,
                                              vector<unique_ptr<Expression>> &arguments) {
	// collect names and deconflict, construct return type
	LogicalType child_type =
	    arguments.empty() ? LogicalType::SQLNULL : ExpressionBinder::GetExpressionReturnType(*arguments[0]);
	for (idx_t i = 1; i < arguments.size(); i++) {
		auto arg_type = ExpressionBinder::GetExpressionReturnType(*arguments[i]);
		if (!LogicalType::TryGetMaxLogicalType(context, child_type, arg_type, child_type)) {
			if (IS_UNPIVOT) {
				string list_arguments = "Full list: ";
				idx_t error_index = list_arguments.size();
				for (idx_t k = 0; k < arguments.size(); k++) {
					if (k > 0) {
						list_arguments += ", ";
					}
					if (k == i) {
						error_index = list_arguments.size();
					}
					list_arguments += arguments[k]->ToString() + " " + arguments[k]->return_type.ToString();
				}
				auto error =
				    StringUtil::Format("Cannot unpivot columns of types %s and %s - an explicit cast is required",
				                       child_type.ToString(), arg_type.ToString());
				throw BinderException(arguments[i]->query_location,
				                      QueryErrorContext::Format(list_arguments, error, error_index, false));
			} else {
				throw BinderException(arguments[i]->query_location,
				                      "Cannot create a list of types %s and %s - an explicit cast is required",
				                      child_type.ToString(), arg_type.ToString());
			}
		}
	}
	child_type = LogicalType::NormalizeType(child_type);

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

ScalarFunction UnpivotListFun::GetFunction() {
	auto fun = ListValueFun::GetFunction();
	fun.name = "unpivot_list";
	fun.bind = ListValueBind<true>;
	return fun;
}

} // namespace duckdb

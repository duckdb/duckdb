#include "duckdb/common/operator/comparison_operators.hpp"
#include "core_functions/scalar/generic_functions.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/enums/order_type.hpp"
#include "duckdb/common/enums/vector_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
class ClientContext;
struct hugeint_t;

namespace {
struct LeastOp {
	using OP = LessThan;

	static OrderByNullType NullOrdering() {
		return OrderByNullType::NULLS_LAST;
	}
};

struct GreaterOp {
	using OP = GreaterThan;

	static OrderByNullType NullOrdering() {
		return OrderByNullType::NULLS_FIRST;
	}
};

template <class OP>
struct LeastOperator {
	template <class T>
	static T Operation(T left, T right) {
		return OP::Operation(left, right) ? left : right;
	}
};

struct LeastGreatestSortKeyState : public FunctionLocalState {
	explicit LeastGreatestSortKeyState(idx_t column_count, OrderByNullType null_ordering)
	    : intermediate(LogicalType::BLOB), modifiers(OrderType::ASCENDING, null_ordering) {
		vector<LogicalType> types;
		// initialize sort key chunk
		for (idx_t i = 0; i < column_count; i++) {
			types.push_back(LogicalType::BLOB);
		}
		sort_keys.Initialize(Allocator::DefaultAllocator(), types);
	}

	DataChunk sort_keys;
	Vector intermediate;
	OrderModifiers modifiers;
};

template <class OP>
unique_ptr<FunctionLocalState> LeastGreatestSortKeyInit(ExpressionState &state, const BoundFunctionExpression &expr,
                                                        FunctionData *bind_data) {
	return make_uniq<LeastGreatestSortKeyState>(expr.children.size(), OP::NullOrdering());
}

template <bool STRING>
struct StandardLeastGreatest {
	static constexpr bool IS_STRING = STRING;

	static DataChunk &Prepare(DataChunk &args, ExpressionState &) {
		return args;
	}

	static Vector &TargetVector(Vector &result, ExpressionState &) {
		return result;
	}

	static void FinalizeResult(idx_t rows, bool result_has_value[], Vector &result, ExpressionState &) {
		auto &result_mask = FlatVector::ValidityMutable(result);
		for (idx_t i = 0; i < rows; i++) {
			if (!result_has_value[i]) {
				result_mask.SetInvalid(i);
			}
		}
	}
};

struct SortKeyLeastGreatest {
	static constexpr bool IS_STRING = false;

	static DataChunk &Prepare(DataChunk &args, ExpressionState &state) {
		auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<LeastGreatestSortKeyState>();
		lstate.sort_keys.Reset();
		for (idx_t c_idx = 0; c_idx < args.ColumnCount(); c_idx++) {
			CreateSortKeyHelpers::CreateSortKey(args.data[c_idx], args.size(), lstate.modifiers,
			                                    lstate.sort_keys.data[c_idx]);
		}
		lstate.sort_keys.SetCardinality(args.size());
		return lstate.sort_keys;
	}

	static Vector &TargetVector(Vector &result, ExpressionState &state) {
		auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<LeastGreatestSortKeyState>();
		return lstate.intermediate;
	}

	static void FinalizeResult(idx_t rows, bool result_has_value[], Vector &result, ExpressionState &state) {
		auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<LeastGreatestSortKeyState>();
		auto result_keys = FlatVector::GetData<string_t>(lstate.intermediate);
		auto &result_mask = FlatVector::ValidityMutable(result);
		for (idx_t i = 0; i < rows; i++) {
			if (!result_has_value[i]) {
				result_mask.SetInvalid(i);
			} else {
				CreateSortKeyHelpers::DecodeSortKey(result_keys[i], result, i, lstate.modifiers);
			}
		}
	}
};

template <class T, class OP, class BASE_OP = StandardLeastGreatest<false>>
void LeastGreatestFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	if (args.ColumnCount() == 1) {
		// single input: nop
		result.Reference(args.data[0]);
		return;
	}
	auto &input = BASE_OP::Prepare(args, state);
	auto &result_vector = BASE_OP::TargetVector(result, state);

	auto result_type = VectorType::CONSTANT_VECTOR;
	for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
		if (args.data[col_idx].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			// non-constant input: result is not a constant vector
			result_type = VectorType::FLAT_VECTOR;
		}
		if (BASE_OP::IS_STRING) {
			// for string vectors we add a reference to the heap of the children
			StringVector::AddHeapReference(result_vector, input.data[col_idx]);
		}
	}

	auto result_data = FlatVector::ScatterWriter<T>(result_vector);
	bool result_has_value[STANDARD_VECTOR_SIZE] {false};
	// perform the operation column-by-column
	for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
		if (input.data[col_idx].GetVectorType() == VectorType::CONSTANT_VECTOR &&
		    ConstantVector::IsNull(input.data[col_idx])) {
			// ignore null vector
			continue;
		}

		auto entries = input.data[col_idx].template Values<T>(input.size());

		if (entries.CanHaveNull()) {
			// potential new null entries: have to check the null mask
			for (idx_t i = 0; i < input.size(); i++) {
				auto entry = entries[i];
				if (entry.IsValid()) {
					// not a null entry: perform the operation and add to new set
					auto ivalue = entry.GetValue();
					if (!result_has_value[i] || OP::template Operation<T>(ivalue, result_data[i])) {
						result_has_value[i] = true;
						result_data[i] = ivalue;
					}
				}
			}
		} else {
			// no new null entries: only need to perform the operation
			for (idx_t i = 0; i < input.size(); i++) {
				auto ivalue = entries.GetValueUnsafe(i);
				if (!result_has_value[i] || OP::template Operation<T>(ivalue, result_data[i])) {
					result_has_value[i] = true;
					result_data[i] = ivalue;
				}
			}
		}
	}
	BASE_OP::FinalizeResult(input.size(), result_has_value, result, state);
	result.SetVectorType(result_type);
}

template <class LEAST_GREATER_OP>
unique_ptr<FunctionData> BindLeastGreatest(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	LogicalType child_type = ExpressionBinder::GetExpressionReturnType(*arguments[0]);
	for (idx_t i = 1; i < arguments.size(); i++) {
		auto arg_type = ExpressionBinder::GetExpressionReturnType(*arguments[i]);
		if (!LogicalType::TryGetMaxLogicalType(context, child_type, arg_type, child_type)) {
			throw BinderException(arguments[i]->GetQueryLocation(),
			                      "Cannot combine types of %s and %s - an explicit cast is required",
			                      child_type.ToString(), arg_type.ToString());
		}
	}
	switch (child_type.id()) {
	case LogicalTypeId::UNKNOWN:
		throw ParameterNotResolvedException();
	case LogicalTypeId::INTEGER_LITERAL:
		child_type = IntegerLiteral::GetType(child_type);
		break;
	case LogicalTypeId::STRING_LITERAL:
		child_type = LogicalType::VARCHAR;
		break;
	default:
		break;
	}
	using OP = typename LEAST_GREATER_OP::OP;
	switch (child_type.InternalType()) {
#ifndef DUCKDB_SMALLER_BINARY
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		bound_function.SetFunctionCallback(LeastGreatestFunction<int8_t, OP>);
		break;
	case PhysicalType::INT16:
		bound_function.SetFunctionCallback(LeastGreatestFunction<int16_t, OP>);
		break;
	case PhysicalType::INT32:
		bound_function.SetFunctionCallback(LeastGreatestFunction<int32_t, OP>);
		break;
	case PhysicalType::INT64:
		bound_function.SetFunctionCallback(LeastGreatestFunction<int64_t, OP>);
		break;
	case PhysicalType::INT128:
		bound_function.SetFunctionCallback(LeastGreatestFunction<hugeint_t, OP>);
		break;
	case PhysicalType::DOUBLE:
		bound_function.SetFunctionCallback(LeastGreatestFunction<double, OP>);
		break;
	case PhysicalType::VARCHAR:
		bound_function.SetFunctionCallback(LeastGreatestFunction<string_t, OP, StandardLeastGreatest<true>>);
		break;
#endif
	default:
		// fallback with sort keys
		bound_function.SetFunctionCallback(LeastGreatestFunction<string_t, OP, SortKeyLeastGreatest>);
		bound_function.SetInitStateCallback(LeastGreatestSortKeyInit<LEAST_GREATER_OP>);
		break;
	}
	bound_function.GetArguments()[0] = child_type;
	bound_function.SetVarArgs(child_type);
	bound_function.SetReturnType(child_type);
	return nullptr;
}

template <class OP>
ScalarFunction GetLeastGreatestFunction() {
	return ScalarFunction({LogicalType::ANY}, LogicalType::ANY, nullptr, BindLeastGreatest<OP>, nullptr, nullptr,
	                      LogicalType::ANY, FunctionStability::CONSISTENT, FunctionNullHandling::SPECIAL_HANDLING);
}

template <class OP>
ScalarFunctionSet GetLeastGreatestFunctions() {
	ScalarFunctionSet fun_set;
	fun_set.AddFunction(GetLeastGreatestFunction<OP>());
	return fun_set;
}

} // namespace

ScalarFunctionSet LeastFun::GetFunctions() {
	return GetLeastGreatestFunctions<LeastOp>();
}

ScalarFunctionSet GreatestFun::GetFunctions() {
	return GetLeastGreatestFunctions<GreaterOp>();
}

} // namespace duckdb

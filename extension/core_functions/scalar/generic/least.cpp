#include "duckdb/common/operator/comparison_operators.hpp"
#include "core_functions/scalar/generic_functions.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

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
		auto &result_mask = FlatVector::Validity(result);
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
		auto &result_mask = FlatVector::Validity(result);
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

	auto result_data = FlatVector::GetData<T>(result_vector);
	bool result_has_value[STANDARD_VECTOR_SIZE] {false};
	// perform the operation column-by-column
	for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
		if (input.data[col_idx].GetVectorType() == VectorType::CONSTANT_VECTOR &&
		    ConstantVector::IsNull(input.data[col_idx])) {
			// ignore null vector
			continue;
		}

		UnifiedVectorFormat vdata;
		input.data[col_idx].ToUnifiedFormat(input.size(), vdata);

		auto input_data = UnifiedVectorFormat::GetData<T>(vdata);
		if (!vdata.validity.AllValid()) {
			// potential new null entries: have to check the null mask
			for (idx_t i = 0; i < input.size(); i++) {
				auto vindex = vdata.sel->get_index(i);
				if (vdata.validity.RowIsValid(vindex)) {
					// not a null entry: perform the operation and add to new set
					auto ivalue = input_data[vindex];
					if (!result_has_value[i] || OP::template Operation<T>(ivalue, result_data[i])) {
						result_has_value[i] = true;
						result_data[i] = ivalue;
					}
				}
			}
		} else {
			// no new null entries: only need to perform the operation
			for (idx_t i = 0; i < input.size(); i++) {
				auto vindex = vdata.sel->get_index(i);

				auto ivalue = input_data[vindex];
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
unique_ptr<FunctionData> BindLeastGreatest(ClientContext &context, ScalarFunction &bound_function,
                                           vector<unique_ptr<Expression>> &arguments) {
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
		bound_function.function = LeastGreatestFunction<int8_t, OP>;
		break;
	case PhysicalType::INT16:
		bound_function.function = LeastGreatestFunction<int16_t, OP>;
		break;
	case PhysicalType::INT32:
		bound_function.function = LeastGreatestFunction<int32_t, OP>;
		break;
	case PhysicalType::INT64:
		bound_function.function = LeastGreatestFunction<int64_t, OP>;
		break;
	case PhysicalType::INT128:
		bound_function.function = LeastGreatestFunction<hugeint_t, OP>;
		break;
	case PhysicalType::DOUBLE:
		bound_function.function = LeastGreatestFunction<double, OP>;
		break;
	case PhysicalType::VARCHAR:
		bound_function.function = LeastGreatestFunction<string_t, OP, StandardLeastGreatest<true>>;
		break;
#endif
	default:
		// fallback with sort keys
		bound_function.function = LeastGreatestFunction<string_t, OP, SortKeyLeastGreatest>;
		bound_function.init_local_state = LeastGreatestSortKeyInit<LEAST_GREATER_OP>;
		break;
	}
	bound_function.arguments[0] = child_type;
	bound_function.varargs = child_type;
	bound_function.SetReturnType(child_type);
	return nullptr;
}

template <class OP>
ScalarFunction GetLeastGreatestFunction() {
	return ScalarFunction({LogicalType::ANY}, LogicalType::ANY, nullptr, BindLeastGreatest<OP>, nullptr, nullptr,
	                      nullptr, LogicalType::ANY, FunctionStability::CONSISTENT,
	                      FunctionNullHandling::SPECIAL_HANDLING);
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

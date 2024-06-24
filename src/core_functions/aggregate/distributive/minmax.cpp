#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/core_functions/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/core_functions/aggregate/sort_key_helpers.hpp"

namespace duckdb {

template <class T>
struct MinMaxState {
	T value;
	bool isset;
};

template <class OP>
static AggregateFunction GetUnaryAggregate(LogicalType type) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		return AggregateFunction::UnaryAggregate<MinMaxState<int8_t>, int8_t, int8_t, OP>(type, type);
	case PhysicalType::INT8:
		return AggregateFunction::UnaryAggregate<MinMaxState<int8_t>, int8_t, int8_t, OP>(type, type);
	case PhysicalType::INT16:
		return AggregateFunction::UnaryAggregate<MinMaxState<int16_t>, int16_t, int16_t, OP>(type, type);
	case PhysicalType::INT32:
		return AggregateFunction::UnaryAggregate<MinMaxState<int32_t>, int32_t, int32_t, OP>(type, type);
	case PhysicalType::INT64:
		return AggregateFunction::UnaryAggregate<MinMaxState<int64_t>, int64_t, int64_t, OP>(type, type);
	case PhysicalType::UINT8:
		return AggregateFunction::UnaryAggregate<MinMaxState<uint8_t>, uint8_t, uint8_t, OP>(type, type);
	case PhysicalType::UINT16:
		return AggregateFunction::UnaryAggregate<MinMaxState<uint16_t>, uint16_t, uint16_t, OP>(type, type);
	case PhysicalType::UINT32:
		return AggregateFunction::UnaryAggregate<MinMaxState<uint32_t>, uint32_t, uint32_t, OP>(type, type);
	case PhysicalType::UINT64:
		return AggregateFunction::UnaryAggregate<MinMaxState<uint64_t>, uint64_t, uint64_t, OP>(type, type);
	case PhysicalType::INT128:
		return AggregateFunction::UnaryAggregate<MinMaxState<hugeint_t>, hugeint_t, hugeint_t, OP>(type, type);
	case PhysicalType::UINT128:
		return AggregateFunction::UnaryAggregate<MinMaxState<uhugeint_t>, uhugeint_t, uhugeint_t, OP>(type, type);
	case PhysicalType::FLOAT:
		return AggregateFunction::UnaryAggregate<MinMaxState<float>, float, float, OP>(type, type);
	case PhysicalType::DOUBLE:
		return AggregateFunction::UnaryAggregate<MinMaxState<double>, double, double, OP>(type, type);
	case PhysicalType::INTERVAL:
		return AggregateFunction::UnaryAggregate<MinMaxState<interval_t>, interval_t, interval_t, OP>(type, type);
	default:
		throw InternalException("Unimplemented type for min/max aggregate");
	}
}

struct MinMaxBase {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.isset = false;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		if (!state.isset) {
			OP::template Assign<INPUT_TYPE, STATE>(state, input, unary_input.input);
			state.isset = true;
		} else {
			OP::template Execute<INPUT_TYPE, STATE>(state, input, unary_input.input);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		if (!state.isset) {
			OP::template Assign<INPUT_TYPE, STATE>(state, input, unary_input.input);
			state.isset = true;
		} else {
			OP::template Execute<INPUT_TYPE, STATE>(state, input, unary_input.input);
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

struct NumericMinMaxBase : public MinMaxBase {
	template <class INPUT_TYPE, class STATE>
	static void Assign(STATE &state, INPUT_TYPE input, AggregateInputData &) {
		state.value = input;
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.isset) {
			finalize_data.ReturnNull();
		} else {
			target = state.value;
		}
	}
};

struct MinOperation : public NumericMinMaxBase {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input, AggregateInputData &) {
		if (LessThan::Operation<INPUT_TYPE>(input, state.value)) {
			state.value = input;
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.isset) {
			// source is NULL, nothing to do
			return;
		}
		if (!target.isset) {
			// target is NULL, use source value directly
			target = source;
		} else if (GreaterThan::Operation(target.value, source.value)) {
			target.value = source.value;
		}
	}
};

struct MaxOperation : public NumericMinMaxBase {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input, AggregateInputData &) {
		if (GreaterThan::Operation<INPUT_TYPE>(input, state.value)) {
			state.value = input;
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.isset) {
			// source is NULL, nothing to do
			return;
		}
		if (!target.isset) {
			// target is NULL, use source value directly
			target = source;
		} else if (LessThan::Operation(target.value, source.value)) {
			target.value = source.value;
		}
	}
};

struct MinMaxStringState : MinMaxState<string_t> {
	void Destroy() {
		if (isset && !value.IsInlined()) {
			delete[] value.GetData();
		}
	}

	void Assign(string_t input) {
		if (input.IsInlined()) {
			// inlined string - we can directly store it into the string_t without having to allocate anything
			Destroy();
			value = input;
		} else {
			// non-inlined string, need to allocate space for it somehow
			auto len = input.GetSize();
			char *ptr;
			if (!isset || value.GetSize() < len) {
				// we cannot fit this into the current slot - destroy it and re-allocate
				Destroy();
				ptr = new char[len];
			} else {
				// this fits into the current slot - take over the pointer
				ptr = value.GetDataWriteable();
			}
			memcpy(ptr, input.GetData(), len);

			value = string_t(ptr, UnsafeNumericCast<uint32_t>(len));
		}
	}
};

struct StringMinMaxBase : public MinMaxBase {
	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		state.Destroy();
	}

	template <class INPUT_TYPE, class STATE>
	static void Assign(STATE &state, INPUT_TYPE input, AggregateInputData &input_data) {
		state.Assign(input);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.isset) {
			finalize_data.ReturnNull();
		} else {
			target = StringVector::AddStringOrBlob(finalize_data.result, state.value);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &input_data) {
		if (!source.isset) {
			// source is NULL, nothing to do
			return;
		}
		if (!target.isset) {
			// target is NULL, use source value directly
			Assign(target, source.value, input_data);
			target.isset = true;
		} else {
			OP::template Execute<string_t, STATE>(target, source.value, input_data);
		}
	}
};

struct MinOperationString : public StringMinMaxBase {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input, AggregateInputData &input_data) {
		if (LessThan::Operation<INPUT_TYPE>(input, state.value)) {
			Assign(state, input, input_data);
		}
	}
};

struct MaxOperationString : public StringMinMaxBase {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input, AggregateInputData &input_data) {
		if (GreaterThan::Operation<INPUT_TYPE>(input, state.value)) {
			Assign(state, input, input_data);
		}
	}
};

template <OrderType ORDER_TYPE_TEMPLATED>
struct VectorMinMaxBase {
	static constexpr OrderType ORDER_TYPE = ORDER_TYPE_TEMPLATED;

	static bool IgnoreNull() {
		return true;
	}

	template <class STATE>
	static void Initialize(STATE &state) {
		state.isset = false;
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		state.Destroy();
	}

	template <class INPUT_TYPE, class STATE>
	static void Assign(STATE &state, INPUT_TYPE input, AggregateInputData &input_data) {
		state.Assign(input);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Execute(STATE &state, INPUT_TYPE input, AggregateInputData &input_data) {
		if (!state.isset) {
			Assign(state, input, input_data);
			state.isset = true;
			return;
		}
		if (LessThan::Operation<INPUT_TYPE>(input, state.value)) {
			Assign(state, input, input_data);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &input_data) {
		if (!source.isset) {
			// source is NULL, nothing to do
			return;
		}
		OP::template Execute<string_t, STATE, OP>(target, source.value, input_data);
	}

	template <class STATE>
	static void Finalize(STATE &state, AggregateFinalizeData &finalize_data) {
		if (!state.isset) {
			finalize_data.ReturnNull();
		} else {
			CreateSortKeyHelpers::DecodeSortKey(state.value, finalize_data.result, finalize_data.result_idx,
			                                    OrderModifiers(ORDER_TYPE, OrderByNullType::NULLS_LAST));
		}
	}

	static unique_ptr<FunctionData> Bind(ClientContext &context, AggregateFunction &function,
	                                     vector<unique_ptr<Expression>> &arguments) {
		function.arguments[0] = arguments[0]->return_type;
		function.return_type = arguments[0]->return_type;
		return nullptr;
	}
};

struct MinOperationVector : VectorMinMaxBase<OrderType::ASCENDING> {};

struct MaxOperationVector : VectorMinMaxBase<OrderType::DESCENDING> {};

template <typename OP, typename STATE>
static AggregateFunction GetMinMaxFunction(const LogicalType &type) {
	return AggregateFunction(
	    {type}, LogicalType::BLOB, AggregateFunction::StateSize<STATE>, AggregateFunction::StateInitialize<STATE, OP>,
	    AggregateSortKeyHelpers::UnaryUpdate<STATE, OP, OP::ORDER_TYPE, false>,
	    AggregateFunction::StateCombine<STATE, OP>, AggregateFunction::StateVoidFinalize<STATE, OP>, nullptr, OP::Bind,
	    AggregateFunction::StateDestroy<STATE, OP>);
}

template <class OP, class OP_STRING, class OP_VECTOR>
static AggregateFunction GetMinMaxOperator(const LogicalType &type) {
	auto internal_type = type.InternalType();
	switch (internal_type) {
	case PhysicalType::VARCHAR:
		return AggregateFunction::UnaryAggregateDestructor<MinMaxStringState, string_t, string_t, OP_STRING>(type.id(),
		                                                                                                     type.id());
	case PhysicalType::LIST:
	case PhysicalType::STRUCT:
	case PhysicalType::ARRAY:
		return GetMinMaxFunction<OP_VECTOR, MinMaxStringState>(type);
	default:
		return GetUnaryAggregate<OP>(type);
	}
}

template <class OP, class OP_STRING, class OP_VECTOR>
unique_ptr<FunctionData> BindMinMax(ClientContext &context, AggregateFunction &function,
                                    vector<unique_ptr<Expression>> &arguments) {
	if (arguments[0]->return_type.id() == LogicalTypeId::VARCHAR) {
		auto str_collation = StringType::GetCollation(arguments[0]->return_type);
		if (!str_collation.empty()) {
			// If aggr function is min/max and uses collations, replace bound_function with arg_min/arg_max
			// to make sure the result's correctness.
			string function_name = function.name == "min" ? "arg_min" : "arg_max";
			QueryErrorContext error_context;
			auto func = Catalog::GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, "", "", function_name,
			                              OnEntryNotFound::RETURN_NULL, error_context);

			auto &func_entry = func->Cast<AggregateFunctionCatalogEntry>();

			FunctionBinder function_binder(context);
			vector<LogicalType> types {arguments[0]->return_type, arguments[0]->return_type};
			ErrorData error;
			auto best_function = function_binder.BindFunction(func_entry.name, func_entry.functions, types, error);
			if (!best_function.IsValid()) {
				throw BinderException(string("Fail to find corresponding function for collation min/max: ") +
				                      error.Message());
			}
			function = func_entry.functions.GetFunctionByOffset(best_function.GetIndex());

			// Create a copied child and PushCollation for it.
			arguments.push_back(arguments[0]->Copy());
			ExpressionBinder::PushCollation(context, arguments[1], arguments[0]->return_type);

			// Bind function like arg_min/arg_max.
			function.arguments[0] = arguments[0]->return_type;
			function.return_type = arguments[0]->return_type;
			return nullptr;
		}
	}

	auto input_type = arguments[0]->return_type;
	if (input_type.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}
	auto name = std::move(function.name);
	function = GetMinMaxOperator<OP, OP_STRING, OP_VECTOR>(input_type);
	function.name = std::move(name);
	function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	if (function.bind) {
		return function.bind(context, function, arguments);
	} else {
		return nullptr;
	}
}

template <class OP, class OP_STRING, class OP_VECTOR>
static void AddMinMaxOperator(AggregateFunctionSet &set) {
	set.AddFunction(AggregateFunction({LogicalType::ANY}, LogicalType::ANY, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                  nullptr, BindMinMax<OP, OP_STRING, OP_VECTOR>));
}

AggregateFunctionSet MinFun::GetFunctions() {
	AggregateFunctionSet min("min");
	AddMinMaxOperator<MinOperation, MinOperationString, MinOperationVector>(min);
	return min;
}

AggregateFunctionSet MaxFun::GetFunctions() {
	AggregateFunctionSet max("max");
	AddMinMaxOperator<MaxOperation, MaxOperationString, MaxOperationVector>(max);
	return max;
}

} // namespace duckdb

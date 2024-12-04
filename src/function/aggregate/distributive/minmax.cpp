#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/aggregate/minmax_n_helpers.hpp"
#include "duckdb/function/aggregate/sort_key_helpers.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

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
		return AggregateFunction::UnaryAggregateDestructor<MinMaxStringState, string_t, string_t, OP_STRING>(type,
		                                                                                                     type);
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
		if (!str_collation.empty() || !DBConfig::GetConfig(context).options.collation.empty()) {
			// If aggr function is min/max and uses collations, replace bound_function with arg_min/arg_max
			// to make sure the result's correctness.
			string function_name = function.name == "min" ? "arg_min" : "arg_max";
			QueryErrorContext error_context;
			auto func = Catalog::GetEntry(context, CatalogType::AGGREGATE_FUNCTION_ENTRY, "", "", function_name,
			                              OnEntryNotFound::RETURN_NULL, error_context);
			if (!func) {
				throw NotImplementedException(
				    "Failure while binding function \"%s\" using collations - arg_min/arg_max do not exist in the "
				    "catalog - load the core_functions module to fix this issue",
				    function.name);
			}

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
	function.distinct_dependent = AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT;
	if (function.bind) {
		return function.bind(context, function, arguments);
	} else {
		return nullptr;
	}
}

template <class OP, class OP_STRING, class OP_VECTOR>
static AggregateFunction GetMinMaxOperator(string name) {
	return AggregateFunction(std::move(name), {LogicalType::ANY}, LogicalType::ANY, nullptr, nullptr, nullptr, nullptr,
	                         nullptr, nullptr, BindMinMax<OP, OP_STRING, OP_VECTOR>);
}

AggregateFunction MinFunction::GetFunction() {
	return GetMinMaxOperator<MinOperation, MinOperationString, MinOperationVector>("min");
}

AggregateFunction MaxFunction::GetFunction() {
	return GetMinMaxOperator<MaxOperation, MaxOperationString, MaxOperationVector>("max");
}

//---------------------------------------------------
// MinMaxN
//---------------------------------------------------

template <class A, class COMPARATOR>
class MinMaxNState {
public:
	using VAL_TYPE = A;
	using T = typename VAL_TYPE::TYPE;

	UnaryAggregateHeap<T, COMPARATOR> heap;
	bool is_initialized = false;

	void Initialize(idx_t nval) {
		heap.Initialize(nval);
		is_initialized = true;
	}

	static const T &GetValue(const T &val) {
		return val;
	}
};

template <class STATE>
static void MinMaxNUpdate(Vector inputs[], AggregateInputData &aggr_input, idx_t input_count, Vector &state_vector,
                          idx_t count) {

	auto &val_vector = inputs[0];
	auto &n_vector = inputs[1];

	UnifiedVectorFormat val_format;
	UnifiedVectorFormat n_format;
	UnifiedVectorFormat state_format;
	;
	auto val_extra_state = STATE::VAL_TYPE::CreateExtraState(val_vector, count);

	STATE::VAL_TYPE::PrepareData(val_vector, count, val_extra_state, val_format);

	n_vector.ToUnifiedFormat(count, n_format);
	state_vector.ToUnifiedFormat(count, state_format);

	auto states = UnifiedVectorFormat::GetData<STATE *>(state_format);

	for (idx_t i = 0; i < count; i++) {
		const auto val_idx = val_format.sel->get_index(i);
		if (!val_format.validity.RowIsValid(val_idx)) {
			continue;
		}
		const auto state_idx = state_format.sel->get_index(i);
		auto &state = *states[state_idx];

		// Initialize the heap if necessary and add the input to the heap
		if (!state.is_initialized) {
			static constexpr int64_t MAX_N = 1000000;
			const auto nidx = n_format.sel->get_index(i);
			if (!n_format.validity.RowIsValid(nidx)) {
				throw InvalidInputException("Invalid input for MIN/MAX: n value cannot be NULL");
			}
			const auto nval = UnifiedVectorFormat::GetData<int64_t>(n_format)[nidx];
			if (nval <= 0) {
				throw InvalidInputException("Invalid input for MIN/MAX: n value must be > 0");
			}
			if (nval >= MAX_N) {
				throw InvalidInputException("Invalid input for MIN/MAX: n value must be < %d", MAX_N);
			}
			state.Initialize(UnsafeNumericCast<idx_t>(nval));
		}

		// Now add the input to the heap
		auto val_val = STATE::VAL_TYPE::Create(val_format, val_idx);
		state.heap.Insert(aggr_input.allocator, val_val);
	}
}

template <class VAL_TYPE, class COMPARATOR>
static void SpecializeMinMaxNFunction(AggregateFunction &function) {
	using STATE = MinMaxNState<VAL_TYPE, COMPARATOR>;
	using OP = MinMaxNOperation;

	function.state_size = AggregateFunction::StateSize<STATE>;
	function.initialize = AggregateFunction::StateInitialize<STATE, OP, AggregateDestructorType::LEGACY>;
	function.combine = AggregateFunction::StateCombine<STATE, OP>;
	function.destructor = AggregateFunction::StateDestroy<STATE, OP>;

	function.finalize = MinMaxNOperation::Finalize<STATE>;
	function.update = MinMaxNUpdate<STATE>;
}

template <class COMPARATOR>
static void SpecializeMinMaxNFunction(PhysicalType arg_type, AggregateFunction &function) {
	switch (arg_type) {
	case PhysicalType::VARCHAR:
		SpecializeMinMaxNFunction<MinMaxStringValue, COMPARATOR>(function);
		break;
	case PhysicalType::INT32:
		SpecializeMinMaxNFunction<MinMaxFixedValue<int32_t>, COMPARATOR>(function);
		break;
	case PhysicalType::INT64:
		SpecializeMinMaxNFunction<MinMaxFixedValue<int64_t>, COMPARATOR>(function);
		break;
	case PhysicalType::FLOAT:
		SpecializeMinMaxNFunction<MinMaxFixedValue<float>, COMPARATOR>(function);
		break;
	case PhysicalType::DOUBLE:
		SpecializeMinMaxNFunction<MinMaxFixedValue<double>, COMPARATOR>(function);
		break;
	default:
		SpecializeMinMaxNFunction<MinMaxFallbackValue, COMPARATOR>(function);
		break;
	}
}

template <class COMPARATOR>
unique_ptr<FunctionData> MinMaxNBind(ClientContext &context, AggregateFunction &function,
                                     vector<unique_ptr<Expression>> &arguments) {

	for (auto &arg : arguments) {
		if (arg->return_type.id() == LogicalTypeId::UNKNOWN) {
			throw ParameterNotResolvedException();
		}
	}

	const auto val_type = arguments[0]->return_type.InternalType();

	// Specialize the function based on the input types
	SpecializeMinMaxNFunction<COMPARATOR>(val_type, function);

	function.return_type = LogicalType::LIST(arguments[0]->return_type);
	return nullptr;
}

template <class COMPARATOR>
static AggregateFunction GetMinMaxNFunction() {
	return AggregateFunction({LogicalTypeId::ANY, LogicalType::BIGINT}, LogicalType::LIST(LogicalType::ANY), nullptr,
	                         nullptr, nullptr, nullptr, nullptr, nullptr, MinMaxNBind<COMPARATOR>, nullptr);
}

//---------------------------------------------------
// Function Registration
//---------------------------------------------------s
AggregateFunctionSet MinFun::GetFunctions() {
	AggregateFunctionSet min("min");
	min.AddFunction(MinFunction::GetFunction());
	min.AddFunction(GetMinMaxNFunction<LessThan>());
	return min;
}

AggregateFunctionSet MaxFun::GetFunctions() {
	AggregateFunctionSet max("max");
	max.AddFunction(MaxFunction::GetFunction());
	max.AddFunction(GetMinMaxNFunction<GreaterThan>());
	return max;
}

} // namespace duckdb

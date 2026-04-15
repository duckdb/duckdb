#include "duckdb/common/exception.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

namespace {

template <class T>
struct FirstState {
	T value;
	bool is_set;
	bool is_null;
};

struct FirstFunctionBase {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.is_set = false;
		state.is_null = false;
	}

	static bool IgnoreNull() {
		return false;
	}
};

template <bool LAST, bool SKIP_NULLS>
struct FirstFunction : public FirstFunctionBase {
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		if (LAST || !state.is_set) {
			if (!unary_input.RowIsValid()) {
				if (!SKIP_NULLS) {
					state.is_set = true;
				}
				state.is_null = true;
			} else {
				state.is_set = true;
				state.is_null = false;
				state.value = input;
			}
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!target.is_set) {
			target = source;
		}
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.is_set || state.is_null) {
			finalize_data.ReturnNull();
		} else {
			target = state.value;
		}
	}
};

template <bool LAST, bool SKIP_NULLS>
struct FirstFunctionStringBase : public FirstFunctionBase {
	template <class STATE, bool COMBINE = false>
	static void SetValue(STATE &state, AggregateInputData &input_data, string_t value, bool is_null) {
		if (LAST && state.is_set) {
			Destroy(state, input_data);
		}
		if (is_null) {
			if (!SKIP_NULLS) {
				state.is_set = true;
				state.is_null = true;
			}
		} else {
			state.is_set = true;
			state.is_null = false;
			if ((COMBINE && !LAST) || value.IsInlined()) {
				// We use the aggregate allocator for 'first', so the allocation is already done when combining
				// Of course, if the value is inlined, we also don't need to allocate
				state.value = value;
			} else {
				// non-inlined string, need to allocate space for it
				auto len = value.GetSize();
				auto ptr = LAST ? new char[len] : char_ptr_cast(input_data.allocator.Allocate(len));
				memcpy(ptr, value.GetData(), len);

				state.value = string_t(ptr, UnsafeNumericCast<uint32_t>(len));
			}
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &input_data) {
		if (source.is_set && (LAST || !target.is_set)) {
			SetValue<STATE, true>(target, input_data, source.value, source.is_null);
		}
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &) {
		if (state.is_set && !state.is_null && !state.value.IsInlined()) {
			delete[] state.value.GetData();
		}
	}
};

template <bool LAST, bool SKIP_NULLS>
struct FirstFunctionString : FirstFunctionStringBase<LAST, SKIP_NULLS> {
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		if (LAST || !state.is_set) {
			FirstFunctionStringBase<LAST, SKIP_NULLS>::template SetValue<STATE>(state, unary_input.input, input,
			                                                                    !unary_input.RowIsValid());
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.is_set || state.is_null) {
			finalize_data.ReturnNull();
		} else {
			target = StringVector::AddStringOrBlob(finalize_data.result, state.value);
		}
	}
};

template <bool LAST, bool SKIP_NULLS>
struct FirstVectorFunction : FirstFunctionStringBase<LAST, SKIP_NULLS> {
	using STATE = FirstState<string_t>;

	static void Update(Vector inputs[], AggregateInputData &input_data, idx_t, Vector &state_vector, idx_t count) {
		auto &input = inputs[0];
		UnifiedVectorFormat idata;
		input.ToUnifiedFormat(count, idata);

		UnifiedVectorFormat sdata;
		state_vector.ToUnifiedFormat(count, sdata);

		sel_t assign_sel[STANDARD_VECTOR_SIZE];
		idx_t assign_count = 0;

		auto states = UnifiedVectorFormat::GetData<STATE *>(sdata);
		for (idx_t i = 0; i < count; i++) {
			const auto idx = idata.sel->get_index(i);
			bool is_null = !idata.validity.RowIsValid(idx);
			if (SKIP_NULLS && is_null) {
				continue;
			}
			auto &state = *states[sdata.sel->get_index(i)];
			if (!LAST && state.is_set) {
				continue;
			}
			assign_sel[assign_count++] = NumericCast<sel_t>(i);
		}
		if (assign_count == 0) {
			// fast path - nothing to set
			return;
		}

		Vector sort_key(LogicalType::BLOB);
		OrderModifiers modifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST);
		// slice with a selection vector and generate sort keys
		if (assign_count == count) {
			CreateSortKeyHelpers::CreateSortKey(input, count, modifiers, sort_key);
		} else {
			SelectionVector sel(assign_sel);
			Vector sliced_input(input, sel, assign_count);
			CreateSortKeyHelpers::CreateSortKey(sliced_input, assign_count, modifiers, sort_key);
		}
		auto sort_key_data = FlatVector::GetData<string_t>(sort_key);

		// now assign sort keys
		for (idx_t i = 0; i < assign_count; i++) {
			const auto state_idx = sdata.sel->get_index(assign_sel[i]);
			auto &state = *states[state_idx];
			if (!LAST && state.is_set) {
				continue;
			}

			const auto idx = idata.sel->get_index(assign_sel[i]);
			bool is_null = !idata.validity.RowIsValid(idx);
			FirstFunctionStringBase<LAST, SKIP_NULLS>::template SetValue<STATE>(state, input_data, sort_key_data[i],
			                                                                    is_null);
		}
	}

	template <class STATE>
	static void Finalize(STATE &state, AggregateFinalizeData &finalize_data) {
		if (!state.is_set || state.is_null) {
			finalize_data.ReturnNull();
		} else {
			CreateSortKeyHelpers::DecodeSortKey(state.value, finalize_data.result, finalize_data.result_idx,
			                                    OrderModifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST));
		}
	}

	static unique_ptr<FunctionData> Bind(BindAggregateFunctionInput &input) {
		auto &function = input.GetBoundFunction();
		auto &arguments = input.GetArguments();

		function.arguments[0] = arguments[0]->return_type;
		function.SetReturnType(arguments[0]->return_type);
		return nullptr;
	}
};

//===--------------------------------------------------------------------===//
// FIRST/LAST for fixed-size ARRAY (constant-size child): copy raw payload.
// Avoids CreateSortKey/DecodeSortKey used by FirstVectorFunction, which is
// prohibitively expensive for large arrays (e.g. DISTINCT ON from INSERT ON
// CONFLICT → MERGE rewrite). See issue #21836.
//===--------------------------------------------------------------------===//
static void CopyArrayRowToBuffer(Vector &array_vec, idx_t row_idx, idx_t array_size, const LogicalType &child_type,
                                 UnifiedVectorFormat &child_fmt, data_ptr_t dest) {
	auto base_flat = row_idx * array_size;
	switch (child_type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8: {
		auto data = UnifiedVectorFormat::GetData<int8_t>(child_fmt);
		auto out = reinterpret_cast<int8_t *>(dest);
		for (idx_t e = 0; e < array_size; e++) {
			auto cidx = child_fmt.sel->get_index(base_flat + e);
			out[e] = data[cidx];
		}
		return;
	}
	case PhysicalType::INT16: {
		auto data = UnifiedVectorFormat::GetData<int16_t>(child_fmt);
		auto out = reinterpret_cast<int16_t *>(dest);
		for (idx_t e = 0; e < array_size; e++) {
			auto cidx = child_fmt.sel->get_index(base_flat + e);
			out[e] = data[cidx];
		}
		return;
	}
	case PhysicalType::INT32: {
		auto data = UnifiedVectorFormat::GetData<int32_t>(child_fmt);
		auto out = reinterpret_cast<int32_t *>(dest);
		for (idx_t e = 0; e < array_size; e++) {
			auto cidx = child_fmt.sel->get_index(base_flat + e);
			out[e] = data[cidx];
		}
		return;
	}
	case PhysicalType::INT64: {
		auto data = UnifiedVectorFormat::GetData<int64_t>(child_fmt);
		auto out = reinterpret_cast<int64_t *>(dest);
		for (idx_t e = 0; e < array_size; e++) {
			auto cidx = child_fmt.sel->get_index(base_flat + e);
			out[e] = data[cidx];
		}
		return;
	}
	case PhysicalType::UINT8: {
		auto data = UnifiedVectorFormat::GetData<uint8_t>(child_fmt);
		auto out = reinterpret_cast<uint8_t *>(dest);
		for (idx_t e = 0; e < array_size; e++) {
			auto cidx = child_fmt.sel->get_index(base_flat + e);
			out[e] = data[cidx];
		}
		return;
	}
	case PhysicalType::UINT16: {
		auto data = UnifiedVectorFormat::GetData<uint16_t>(child_fmt);
		auto out = reinterpret_cast<uint16_t *>(dest);
		for (idx_t e = 0; e < array_size; e++) {
			auto cidx = child_fmt.sel->get_index(base_flat + e);
			out[e] = data[cidx];
		}
		return;
	}
	case PhysicalType::UINT32: {
		auto data = UnifiedVectorFormat::GetData<uint32_t>(child_fmt);
		auto out = reinterpret_cast<uint32_t *>(dest);
		for (idx_t e = 0; e < array_size; e++) {
			auto cidx = child_fmt.sel->get_index(base_flat + e);
			out[e] = data[cidx];
		}
		return;
	}
	case PhysicalType::UINT64: {
		auto data = UnifiedVectorFormat::GetData<uint64_t>(child_fmt);
		auto out = reinterpret_cast<uint64_t *>(dest);
		for (idx_t e = 0; e < array_size; e++) {
			auto cidx = child_fmt.sel->get_index(base_flat + e);
			out[e] = data[cidx];
		}
		return;
	}
	case PhysicalType::INT128: {
		auto data = UnifiedVectorFormat::GetData<hugeint_t>(child_fmt);
		auto out = reinterpret_cast<hugeint_t *>(dest);
		for (idx_t e = 0; e < array_size; e++) {
			auto cidx = child_fmt.sel->get_index(base_flat + e);
			out[e] = data[cidx];
		}
		return;
	}
	case PhysicalType::UINT128: {
		auto data = UnifiedVectorFormat::GetData<uhugeint_t>(child_fmt);
		auto out = reinterpret_cast<uhugeint_t *>(dest);
		for (idx_t e = 0; e < array_size; e++) {
			auto cidx = child_fmt.sel->get_index(base_flat + e);
			out[e] = data[cidx];
		}
		return;
	}
	case PhysicalType::FLOAT: {
		auto data = UnifiedVectorFormat::GetData<float>(child_fmt);
		auto out = reinterpret_cast<float *>(dest);
		for (idx_t e = 0; e < array_size; e++) {
			auto cidx = child_fmt.sel->get_index(base_flat + e);
			out[e] = data[cidx];
		}
		return;
	}
	case PhysicalType::DOUBLE: {
		auto data = UnifiedVectorFormat::GetData<double>(child_fmt);
		auto out = reinterpret_cast<double *>(dest);
		for (idx_t e = 0; e < array_size; e++) {
			auto cidx = child_fmt.sel->get_index(base_flat + e);
			out[e] = data[cidx];
		}
		return;
	}
	case PhysicalType::INTERVAL: {
		auto data = UnifiedVectorFormat::GetData<interval_t>(child_fmt);
		auto out = reinterpret_cast<interval_t *>(dest);
		for (idx_t e = 0; e < array_size; e++) {
			auto cidx = child_fmt.sel->get_index(base_flat + e);
			out[e] = data[cidx];
		}
		return;
	}
	default:
		throw NotImplementedException("FIRST aggregate: unsupported ARRAY child type %s", child_type.ToString());
	}
}

template <bool LAST, bool SKIP_NULLS>
struct FirstArrayFunction : public FirstFunctionBase {
	using STATE = FirstState<string_t>;

	template <class STATE_TYPE, bool COMBINE = false>
	static void SetValue(STATE_TYPE &state, AggregateInputData &input_data, string_t value, bool is_null) {
		if (LAST && state.is_set) {
			Destroy(state, input_data);
		}
		if (is_null) {
			if (!SKIP_NULLS) {
				state.is_set = true;
				state.is_null = true;
			}
		} else {
			state.is_set = true;
			state.is_null = false;
			if (COMBINE && !LAST) {
				auto len = value.GetSize();
				auto ptr = input_data.allocator.Allocate(len);
				memcpy(ptr, value.GetData(), len);
				state.value = string_t(const_char_ptr_cast(ptr), UnsafeNumericCast<uint32_t>(len));
			} else {
				auto len = value.GetSize();
				data_ptr_t ptr;
				if (LAST) {
					ptr = data_ptr_cast(new char[len]);
				} else {
					ptr = input_data.allocator.Allocate(len);
				}
				memcpy(ptr, value.GetData(), len);
				state.value = string_t(const_char_ptr_cast(ptr), UnsafeNumericCast<uint32_t>(len));
			}
		}
	}

	template <class STATE_TYPE>
	static void Destroy(STATE_TYPE &state, AggregateInputData &) {
		if (!LAST || !state.is_set || state.is_null || state.value.GetSize() == 0) {
			return;
		}
		delete[] state.value.GetData();
	}

	template <class STATE_TYPE, class OP>
	static void Combine(const STATE_TYPE &source, STATE_TYPE &target, AggregateInputData &input_data) {
		if (source.is_set && (LAST || !target.is_set)) {
			SetValue<STATE_TYPE, true>(target, input_data, source.value, source.is_null);
		}
	}

	static void Update(Vector inputs[], AggregateInputData &input_data, idx_t, Vector &state_vector, idx_t count) {
		auto &input = inputs[0];
		D_ASSERT(input.GetType().id() == LogicalTypeId::ARRAY);
		auto array_size = ArrayType::GetSize(input.GetType());
		auto &child_type = ArrayType::GetChildType(input.GetType());
		auto elem_size = GetTypeIdSize(child_type.InternalType());
		auto byte_len = array_size * elem_size;

		UnifiedVectorFormat idata;
		input.ToUnifiedFormat(count, idata);
		UnifiedVectorFormat sdata;
		state_vector.ToUnifiedFormat(count, sdata);

		auto &child = ArrayVector::GetEntry(input);
		UnifiedVectorFormat child_fmt;
		child.ToUnifiedFormat(count * array_size, child_fmt);

		sel_t assign_sel[STANDARD_VECTOR_SIZE];
		idx_t assign_count = 0;
		auto states = UnifiedVectorFormat::GetData<STATE *>(sdata);
		for (idx_t i = 0; i < count; i++) {
			const auto idx = idata.sel->get_index(i);
			bool is_null = !idata.validity.RowIsValid(idx);
			if (SKIP_NULLS && is_null) {
				continue;
			}
			auto &state = *states[sdata.sel->get_index(i)];
			if (!LAST && state.is_set) {
				continue;
			}
			assign_sel[assign_count++] = NumericCast<sel_t>(i);
		}
		if (assign_count == 0) {
			return;
		}

		for (idx_t i = 0; i < assign_count; i++) {
			const auto state_idx = sdata.sel->get_index(assign_sel[i]);
			auto &state = *states[state_idx];
			if (!LAST && state.is_set) {
				continue;
			}
			const auto idx = idata.sel->get_index(assign_sel[i]);
			bool is_null = !idata.validity.RowIsValid(idx);
			if (!is_null) {
				if (LAST && state.is_set) {
					Destroy(state, input_data);
				}
				data_ptr_t ptr;
				if (LAST) {
					ptr = data_ptr_cast(new char[byte_len]);
				} else {
					ptr = input_data.allocator.Allocate(byte_len);
				}
				CopyArrayRowToBuffer(input, idx, array_size, child_type, child_fmt, ptr);
				state.is_set = true;
				state.is_null = false;
				state.value = string_t(const_char_ptr_cast(ptr), UnsafeNumericCast<uint32_t>(byte_len));
			} else {
				if (!SKIP_NULLS) {
					state.is_set = true;
					state.is_null = true;
				}
			}
		}
	}

	template <class STATE_TYPE>
	static void Finalize(STATE_TYPE &state, AggregateFinalizeData &finalize_data) {
		if (!state.is_set || state.is_null) {
			finalize_data.ReturnNull();
			return;
		}
		auto &result = finalize_data.result;
		D_ASSERT(result.GetType().id() == LogicalTypeId::ARRAY);
		auto array_size = ArrayType::GetSize(result.GetType());
		auto &child_type = ArrayType::GetChildType(result.GetType());
		auto elem_size = GetTypeIdSize(child_type.InternalType());
		D_ASSERT(state.value.GetSize() == array_size * elem_size);

		FlatVector::SetNull(result, finalize_data.result_idx, false);
		auto &result_child = ArrayVector::GetEntry(result);
		auto dest_row_base = finalize_data.result_idx * array_size;
		auto src = const_data_ptr_cast(state.value.GetData());
		auto byte_len = state.value.GetSize();
		switch (child_type.InternalType()) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
			memcpy(FlatVector::GetDataMutable<int8_t>(result_child) + dest_row_base, src, byte_len);
			break;
		case PhysicalType::INT16:
			memcpy(FlatVector::GetDataMutable<int16_t>(result_child) + dest_row_base, src, byte_len);
			break;
		case PhysicalType::INT32:
			memcpy(FlatVector::GetDataMutable<int32_t>(result_child) + dest_row_base, src, byte_len);
			break;
		case PhysicalType::INT64:
			memcpy(FlatVector::GetDataMutable<int64_t>(result_child) + dest_row_base, src, byte_len);
			break;
		case PhysicalType::UINT8:
			memcpy(FlatVector::GetDataMutable<uint8_t>(result_child) + dest_row_base, src, byte_len);
			break;
		case PhysicalType::UINT16:
			memcpy(FlatVector::GetDataMutable<uint16_t>(result_child) + dest_row_base, src, byte_len);
			break;
		case PhysicalType::UINT32:
			memcpy(FlatVector::GetDataMutable<uint32_t>(result_child) + dest_row_base, src, byte_len);
			break;
		case PhysicalType::UINT64:
			memcpy(FlatVector::GetDataMutable<uint64_t>(result_child) + dest_row_base, src, byte_len);
			break;
		case PhysicalType::INT128:
			memcpy(FlatVector::GetDataMutable<hugeint_t>(result_child) + dest_row_base, src, byte_len);
			break;
		case PhysicalType::UINT128:
			memcpy(FlatVector::GetDataMutable<uhugeint_t>(result_child) + dest_row_base, src, byte_len);
			break;
		case PhysicalType::FLOAT:
			memcpy(FlatVector::GetDataMutable<float>(result_child) + dest_row_base, src, byte_len);
			break;
		case PhysicalType::DOUBLE:
			memcpy(FlatVector::GetDataMutable<double>(result_child) + dest_row_base, src, byte_len);
			break;
		case PhysicalType::INTERVAL:
			memcpy(FlatVector::GetDataMutable<interval_t>(result_child) + dest_row_base, src, byte_len);
			break;
		default:
			throw InternalException("FIRST aggregate finalize: unsupported ARRAY child type %s", child_type.ToString());
		}
		auto &child_validity = FlatVector::Validity(result_child);
		for (idx_t e = 0; e < array_size; e++) {
			child_validity.SetValid(dest_row_base + e);
		}
	}

	static unique_ptr<FunctionData> Bind(BindAggregateFunctionInput &input) {
		auto &function = input.GetBoundFunction();
		auto &arguments = input.GetArguments();
		function.arguments[0] = arguments[0]->return_type;
		function.SetReturnType(arguments[0]->return_type);
		return nullptr;
	}
};

LogicalType GetFirstStateType(const AggregateFunction &function) {
	child_list_t<LogicalType> child_types;
	LogicalType value_type = function.arguments[0];
	child_types.emplace_back("value", value_type);
	child_types.emplace_back("is_set", LogicalType::BOOLEAN);
	child_types.emplace_back("is_null", LogicalType::BOOLEAN);
	return LogicalType::STRUCT(std::move(child_types));
}

template <bool LAST, bool SKIP_NULLS>
AggregateFunction GetFirstArrayAggregate(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::ARRAY);
	using OP = FirstArrayFunction<LAST, SKIP_NULLS>;
	using STATE = FirstState<string_t>;
	auto fun = AggregateFunction(
	    {type}, type, AggregateFunction::StateSize<STATE>, AggregateFunction::StateInitialize<STATE, OP>, OP::Update,
	    AggregateFunction::StateCombine<STATE, OP>, AggregateFunction::StateVoidFinalize<STATE, OP>, nullptr, OP::Bind,
	    LAST ? AggregateFunction::StateDestroy<STATE, OP> : nullptr, nullptr, nullptr);
	fun.SetStructStateExport(GetFirstStateType);
	return fun;
}

template <class T, bool LAST, bool SKIP_NULLS>
void FirstFunctionSimpleUpdate(Vector inputs[], AggregateInputData &aggregate_input_data, idx_t input_count,
                               data_ptr_t state, idx_t count) {
	auto agg_state = reinterpret_cast<FirstState<T> *>(state);
	if (LAST) {
		// For LAST, iterate backward within each batch to find the last value
		// This saves iterating through all elements when we only need the last one
		D_ASSERT(input_count == 1);
		UnifiedVectorFormat idata;
		inputs[0].ToUnifiedFormat(count, idata);
		auto input_data = UnifiedVectorFormat::GetData<T>(idata);

		for (idx_t i = count; i-- > 0;) {
			const auto idx = idata.sel->get_index(i);
			const auto row_valid = idata.validity.RowIsValid(idx);
			if (SKIP_NULLS && !row_valid) {
				continue;
			}
			// Found the last value in this batch - update state and exit
			agg_state->is_set = true;
			agg_state->is_null = !row_valid;
			if (row_valid) {
				agg_state->value = input_data[idx];
			}
			break;
		}
		// If we get here with SKIP_NULLS, all values were NULL - keep previous state
	} else if (!agg_state->is_set) {
		// For FIRST, this skips looping over the input once the aggregate state has been set
		AggregateFunction::UnaryUpdate<FirstState<T>, T, FirstFunction<LAST, SKIP_NULLS>>(inputs, aggregate_input_data,
		                                                                                  input_count, state, count);
	}
}

template <class T, bool LAST, bool SKIP_NULLS>
AggregateFunction GetFirstAggregateTemplated(const LogicalType &type) {
	auto result = AggregateFunction::UnaryAggregate<FirstState<T>, T, T, FirstFunction<LAST, SKIP_NULLS>>(type, type);
	result.SetStateSimpleUpdateCallback(FirstFunctionSimpleUpdate<T, LAST, SKIP_NULLS>);
	result.SetStructStateExport(GetFirstStateType);
	return result;
}

template <bool LAST, bool SKIP_NULLS>
AggregateFunction GetFirstFunction(const LogicalType &type);

template <bool LAST, bool SKIP_NULLS>
AggregateFunction GetDecimalFirstFunction(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::DECIMAL);
	switch (type.InternalType()) {
	case PhysicalType::INT16:
		return GetFirstFunction<LAST, SKIP_NULLS>(LogicalType::SMALLINT);
	case PhysicalType::INT32:
		return GetFirstFunction<LAST, SKIP_NULLS>(LogicalType::INTEGER);
	case PhysicalType::INT64:
		return GetFirstFunction<LAST, SKIP_NULLS>(LogicalType::BIGINT);
	default:
		return GetFirstFunction<LAST, SKIP_NULLS>(LogicalType::HUGEINT);
	}
}
template <bool LAST, bool SKIP_NULLS>
AggregateFunction GetFirstFunction(const LogicalType &type) {
	if (type.id() == LogicalTypeId::DECIMAL) {
		type.Verify();
		AggregateFunction function = GetDecimalFirstFunction<LAST, SKIP_NULLS>(type);
		function.arguments[0] = type;
		function.SetReturnType(type);
		return function;
	}
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return GetFirstAggregateTemplated<int8_t, LAST, SKIP_NULLS>(type);
	case PhysicalType::INT16:
		return GetFirstAggregateTemplated<int16_t, LAST, SKIP_NULLS>(type);
	case PhysicalType::INT32:
		return GetFirstAggregateTemplated<int32_t, LAST, SKIP_NULLS>(type);
	case PhysicalType::INT64:
		return GetFirstAggregateTemplated<int64_t, LAST, SKIP_NULLS>(type);
	case PhysicalType::UINT8:
		return GetFirstAggregateTemplated<uint8_t, LAST, SKIP_NULLS>(type);
	case PhysicalType::UINT16:
		return GetFirstAggregateTemplated<uint16_t, LAST, SKIP_NULLS>(type);
	case PhysicalType::UINT32:
		return GetFirstAggregateTemplated<uint32_t, LAST, SKIP_NULLS>(type);
	case PhysicalType::UINT64:
		return GetFirstAggregateTemplated<uint64_t, LAST, SKIP_NULLS>(type);
	case PhysicalType::INT128:
		return GetFirstAggregateTemplated<hugeint_t, LAST, SKIP_NULLS>(type);
	case PhysicalType::UINT128:
		return GetFirstAggregateTemplated<uhugeint_t, LAST, SKIP_NULLS>(type);
	case PhysicalType::FLOAT:
		return GetFirstAggregateTemplated<float, LAST, SKIP_NULLS>(type);
	case PhysicalType::DOUBLE:
		return GetFirstAggregateTemplated<double, LAST, SKIP_NULLS>(type);
	case PhysicalType::INTERVAL:
		return GetFirstAggregateTemplated<interval_t, LAST, SKIP_NULLS>(type);
	case PhysicalType::VARCHAR:
		if (LAST) {
			auto fun = AggregateFunction::UnaryAggregateDestructor<FirstState<string_t>, string_t, string_t,
			                                                       FirstFunctionString<LAST, SKIP_NULLS>>(type, type);
			fun.SetStructStateExport(GetFirstStateType);
			return fun;
		} else {
			auto fun = AggregateFunction::UnaryAggregate<FirstState<string_t>, string_t, string_t,
			                                             FirstFunctionString<LAST, SKIP_NULLS>>(type, type);
			fun.SetStructStateExport(GetFirstStateType);
			return fun;
		}
	case PhysicalType::ARRAY:
		if (TypeIsConstantSize(ArrayType::GetChildType(type).InternalType())) {
			return GetFirstArrayAggregate<LAST, SKIP_NULLS>(type);
		}
		[[fallthrough]];
	default: {
		using OP = FirstVectorFunction<LAST, SKIP_NULLS>;
		using STATE = FirstState<string_t>;
		auto fun = AggregateFunction(
		    {type}, type, AggregateFunction::StateSize<STATE>, AggregateFunction::StateInitialize<STATE, OP>,
		    OP::Update, AggregateFunction::StateCombine<STATE, OP>, AggregateFunction::StateVoidFinalize<STATE, OP>,
		    nullptr, OP::Bind, LAST ? AggregateFunction::StateDestroy<STATE, OP> : nullptr, nullptr, nullptr);
		fun.SetStructStateExport(GetFirstStateType);
		return fun;
	}
	}
}

template <bool LAST, bool SKIP_NULLS>
unique_ptr<FunctionData> BindDecimalFirst(BindAggregateFunctionInput &input) {
	auto &function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();

	auto decimal_type = arguments[0]->return_type;
	auto name = std::move(function.name);
	function = GetFirstFunction<LAST, SKIP_NULLS>(decimal_type);
	function.name = std::move(name);
	function.SetDistinctDependent(AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT);
	function.SetReturnType(decimal_type);
	return nullptr;
}

template <bool LAST, bool SKIP_NULLS>
AggregateFunction GetFirstOperator(const LogicalType &type) {
	if (type.id() == LogicalTypeId::DECIMAL) {
		throw InternalException("FIXME: this shouldn't happen...");
	}
	return GetFirstFunction<LAST, SKIP_NULLS>(type);
}

template <bool LAST, bool SKIP_NULLS>
unique_ptr<FunctionData> BindFirst(BindAggregateFunctionInput &input) {
	auto &function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();

	auto input_type = arguments[0]->return_type;
	auto name = std::move(function.name);
	function = GetFirstOperator<LAST, SKIP_NULLS>(input_type);
	function.name = std::move(name);
	function.SetDistinctDependent(AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT);
	if (function.HasBindCallback()) {
		return function.Bind(input.GetClientContext(), arguments);
		;
	} else {
		return nullptr;
	}
}

template <bool LAST, bool SKIP_NULLS>
void AddFirstOperator(AggregateFunctionSet &set) {
	set.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL, nullptr, nullptr, nullptr,
	                                  nullptr, nullptr, nullptr, BindDecimalFirst<LAST, SKIP_NULLS>));
	set.AddFunction(AggregateFunction({LogicalType::ANY}, LogicalType::ANY, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                  nullptr, BindFirst<LAST, SKIP_NULLS>));
}

} // namespace

AggregateFunction FirstFunctionGetter::GetFunction(const LogicalType &type) {
	auto fun = GetFirstFunction<false, false>(type);
	fun.name = "first";
	return fun;
}

AggregateFunction LastFunctionGetter::GetFunction(const LogicalType &type) {
	auto fun = GetFirstFunction<true, false>(type);
	fun.name = "last";
	return fun;
}

AggregateFunctionSet FirstFun::GetFunctions() {
	AggregateFunctionSet first("first");
	AddFirstOperator<false, false>(first);
	return first;
}

AggregateFunctionSet LastFun::GetFunctions() {
	AggregateFunctionSet last("last");
	AddFirstOperator<true, false>(last);
	return last;
}

AggregateFunctionSet AnyValueFun::GetFunctions() {
	AggregateFunctionSet any_value("any_value");
	AddFirstOperator<false, true>(any_value);
	return any_value;
}

} // namespace duckdb

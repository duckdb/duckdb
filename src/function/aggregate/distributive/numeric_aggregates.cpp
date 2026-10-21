#include "duckdb/function/aggregate/numeric_aggregates.hpp"

#include "duckdb/common/clustered_aggregate.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/numeric_blob.hpp"
#include "duckdb/common/types/numeric_type.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/numeric_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/function/aggregate_state.hpp"

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// Values
//===--------------------------------------------------------------------===//
//! An exact value: unscaled * 10^-scale when it fits 128 bits, otherwise (wide decimals) its canonical blob
struct NumericValueRef {
	bool wide = false;
	uint8_t scale = 0;
	hugeint_t unscaled;
	string_t blob;
};

int CompareBytes(const_data_ptr_t left, idx_t left_size, const_data_ptr_t right, idx_t right_size) {
	auto common = MinValue(left_size, right_size);
	auto result = memcmp(left, right, common);
	if (result != 0) {
		return result < 0 ? -1 : 1;
	}
	if (left_size == right_size) {
		return 0;
	}
	return left_size < right_size ? -1 : 1;
}

int CompareUnscaled(hugeint_t left, hugeint_t right) {
	if (left < right) {
		return -1;
	}
	return left > right ? 1 : 0;
}

//! Orders two values: at a common scale when 128 bits suffice, otherwise through the canonical blobs
int CompareValues(const NumericValueRef &left, const NumericValueRef &right) {
	if (!left.wide && !right.wide) {
		if (left.scale == right.scale) {
			return CompareUnscaled(left.unscaled, right.unscaled);
		}
		hugeint_t left_aligned = left.unscaled;
		hugeint_t right_aligned = right.unscaled;
		bool aligned;
		if (left.scale < right.scale) {
			aligned =
			    Hugeint::TryMultiply(left_aligned, Hugeint::POWERS_OF_TEN[right.scale - left.scale], left_aligned);
		} else {
			aligned =
			    Hugeint::TryMultiply(right_aligned, Hugeint::POWERS_OF_TEN[left.scale - right.scale], right_aligned);
		}
		if (aligned) {
			return CompareUnscaled(left_aligned, right_aligned);
		}
	}
	data_t left_buffer[NumericBlob::MAX_BLOB_SIZE];
	data_t right_buffer[NumericBlob::MAX_BLOB_SIZE];
	auto left_data = left.wide ? const_data_ptr_cast(left.blob.GetData()) : left_buffer;
	auto left_size =
	    left.wide ? left.blob.GetSize() : NumericBlob::EncodeUnscaled(left.unscaled, left.scale, left_buffer);
	auto right_data = right.wide ? const_data_ptr_cast(right.blob.GetData()) : right_buffer;
	auto right_size =
	    right.wide ? right.blob.GetSize() : NumericBlob::EncodeUnscaled(right.unscaled, right.scale, right_buffer);
	return CompareBytes(left_data, left_size, right_data, right_size);
}

//! A value held by an aggregate state: blobs live in the arena of the aggregate, re-using their allocation
struct NumericStateValue : public NumericValueRef {
	idx_t alloc_size;

	void SetUnscaled(hugeint_t unscaled_p, uint8_t scale_p) {
		wide = false;
		unscaled = unscaled_p;
		scale = scale_p;
	}

	void SetBlob(string_t input, AggregateInputData &input_data) {
		wide = true;
		if (input.IsInlined()) {
			blob = input;
			return;
		}
		auto size = input.GetSize();
		char *ptr;
		if (alloc_size >= size) {
			ptr = blob.GetDataWriteable();
		} else {
			alloc_size = NextPowerOfTwo(size);
			ptr = char_ptr_cast(input_data.allocator.Allocate(alloc_size));
		}
		memcpy(ptr, input.GetData(), size);
		blob = string_t(ptr, UnsafeNumericCast<uint32_t>(size));
	}

	void Set(const NumericValueRef &value, AggregateInputData &input_data) {
		if (value.wide) {
			SetBlob(value.blob, input_data);
		} else {
			SetUnscaled(value.unscaled, value.scale);
		}
	}

	string_t Finalize(Vector &result) const {
		if (wide) {
			return StringVector::AddStringOrBlob(result, blob);
		}
		return NumericBlob::AddUnscaled(result, unscaled, scale);
	}
};

//===--------------------------------------------------------------------===//
// Input
//===--------------------------------------------------------------------===//
//! Whether a representation holds its unscaled values in a fixed-size signed-compatible integer
bool IsFixedRepresentation(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
		return true;
	default:
		return false;
	}
}

//! A NUMERIC input vector as its rows are read: typed unscaled values of one scale, or canonical blobs
struct NumericInput {
	explicit NumericInput(Vector &input, idx_t count) {
		D_ASSERT(input.GetType().id() == LogicalTypeId::NUMERIC);
		if (NumericVector::TryGetTyped(input, typed)) {
			if (IsFixedRepresentation(typed->GetType())) {
				SetTyped();
				return;
			}
			// wide decimals compare as canonical blobs
			canonical = make_uniq<Vector>(LogicalType::NUMERIC, count);
			NumericBlob::Encode(*typed, count, *canonical);
			canonical->ToUnifiedFormat(data);
			return;
		}
		auto type = NumericBlob::RangeType(input, count);
		if (!IsFixedRepresentation(type)) {
			input.ToUnifiedFormat(data);
			return;
		}
		typed = make_uniq<Vector>(type, count);
		NumericBlob::Decode(input, count, *typed);
		SetTyped();
	}

	void SetTyped() {
		auto &type = typed->GetType();
		physical = type.InternalType();
		scale = type.id() == LogicalTypeId::DECIMAL ? DecimalType::GetScale(type) : 0;
		typed->ToUnifiedFormat(data);
	}

	bool IsTyped() const {
		return physical != PhysicalType::INVALID;
	}

	inline string_t Blob(idx_t idx) const {
		return UnifiedVectorFormat::GetData<string_t>(data)[idx];
	}

	unique_ptr<Vector> typed;
	unique_ptr<Vector> canonical;
	UnifiedVectorFormat data;
	PhysicalType physical = PhysicalType::INVALID;
	uint8_t scale = 0;
};

//! Runs OP::Typed<T> for the fixed-size representation of the input, OP::Blob for canonical blobs
template <class OP, class... ARGS>
void DispatchInput(const NumericInput &input, ARGS &&...args) {
	switch (input.physical) {
	case PhysicalType::INT8:
		return OP::template Typed<int8_t>(input, std::forward<ARGS>(args)...);
	case PhysicalType::INT16:
		return OP::template Typed<int16_t>(input, std::forward<ARGS>(args)...);
	case PhysicalType::INT32:
		return OP::template Typed<int32_t>(input, std::forward<ARGS>(args)...);
	case PhysicalType::INT64:
		return OP::template Typed<int64_t>(input, std::forward<ARGS>(args)...);
	case PhysicalType::INT128:
		return OP::template Typed<hugeint_t>(input, std::forward<ARGS>(args)...);
	case PhysicalType::UINT8:
		return OP::template Typed<uint8_t>(input, std::forward<ARGS>(args)...);
	case PhysicalType::UINT16:
		return OP::template Typed<uint16_t>(input, std::forward<ARGS>(args)...);
	case PhysicalType::UINT32:
		return OP::template Typed<uint32_t>(input, std::forward<ARGS>(args)...);
	case PhysicalType::UINT64:
		return OP::template Typed<uint64_t>(input, std::forward<ARGS>(args)...);
	default:
		return OP::Blob(input, std::forward<ARGS>(args)...);
	}
}

template <class T>
inline hugeint_t ToUnscaled(const T &value) {
	return Hugeint::Convert(value);
}

template <>
inline hugeint_t ToUnscaled(const hugeint_t &value) {
	return value;
}

template <class STATE>
STATE *const *StatePointers(Vector &states, UnifiedVectorFormat &sdata) {
	states.ToUnifiedFormat(sdata);
	return UnifiedVectorFormat::GetData<STATE *>(sdata);
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
template <class STATE>
inline bool HasValue(const STATE &state);

template <class STATE>
void FinalizeStates(Vector &states, AggregateFinalizeInputData &finalize_input, Vector &result, idx_t count,
                    idx_t offset) {
	if (states.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		FlatVector::SetSize(result, count);
		auto &state = **ConstantVector::GetData<STATE *>(states);
		AggregateFinalizeData finalize_data(result, finalize_input, count);
		if (!HasValue(state)) {
			finalize_data.ReturnNull();
		} else {
			*ConstantVector::GetData<string_t>(result) = state.value.Finalize(result);
		}
		return;
	}
	D_ASSERT(states.GetVectorType() == VectorType::FLAT_VECTOR);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto sdata = FlatVector::GetData<STATE *>(states);
	auto rdata = FlatVector::GetDataMutable<string_t>(result);
	AggregateFinalizeData finalize_data(result, finalize_input, count);
	for (idx_t i = 0; i < count; i++) {
		finalize_data.result_idx = i + offset;
		auto &state = *sdata[i];
		if (!HasValue(state)) {
			finalize_data.ReturnNull();
		} else {
			rdata[finalize_data.result_idx] = state.value.Finalize(result);
		}
	}
}

//===--------------------------------------------------------------------===//
// min / max
//===--------------------------------------------------------------------===//
struct MinOp {
	template <class T>
	static inline bool Better(const T &candidate, const T &current) {
		return candidate < current;
	}
	static inline bool Replaces(int comparison) {
		return comparison < 0;
	}
};

struct MaxOp {
	template <class T>
	static inline bool Better(const T &candidate, const T &current) {
		return candidate > current;
	}
	static inline bool Replaces(int comparison) {
		return comparison > 0;
	}
};

struct NumericMinMaxState {
	bool is_set;
	NumericStateValue value;
};

template <>
inline bool HasValue(const NumericMinMaxState &state) {
	return state.is_set;
}

template <class OP>
struct NumericMinMax {
	using STATE = NumericMinMaxState;

	static void Initialize(AggregateStateInput &, data_ptr_t *states, idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			auto &state = *reinterpret_cast<STATE *>(states[i]);
			state.is_set = false;
			state.value.wide = false;
			state.value.alloc_size = 0;
		}
	}

	static inline void MergeUnscaled(STATE &state, hugeint_t unscaled, uint8_t scale) {
		if (!state.is_set) {
			state.is_set = true;
			state.value.SetUnscaled(unscaled, scale);
			return;
		}
		if (!state.value.wide && state.value.scale == scale) {
			if (OP::Better(unscaled, state.value.unscaled)) {
				state.value.unscaled = unscaled;
			}
			return;
		}
		NumericValueRef candidate;
		candidate.scale = scale;
		candidate.unscaled = unscaled;
		if (OP::Replaces(CompareValues(candidate, state.value))) {
			state.value.SetUnscaled(unscaled, scale);
		}
	}

	static inline void MergeBlob(STATE &state, string_t blob, AggregateInputData &input_data) {
		NumericValueRef candidate;
		candidate.wide = true;
		candidate.blob = blob;
		if (!state.is_set) {
			state.is_set = true;
			state.value.SetBlob(blob, input_data);
			return;
		}
		if (OP::Replaces(CompareValues(candidate, state.value))) {
			state.value.SetBlob(blob, input_data);
		}
	}

	struct UpdateOp {
		template <class T>
		static void Typed(const NumericInput &input, STATE *const *states, const UnifiedVectorFormat &sdata,
		                  idx_t count, AggregateInputData &) {
			auto data = UnifiedVectorFormat::GetData<T>(input.data);
			for (idx_t i = 0; i < count; i++) {
				auto idx = input.data.sel->get_index(i);
				if (!input.data.validity.RowIsValid(idx)) {
					continue;
				}
				MergeUnscaled(*states[sdata.sel->get_index(i)], ToUnscaled(data[idx]), input.scale);
			}
		}
		static void Blob(const NumericInput &input, STATE *const *states, const UnifiedVectorFormat &sdata, idx_t count,
		                 AggregateInputData &input_data) {
			for (idx_t i = 0; i < count; i++) {
				auto idx = input.data.sel->get_index(i);
				if (!input.data.validity.RowIsValid(idx)) {
					continue;
				}
				MergeBlob(*states[sdata.sel->get_index(i)], input.Blob(idx), input_data);
			}
		}
	};

	struct ClusterUpdateOp {
		template <class T>
		static void Typed(const NumericInput &input, const ClusteredAggr &clustered, AggregateInputData &) {
			// the extreme of every run in the input representation, merged into its state once
			auto data = UnifiedVectorFormat::GetData<T>(input.data);
			for (auto &run : clustered.runs()) {
				bool found = false;
				T best = T();
				for (idx_t k = 0; k < run.count; k++) {
					auto row = run.sel ? run.sel[k] : k;
					auto idx = input.data.sel->get_index(row);
					if (!input.data.validity.RowIsValid(idx)) {
						continue;
					}
					if (!found || OP::Better(data[idx], best)) {
						best = data[idx];
						found = true;
					}
				}
				if (found) {
					MergeUnscaled(*reinterpret_cast<STATE *>(run.state), ToUnscaled(best), input.scale);
				}
			}
		}
		static void Blob(const NumericInput &input, const ClusteredAggr &clustered, AggregateInputData &input_data) {
			for (auto &run : clustered.runs()) {
				auto &state = *reinterpret_cast<STATE *>(run.state);
				for (idx_t k = 0; k < run.count; k++) {
					auto row = run.sel ? run.sel[k] : k;
					auto idx = input.data.sel->get_index(row);
					if (!input.data.validity.RowIsValid(idx)) {
						continue;
					}
					MergeBlob(state, input.Blob(idx), input_data);
				}
			}
		}
	};

	static void Update(Vector inputs[], AggregateInputData &input_data, idx_t, Vector &state_vector, idx_t count) {
		NumericInput input(inputs[0], count);
		UnifiedVectorFormat sdata;
		auto states = StatePointers<STATE>(state_vector, sdata);
		DispatchInput<UpdateOp>(input, states, sdata, count, input_data);
	}

	static void ClusterUpdate(Vector inputs[], AggregateInputData &input_data, idx_t, const ClusteredAggr &clustered,
	                          idx_t count) {
		NumericInput input(inputs[0], count);
		DispatchInput<ClusterUpdateOp>(input, clustered, input_data);
	}

	static void Combine(Vector &source, Vector &target, AggregateInputData &input_data, idx_t count) {
		auto sources = FlatVector::GetData<STATE *>(source);
		auto targets = FlatVector::GetData<STATE *>(target);
		for (idx_t i = 0; i < count; i++) {
			auto &source_state = *sources[i];
			auto &target_state = *targets[i];
			if (!source_state.is_set) {
				continue;
			}
			if (!target_state.is_set) {
				target_state.is_set = true;
				target_state.value.Set(source_state.value, input_data);
				continue;
			}
			if (OP::Replaces(CompareValues(source_state.value, target_state.value))) {
				target_state.value.Set(source_state.value, input_data);
			}
		}
	}

	static void Finalize(Vector &states, AggregateFinalizeInputData &finalize_input, Vector &result, idx_t count,
	                     idx_t offset) {
		FinalizeStates<STATE>(states, finalize_input, result, count, offset);
	}
};

//===--------------------------------------------------------------------===//
// first / last / any_value
//===--------------------------------------------------------------------===//
struct NumericFirstState {
	//! Whether a row has been recorded
	bool is_set;
	//! Whether the recorded row is not NULL
	bool value_is_valid;
	NumericStateValue value;
};

template <>
inline bool HasValue(const NumericFirstState &state) {
	return state.is_set && state.value_is_valid;
}

template <bool LAST, bool SKIP_NULLS>
struct NumericFirst {
	using STATE = NumericFirstState;

	static void Initialize(AggregateStateInput &, data_ptr_t *states, idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			auto &state = *reinterpret_cast<STATE *>(states[i]);
			state.is_set = false;
			state.value_is_valid = false;
			state.value.wide = false;
			state.value.alloc_size = 0;
		}
	}

	//! Whether row i is recorded into the state; sets the NULL flags when it is
	static inline bool Record(STATE &state, bool is_null) {
		if (SKIP_NULLS && is_null) {
			return false;
		}
		if (!LAST && state.is_set) {
			return false;
		}
		state.is_set = true;
		state.value_is_valid = !is_null;
		return !is_null;
	}

	struct UpdateOp {
		template <class T>
		static void Typed(const NumericInput &input, STATE *const *states, const UnifiedVectorFormat &sdata,
		                  idx_t count, AggregateInputData &) {
			auto data = UnifiedVectorFormat::GetData<T>(input.data);
			for (idx_t i = 0; i < count; i++) {
				auto idx = input.data.sel->get_index(i);
				auto &state = *states[sdata.sel->get_index(i)];
				if (Record(state, !input.data.validity.RowIsValid(idx))) {
					state.value.SetUnscaled(ToUnscaled(data[idx]), input.scale);
				}
			}
		}
		static void Blob(const NumericInput &input, STATE *const *states, const UnifiedVectorFormat &sdata, idx_t count,
		                 AggregateInputData &input_data) {
			for (idx_t i = 0; i < count; i++) {
				auto idx = input.data.sel->get_index(i);
				auto &state = *states[sdata.sel->get_index(i)];
				if (Record(state, !input.data.validity.RowIsValid(idx))) {
					state.value.SetBlob(input.Blob(idx), input_data);
				}
			}
		}
	};

	//! The row of a run that its state records: the last candidate for LAST, otherwise the first
	static bool FindRow(const NumericInput &input, const ClusteredAggr::GroupRun &run, STATE &state, idx_t &row) {
		if (!LAST && state.is_set) {
			return false;
		}
		for (idx_t k = 0; k < run.count; k++) {
			auto position = LAST ? run.count - 1 - k : k;
			row = run.sel ? run.sel[position] : position;
			auto idx = input.data.sel->get_index(row);
			if (!SKIP_NULLS || input.data.validity.RowIsValid(idx)) {
				return true;
			}
		}
		return false;
	}

	struct ClusterUpdateOp {
		template <class T>
		static void Typed(const NumericInput &input, const ClusteredAggr &clustered, AggregateInputData &) {
			auto data = UnifiedVectorFormat::GetData<T>(input.data);
			for (auto &run : clustered.runs()) {
				auto &state = *reinterpret_cast<STATE *>(run.state);
				idx_t row;
				if (!FindRow(input, run, state, row)) {
					continue;
				}
				auto idx = input.data.sel->get_index(row);
				if (Record(state, !input.data.validity.RowIsValid(idx))) {
					state.value.SetUnscaled(ToUnscaled(data[idx]), input.scale);
				}
			}
		}
		static void Blob(const NumericInput &input, const ClusteredAggr &clustered, AggregateInputData &input_data) {
			for (auto &run : clustered.runs()) {
				auto &state = *reinterpret_cast<STATE *>(run.state);
				idx_t row;
				if (!FindRow(input, run, state, row)) {
					continue;
				}
				auto idx = input.data.sel->get_index(row);
				if (Record(state, !input.data.validity.RowIsValid(idx))) {
					state.value.SetBlob(input.Blob(idx), input_data);
				}
			}
		}
	};

	static void Update(Vector inputs[], AggregateInputData &input_data, idx_t, Vector &state_vector, idx_t count) {
		NumericInput input(inputs[0], count);
		UnifiedVectorFormat sdata;
		auto states = StatePointers<STATE>(state_vector, sdata);
		DispatchInput<UpdateOp>(input, states, sdata, count, input_data);
	}

	static void ClusterUpdate(Vector inputs[], AggregateInputData &input_data, idx_t, const ClusteredAggr &clustered,
	                          idx_t count) {
		NumericInput input(inputs[0], count);
		DispatchInput<ClusterUpdateOp>(input, clustered, input_data);
	}

	static void Combine(Vector &source, Vector &target, AggregateInputData &input_data, idx_t count) {
		auto sources = FlatVector::GetData<STATE *>(source);
		auto targets = FlatVector::GetData<STATE *>(target);
		for (idx_t i = 0; i < count; i++) {
			auto &source_state = *sources[i];
			auto &target_state = *targets[i];
			if (!source_state.is_set) {
				continue;
			}
			if (!LAST && target_state.is_set) {
				continue;
			}
			target_state.is_set = true;
			target_state.value_is_valid = source_state.value_is_valid;
			if (source_state.value_is_valid) {
				target_state.value.Set(source_state.value, input_data);
			}
		}
	}

	static void Finalize(Vector &states, AggregateFinalizeInputData &finalize_input, Vector &result, idx_t count,
	                     idx_t offset) {
		FinalizeStates<STATE>(states, finalize_input, result, count, offset);
	}
};

template <class FUNCTION>
idx_t NumericStateSize(AggregateStateInput &) {
	return sizeof(typename FUNCTION::STATE);
}

template <class FUNCTION>
AggregateFunction MakeFunction() {
	return AggregateFunction({LogicalType::NUMERIC}, LogicalType::NUMERIC, NumericStateSize<FUNCTION>,
	                         FUNCTION::Initialize, FUNCTION::Update, FUNCTION::Combine, FUNCTION::Finalize,
	                         FunctionNullHandling::DEFAULT_NULL_HANDLING, FUNCTION::ClusterUpdate);
}

} // namespace

AggregateFunction NumericAggregates::GetMinMax(bool is_min) {
	if (is_min) {
		return MakeFunction<NumericMinMax<MinOp>>();
	}
	return MakeFunction<NumericMinMax<MaxOp>>();
}

AggregateFunction NumericAggregates::GetFirst(bool last, bool skip_nulls) {
	if (last) {
		return skip_nulls ? MakeFunction<NumericFirst<true, true>>() : MakeFunction<NumericFirst<true, false>>();
	}
	return skip_nulls ? MakeFunction<NumericFirst<false, true>>() : MakeFunction<NumericFirst<false, false>>();
}

} // namespace duckdb

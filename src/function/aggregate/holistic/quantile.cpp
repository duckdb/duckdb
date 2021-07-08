#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate/holistic_functions.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include <algorithm>
#include <queue>
#include <stdlib.h>
#include <utility>

namespace duckdb {

template <>
timestamp_t Cast::Operation(date_t date) {
	return Timestamp::FromDatetime(date, dtime_t(0));
}

using FrameBounds = std::pair<idx_t, idx_t>;

struct QuantileState {
	data_ptr_t v;
	idx_t len;
	idx_t pos;

	// List temporaries
	std::vector<idx_t> disturbed;
	std::vector<idx_t> lower;
	std::vector<idx_t> upper;

	QuantileState() : v(nullptr), len(0), pos(0) {
	}

	~QuantileState() {
		if (v) {
			free(v);
			v = nullptr;
		}
	}

	template <typename T>
	void Resize(idx_t new_len) {
		if (new_len <= len) {
			return;
		}
		v = (data_ptr_t)realloc(v, new_len * sizeof(T));
		if (!v) {
			throw InternalException("Memory allocation failure");
		}
		len = new_len;
	}
};

void ReuseIndexes(idx_t *index, const FrameBounds &frame, const FrameBounds &prev) {
	idx_t j = 0;

	//  Copy overlapping indices
	for (idx_t p = 0; p < (prev.second - prev.first); ++p) {
		auto idx = index[p];

		//  Shift down into any hole
		if (j != p) {
			index[j] = idx;
		}

		//  Skip overlapping values
		if (frame.first <= idx && idx < frame.second) {
			++j;
		}
	}

	//  Insert new indices
	if (j > 0) {
		// Overlap: append the new ends
		for (auto f = frame.first; f < prev.first; ++f, ++j) {
			index[j] = f;
		}
		for (auto f = prev.second; f < frame.second; ++f, ++j) {
			index[j] = f;
		}
	} else {
		//  No overlap: overwrite with new values
		for (auto f = frame.first; f < frame.second; ++f, ++j) {
			index[j] = f;
		}
	}
}

template <class STATE>
static idx_t ReplaceIndex(STATE *state, const FrameBounds &frame, const FrameBounds &prev) {
	D_ASSERT(state->v);
	auto index = (idx_t *)state->v;

	idx_t j = 0;
	for (idx_t p = 0; p < (prev.second - prev.first); ++p) {
		auto idx = index[p];
		if (j != p) {
			break;
		}

		if (frame.first <= idx && idx < frame.second) {
			++j;
		}
	}
	index[j] = frame.second - 1;

	return j;
}

template <class INPUT_TYPE, class STATE>
static inline bool CanReplace(STATE *state, const INPUT_TYPE *fdata, const idx_t j, const idx_t k0, const idx_t k1) {
	auto same = false;

	D_ASSERT(state->v);
	auto index = (idx_t *)state->v;

	auto curr = fdata[index[j]];
	if (k1 < j) {
		auto hi = fdata[index[k1]];
		same = hi < curr;
	} else if (j < k0) {
		auto lo = fdata[index[k0]];
		same = curr < lo;
	}

	return same;
}

struct IndirectNotNull {
	inline explicit IndirectNotNull(const ValidityMask &mask_p, idx_t bias_p) : mask(mask_p), bias(bias_p) {
	}

	inline bool operator()(const idx_t &idx) const {
		return mask.RowIsValid(idx - bias);
	}
	const ValidityMask &mask;
	const idx_t bias;
};

template <class INPUT_TYPE>
struct IndirectLess {
	inline explicit IndirectLess(const INPUT_TYPE *inputs_p) : inputs(inputs_p) {
	}

	inline bool operator()(const idx_t &lhi, const idx_t &rhi) const {
		return inputs[lhi] < inputs[rhi];
	}

	const INPUT_TYPE *inputs;
};

struct QuantileBindData : public FunctionData {
	explicit QuantileBindData(float quantile_p) : quantiles(1, quantile_p), order(1, 0) {
	}

	explicit QuantileBindData(const vector<float> &quantiles_p) : quantiles(quantiles_p) {
		for (idx_t i = 0; i < quantiles.size(); ++i) {
			order.push_back(i);
		}

		IndirectLess<float> lt(quantiles.data());
		std::sort(order.begin(), order.end(), lt);
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<QuantileBindData>(quantiles);
	}

	bool Equals(FunctionData &other_p) override {
		auto &other = (QuantileBindData &)other_p;
		return quantiles == other.quantiles;
	}

	vector<float> quantiles;
	vector<idx_t> order;
};

template <typename SAVE_TYPE>
struct QuantileOperation {
	template <class STATE>
	static void Initialize(STATE *state) {
		new (state) STATE;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, FunctionData *bind_data, INPUT_TYPE *input, ValidityMask &mask,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, bind_data, input, mask, 0);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data_p, INPUT_TYPE *data, ValidityMask &mask, idx_t idx) {
		if (state->pos == state->len) {
			// growing conservatively here since we could be running this on many small groups
			state->template Resize<SAVE_TYPE>(state->len == 0 ? 1 : state->len * 2);
		}
		D_ASSERT(state->v);
		((SAVE_TYPE *)state->v)[state->pos++] = data[idx];
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target) {
		if (source.pos == 0) {
			return;
		}
		target->template Resize<SAVE_TYPE>(target->pos + source.pos);
		memcpy(target->v + target->pos * sizeof(SAVE_TYPE), source.v, source.pos * sizeof(SAVE_TYPE));
		target->pos += source.pos;
	}

	template <class STATE>
	static void Destroy(STATE *state) {
		state->~STATE();
	}

	static bool IgnoreNull() {
		return true;
	}
};

template <class STATE_TYPE, class RESULT_TYPE, class OP>
static void ExecuteListFinalize(Vector &states, FunctionData *bind_data, Vector &result, idx_t count) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);

	if (states.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);

		auto sdata = ConstantVector::GetData<STATE_TYPE *>(states);
		auto rdata = ConstantVector::GetData<RESULT_TYPE>(result);
		auto &mask = ConstantVector::Validity(result);
		OP::template Finalize<RESULT_TYPE, STATE_TYPE>(result, bind_data, sdata[0], rdata, mask, 0);
	} else {
		D_ASSERT(states.GetVectorType() == VectorType::FLAT_VECTOR);
		result.SetVectorType(VectorType::FLAT_VECTOR);

		auto sdata = FlatVector::GetData<STATE_TYPE *>(states);
		auto rdata = FlatVector::GetData<RESULT_TYPE>(result);
		auto &mask = FlatVector::Validity(result);
		for (idx_t i = 0; i < count; i++) {
			OP::template Finalize<RESULT_TYPE, STATE_TYPE>(result, bind_data, sdata[i], rdata, mask, i);
		}
	}

	result.Verify(count);
}

template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
static AggregateFunction QuantileListAggregate(const LogicalType &input_type, const LogicalType &child_type) {
	LogicalType result_type = LogicalType::LIST(child_type);
	return AggregateFunction(
	    {input_type}, result_type, AggregateFunction::StateSize<STATE>, AggregateFunction::StateInitialize<STATE, OP>,
	    AggregateFunction::UnaryScatterUpdate<STATE, INPUT_TYPE, OP>, AggregateFunction::StateCombine<STATE, OP>,
	    ExecuteListFinalize<STATE, RESULT_TYPE, OP>, AggregateFunction::UnaryUpdate<STATE, INPUT_TYPE, OP>, nullptr,
	    AggregateFunction::StateDestroy<STATE, OP>);
}

template <class SAVE_TYPE>
struct DiscreteQuantileOperation : public QuantileOperation<SAVE_TYPE> {

	template <class RESULT_TYPE, class STATE>
	static void Finalize(Vector &result, FunctionData *bind_data_p, STATE *state, RESULT_TYPE *target,
	                     ValidityMask &mask, idx_t idx) {
		if (state->pos == 0) {
			mask.SetInvalid(idx);
			return;
		}
		D_ASSERT(state->v);
		D_ASSERT(bind_data_p);
		auto bind_data = (QuantileBindData *)bind_data_p;
		D_ASSERT(bind_data->quantiles.size() == 1);
		auto v_t = (SAVE_TYPE *)state->v;
		auto offset = (idx_t)((double)(state->pos - 1) * bind_data->quantiles[0]);
		std::nth_element(v_t, v_t + offset, v_t + state->pos);
		target[idx] = v_t[offset];
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE>
	static void Window(const INPUT_TYPE *data, const ValidityMask &dmask, FunctionData *bind_data_p, STATE *state,
	                   const FrameBounds &frame, const FrameBounds &prev, Vector &result) {
		auto rdata = ConstantVector::GetData<RESULT_TYPE>(result);
		auto &rmask = ConstantVector::Validity(result);

		//  Lazily initialise frame state
		state->pos = frame.second - frame.first;
		state->template Resize<idx_t>(state->pos);

		D_ASSERT(state->v);
		auto index = (idx_t *)state->v;

		D_ASSERT(bind_data_p);
		auto bind_data = (QuantileBindData *)bind_data_p;
		auto offset = (idx_t)(double(state->pos - 1) * bind_data->quantiles[0]);
		auto same = false;

		if (dmask.AllValid() && frame.first == prev.first + 1 && frame.second == prev.second + 1) {
			//  Fixed frame size
			auto j = ReplaceIndex(state, frame, prev);
			same = CanReplace(state, data, j, offset, offset);
		} else {
			ReuseIndexes(index, frame, prev);
		}

		if (!same) {
			auto valid = state->pos;
			if (!dmask.AllValid()) {
				IndirectNotNull not_null(dmask, MinValue(frame.first, prev.first));
				valid = std::partition(index, index + valid, not_null) - index;
				offset = (idx_t)(double(valid - 1) * bind_data->quantiles[0]);
			}
			if (valid) {
				IndirectLess<INPUT_TYPE> lt(data);
				std::nth_element(index, index + offset, index + valid, lt);
			} else {
				rmask.Set(0, false);
			}
		}
		rdata[0] = RESULT_TYPE(data[index[offset]]);
	}
};

template <typename INPUT_TYPE>
AggregateFunction GetTypedDiscreteQuantileAggregateFunction(const LogicalType &type) {
	using STATE = QuantileState;
	using OP = DiscreteQuantileOperation<INPUT_TYPE>;
	auto fun = AggregateFunction::UnaryAggregateDestructor<STATE, INPUT_TYPE, INPUT_TYPE, OP>(type, type);
	fun.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, INPUT_TYPE, OP>;
	return fun;
}

AggregateFunction GetDiscreteQuantileAggregateFunction(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return GetTypedDiscreteQuantileAggregateFunction<int8_t>(type);
	case LogicalTypeId::SMALLINT:
		return GetTypedDiscreteQuantileAggregateFunction<int16_t>(type);
	case LogicalTypeId::INTEGER:
		return GetTypedDiscreteQuantileAggregateFunction<int32_t>(type);
	case LogicalTypeId::BIGINT:
		return GetTypedDiscreteQuantileAggregateFunction<int64_t>(type);
	case LogicalTypeId::HUGEINT:
		return GetTypedDiscreteQuantileAggregateFunction<hugeint_t>(type);

	case LogicalTypeId::FLOAT:
		return GetTypedDiscreteQuantileAggregateFunction<float>(type);
	case LogicalTypeId::DOUBLE:
		return GetTypedDiscreteQuantileAggregateFunction<double>(type);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return GetTypedDiscreteQuantileAggregateFunction<int16_t>(type);
		case PhysicalType::INT32:
			return GetTypedDiscreteQuantileAggregateFunction<int32_t>(type);
		case PhysicalType::INT64:
			return GetTypedDiscreteQuantileAggregateFunction<int64_t>(type);
		case PhysicalType::INT128:
			return GetTypedDiscreteQuantileAggregateFunction<hugeint_t>(type);
		default:
			throw NotImplementedException("Unimplemented discrete quantile aggregate");
		}
		break;

	case LogicalTypeId::DATE:
		return GetTypedDiscreteQuantileAggregateFunction<int32_t>(type);
	case LogicalTypeId::TIMESTAMP:
		return GetTypedDiscreteQuantileAggregateFunction<int64_t>(type);
	case LogicalTypeId::TIME:
		return GetTypedDiscreteQuantileAggregateFunction<int64_t>(type);
	case LogicalTypeId::INTERVAL:
		return GetTypedDiscreteQuantileAggregateFunction<interval_t>(type);

	default:
		throw NotImplementedException("Unimplemented discrete quantile aggregate");
	}
}

template <class SAVE_TYPE>
struct DiscreteQuantileListOperation : public QuantileOperation<SAVE_TYPE> {

	template <class RESULT_TYPE, class STATE>
	static void Finalize(Vector &result_list, FunctionData *bind_data_p, STATE *state, RESULT_TYPE *target,
	                     ValidityMask &mask, idx_t idx) {
		if (state->pos == 0) {
			mask.SetInvalid(idx);
			return;
		}
		D_ASSERT(state->v);
		D_ASSERT(bind_data_p);
		auto bind_data = (QuantileBindData *)bind_data_p;
		target[idx].offset = ListVector::GetListSize(result_list);
		auto v_t = (SAVE_TYPE *)state->v;
		for (const auto &quantile : bind_data->quantiles) {
			auto offset = (idx_t)((double)(state->pos - 1) * quantile);
			std::nth_element(v_t, v_t + offset, v_t + state->pos);
			auto val = Value::CreateValue(v_t[offset]);
			ListVector::PushBack(result_list, val);
		}
		target[idx].length = bind_data->quantiles.size();
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE>
	static void Window(const INPUT_TYPE *data, const ValidityMask &dmask, FunctionData *bind_data_p, STATE *state,
	                   const FrameBounds &frame, const FrameBounds &prev, Vector &list) {
		D_ASSERT(bind_data_p);
		auto bind_data = (QuantileBindData *)bind_data_p;

		// Result is a constant LIST<RESULT_TYPE> with a fixed size
		auto ldata = ConstantVector::GetData<list_entry_t>(list);
		auto &lmask = ConstantVector::Validity(list);
		ldata[0].offset = 0;
		ldata[0].length = bind_data->quantiles.size();

		ListVector::SetListSize(list, ldata[0].length);
		auto &result = ListVector::GetEntry(list);
		auto rdata = FlatVector::GetData<RESULT_TYPE>(result);

		//  Lazily initialise frame state
		state->pos = frame.second - frame.first;
		state->template Resize<idx_t>(state->pos);

		D_ASSERT(state->v);
		auto index = (idx_t *)state->v;

		bool fixed = false;
		auto j = state->pos;
		auto valid = state->pos;
		if (dmask.AllValid() && frame.first == prev.first + 1 && frame.second == prev.second + 1) {
			//  Fixed frame size
			j = ReplaceIndex(state, frame, prev);
			fixed = true;
		} else {
			ReuseIndexes(index, frame, prev);
			if (!dmask.AllValid()) {
				IndirectNotNull not_null(dmask, MinValue(frame.first, prev.first));
				valid = std::partition(index, index + valid, not_null) - index;
			}
		}

		if (!valid) {
			lmask.Set(0, false);
			return;
		}

		// First pass: Fill in the undisturbed values and find the islands of stability.
		state->disturbed.clear();
		state->lower.clear();
		state->upper.clear();
		idx_t lb = 0;
		for (idx_t i = 0; i < bind_data->order.size(); ++i) {
			const auto q = bind_data->order[i];
			const auto &quantile = bind_data->quantiles[q];
			auto offset = (idx_t)(double(valid - 1) * quantile);
			if (fixed && CanReplace(state, data, j, offset, offset)) {
				rdata[q] = RESULT_TYPE(data[index[offset]]);
				state->upper.resize(state->lower.size(), offset);
				lb = offset + 1;
			} else {
				state->disturbed.push_back(q);
				state->lower.push_back(lb);
			}
		}
		state->upper.resize(state->lower.size(), valid);

		//	Second pass: select the disturbed values
		IndirectLess<INPUT_TYPE> lt(data);
		for (idx_t i = 0; i < state->disturbed.size(); ++i) {
			const auto &q = state->disturbed[i];
			const auto &quantile = bind_data->quantiles[q];
			const auto offset = (idx_t)(double(valid - 1) * quantile);

			std::nth_element(index + state->lower[i], index + offset, index + state->upper[i], lt);
			rdata[q] = RESULT_TYPE(data[index[offset]]);
		}
	}
};

template <typename INPUT_TYPE>
AggregateFunction GetTypedDiscreteQuantileListAggregateFunction(const LogicalType &type) {
	using STATE = QuantileState;
	using OP = DiscreteQuantileListOperation<INPUT_TYPE>;
	auto fun = QuantileListAggregate<STATE, INPUT_TYPE, list_entry_t, OP>(type, type);
	fun.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, INPUT_TYPE, OP>;
	return fun;
}

AggregateFunction GetDiscreteQuantileListAggregateFunction(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return GetTypedDiscreteQuantileListAggregateFunction<int8_t>(type);
	case LogicalTypeId::SMALLINT:
		return GetTypedDiscreteQuantileListAggregateFunction<int16_t>(type);
	case LogicalTypeId::INTEGER:
		return GetTypedDiscreteQuantileListAggregateFunction<int32_t>(type);
	case LogicalTypeId::BIGINT:
		return GetTypedDiscreteQuantileListAggregateFunction<int64_t>(type);
	case LogicalTypeId::HUGEINT:
		return GetTypedDiscreteQuantileListAggregateFunction<hugeint_t>(type);

	case LogicalTypeId::FLOAT:
		return GetTypedDiscreteQuantileListAggregateFunction<float>(type);
	case LogicalTypeId::DOUBLE:
		return GetTypedDiscreteQuantileListAggregateFunction<double>(type);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return GetTypedDiscreteQuantileListAggregateFunction<int16_t>(type);
		case PhysicalType::INT32:
			return GetTypedDiscreteQuantileListAggregateFunction<int32_t>(type);
		case PhysicalType::INT64:
			return GetTypedDiscreteQuantileListAggregateFunction<int64_t>(type);
		case PhysicalType::INT128:
			return GetTypedDiscreteQuantileListAggregateFunction<hugeint_t>(type);
		default:
			throw NotImplementedException("Unimplemented discrete quantile list aggregate");
		}
		break;

	case LogicalTypeId::DATE:
		return GetTypedDiscreteQuantileListAggregateFunction<date_t>(type);
	case LogicalTypeId::TIMESTAMP:
		return GetTypedDiscreteQuantileListAggregateFunction<timestamp_t>(type);
	case LogicalTypeId::TIME:
		return GetTypedDiscreteQuantileListAggregateFunction<dtime_t>(type);
	case LogicalTypeId::INTERVAL:
		return GetTypedDiscreteQuantileListAggregateFunction<interval_t>(type);

	default:
		throw NotImplementedException("Unimplemented discrete quantile list aggregate");
	}
}

template <class INPUT_TYPE, class TARGET_TYPE, bool DISCRETE>
static TARGET_TYPE Interpolate(INPUT_TYPE *v_t, const float q, const idx_t n) {
	const auto RN = ((double)(n - 1) * q);
	const auto FRN = idx_t(floor(RN));
	const auto CRN = idx_t(ceil(RN));

	if (DISCRETE || CRN == FRN) {
		std::nth_element(v_t, v_t + FRN, v_t + n);
		return Cast::Operation<INPUT_TYPE, TARGET_TYPE>(v_t[FRN]);
	} else {
		std::nth_element(v_t, v_t + FRN, v_t + n);
		std::nth_element(v_t + FRN, v_t + CRN, v_t + n);
		auto lo = Cast::Operation<INPUT_TYPE, TARGET_TYPE>(v_t[FRN]);
		auto hi = Cast::Operation<INPUT_TYPE, TARGET_TYPE>(v_t[CRN]);
		auto delta = hi - lo;
		return lo + delta * (RN - FRN);
	}
}

template <class INPUT_TYPE, class TARGET_TYPE, bool DISCRETE>
static TARGET_TYPE Interpolate(const INPUT_TYPE *v_t, const idx_t *index, const float q, const idx_t n) {
	const auto RN = ((double)(n - 1) * q);
	const auto FRN = idx_t(floor(RN));
	const auto CRN = idx_t(ceil(RN));

	if (DISCRETE || CRN == FRN) {
		return Cast::Operation<INPUT_TYPE, TARGET_TYPE>(v_t[index[FRN]]);
	} else {
		auto lo = Cast::Operation<INPUT_TYPE, TARGET_TYPE>(v_t[index[FRN]]);
		auto hi = Cast::Operation<INPUT_TYPE, TARGET_TYPE>(v_t[index[CRN]]);
		auto delta = hi - lo;
		return lo + delta * (RN - FRN);
	}
}

template <class SAVE_TYPE>
struct ContinuousQuantileOperation : public QuantileOperation<SAVE_TYPE> {

	template <class RESULT_TYPE, class STATE>
	static void Finalize(Vector &result, FunctionData *bind_data_p, STATE *state, RESULT_TYPE *target,
	                     ValidityMask &mask, idx_t idx) {
		if (state->pos == 0) {
			mask.SetInvalid(idx);
			return;
		}
		D_ASSERT(state->v);
		D_ASSERT(bind_data_p);
		auto bind_data = (QuantileBindData *)bind_data_p;
		D_ASSERT(bind_data->quantiles.size() == 1);
		auto v_t = (SAVE_TYPE *)state->v;
		target[idx] = Interpolate<SAVE_TYPE, RESULT_TYPE, false>(v_t, bind_data->quantiles[0], state->pos);
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE>
	static void Window(const INPUT_TYPE *data, const ValidityMask &dmask, FunctionData *bind_data_p, STATE *state,
	                   const FrameBounds &frame, const FrameBounds &prev, Vector &result) {
		auto rdata = ConstantVector::GetData<RESULT_TYPE>(result);
		auto &rmask = ConstantVector::Validity(result);

		//  Lazily initialise frame state
		state->pos = frame.second - frame.first;
		state->template Resize<idx_t>(state->pos);

		D_ASSERT(state->v);
		auto index = (idx_t *)state->v;

		D_ASSERT(bind_data_p);
		auto bind_data = (QuantileBindData *)bind_data_p;

		// Find the two positions needed
		const auto q = bind_data->quantiles[0];

		bool same = false;
		if (dmask.AllValid() && frame.first == prev.first + 1 && frame.second == prev.second + 1) {
			//  Fixed frame size
			const auto RN = ((double)(state->pos - 1) * q);
			const auto FRN = idx_t(floor(RN));
			const auto CRN = idx_t(ceil(RN));
			const auto j = ReplaceIndex(state, frame, prev);
			same = CanReplace(state, data, j, FRN, CRN);
		} else {
			ReuseIndexes(index, frame, prev);
		}

		if (!same) {
			auto valid = state->pos;
			if (!dmask.AllValid()) {
				IndirectNotNull not_null(dmask, MinValue(frame.first, prev.first));
				valid = std::partition(index, index + valid, not_null) - index;
			}
			if (valid) {
				// Update the floor/ceiling
				const auto RN = ((double)(valid - 1) * q);
				const auto FRN = idx_t(floor(RN));
				const auto CRN = idx_t(ceil(RN));
				IndirectLess<INPUT_TYPE> lt(data);
				std::nth_element(index, index + FRN, index + valid, lt);
				if (CRN != FRN) {
					std::nth_element(index + CRN, index + CRN, index + valid, lt);
				}
				rdata[0] = Interpolate<INPUT_TYPE, RESULT_TYPE, false>(data, index, q, valid);
			} else {
				rmask.Set(0, false);
			}
		} else {
			rdata[0] = Interpolate<INPUT_TYPE, RESULT_TYPE, false>(data, index, q, state->pos);
		}
	}
};

template <typename INPUT_TYPE, typename TARGET_TYPE>
AggregateFunction GetTypedContinuousQuantileAggregateFunction(const LogicalType &input_type,
                                                              const LogicalType &target_type) {
	using STATE = QuantileState;
	using OP = ContinuousQuantileOperation<INPUT_TYPE>;
	auto fun = AggregateFunction::UnaryAggregateDestructor<STATE, INPUT_TYPE, TARGET_TYPE, OP>(input_type, target_type);
	fun.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, TARGET_TYPE, OP>;
	return fun;
}

AggregateFunction GetContinuousQuantileAggregateFunction(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return GetTypedContinuousQuantileAggregateFunction<int8_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::SMALLINT:
		return GetTypedContinuousQuantileAggregateFunction<int16_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::INTEGER:
		return GetTypedContinuousQuantileAggregateFunction<int32_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::BIGINT:
		return GetTypedContinuousQuantileAggregateFunction<int64_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::HUGEINT:
		return GetTypedContinuousQuantileAggregateFunction<hugeint_t, double>(type, LogicalType::DOUBLE);

	case LogicalTypeId::FLOAT:
		return GetTypedContinuousQuantileAggregateFunction<float, float>(type, type);
	case LogicalTypeId::DOUBLE:
		return GetTypedContinuousQuantileAggregateFunction<double, double>(type, type);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return GetTypedContinuousQuantileAggregateFunction<int16_t, int16_t>(type, type);
		case PhysicalType::INT32:
			return GetTypedContinuousQuantileAggregateFunction<int32_t, int32_t>(type, type);
		case PhysicalType::INT64:
			return GetTypedContinuousQuantileAggregateFunction<int64_t, int64_t>(type, type);
		case PhysicalType::INT128:
			return GetTypedContinuousQuantileAggregateFunction<hugeint_t, hugeint_t>(type, type);
		default:
			throw NotImplementedException("Unimplemented discrete quantile list aggregate");
		}
		break;

	case LogicalTypeId::DATE:
		return GetTypedContinuousQuantileAggregateFunction<date_t, timestamp_t>(type, LogicalType::TIMESTAMP);
	case LogicalTypeId::TIMESTAMP:
		return GetTypedContinuousQuantileAggregateFunction<timestamp_t, timestamp_t>(type, type);
	case LogicalTypeId::TIME:
		return GetTypedContinuousQuantileAggregateFunction<dtime_t, dtime_t>(type, type);

	default:
		throw NotImplementedException("Unimplemented discrete quantile list aggregate");
	}
}

template <class SAVE_TYPE, class CHILD_TYPE>
struct ContinuousQuantileListOperation : public QuantileOperation<SAVE_TYPE> {

	template <class RESULT_TYPE, class STATE>
	static void Finalize(Vector &result_list, FunctionData *bind_data_p, STATE *state, RESULT_TYPE *target,
	                     ValidityMask &mask, idx_t idx) {
		if (state->pos == 0) {
			mask.SetInvalid(idx);
			return;
		}
		D_ASSERT(state->v);
		D_ASSERT(bind_data_p);
		auto bind_data = (QuantileBindData *)bind_data_p;
		target[idx].offset = ListVector::GetListSize(result_list);
		auto v_t = (SAVE_TYPE *)state->v;
		for (const auto &quantile : bind_data->quantiles) {
			auto child = Interpolate<SAVE_TYPE, CHILD_TYPE, false>(v_t, quantile, state->pos);
			auto val = Value::CreateValue(child);
			ListVector::PushBack(result_list, val);
		}
		target[idx].length = bind_data->quantiles.size();
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE>
	static void Window(const INPUT_TYPE *data, const ValidityMask &dmask, FunctionData *bind_data_p, STATE *state,
	                   const FrameBounds &frame, const FrameBounds &prev, Vector &list) {
		D_ASSERT(bind_data_p);
		auto bind_data = (QuantileBindData *)bind_data_p;

		// Result is a constant LIST<RESULT_TYPE> with a fixed size
		auto ldata = ConstantVector::GetData<list_entry_t>(list);
		auto &lmask = ConstantVector::Validity(list);
		ldata[0].offset = 0;
		ldata[0].length = bind_data->quantiles.size();

		ListVector::SetListSize(list, ldata[0].length);
		auto &result = ListVector::GetEntry(list);
		auto rdata = FlatVector::GetData<RESULT_TYPE>(result);

		//  Lazily initialise frame state
		state->pos = frame.second - frame.first;
		state->template Resize<idx_t>(state->pos);

		D_ASSERT(state->v);
		auto index = (idx_t *)state->v;

		bool fixed = false;
		auto j = state->pos;
		auto valid = state->pos;
		if (dmask.AllValid() && frame.first == prev.first + 1 && frame.second == prev.second + 1) {
			//  Fixed frame size
			j = ReplaceIndex(state, frame, prev);
			fixed = true;
		} else {
			ReuseIndexes(index, frame, prev);
			if (!dmask.AllValid()) {
				IndirectNotNull not_null(dmask, MinValue(frame.first, prev.first));
				valid = std::partition(index, index + valid, not_null) - index;
			}
		}

		if (!valid) {
			lmask.Set(0, false);
			return;
		}

		// First pass: Fill in the undisturbed values and find the islands of stability.
		state->disturbed.clear();
		state->lower.clear();
		state->upper.clear();
		idx_t lb = 0;
		for (idx_t i = 0; i < bind_data->order.size(); ++i) {
			const auto q = bind_data->order[i];
			const auto &quantile = bind_data->quantiles[q];
			const auto RN = ((double)(state->pos - 1) * quantile);
			const auto FRN = idx_t(floor(RN));
			const auto CRN = idx_t(ceil(RN));

			if (fixed && CanReplace(state, data, j, FRN, CRN)) {
				rdata[q] = Interpolate<INPUT_TYPE, RESULT_TYPE, false>(data, index, quantile, state->pos);
				state->upper.resize(state->lower.size(), FRN);
				lb = CRN + 1;
			} else {
				state->disturbed.push_back(q);
				state->lower.push_back(lb);
			}
		}
		state->upper.resize(state->lower.size(), valid);

		//	Second pass: select the disturbed values
		IndirectLess<INPUT_TYPE> lt(data);
		for (idx_t i = 0; i < state->disturbed.size(); ++i) {
			const auto &q = state->disturbed[i];
			const auto &quantile = bind_data->quantiles[q];
			// Update the floor/ceiling
			const auto RN = ((double)(valid - 1) * quantile);
			const auto FRN = idx_t(floor(RN));
			const auto CRN = idx_t(ceil(RN));
			IndirectLess<INPUT_TYPE> lt(data);
			std::nth_element(index + state->lower[i], index + FRN, index + state->upper[i], lt);
			if (CRN != FRN) {
				std::nth_element(index + CRN, index + CRN, index + state->upper[i], lt);
			}
			rdata[q] = Interpolate<INPUT_TYPE, RESULT_TYPE, false>(data, index, quantile, valid);
		}
	}
};

template <typename INPUT_TYPE, typename CHILD_TYPE>
AggregateFunction GetTypedContinuousQuantileListAggregateFunction(const LogicalType &input_type,
                                                                  const LogicalType &result_type) {
	using STATE = QuantileState;
	using OP = ContinuousQuantileListOperation<INPUT_TYPE, CHILD_TYPE>;
	auto fun = QuantileListAggregate<STATE, INPUT_TYPE, list_entry_t, OP>(input_type, result_type);
	fun.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, CHILD_TYPE, OP>;
	return fun;
}

AggregateFunction GetContinuousQuantileListAggregateFunction(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return GetTypedContinuousQuantileListAggregateFunction<int8_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::SMALLINT:
		return GetTypedContinuousQuantileListAggregateFunction<int16_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::INTEGER:
		return GetTypedContinuousQuantileListAggregateFunction<int32_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::BIGINT:
		return GetTypedContinuousQuantileListAggregateFunction<int64_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::HUGEINT:
		return GetTypedContinuousQuantileListAggregateFunction<hugeint_t, double>(type, LogicalType::DOUBLE);

	case LogicalTypeId::FLOAT:
		return GetTypedContinuousQuantileListAggregateFunction<float, float>(type, type);
	case LogicalTypeId::DOUBLE:
		return GetTypedContinuousQuantileListAggregateFunction<double, double>(type, type);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return GetTypedContinuousQuantileListAggregateFunction<int16_t, double>(type, LogicalType::DOUBLE);
		case PhysicalType::INT32:
			return GetTypedContinuousQuantileListAggregateFunction<int32_t, double>(type, LogicalType::DOUBLE);
		case PhysicalType::INT64:
			return GetTypedContinuousQuantileListAggregateFunction<int64_t, double>(type, LogicalType::DOUBLE);
		case PhysicalType::INT128:
			return GetTypedContinuousQuantileListAggregateFunction<hugeint_t, double>(type, LogicalType::DOUBLE);
		default:
			throw NotImplementedException("Unimplemented discrete quantile list aggregate");
		}
		break;

	case LogicalTypeId::DATE:
		return GetTypedContinuousQuantileListAggregateFunction<date_t, timestamp_t>(type, LogicalType::TIMESTAMP);
	case LogicalTypeId::TIMESTAMP:
		return GetTypedContinuousQuantileListAggregateFunction<timestamp_t, timestamp_t>(type, type);
	case LogicalTypeId::TIME:
		return GetTypedContinuousQuantileListAggregateFunction<dtime_t, dtime_t>(type, type);

	default:
		throw NotImplementedException("Unimplemented discrete quantile list aggregate");
	}
}

unique_ptr<FunctionData> BindMedian(ClientContext &context, AggregateFunction &function,
                                    vector<unique_ptr<Expression>> &arguments) {
	return make_unique<QuantileBindData>(0.5);
}

unique_ptr<FunctionData> BindMedianDecimal(ClientContext &context, AggregateFunction &function,
                                           vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindMedian(context, function, arguments);

	function = GetDiscreteQuantileAggregateFunction(arguments[0]->return_type);
	function.name = "median";
	return bind_data;
}

static float CheckQuantile(const Value &quantile_val) {
	auto quantile = quantile_val.GetValue<float>();

	if (quantile_val.is_null || quantile < 0 || quantile > 1) {
		throw BinderException("QUANTILE can only take parameters in the range [0, 1]");
	}

	return quantile;
}

unique_ptr<FunctionData> BindQuantile(ClientContext &context, AggregateFunction &function,
                                      vector<unique_ptr<Expression>> &arguments) {
	if (!arguments[1]->IsScalar()) {
		throw BinderException("QUANTILE can only take constant parameters");
	}
	Value quantile_val = ExpressionExecutor::EvaluateScalar(*arguments[1]);
	vector<float> quantiles;
	if (quantile_val.type().id() != LogicalTypeId::LIST) {
		quantiles.push_back(CheckQuantile(quantile_val));
	} else {
		for (const auto &element_val : quantile_val.list_value) {
			quantiles.push_back(CheckQuantile(element_val));
		}
	}

	arguments.pop_back();
	return make_unique<QuantileBindData>(quantiles);
}

unique_ptr<FunctionData> BindDiscreteQuantileDecimal(ClientContext &context, AggregateFunction &function,
                                                     vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindQuantile(context, function, arguments);
	function = GetDiscreteQuantileAggregateFunction(arguments[0]->return_type);
	function.name = "quantile_disc";
	return bind_data;
}

unique_ptr<FunctionData> BindContinuousQuantileDecimal(ClientContext &context, AggregateFunction &function,
                                                       vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindQuantile(context, function, arguments);
	function = GetContinuousQuantileAggregateFunction(arguments[0]->return_type);
	function.name = "quantile_cont";
	return bind_data;
}

AggregateFunction GetMedianAggregate(const LogicalType &type) {
	auto fun = (type.id() != LogicalTypeId::INTERVAL) ? GetContinuousQuantileAggregateFunction(type)
	                                                  : GetDiscreteQuantileAggregateFunction(type);
	fun.bind = BindMedian;
	return fun;
}

AggregateFunction GetDiscreteQuantileAggregate(const LogicalType &type) {
	auto fun = GetDiscreteQuantileAggregateFunction(type);
	fun.bind = BindQuantile;
	// temporarily push an argument so we can bind the actual quantile
	fun.arguments.push_back(LogicalType::FLOAT);
	return fun;
}

AggregateFunction GetDiscreteQuantileListAggregate(const LogicalType &type) {
	auto fun = GetDiscreteQuantileListAggregateFunction(type);
	fun.bind = BindQuantile;
	// temporarily push an argument so we can bind the actual quantile
	auto list_of_float = LogicalType::LIST(LogicalType::FLOAT);
	fun.arguments.push_back(list_of_float);
	return fun;
}

AggregateFunction GetContinuousQuantileAggregate(const LogicalType &type) {
	auto fun = GetContinuousQuantileAggregateFunction(type);
	fun.bind = BindQuantile;
	// temporarily push an argument so we can bind the actual quantile
	fun.arguments.push_back(LogicalType::FLOAT);
	return fun;
}

AggregateFunction GetContinuousQuantileListAggregate(const LogicalType &type) {
	auto fun = GetContinuousQuantileListAggregateFunction(type);
	fun.bind = BindQuantile;
	// temporarily push an argument so we can bind the actual quantile
	auto list_of_double = LogicalType::LIST(LogicalType::DOUBLE);
	fun.arguments.push_back(list_of_double);
	return fun;
}

void QuantileFun::RegisterFunction(BuiltinFunctions &set) {
	const vector<LogicalType> QUANTILES = {LogicalType::TINYINT, LogicalType::SMALLINT, LogicalType::INTEGER,
	                                       LogicalType::BIGINT,  LogicalType::HUGEINT,  LogicalType::FLOAT,
	                                       LogicalType::DOUBLE,  LogicalType::DATE,     LogicalType::TIMESTAMP,
	                                       LogicalType::TIME,    LogicalType::INTERVAL};

	AggregateFunctionSet median("median");
	median.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL, nullptr, nullptr, nullptr,
	                                     nullptr, nullptr, nullptr, BindMedianDecimal));

	AggregateFunctionSet quantile_disc("quantile_disc");
	quantile_disc.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL, LogicalType::FLOAT}, LogicalTypeId::DECIMAL,
	                                            nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                            BindDiscreteQuantileDecimal));

	AggregateFunctionSet quantile_cont("quantile_cont");
	quantile_cont.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL, LogicalType::FLOAT}, LogicalTypeId::DECIMAL,
	                                            nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                            BindContinuousQuantileDecimal));

	for (const auto &type : QUANTILES) {
		median.AddFunction(GetMedianAggregate(type));
		quantile_disc.AddFunction(GetDiscreteQuantileAggregate(type));
		quantile_disc.AddFunction(GetDiscreteQuantileListAggregate(type));
		if (type.id() != LogicalTypeId::INTERVAL) {
			quantile_cont.AddFunction(GetContinuousQuantileAggregate(type));
			quantile_cont.AddFunction(GetContinuousQuantileListAggregate(type));
		}
	}

	set.AddFunction(median);
	set.AddFunction(quantile_disc);
	set.AddFunction(quantile_cont);

	quantile_disc.name = "quantile";
	set.AddFunction(quantile_disc);
}

} // namespace duckdb

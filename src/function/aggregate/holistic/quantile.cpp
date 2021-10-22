#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate/holistic_functions.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/abs.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include <algorithm>
#include <queue>
#include <stdlib.h>
#include <utility>

namespace duckdb {

// Hugeint arithmetic
hugeint_t operator*(const hugeint_t &h, const double &d) {
	return Hugeint::Convert(Hugeint::Cast<double>(h) * d);
}

// Temporal arithmetic
interval_t operator*(const interval_t &i, const double &d) {
	interval_t result = {0, 0, std::llround(Interval::GetNanoseconds(i) * d / Interval::NANOS_PER_MICRO)};
	return result;
}

inline interval_t operator+(const interval_t &lhs, const interval_t &rhs) {
	return AddOperator::Operation<interval_t, interval_t, interval_t>(lhs, rhs);
}

inline interval_t operator-(const interval_t &lhs, const interval_t &rhs) {
	return SubtractOperator::Operation<interval_t, interval_t, interval_t>(lhs, rhs);
}

inline interval_t operator-(const date_t &lhs, const timestamp_t &rhs) {
	const auto d = Cast::Operation<date_t, timestamp_t>(lhs);
	return {0, 0, d - rhs};
}

using FrameBounds = std::pair<idx_t, idx_t>;

template <typename SAVE_TYPE>
struct QuantileState {
	using SaveType = SAVE_TYPE;

	// Regular aggregation
	std::vector<SaveType> v;

	// Windowed Aggregation
	std::vector<idx_t> w;
	idx_t pos;

	// List temporaries
	std::vector<idx_t> disturbed;
	std::vector<idx_t> lower;
	std::vector<idx_t> upper;

	QuantileState() : pos(0) {
	}

	~QuantileState() {
	}

	inline void SetPos(size_t pos_p) {
		pos = pos_p;
		if (pos >= w.size()) {
			w.resize(pos);
		}
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

static idx_t ReplaceIndex(idx_t *index, const FrameBounds &frame, const FrameBounds &prev) {
	D_ASSERT(index);

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

template <class INPUT_TYPE>
static inline bool CanReplace(const idx_t *index, const INPUT_TYPE *fdata, const idx_t j, const idx_t k0,
                              const idx_t k1) {
	auto same = false;

	D_ASSERT(index);

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

struct CastInterpolation {

	template <class INPUT_TYPE, class TARGET_TYPE>
	static inline TARGET_TYPE Cast(const INPUT_TYPE &src, Vector &result) {
		return Cast::Operation<INPUT_TYPE, TARGET_TYPE>(src);
	}
};

template <>
interval_t CastInterpolation::Cast(const dtime_t &src, Vector &result) {
	return {0, 0, src.micros};
}

template <>
string_t CastInterpolation::Cast(const std::string &src, Vector &result) {
	return StringVector::AddString(result, src);
}

template <>
string_t CastInterpolation::Cast(const string_t &src, Vector &result) {
	return StringVector::AddString(result, src);
}

// Indirect comparison
template <typename ACCESSOR>
struct QuantileLess {
	using INPUT_TYPE = typename ACCESSOR::INPUT_TYPE;
	const ACCESSOR &accessor;
	explicit QuantileLess(const ACCESSOR &accessor_p) : accessor(accessor_p) {
	}

	inline bool operator()(const INPUT_TYPE &lhs, const INPUT_TYPE &rhs) const {
		return accessor(lhs) < accessor(rhs);
	}
};

// Direct access
template <typename T>
struct QuantileAccessor {
	using INPUT_TYPE = T;
	using RESULT_TYPE = T;

	inline const INPUT_TYPE &operator()(const INPUT_TYPE &x) const {
		return x;
	}
};

// Continuous interpolation
template <bool DISCRETE>
struct Interpolator {
	Interpolator(const double q, const idx_t n_p) : n(n_p), RN((double)(n_p - 1) * q), FRN(floor(RN)), CRN(ceil(RN)) {
	}

	template <class INPUT_TYPE, class TARGET_TYPE, typename ACCESSOR = QuantileAccessor<INPUT_TYPE>>
	TARGET_TYPE Operation(INPUT_TYPE *v_t, Vector &result, const ACCESSOR &accessor = ACCESSOR()) const {
		using ACCESS_TYPE = typename ACCESSOR::RESULT_TYPE;
		QuantileLess<ACCESSOR> comp(accessor);
		if (CRN == FRN) {
			std::nth_element(v_t, v_t + FRN, v_t + n, comp);
			return CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(v_t[FRN]), result);
		} else {
			std::nth_element(v_t, v_t + FRN, v_t + n, comp);
			std::nth_element(v_t + FRN, v_t + CRN, v_t + n, comp);
			auto lo = CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(v_t[FRN]), result);
			auto hi = CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(v_t[CRN]), result);
			auto delta = hi - lo;
			return lo + delta * (RN - FRN);
		}
	}

	template <class INPUT_TYPE, class TARGET_TYPE>
	TARGET_TYPE Operation(const INPUT_TYPE *v_t, const idx_t *index, Vector &result) const {
		if (CRN == FRN) {
			return CastInterpolation::Cast<INPUT_TYPE, TARGET_TYPE>(v_t[index[FRN]], result);
		} else {
			auto lo = CastInterpolation::Cast<INPUT_TYPE, TARGET_TYPE>(v_t[index[FRN]], result);
			auto hi = CastInterpolation::Cast<INPUT_TYPE, TARGET_TYPE>(v_t[index[CRN]], result);
			auto delta = hi - lo;
			return lo + delta * (RN - FRN);
		}
	}

	const idx_t n;
	const double RN;
	const idx_t FRN;
	const idx_t CRN;
};

// Discrete "interpolation"
template <>
struct Interpolator<true> {
	Interpolator(const double q, const idx_t n_p) : n(n_p), RN((double)(n_p - 1) * q), FRN(floor(RN)), CRN(FRN) {
	}

	template <class INPUT_TYPE, class TARGET_TYPE, typename ACCESSOR = QuantileAccessor<INPUT_TYPE>>
	TARGET_TYPE Operation(INPUT_TYPE *v_t, Vector &result, const ACCESSOR &accessor = ACCESSOR()) const {
		using ACCESS_TYPE = typename ACCESSOR::RESULT_TYPE;
		QuantileLess<ACCESSOR> comp(accessor);
		std::nth_element(v_t, v_t + FRN, v_t + n, comp);
		return CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(v_t[FRN]), result);
	}

	template <class INPUT_TYPE, class TARGET_TYPE>
	TARGET_TYPE Operation(const INPUT_TYPE *v_t, const idx_t *index, Vector &result) {
		return CastInterpolation::Cast<INPUT_TYPE, TARGET_TYPE>(v_t[index[FRN]], result);
	}

	const idx_t n;
	const double RN;
	const idx_t FRN;
	const idx_t CRN;
};

struct QuantileBindData : public FunctionData {
	explicit QuantileBindData(double quantile_p) : quantiles(1, quantile_p), order(1, 0) {
	}

	explicit QuantileBindData(const vector<double> &quantiles_p) : quantiles(quantiles_p) {
		for (idx_t i = 0; i < quantiles.size(); ++i) {
			order.push_back(i);
		}

		IndirectLess<double> lt(quantiles.data());
		std::sort(order.begin(), order.end(), lt);
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<QuantileBindData>(quantiles);
	}

	bool Equals(FunctionData &other_p) override {
		auto &other = (QuantileBindData &)other_p;
		return quantiles == other.quantiles;
	}

	vector<double> quantiles;
	vector<idx_t> order;
};

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
		state->v.emplace_back(data[idx]);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target) {
		if (source.v.empty()) {
			return;
		}
		target->v.insert(target->v.end(), source.v.begin(), source.v.end());
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
static void ExecuteListFinalize(Vector &states, FunctionData *bind_data_p, Vector &result, idx_t count, idx_t offset) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);

	D_ASSERT(bind_data_p);
	auto bind_data = (QuantileBindData *)bind_data_p;
	ListVector::SetListSize(result, 0);

	if (states.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ListVector::Reserve(result, bind_data->quantiles.size());

		auto sdata = ConstantVector::GetData<STATE_TYPE *>(states);
		auto rdata = ConstantVector::GetData<RESULT_TYPE>(result);
		auto &mask = ConstantVector::Validity(result);
		OP::template Finalize<RESULT_TYPE, STATE_TYPE>(result, bind_data, sdata[0], rdata, mask, 0);
	} else {
		D_ASSERT(states.GetVectorType() == VectorType::FLAT_VECTOR);
		result.SetVectorType(VectorType::FLAT_VECTOR);
		ListVector::Reserve(result, count * bind_data->quantiles.size());

		auto sdata = FlatVector::GetData<STATE_TYPE *>(states);
		auto rdata = FlatVector::GetData<RESULT_TYPE>(result);
		auto &mask = FlatVector::Validity(result);
		for (idx_t i = 0; i < count; i++) {
			OP::template Finalize<RESULT_TYPE, STATE_TYPE>(result, bind_data, sdata[i], rdata, mask, i + offset);
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

template <bool DISCRETE>
struct QuantileScalarOperation : public QuantileOperation {

	template <class RESULT_TYPE, class STATE>
	static void Finalize(Vector &result, FunctionData *bind_data_p, STATE *state, RESULT_TYPE *target,
	                     ValidityMask &mask, idx_t idx) {
		if (state->v.empty()) {
			mask.SetInvalid(idx);
			return;
		}
		D_ASSERT(bind_data_p);
		auto bind_data = (QuantileBindData *)bind_data_p;
		D_ASSERT(bind_data->quantiles.size() == 1);
		Interpolator<DISCRETE> interp(bind_data->quantiles[0], state->v.size());
		target[idx] = interp.template Operation<typename STATE::SaveType, RESULT_TYPE>(state->v.data(), result);
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE>
	static void Window(const INPUT_TYPE *data, const ValidityMask &dmask, FunctionData *bind_data_p, STATE *state,
	                   const FrameBounds &frame, const FrameBounds &prev, Vector &result, idx_t ridx) {
		auto rdata = FlatVector::GetData<RESULT_TYPE>(result);
		auto &rmask = FlatVector::Validity(result);

		//  Lazily initialise frame state
		const auto prev_valid = state->pos == (prev.second - prev.first);
		state->SetPos(frame.second - frame.first);

		auto index = state->w.data();
		D_ASSERT(index);

		D_ASSERT(bind_data_p);
		auto bind_data = (QuantileBindData *)bind_data_p;

		// Find the two positions needed
		const auto q = bind_data->quantiles[0];

		bool same = false;
		if (prev_valid && dmask.AllValid() && frame.first == prev.first + 1 && frame.second == prev.second + 1) {
			//  Fixed frame size
			const auto j = ReplaceIndex(index, frame, prev);
			Interpolator<DISCRETE> interp(q, state->pos);
			same = CanReplace(index, data, j, interp.FRN, interp.CRN);
		} else {
			ReuseIndexes(index, frame, prev);
		}

		if (!same) {
			if (!dmask.AllValid()) {
				// Remove the NULLs
				IndirectNotNull not_null(dmask, MinValue(frame.first, prev.first));
				state->pos = std::partition(index, index + state->pos, not_null) - index;
			}
			if (state->pos) {
				Interpolator<DISCRETE> interp(q, state->pos);
				IndirectLess<INPUT_TYPE> lt(data);
				std::nth_element(index, index + interp.FRN, index + state->pos, lt);
				if (interp.CRN != interp.FRN) {
					std::nth_element(index + interp.CRN, index + interp.CRN, index + interp.n, lt);
				}
				rdata[ridx] = interp.template Operation<INPUT_TYPE, RESULT_TYPE>(data, index, result);
			} else {
				rmask.Set(ridx, false);
			}
		} else {
			Interpolator<DISCRETE> interp(q, state->pos);
			rdata[ridx] = interp.template Operation<INPUT_TYPE, RESULT_TYPE>(data, index, result);
		}
	}
};

template <typename INPUT_TYPE, typename SAVED_TYPE>
AggregateFunction GetTypedDiscreteQuantileAggregateFunction(const LogicalType &type) {
	using STATE = QuantileState<SAVED_TYPE>;
	using OP = QuantileScalarOperation<true>;
	auto fun = AggregateFunction::UnaryAggregateDestructor<STATE, INPUT_TYPE, INPUT_TYPE, OP>(type, type);
	fun.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, INPUT_TYPE, OP>;
	return fun;
}

AggregateFunction GetDiscreteQuantileAggregateFunction(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return GetTypedDiscreteQuantileAggregateFunction<int8_t, int8_t>(type);
	case LogicalTypeId::SMALLINT:
		return GetTypedDiscreteQuantileAggregateFunction<int16_t, int16_t>(type);
	case LogicalTypeId::INTEGER:
		return GetTypedDiscreteQuantileAggregateFunction<int32_t, int32_t>(type);
	case LogicalTypeId::BIGINT:
		return GetTypedDiscreteQuantileAggregateFunction<int64_t, int64_t>(type);
	case LogicalTypeId::HUGEINT:
		return GetTypedDiscreteQuantileAggregateFunction<hugeint_t, hugeint_t>(type);

	case LogicalTypeId::FLOAT:
		return GetTypedDiscreteQuantileAggregateFunction<float, float>(type);
	case LogicalTypeId::DOUBLE:
		return GetTypedDiscreteQuantileAggregateFunction<double, double>(type);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return GetTypedDiscreteQuantileAggregateFunction<int16_t, int16_t>(type);
		case PhysicalType::INT32:
			return GetTypedDiscreteQuantileAggregateFunction<int32_t, int32_t>(type);
		case PhysicalType::INT64:
			return GetTypedDiscreteQuantileAggregateFunction<int64_t, int64_t>(type);
		case PhysicalType::INT128:
			return GetTypedDiscreteQuantileAggregateFunction<hugeint_t, hugeint_t>(type);
		default:
			throw NotImplementedException("Unimplemented discrete quantile aggregate");
		}
		break;

	case LogicalTypeId::DATE:
		return GetTypedDiscreteQuantileAggregateFunction<int32_t, int32_t>(type);
	case LogicalTypeId::TIMESTAMP:
		return GetTypedDiscreteQuantileAggregateFunction<int64_t, int64_t>(type);
	case LogicalTypeId::TIME:
		return GetTypedDiscreteQuantileAggregateFunction<int64_t, int64_t>(type);
	case LogicalTypeId::INTERVAL:
		return GetTypedDiscreteQuantileAggregateFunction<interval_t, interval_t>(type);

	case LogicalTypeId::VARCHAR:
		return GetTypedDiscreteQuantileAggregateFunction<string_t, std::string>(type);

	default:
		throw NotImplementedException("Unimplemented discrete quantile aggregate");
	}
}

template <class CHILD_TYPE, bool DISCRETE>
struct QuantileListOperation : public QuantileOperation {

	template <class RESULT_TYPE, class STATE>
	static void Finalize(Vector &result_list, FunctionData *bind_data_p, STATE *state, RESULT_TYPE *target,
	                     ValidityMask &mask, idx_t idx) {
		if (state->v.empty()) {
			mask.SetInvalid(idx);
			return;
		}

		D_ASSERT(bind_data_p);
		auto bind_data = (QuantileBindData *)bind_data_p;

		auto &result = ListVector::GetEntry(result_list);
		auto ridx = ListVector::GetListSize(result_list);
		ListVector::Reserve(result_list, ridx + bind_data->quantiles.size());
		auto rdata = FlatVector::GetData<CHILD_TYPE>(result);

		auto v_t = state->v.data();
		D_ASSERT(v_t);

		target[idx].offset = ridx;
		for (const auto &quantile : bind_data->quantiles) {
			Interpolator<DISCRETE> interp(quantile, state->v.size());
			rdata[ridx] = interp.template Operation<typename STATE::SaveType, CHILD_TYPE>(v_t, result);
			++ridx;
		}
		target[idx].length = bind_data->quantiles.size();

		ListVector::SetListSize(result_list, ridx);
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE>
	static void Window(const INPUT_TYPE *data, const ValidityMask &dmask, FunctionData *bind_data_p, STATE *state,
	                   const FrameBounds &frame, const FrameBounds &prev, Vector &list, idx_t lidx) {
		D_ASSERT(bind_data_p);
		auto bind_data = (QuantileBindData *)bind_data_p;

		// Result is a constant LIST<RESULT_TYPE> with a fixed length
		auto ldata = FlatVector::GetData<RESULT_TYPE>(list);
		auto &lmask = FlatVector::Validity(list);
		auto &lentry = ldata[lidx];
		lentry.offset = ListVector::GetListSize(list);
		lentry.length = bind_data->quantiles.size();

		ListVector::Reserve(list, lentry.offset + lentry.length);
		ListVector::SetListSize(list, lentry.offset + lentry.length);
		auto &result = ListVector::GetEntry(list);
		auto rdata = FlatVector::GetData<CHILD_TYPE>(result);

		//  Lazily initialise frame state
		const auto prev_valid = state->w.size() == (prev.second - prev.first);
		state->SetPos(frame.second - frame.first);

		auto index = state->w.data();

		bool fixed = false;
		auto j = state->pos;
		if (prev_valid && dmask.AllValid() && frame.first == prev.first + 1 && frame.second == prev.second + 1) {
			//  Fixed frame size
			j = ReplaceIndex(index, frame, prev);
			fixed = true;
		} else {
			ReuseIndexes(index, frame, prev);
			if (!dmask.AllValid()) {
				IndirectNotNull not_null(dmask, MinValue(frame.first, prev.first));
				state->pos = std::partition(index, index + state->pos, not_null) - index;
			}
		}

		if (!state->pos) {
			lmask.Set(lidx, false);
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
			Interpolator<DISCRETE> interp(quantile, state->pos);

			if (fixed && CanReplace(index, data, j, interp.FRN, interp.CRN)) {
				rdata[lentry.offset + q] = interp.template Operation<INPUT_TYPE, CHILD_TYPE>(data, index, result);
				state->upper.resize(state->lower.size(), interp.FRN);
			} else {
				state->disturbed.push_back(q);
				state->lower.push_back(MinValue(lb, interp.FRN));
			}
			lb = interp.CRN + 1;
		}
		state->upper.resize(state->lower.size(), state->pos);

		// Second pass: select the disturbed values
		for (idx_t i = 0; i < state->disturbed.size(); ++i) {
			const auto &q = state->disturbed[i];
			const auto &quantile = bind_data->quantiles[q];
			Interpolator<DISCRETE> interp(quantile, state->pos);

			IndirectLess<INPUT_TYPE> lt(data);
			std::nth_element(index + state->lower[i], index + interp.FRN, index + state->upper[i], lt);
			if (interp.CRN != interp.FRN) {
				std::nth_element(index + interp.CRN, index + interp.CRN, index + state->upper[i], lt);
			}
			rdata[lentry.offset + q] = interp.template Operation<INPUT_TYPE, CHILD_TYPE>(data, index, result);
		}
	}
};

template <typename INPUT_TYPE, typename SAVE_TYPE>
AggregateFunction GetTypedDiscreteQuantileListAggregateFunction(const LogicalType &type) {
	using STATE = QuantileState<SAVE_TYPE>;
	using OP = QuantileListOperation<INPUT_TYPE, true>;
	auto fun = QuantileListAggregate<STATE, INPUT_TYPE, list_entry_t, OP>(type, type);
	fun.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, list_entry_t, OP>;
	return fun;
}

AggregateFunction GetDiscreteQuantileListAggregateFunction(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return GetTypedDiscreteQuantileListAggregateFunction<int8_t, int8_t>(type);
	case LogicalTypeId::SMALLINT:
		return GetTypedDiscreteQuantileListAggregateFunction<int16_t, int16_t>(type);
	case LogicalTypeId::INTEGER:
		return GetTypedDiscreteQuantileListAggregateFunction<int32_t, int32_t>(type);
	case LogicalTypeId::BIGINT:
		return GetTypedDiscreteQuantileListAggregateFunction<int64_t, int64_t>(type);
	case LogicalTypeId::HUGEINT:
		return GetTypedDiscreteQuantileListAggregateFunction<hugeint_t, hugeint_t>(type);

	case LogicalTypeId::FLOAT:
		return GetTypedDiscreteQuantileListAggregateFunction<float, float>(type);
	case LogicalTypeId::DOUBLE:
		return GetTypedDiscreteQuantileListAggregateFunction<double, double>(type);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return GetTypedDiscreteQuantileListAggregateFunction<int16_t, int16_t>(type);
		case PhysicalType::INT32:
			return GetTypedDiscreteQuantileListAggregateFunction<int32_t, int32_t>(type);
		case PhysicalType::INT64:
			return GetTypedDiscreteQuantileListAggregateFunction<int64_t, int64_t>(type);
		case PhysicalType::INT128:
			return GetTypedDiscreteQuantileListAggregateFunction<hugeint_t, hugeint_t>(type);
		default:
			throw NotImplementedException("Unimplemented discrete quantile list aggregate");
		}
		break;

	case LogicalTypeId::DATE:
		return GetTypedDiscreteQuantileListAggregateFunction<date_t, date_t>(type);
	case LogicalTypeId::TIMESTAMP:
		return GetTypedDiscreteQuantileListAggregateFunction<timestamp_t, timestamp_t>(type);
	case LogicalTypeId::TIME:
		return GetTypedDiscreteQuantileListAggregateFunction<dtime_t, dtime_t>(type);
	case LogicalTypeId::INTERVAL:
		return GetTypedDiscreteQuantileListAggregateFunction<interval_t, interval_t>(type);

	case LogicalTypeId::VARCHAR:
		return GetTypedDiscreteQuantileListAggregateFunction<string_t, std::string>(type);

	default:
		throw NotImplementedException("Unimplemented discrete quantile list aggregate");
	}
}

template <typename INPUT_TYPE, typename TARGET_TYPE>
AggregateFunction GetTypedContinuousQuantileAggregateFunction(const LogicalType &input_type,
                                                              const LogicalType &target_type) {
	using STATE = QuantileState<INPUT_TYPE>;
	using OP = QuantileScalarOperation<false>;
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
			throw NotImplementedException("Unimplemented continuous quantile DECIMAL aggregate");
		}
		break;

	case LogicalTypeId::DATE:
		return GetTypedContinuousQuantileAggregateFunction<date_t, timestamp_t>(type, LogicalType::TIMESTAMP);
	case LogicalTypeId::TIMESTAMP:
		return GetTypedContinuousQuantileAggregateFunction<timestamp_t, timestamp_t>(type, type);
	case LogicalTypeId::TIME:
		return GetTypedContinuousQuantileAggregateFunction<dtime_t, dtime_t>(type, type);

	default:
		throw NotImplementedException("Unimplemented continuous quantile aggregate");
	}
}

template <typename INPUT_TYPE, typename CHILD_TYPE>
AggregateFunction GetTypedContinuousQuantileListAggregateFunction(const LogicalType &input_type,
                                                                  const LogicalType &result_type) {
	using STATE = QuantileState<INPUT_TYPE>;
	using OP = QuantileListOperation<CHILD_TYPE, false>;
	auto fun = QuantileListAggregate<STATE, INPUT_TYPE, list_entry_t, OP>(input_type, result_type);
	fun.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, list_entry_t, OP>;
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
			return GetTypedContinuousQuantileListAggregateFunction<int16_t, int16_t>(type, type);
		case PhysicalType::INT32:
			return GetTypedContinuousQuantileListAggregateFunction<int32_t, int32_t>(type, type);
		case PhysicalType::INT64:
			return GetTypedContinuousQuantileListAggregateFunction<int64_t, int64_t>(type, type);
		case PhysicalType::INT128:
			return GetTypedContinuousQuantileListAggregateFunction<hugeint_t, hugeint_t>(type, type);
		default:
			throw NotImplementedException("Unimplemented discrete quantile DECIMAL list aggregate");
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

template <typename T, typename R, typename MEDIAN_TYPE>
struct MadAccessor {
	using INPUT_TYPE = T;
	using RESULT_TYPE = R;
	const MEDIAN_TYPE &median;
	explicit MadAccessor(const MEDIAN_TYPE &median_p) : median(median_p) {
	}

	inline RESULT_TYPE operator()(const INPUT_TYPE &input) const {
		const auto delta = input - median;
		return AbsOperator::Operation<RESULT_TYPE, RESULT_TYPE>(delta);
	}
};

// hugeint_t - double => undefined
template <>
struct MadAccessor<hugeint_t, double, double> {
	using INPUT_TYPE = hugeint_t;
	using RESULT_TYPE = double;
	using MEDIAN_TYPE = double;
	const MEDIAN_TYPE &median;
	explicit MadAccessor(const MEDIAN_TYPE &median_p) : median(median_p) {
	}
	inline RESULT_TYPE operator()(const INPUT_TYPE &input) const {
		const auto delta = Hugeint::Cast<double>(input) - median;
		return AbsOperator::Operation<double, double>(delta);
	}
};

// date_t - date_t => date_t
template <>
struct MadAccessor<date_t, interval_t, timestamp_t> {
	using INPUT_TYPE = date_t;
	using RESULT_TYPE = interval_t;
	using MEDIAN_TYPE = timestamp_t;
	const MEDIAN_TYPE &median;
	explicit MadAccessor(const MEDIAN_TYPE &median_p) : median(median_p) {
	}
	inline RESULT_TYPE operator()(const INPUT_TYPE &input) const {
		const auto delta = input - median;
		return {0, AbsOperator::Operation<int32_t, int32_t>(delta.days),
		        AbsOperator::Operation<int64_t, int64_t>(delta.micros)};
	}
};

// timestamp_t - timestamp_t => int64_t
template <>
struct MadAccessor<timestamp_t, interval_t, timestamp_t> {
	using INPUT_TYPE = timestamp_t;
	using RESULT_TYPE = interval_t;
	using MEDIAN_TYPE = timestamp_t;
	const MEDIAN_TYPE &median;
	explicit MadAccessor(const MEDIAN_TYPE &median_p) : median(median_p) {
	}
	inline RESULT_TYPE operator()(const INPUT_TYPE &input) const {
		const auto delta = input - median;
		return {0, 0, AbsOperator::Operation<int64_t, int64_t>(delta)};
	}
};

// dtime_t - dtime_t => int64_t
template <>
struct MadAccessor<dtime_t, interval_t, dtime_t> {
	using INPUT_TYPE = dtime_t;
	using RESULT_TYPE = interval_t;
	using MEDIAN_TYPE = dtime_t;
	const MEDIAN_TYPE &median;
	explicit MadAccessor(const MEDIAN_TYPE &median_p) : median(median_p) {
	}
	inline RESULT_TYPE operator()(const INPUT_TYPE &input) const {
		const auto delta = input - median;
		return {0, 0, AbsOperator::Operation<int64_t, int64_t>(delta)};
	}
};

template <typename MEDIAN_TYPE>
struct MedianAbsoluteDeviationOperation : public QuantileOperation {

	template <class RESULT_TYPE, class STATE>
	static void Finalize(Vector &result, FunctionData *bind_data_p, STATE *state, RESULT_TYPE *target,
	                     ValidityMask &mask, idx_t idx) {
		if (state->v.empty()) {
			mask.SetInvalid(idx);
			return;
		}
		using SAVE_TYPE = typename STATE::SaveType;
		Interpolator<false> interp(0.5, state->v.size());
		const auto med = interp.template Operation<SAVE_TYPE, MEDIAN_TYPE>(state->v.data(), result);

		MadAccessor<SAVE_TYPE, RESULT_TYPE, MEDIAN_TYPE> accessor(med);
		target[idx] = interp.template Operation<SAVE_TYPE, RESULT_TYPE>(state->v.data(), result, accessor);
	}
};

template <typename INPUT_TYPE, typename MEDIAN_TYPE, typename TARGET_TYPE>
AggregateFunction GetTypedMedianAbsoluteDeviationAggregateFunction(const LogicalType &input_type,
                                                                   const LogicalType &target_type) {
	using STATE = QuantileState<INPUT_TYPE>;
	using OP = MedianAbsoluteDeviationOperation<MEDIAN_TYPE>;
	auto fun = AggregateFunction::UnaryAggregateDestructor<STATE, INPUT_TYPE, TARGET_TYPE, OP>(input_type, target_type);
	// fun.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, TARGET_TYPE, OP>;
	return fun;
}

AggregateFunction GetMedianAbsoluteDeviationAggregateFunction(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return GetTypedMedianAbsoluteDeviationAggregateFunction<int8_t, double, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::SMALLINT:
		return GetTypedMedianAbsoluteDeviationAggregateFunction<int16_t, double, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::INTEGER:
		return GetTypedMedianAbsoluteDeviationAggregateFunction<int32_t, double, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::BIGINT:
		return GetTypedMedianAbsoluteDeviationAggregateFunction<int64_t, double, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::HUGEINT:
		return GetTypedMedianAbsoluteDeviationAggregateFunction<hugeint_t, double, double>(type, LogicalType::DOUBLE);

	case LogicalTypeId::FLOAT:
		return GetTypedMedianAbsoluteDeviationAggregateFunction<float, float, float>(type, type);
	case LogicalTypeId::DOUBLE:
		return GetTypedMedianAbsoluteDeviationAggregateFunction<double, double, double>(type, type);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return GetTypedMedianAbsoluteDeviationAggregateFunction<int16_t, int16_t, int16_t>(type, type);
		case PhysicalType::INT32:
			return GetTypedMedianAbsoluteDeviationAggregateFunction<int32_t, int32_t, int32_t>(type, type);
		case PhysicalType::INT64:
			return GetTypedMedianAbsoluteDeviationAggregateFunction<int64_t, int64_t, int64_t>(type, type);
		case PhysicalType::INT128:
			return GetTypedMedianAbsoluteDeviationAggregateFunction<hugeint_t, hugeint_t, hugeint_t>(type, type);
		default:
			throw NotImplementedException("Unimplemented Median Absolute Deviation DECIMAL aggregate");
		}
		break;

	case LogicalTypeId::DATE:
		return GetTypedMedianAbsoluteDeviationAggregateFunction<date_t, timestamp_t, interval_t>(type,
		                                                                                         LogicalType::INTERVAL);
	case LogicalTypeId::TIMESTAMP:
		return GetTypedMedianAbsoluteDeviationAggregateFunction<timestamp_t, timestamp_t, interval_t>(
		    type, LogicalType::INTERVAL);
	case LogicalTypeId::TIME:
		return GetTypedMedianAbsoluteDeviationAggregateFunction<dtime_t, dtime_t, interval_t>(type,
		                                                                                      LogicalType::INTERVAL);

	default:
		throw NotImplementedException("Unimplemented Median Absolute Deviation aggregate");
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

unique_ptr<FunctionData> BindMedianAbsoluteDeviationDecimal(ClientContext &context, AggregateFunction &function,
                                                            vector<unique_ptr<Expression>> &arguments) {
	function = GetMedianAbsoluteDeviationAggregateFunction(arguments[0]->return_type);
	function.name = "mad";
	return nullptr;
}

static double CheckQuantile(const Value &quantile_val) {
	auto quantile = quantile_val.GetValue<double>();

	if (quantile_val.is_null || quantile < 0 || quantile > 1) {
		throw BinderException("QUANTILE can only take parameters in the range [0, 1]");
	}

	return quantile;
}

unique_ptr<FunctionData> BindQuantile(ClientContext &context, AggregateFunction &function,
                                      vector<unique_ptr<Expression>> &arguments) {
	if (!arguments[1]->IsFoldable()) {
		throw BinderException("QUANTILE can only take constant parameters");
	}
	Value quantile_val = ExpressionExecutor::EvaluateScalar(*arguments[1]);
	vector<double> quantiles;
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

unique_ptr<FunctionData> BindDiscreteQuantileDecimalList(ClientContext &context, AggregateFunction &function,
                                                         vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindQuantile(context, function, arguments);
	function = GetDiscreteQuantileListAggregateFunction(arguments[0]->return_type);
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

unique_ptr<FunctionData> BindContinuousQuantileDecimalList(ClientContext &context, AggregateFunction &function,
                                                           vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindQuantile(context, function, arguments);
	function = GetContinuousQuantileListAggregateFunction(arguments[0]->return_type);
	function.name = "quantile_cont";
	return bind_data;
}

static bool CanInterpolate(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::VARCHAR:
		return false;
	default:
		return true;
	}
}

AggregateFunction GetMedianAggregate(const LogicalType &type) {
	auto fun = CanInterpolate(type) ? GetContinuousQuantileAggregateFunction(type)
	                                : GetDiscreteQuantileAggregateFunction(type);
	fun.bind = BindMedian;
	return fun;
}

AggregateFunction GetDiscreteQuantileAggregate(const LogicalType &type) {
	auto fun = GetDiscreteQuantileAggregateFunction(type);
	fun.bind = BindQuantile;
	// temporarily push an argument so we can bind the actual quantile
	fun.arguments.push_back(LogicalType::DOUBLE);
	return fun;
}

AggregateFunction GetDiscreteQuantileListAggregate(const LogicalType &type) {
	auto fun = GetDiscreteQuantileListAggregateFunction(type);
	fun.bind = BindQuantile;
	// temporarily push an argument so we can bind the actual quantile
	auto list_of_double = LogicalType::LIST(LogicalType::DOUBLE);
	fun.arguments.push_back(list_of_double);
	return fun;
}

AggregateFunction GetContinuousQuantileAggregate(const LogicalType &type) {
	auto fun = GetContinuousQuantileAggregateFunction(type);
	fun.bind = BindQuantile;
	// temporarily push an argument so we can bind the actual quantile
	fun.arguments.push_back(LogicalType::DOUBLE);
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
	                                       LogicalType::TIME,    LogicalType::INTERVAL, LogicalType::VARCHAR};

	AggregateFunctionSet median("median");
	median.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL, nullptr, nullptr, nullptr,
	                                     nullptr, nullptr, nullptr, BindMedianDecimal));

	AggregateFunctionSet mad("mad");
	mad.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL, nullptr, nullptr, nullptr,
	                                  nullptr, nullptr, nullptr, BindMedianAbsoluteDeviationDecimal));

	AggregateFunctionSet quantile_disc("quantile_disc");
	quantile_disc.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL, LogicalType::DOUBLE}, LogicalTypeId::DECIMAL,
	                                            nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                            BindDiscreteQuantileDecimal));
	quantile_disc.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL, LogicalType::LIST(LogicalType::DOUBLE)},
	                                            LogicalType::LIST(LogicalTypeId::DECIMAL), nullptr, nullptr, nullptr,
	                                            nullptr, nullptr, nullptr, BindDiscreteQuantileDecimalList));

	AggregateFunctionSet quantile_cont("quantile_cont");
	quantile_cont.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL, LogicalType::DOUBLE}, LogicalTypeId::DECIMAL,
	                                            nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                            BindContinuousQuantileDecimal));
	quantile_cont.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL, LogicalType::LIST(LogicalType::DOUBLE)},
	                                            LogicalType::LIST(LogicalTypeId::DECIMAL), nullptr, nullptr, nullptr,
	                                            nullptr, nullptr, nullptr, BindContinuousQuantileDecimalList));

	for (const auto &type : QUANTILES) {
		median.AddFunction(GetMedianAggregate(type));
		quantile_disc.AddFunction(GetDiscreteQuantileAggregate(type));
		quantile_disc.AddFunction(GetDiscreteQuantileListAggregate(type));
		if (CanInterpolate(type)) {
			quantile_cont.AddFunction(GetContinuousQuantileAggregate(type));
			quantile_cont.AddFunction(GetContinuousQuantileListAggregate(type));
			mad.AddFunction(GetMedianAbsoluteDeviationAggregateFunction(type));
		}
	}

	set.AddFunction(median);
	set.AddFunction(mad);
	set.AddFunction(quantile_disc);
	set.AddFunction(quantile_cont);

	quantile_disc.name = "quantile";
	set.AddFunction(quantile_disc);
}

} // namespace duckdb

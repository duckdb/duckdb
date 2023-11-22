#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/core_functions/aggregate/holistic_functions.hpp"
#include "duckdb/execution/merge_sort_tree.hpp"
#include "duckdb/core_functions/aggregate/quantile_enum.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/abs.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/queue.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

#include "SkipList.h"

#include <algorithm>
#include <numeric>
#include <stdlib.h>
#include <utility>

namespace duckdb {

// Hugeint arithmetic
static hugeint_t MultiplyByDouble(const hugeint_t &h, const double &d) {
	D_ASSERT(d >= 0 && d <= 1);
	return Hugeint::Convert(Hugeint::Cast<double>(h) * d);
}

// Interval arithmetic
static interval_t MultiplyByDouble(const interval_t &i, const double &d) { // NOLINT
	D_ASSERT(d >= 0 && d <= 1);
	return Interval::FromMicro(std::llround(Interval::GetMicro(i) * d));
}

inline interval_t operator+(const interval_t &lhs, const interval_t &rhs) {
	return Interval::FromMicro(Interval::GetMicro(lhs) + Interval::GetMicro(rhs));
}

inline interval_t operator-(const interval_t &lhs, const interval_t &rhs) {
	return Interval::FromMicro(Interval::GetMicro(lhs) - Interval::GetMicro(rhs));
}

struct FrameSet {
	inline explicit FrameSet(const SubFrames &frames_p) : frames(frames_p) {
	}

	inline idx_t Size() const {
		idx_t result = 0;
		for (const auto &frame : frames) {
			result += frame.end - frame.start;
		}

		return result;
	}

	inline bool Contains(idx_t i) const {
		for (idx_t f = 0; f < frames.size(); ++f) {
			const auto &frame = frames[f];
			if (frame.start <= i && i < frame.end) {
				return true;
			}
		}
		return false;
	}
	const SubFrames &frames;
};

struct QuantileIncluded {
	inline explicit QuantileIncluded(const ValidityMask &fmask_p, const ValidityMask &dmask_p)
	    : fmask(fmask_p), dmask(dmask_p) {
	}

	inline bool operator()(const idx_t &idx) const {
		return fmask.RowIsValid(idx) && dmask.RowIsValid(idx);
	}

	inline bool AllValid() const {
		return fmask.AllValid() && dmask.AllValid();
	}

	const ValidityMask &fmask;
	const ValidityMask &dmask;
};

struct QuantileReuseUpdater {
	idx_t *index;
	idx_t j;

	inline QuantileReuseUpdater(idx_t *index, idx_t j) : index(index), j(j) {
	}

	inline void Neither(idx_t begin, idx_t end) {
	}

	inline void Left(idx_t begin, idx_t end) {
	}

	inline void Right(idx_t begin, idx_t end) {
		for (; begin < end; ++begin) {
			index[j++] = begin;
		}
	}

	inline void Both(idx_t begin, idx_t end) {
	}
};

void ReuseIndexes(idx_t *index, const SubFrames &currs, const SubFrames &prevs) {

	//  Copy overlapping indices by scanning the previous set and copying down into holes.
	//	We copy instead of leaving gaps in case there are fewer values in the current frame.
	FrameSet prev_set(prevs);
	FrameSet curr_set(currs);
	const auto prev_count = prev_set.Size();
	idx_t j = 0;
	for (idx_t p = 0; p < prev_count; ++p) {
		auto idx = index[p];

		//  Shift down into any hole
		if (j != p) {
			index[j] = idx;
		}

		//  Skip overlapping values
		if (curr_set.Contains(idx)) {
			++j;
		}
	}

	//  Insert new indices
	if (j > 0) {
		QuantileReuseUpdater updater(index, j);
		AggregateExecutor::IntersectFrames(prevs, currs, updater);
	} else {
		//  No overlap: overwrite with new values
		for (const auto &curr : currs) {
			for (auto idx = curr.start; idx < curr.end; ++idx) {
				index[j++] = idx;
			}
		}
	}
}

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
	template <typename TARGET_TYPE>
	static inline TARGET_TYPE Interpolate(const TARGET_TYPE &lo, const double d, const TARGET_TYPE &hi) {
		const auto delta = hi - lo;
		return lo + delta * d;
	}
};

template <>
interval_t CastInterpolation::Cast(const dtime_t &src, Vector &result) {
	return {0, 0, src.micros};
}

template <>
double CastInterpolation::Interpolate(const double &lo, const double d, const double &hi) {
	return lo * (1.0 - d) + hi * d;
}

template <>
dtime_t CastInterpolation::Interpolate(const dtime_t &lo, const double d, const dtime_t &hi) {
	return dtime_t(std::llround(lo.micros * (1.0 - d) + hi.micros * d));
}

template <>
timestamp_t CastInterpolation::Interpolate(const timestamp_t &lo, const double d, const timestamp_t &hi) {
	return timestamp_t(std::llround(lo.value * (1.0 - d) + hi.value * d));
}

template <>
hugeint_t CastInterpolation::Interpolate(const hugeint_t &lo, const double d, const hugeint_t &hi) {
	const hugeint_t delta = hi - lo;
	return lo + MultiplyByDouble(delta, d);
}

template <>
interval_t CastInterpolation::Interpolate(const interval_t &lo, const double d, const interval_t &hi) {
	const interval_t delta = hi - lo;
	return lo + MultiplyByDouble(delta, d);
}

template <>
string_t CastInterpolation::Cast(const std::string &src, Vector &result) {
	return StringVector::AddString(result, src);
}

template <>
string_t CastInterpolation::Cast(const string_t &src, Vector &result) {
	return StringVector::AddString(result, src);
}

// Direct access
template <typename T>
struct QuantileDirect {
	using INPUT_TYPE = T;
	using RESULT_TYPE = T;

	inline const INPUT_TYPE &operator()(const INPUT_TYPE &x) const {
		return x;
	}
};

// Indirect access
template <typename T>
struct QuantileIndirect {
	using INPUT_TYPE = idx_t;
	using RESULT_TYPE = T;
	const RESULT_TYPE *data;

	explicit QuantileIndirect(const RESULT_TYPE *data_p) : data(data_p) {
	}

	inline RESULT_TYPE operator()(const idx_t &input) const {
		return data[input];
	}
};

// Composed access
template <typename OUTER, typename INNER>
struct QuantileComposed {
	using INPUT_TYPE = typename INNER::INPUT_TYPE;
	using RESULT_TYPE = typename OUTER::RESULT_TYPE;

	const OUTER &outer;
	const INNER &inner;

	explicit QuantileComposed(const OUTER &outer_p, const INNER &inner_p) : outer(outer_p), inner(inner_p) {
	}

	inline RESULT_TYPE operator()(const idx_t &input) const {
		return outer(inner(input));
	}
};

// Accessed comparison
template <typename ACCESSOR>
struct QuantileCompare {
	using INPUT_TYPE = typename ACCESSOR::INPUT_TYPE;
	const ACCESSOR &accessor;
	const bool desc;
	explicit QuantileCompare(const ACCESSOR &accessor_p, bool desc_p) : accessor(accessor_p), desc(desc_p) {
	}

	inline bool operator()(const INPUT_TYPE &lhs, const INPUT_TYPE &rhs) const {
		const auto lval = accessor(lhs);
		const auto rval = accessor(rhs);

		return desc ? (rval < lval) : (lval < rval);
	}
};

//	Avoid using naked Values in inner loops...
struct QuantileValue {
	explicit QuantileValue(const Value &v) : val(v), dbl(v.GetValue<double>()) {
		const auto &type = val.type();
		switch (type.id()) {
		case LogicalTypeId::DECIMAL: {
			integral = IntegralValue::Get(v);
			scaling = Hugeint::POWERS_OF_TEN[DecimalType::GetScale(type)];
			break;
		}
		default:
			break;
		}
	}

	Value val;

	//	DOUBLE
	double dbl;

	//	DECIMAL
	hugeint_t integral;
	hugeint_t scaling;
};

bool operator==(const QuantileValue &x, const QuantileValue &y) {
	return x.val == y.val;
}

// Continuous interpolation
template <bool DISCRETE>
struct Interpolator {
	Interpolator(const QuantileValue &q, const idx_t n_p, const bool desc_p)
	    : desc(desc_p), RN((double)(n_p - 1) * q.dbl), FRN(floor(RN)), CRN(ceil(RN)), begin(0), end(n_p) {
	}

	template <class INPUT_TYPE, class TARGET_TYPE, typename ACCESSOR = QuantileDirect<INPUT_TYPE>>
	TARGET_TYPE Interpolate(INPUT_TYPE lidx, INPUT_TYPE hidx, Vector &result, const ACCESSOR &accessor) const {
		using ACCESS_TYPE = typename ACCESSOR::RESULT_TYPE;
		if (lidx == hidx) {
			return CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(lidx), result);
		} else {
			auto lo = CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(lidx), result);
			auto hi = CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(hidx), result);
			return CastInterpolation::Interpolate<TARGET_TYPE>(lo, RN - FRN, hi);
		}
	}

	template <class INPUT_TYPE, class TARGET_TYPE, typename ACCESSOR = QuantileDirect<INPUT_TYPE>>
	TARGET_TYPE Operation(INPUT_TYPE *v_t, Vector &result, const ACCESSOR &accessor = ACCESSOR()) const {
		using ACCESS_TYPE = typename ACCESSOR::RESULT_TYPE;
		QuantileCompare<ACCESSOR> comp(accessor, desc);
		if (CRN == FRN) {
			std::nth_element(v_t + begin, v_t + FRN, v_t + end, comp);
			return CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(v_t[FRN]), result);
		} else {
			std::nth_element(v_t + begin, v_t + FRN, v_t + end, comp);
			std::nth_element(v_t + FRN, v_t + CRN, v_t + end, comp);
			auto lo = CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(v_t[FRN]), result);
			auto hi = CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(v_t[CRN]), result);
			return CastInterpolation::Interpolate<TARGET_TYPE>(lo, RN - FRN, hi);
		}
	}

	template <class INPUT_TYPE, class TARGET_TYPE>
	inline TARGET_TYPE Extract(const INPUT_TYPE **dest, Vector &result) const {
		if (CRN == FRN) {
			return CastInterpolation::Cast<INPUT_TYPE, TARGET_TYPE>(*dest[0], result);
		} else {
			auto lo = CastInterpolation::Cast<INPUT_TYPE, TARGET_TYPE>(*dest[0], result);
			auto hi = CastInterpolation::Cast<INPUT_TYPE, TARGET_TYPE>(*dest[1], result);
			return CastInterpolation::Interpolate<TARGET_TYPE>(lo, RN - FRN, hi);
		}
	}

	const bool desc;
	const double RN;
	const idx_t FRN;
	const idx_t CRN;

	idx_t begin;
	idx_t end;
};

// Discrete "interpolation"
template <>
struct Interpolator<true> {
	static inline idx_t Index(const QuantileValue &q, const idx_t n) {
		idx_t floored;
		switch (q.val.type().id()) {
		case LogicalTypeId::DECIMAL: {
			//	Integer arithmetic for accuracy
			const auto integral = q.integral;
			const auto scaling = q.scaling;
			const auto scaled_q = DecimalMultiplyOverflowCheck::Operation<hugeint_t, hugeint_t, hugeint_t>(n, integral);
			const auto scaled_n = DecimalMultiplyOverflowCheck::Operation<hugeint_t, hugeint_t, hugeint_t>(n, scaling);
			floored = Cast::Operation<hugeint_t, idx_t>((scaled_n - scaled_q) / scaling);
			break;
		}
		default:
			const auto scaled_q = (double)(n * q.dbl);
			floored = floor(n - scaled_q);
			break;
		}

		return MaxValue<idx_t>(1, n - floored) - 1;
	}

	Interpolator(const QuantileValue &q, const idx_t n_p, bool desc_p)
	    : desc(desc_p), FRN(Index(q, n_p)), CRN(FRN), begin(0), end(n_p) {
	}

	template <class INPUT_TYPE, class TARGET_TYPE, typename ACCESSOR = QuantileDirect<INPUT_TYPE>>
	TARGET_TYPE Interpolate(INPUT_TYPE lidx, INPUT_TYPE hidx, Vector &result, const ACCESSOR &accessor) const {
		using ACCESS_TYPE = typename ACCESSOR::RESULT_TYPE;
		return CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(lidx), result);
	}

	template <class INPUT_TYPE, class TARGET_TYPE, typename ACCESSOR = QuantileDirect<INPUT_TYPE>>
	TARGET_TYPE Operation(INPUT_TYPE *v_t, Vector &result, const ACCESSOR &accessor = ACCESSOR()) const {
		using ACCESS_TYPE = typename ACCESSOR::RESULT_TYPE;
		QuantileCompare<ACCESSOR> comp(accessor, desc);
		std::nth_element(v_t + begin, v_t + FRN, v_t + end, comp);
		return CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(v_t[FRN]), result);
	}

	template <class INPUT_TYPE, class TARGET_TYPE>
	TARGET_TYPE Extract(const INPUT_TYPE **dest, Vector &result) const {
		return CastInterpolation::Cast<INPUT_TYPE, TARGET_TYPE>(*dest[0], result);
	}

	const bool desc;
	const idx_t FRN;
	const idx_t CRN;

	idx_t begin;
	idx_t end;
};

template <typename T>
static inline T QuantileAbs(const T &t) {
	return AbsOperator::Operation<T, T>(t);
}

template <>
inline Value QuantileAbs(const Value &v) {
	const auto &type = v.type();
	switch (type.id()) {
	case LogicalTypeId::DECIMAL: {
		const auto integral = IntegralValue::Get(v);
		const auto width = DecimalType::GetWidth(type);
		const auto scale = DecimalType::GetScale(type);
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return Value::DECIMAL(QuantileAbs<int16_t>(Cast::Operation<hugeint_t, int16_t>(integral)), width, scale);
		case PhysicalType::INT32:
			return Value::DECIMAL(QuantileAbs<int32_t>(Cast::Operation<hugeint_t, int32_t>(integral)), width, scale);
		case PhysicalType::INT64:
			return Value::DECIMAL(QuantileAbs<int64_t>(Cast::Operation<hugeint_t, int64_t>(integral)), width, scale);
		case PhysicalType::INT128:
			return Value::DECIMAL(QuantileAbs<hugeint_t>(integral), width, scale);
		default:
			throw InternalException("Unknown DECIMAL type");
		}
	}
	default:
		return Value::DOUBLE(QuantileAbs<double>(v.GetValue<double>()));
	}
}

void BindQuantileInner(AggregateFunction &function, const LogicalType &type, QuantileSerializationType quantile_type);

struct QuantileBindData : public FunctionData {
	QuantileBindData() {
	}

	explicit QuantileBindData(const Value &quantile_p)
	    : quantiles(1, QuantileValue(QuantileAbs(quantile_p))), order(1, 0), desc(quantile_p < 0) {
	}

	explicit QuantileBindData(const vector<Value> &quantiles_p) {
		vector<Value> normalised;
		size_t pos = 0;
		size_t neg = 0;
		for (idx_t i = 0; i < quantiles_p.size(); ++i) {
			const auto &q = quantiles_p[i];
			pos += (q > 0);
			neg += (q < 0);
			normalised.emplace_back(QuantileAbs(q));
			order.push_back(i);
		}
		if (pos && neg) {
			throw BinderException("QUANTILE parameters must have consistent signs");
		}
		desc = (neg > 0);

		IndirectLess<Value> lt(normalised.data());
		std::sort(order.begin(), order.end(), lt);

		for (const auto &q : normalised) {
			quantiles.emplace_back(QuantileValue(q));
		}
	}

	QuantileBindData(const QuantileBindData &other) : order(other.order), desc(other.desc) {
		for (const auto &q : other.quantiles) {
			quantiles.emplace_back(q);
		}
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<QuantileBindData>(*this);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<QuantileBindData>();
		return desc == other.desc && quantiles == other.quantiles && order == other.order;
	}

	static void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                      const AggregateFunction &function) {
		auto &bind_data = bind_data_p->Cast<QuantileBindData>();
		vector<Value> raw;
		for (const auto &q : bind_data.quantiles) {
			raw.emplace_back(q.val);
		}
		serializer.WriteProperty(100, "quantiles", raw);
		serializer.WriteProperty(101, "order", bind_data.order);
		serializer.WriteProperty(102, "desc", bind_data.desc);
	}

	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, AggregateFunction &function) {
		auto result = make_uniq<QuantileBindData>();
		vector<Value> raw;
		deserializer.ReadProperty(100, "quantiles", raw);
		deserializer.ReadProperty(101, "order", result->order);
		deserializer.ReadProperty(102, "desc", result->desc);
		QuantileSerializationType deserialization_type;
		deserializer.ReadPropertyWithDefault(103, "quantile_type", deserialization_type,
		                                     QuantileSerializationType::NON_DECIMAL);

		if (deserialization_type != QuantileSerializationType::NON_DECIMAL) {
			LogicalType arg_type;
			deserializer.ReadProperty(104, "logical_type", arg_type);

			BindQuantileInner(function, arg_type, deserialization_type);
		}

		for (const auto &r : raw) {
			result->quantiles.emplace_back(QuantileValue(r));
		}
		return std::move(result);
	}

	static void SerializeDecimalDiscrete(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                                     const AggregateFunction &function) {
		Serialize(serializer, bind_data_p, function);

		serializer.WritePropertyWithDefault<QuantileSerializationType>(
		    103, "quantile_type", QuantileSerializationType::DECIMAL_DISCRETE, QuantileSerializationType::NON_DECIMAL);
		serializer.WriteProperty(104, "logical_type", function.arguments[0]);
	}
	static void SerializeDecimalDiscreteList(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                                         const AggregateFunction &function) {

		Serialize(serializer, bind_data_p, function);

		serializer.WritePropertyWithDefault<QuantileSerializationType>(103, "quantile_type",
		                                                               QuantileSerializationType::DECIMAL_DISCRETE_LIST,
		                                                               QuantileSerializationType::NON_DECIMAL);
		serializer.WriteProperty(104, "logical_type", function.arguments[0]);
	}
	static void SerializeDecimalContinuous(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                                       const AggregateFunction &function) {
		Serialize(serializer, bind_data_p, function);

		serializer.WritePropertyWithDefault<QuantileSerializationType>(103, "quantile_type",
		                                                               QuantileSerializationType::DECIMAL_CONTINUOUS,
		                                                               QuantileSerializationType::NON_DECIMAL);
		serializer.WriteProperty(104, "logical_type", function.arguments[0]);
	}
	static void SerializeDecimalContinuousList(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                                           const AggregateFunction &function) {

		Serialize(serializer, bind_data_p, function);

		serializer.WritePropertyWithDefault<QuantileSerializationType>(
		    103, "quantile_type", QuantileSerializationType::DECIMAL_CONTINUOUS_LIST,
		    QuantileSerializationType::NON_DECIMAL);
		serializer.WriteProperty(104, "logical_type", function.arguments[0]);
	}

	vector<QuantileValue> quantiles;
	vector<idx_t> order;
	bool desc;
};

template <typename IDX>
struct QuantileSortTree : public MergeSortTree<IDX, IDX> {

	using BaseTree = MergeSortTree<IDX, IDX>;
	using Elements = typename BaseTree::Elements;

	explicit QuantileSortTree(Elements &&lowest_level) : BaseTree(std::move(lowest_level)) {
	}

	template <class INPUT_TYPE>
	static unique_ptr<QuantileSortTree> WindowInit(const INPUT_TYPE *data, AggregateInputData &aggr_input_data,
	                                               const ValidityMask &data_mask, const ValidityMask &filter_mask,
	                                               idx_t count) {
		//	Build the indirection array
		using ElementType = typename QuantileSortTree::ElementType;
		vector<ElementType> sorted(count);
		if (filter_mask.AllValid() && data_mask.AllValid()) {
			std::iota(sorted.begin(), sorted.end(), 0);
		} else {
			size_t valid = 0;
			QuantileIncluded included(filter_mask, data_mask);
			for (ElementType i = 0; i < count; ++i) {
				if (included(i)) {
					sorted[valid++] = i;
				}
			}
			sorted.resize(valid);
		}

		//	Sort it
		auto &bind_data = aggr_input_data.bind_data->Cast<QuantileBindData>();
		using Accessor = QuantileIndirect<INPUT_TYPE>;
		Accessor indirect(data);
		QuantileCompare<Accessor> cmp(indirect, bind_data.desc);
		std::sort(sorted.begin(), sorted.end(), cmp);

		return make_uniq<QuantileSortTree>(std::move(sorted));
	}

	inline IDX SelectNth(const SubFrames &frames, size_t n) const {
		return BaseTree::NthElement(BaseTree::SelectNth(frames, n));
	}

	template <typename INPUT_TYPE, typename RESULT_TYPE, bool DISCRETE>
	RESULT_TYPE WindowScalar(const INPUT_TYPE *data, const SubFrames &frames, const idx_t n, Vector &result,
	                         const QuantileValue &q) const {
		D_ASSERT(n > 0);

		//	Find the interpolated indicies within the frame
		Interpolator<DISCRETE> interp(q, n, false);
		const auto lo_data = SelectNth(frames, interp.FRN);
		auto hi_data = lo_data;
		if (interp.CRN != interp.FRN) {
			hi_data = SelectNth(frames, interp.CRN);
		}

		//	Interpolate indirectly
		using ID = QuantileIndirect<INPUT_TYPE>;
		ID indirect(data);
		return interp.template Interpolate<idx_t, RESULT_TYPE, ID>(lo_data, hi_data, result, indirect);
	}

	template <typename INPUT_TYPE, typename CHILD_TYPE, bool DISCRETE>
	void WindowList(const INPUT_TYPE *data, const SubFrames &frames, const idx_t n, Vector &list, const idx_t lidx,
	                const QuantileBindData &bind_data) const {
		D_ASSERT(n > 0);

		// Result is a constant LIST<CHILD_TYPE> with a fixed length
		auto ldata = FlatVector::GetData<list_entry_t>(list);
		auto &lentry = ldata[lidx];
		lentry.offset = ListVector::GetListSize(list);
		lentry.length = bind_data.quantiles.size();

		ListVector::Reserve(list, lentry.offset + lentry.length);
		ListVector::SetListSize(list, lentry.offset + lentry.length);
		auto &result = ListVector::GetEntry(list);
		auto rdata = FlatVector::GetData<CHILD_TYPE>(result);

		using ID = QuantileIndirect<INPUT_TYPE>;
		ID indirect(data);
		for (const auto &q : bind_data.order) {
			const auto &quantile = bind_data.quantiles[q];
			Interpolator<DISCRETE> interp(quantile, n, false);

			const auto lo_data = SelectNth(frames, interp.FRN);
			auto hi_data = lo_data;
			if (interp.CRN != interp.FRN) {
				hi_data = SelectNth(frames, interp.CRN);
			}

			//	Interpolate indirectly
			rdata[lentry.offset + q] =
			    interp.template Interpolate<idx_t, CHILD_TYPE, ID>(lo_data, hi_data, result, indirect);
		}
	}
};

template <class T>
struct PointerLess {
	inline bool operator()(const T &lhi, const T &rhi) const {
		return *lhi < *rhi;
	}
};

template <typename INPUT_TYPE, typename SAVE_TYPE>
struct QuantileState {
	using SaveType = SAVE_TYPE;
	using InputType = INPUT_TYPE;

	// Regular aggregation
	vector<SaveType> v;

	// Windowed Quantile merge sort trees
	using QuantileSortTree32 = QuantileSortTree<uint32_t>;
	using QuantileSortTree64 = QuantileSortTree<uint64_t>;
	unique_ptr<QuantileSortTree32> qst32;
	unique_ptr<QuantileSortTree64> qst64;

	// Windowed Quantile skip lists
	using PointerType = const InputType *;
	using SkipListType = duckdb_skiplistlib::skip_list::HeadNode<PointerType, PointerLess<PointerType>>;
	SubFrames prevs;
	unique_ptr<SkipListType> s;
	mutable vector<PointerType> dest;

	// Windowed MAD indirection
	idx_t count;
	vector<idx_t> m;

	QuantileState() : count(0) {
	}

	~QuantileState() {
	}

	inline void SetCount(size_t count_p) {
		count = count_p;
		if (count >= m.size()) {
			m.resize(count);
		}
	}

	inline SkipListType &GetSkipList(bool reset = false) {
		if (reset || !s) {
			s.reset();
			s = make_uniq<SkipListType>();
		}
		return *s;
	}

	struct SkipListUpdater {
		SkipListType &skip;
		const INPUT_TYPE *data;
		const QuantileIncluded &included;

		inline SkipListUpdater(SkipListType &skip, const INPUT_TYPE *data, const QuantileIncluded &included)
		    : skip(skip), data(data), included(included) {
		}

		inline void Neither(idx_t begin, idx_t end) {
		}

		inline void Left(idx_t begin, idx_t end) {
			for (; begin < end; ++begin) {
				if (included(begin)) {
					skip.remove(data + begin);
				}
			}
		}

		inline void Right(idx_t begin, idx_t end) {
			for (; begin < end; ++begin) {
				if (included(begin)) {
					skip.insert(data + begin);
				}
			}
		}

		inline void Both(idx_t begin, idx_t end) {
		}
	};

	void UpdateSkip(const INPUT_TYPE *data, const SubFrames &frames, const QuantileIncluded &included) {
		//	No overlap, or no data
		if (!s || prevs.back().end <= frames.front().start || frames.back().end <= prevs.front().start) {
			auto &skip = GetSkipList(true);
			for (const auto &frame : frames) {
				for (auto i = frame.start; i < frame.end; ++i) {
					if (included(i)) {
						skip.insert(data + i);
					}
				}
			}
		} else {
			auto &skip = GetSkipList();
			SkipListUpdater updater(skip, data, included);
			AggregateExecutor::IntersectFrames(prevs, frames, updater);
		}
	}

	bool HasTrees() const {
		return qst32 || qst64;
	}

	template <typename RESULT_TYPE, bool DISCRETE>
	RESULT_TYPE WindowScalar(const INPUT_TYPE *data, const SubFrames &frames, const idx_t n, Vector &result,
	                         const QuantileValue &q) const {
		D_ASSERT(n > 0);
		if (qst32) {
			return qst32->WindowScalar<INPUT_TYPE, RESULT_TYPE, DISCRETE>(data, frames, n, result, q);
		} else if (qst64) {
			return qst64->WindowScalar<INPUT_TYPE, RESULT_TYPE, DISCRETE>(data, frames, n, result, q);
		} else if (s) {
			// Find the position(s) needed
			try {
				Interpolator<DISCRETE> interp(q, s->size(), false);
				s->at(interp.FRN, interp.CRN - interp.FRN + 1, dest);
				return interp.template Extract<INPUT_TYPE, RESULT_TYPE>(dest.data(), result);
			} catch (const duckdb_skiplistlib::skip_list::IndexError &idx_err) {
				throw InternalException(idx_err.message());
			}
		} else {
			throw InternalException("No accelerator for scalar QUANTILE");
		}
	}

	template <typename CHILD_TYPE, bool DISCRETE>
	void WindowList(const INPUT_TYPE *data, const SubFrames &frames, const idx_t n, Vector &list, const idx_t lidx,
	                const QuantileBindData &bind_data) const {
		D_ASSERT(n > 0);
		// Result is a constant LIST<CHILD_TYPE> with a fixed length
		auto ldata = FlatVector::GetData<list_entry_t>(list);
		auto &lentry = ldata[lidx];
		lentry.offset = ListVector::GetListSize(list);
		lentry.length = bind_data.quantiles.size();

		ListVector::Reserve(list, lentry.offset + lentry.length);
		ListVector::SetListSize(list, lentry.offset + lentry.length);
		auto &result = ListVector::GetEntry(list);
		auto rdata = FlatVector::GetData<CHILD_TYPE>(result);

		for (const auto &q : bind_data.order) {
			const auto &quantile = bind_data.quantiles[q];
			rdata[lentry.offset + q] = WindowScalar<CHILD_TYPE, DISCRETE>(data, frames, n, result, quantile);
		}
	}
};

struct QuantileOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		new (&state) STATE();
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &) {
		state.v.emplace_back(input);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (source.v.empty()) {
			return;
		}
		target.v.insert(target.v.end(), source.v.begin(), source.v.end());
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		state.~STATE();
	}

	static bool IgnoreNull() {
		return true;
	}

	template <class STATE, class INPUT_TYPE>
	static void WindowInit(AggregateInputData &aggr_input_data, const WindowPartitionInput &partition,
	                       data_ptr_t g_state) {
		D_ASSERT(partition.input_count == 1);

		auto inputs = partition.inputs;
		const auto count = partition.count;
		const auto &filter_mask = partition.filter_mask;
		const auto &stats = partition.stats;

		//	If frames overlap significantly, then use local skip lists.
		if (stats[0].end <= stats[1].begin) {
			//	Frames can overlap
			const auto overlap = double(stats[1].begin - stats[0].end);
			const auto cover = double(stats[1].end - stats[0].begin);
			const auto ratio = overlap / cover;
			if (ratio > .75) {
				return;
			}
		}

		const auto data = FlatVector::GetData<const INPUT_TYPE>(inputs[0]);
		const auto &data_mask = FlatVector::Validity(inputs[0]);

		//	Build the tree
		auto &state = *reinterpret_cast<STATE *>(g_state);
		if (count < std::numeric_limits<uint32_t>::max()) {
			state.qst32 = QuantileSortTree<uint32_t>::WindowInit<INPUT_TYPE>(data, aggr_input_data, data_mask,
			                                                                 filter_mask, count);
		} else {
			state.qst64 = QuantileSortTree<uint64_t>::WindowInit<INPUT_TYPE>(data, aggr_input_data, data_mask,
			                                                                 filter_mask, count);
		}
	}

	static idx_t FrameSize(const QuantileIncluded &included, const SubFrames &frames) {
		//	Count the number of valid values
		idx_t n = 0;
		if (included.AllValid()) {
			for (const auto &frame : frames) {
				n += frame.end - frame.start;
			}
		} else {
			//	NULLs or FILTERed values,
			for (const auto &frame : frames) {
				for (auto i = frame.start; i < frame.end; ++i) {
					n += included(i);
				}
			}
		}

		return n;
	}
};

template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
static AggregateFunction QuantileListAggregate(const LogicalType &input_type, const LogicalType &child_type) { // NOLINT
	LogicalType result_type = LogicalType::LIST(child_type);
	return AggregateFunction(
	    {input_type}, result_type, AggregateFunction::StateSize<STATE>, AggregateFunction::StateInitialize<STATE, OP>,
	    AggregateFunction::UnaryScatterUpdate<STATE, INPUT_TYPE, OP>, AggregateFunction::StateCombine<STATE, OP>,
	    AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>, AggregateFunction::UnaryUpdate<STATE, INPUT_TYPE, OP>,
	    nullptr, AggregateFunction::StateDestroy<STATE, OP>);
}

template <bool DISCRETE>
struct QuantileScalarOperation : public QuantileOperation {

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.v.empty()) {
			finalize_data.ReturnNull();
			return;
		}
		D_ASSERT(finalize_data.input.bind_data);
		auto &bind_data = finalize_data.input.bind_data->Cast<QuantileBindData>();
		D_ASSERT(bind_data.quantiles.size() == 1);
		Interpolator<DISCRETE> interp(bind_data.quantiles[0], state.v.size(), bind_data.desc);
		target = interp.template Operation<typename STATE::SaveType, T>(state.v.data(), finalize_data.result);
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE>
	static void Window(const INPUT_TYPE *data, const ValidityMask &fmask, const ValidityMask &dmask,
	                   AggregateInputData &aggr_input_data, STATE &state, const SubFrames &frames, Vector &result,
	                   idx_t ridx, const STATE *gstate) {
		QuantileIncluded included(fmask, dmask);
		const auto n = FrameSize(included, frames);

		D_ASSERT(aggr_input_data.bind_data);
		auto &bind_data = aggr_input_data.bind_data->Cast<QuantileBindData>();

		auto rdata = FlatVector::GetData<RESULT_TYPE>(result);
		auto &rmask = FlatVector::Validity(result);

		if (!n) {
			rmask.Set(ridx, false);
			return;
		}

		const auto &quantile = bind_data.quantiles[0];
		if (gstate && gstate->HasTrees()) {
			rdata[ridx] = gstate->template WindowScalar<RESULT_TYPE, DISCRETE>(data, frames, n, result, quantile);
		} else {
			//	Update the skip list
			state.UpdateSkip(data, frames, included);

			// Find the position(s) needed
			rdata[ridx] = state.template WindowScalar<RESULT_TYPE, DISCRETE>(data, frames, n, result, quantile);

			//	Save the previous state for next time
			state.prevs = frames;
		}
	}
};

template <typename INPUT_TYPE, typename SAVED_TYPE>
AggregateFunction GetTypedDiscreteQuantileAggregateFunction(const LogicalType &type) {
	using STATE = QuantileState<INPUT_TYPE, SAVED_TYPE>;
	using OP = QuantileScalarOperation<true>;
	auto fun = AggregateFunction::UnaryAggregateDestructor<STATE, INPUT_TYPE, INPUT_TYPE, OP>(type, type);
	fun.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, INPUT_TYPE, OP>;
	fun.window_init = OP::WindowInit<STATE, INPUT_TYPE>;
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
	case LogicalTypeId::DATE:
		return GetTypedDiscreteQuantileAggregateFunction<int32_t, int32_t>(type);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return GetTypedDiscreteQuantileAggregateFunction<int64_t, int64_t>(type);
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
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

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.v.empty()) {
			finalize_data.ReturnNull();
			return;
		}

		D_ASSERT(finalize_data.input.bind_data);
		auto &bind_data = finalize_data.input.bind_data->Cast<QuantileBindData>();

		auto &result = ListVector::GetEntry(finalize_data.result);
		auto ridx = ListVector::GetListSize(finalize_data.result);
		ListVector::Reserve(finalize_data.result, ridx + bind_data.quantiles.size());
		auto rdata = FlatVector::GetData<CHILD_TYPE>(result);

		auto v_t = state.v.data();
		D_ASSERT(v_t);

		auto &entry = target;
		entry.offset = ridx;
		idx_t lower = 0;
		for (const auto &q : bind_data.order) {
			const auto &quantile = bind_data.quantiles[q];
			Interpolator<DISCRETE> interp(quantile, state.v.size(), bind_data.desc);
			interp.begin = lower;
			rdata[ridx + q] = interp.template Operation<typename STATE::SaveType, CHILD_TYPE>(v_t, result);
			lower = interp.FRN;
		}
		entry.length = bind_data.quantiles.size();

		ListVector::SetListSize(finalize_data.result, entry.offset + entry.length);
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE>
	static void Window(const INPUT_TYPE *data, const ValidityMask &fmask, const ValidityMask &dmask,
	                   AggregateInputData &aggr_input_data, STATE &state, const SubFrames &frames, Vector &list,
	                   idx_t lidx, const STATE *gstate) {
		D_ASSERT(aggr_input_data.bind_data);
		auto &bind_data = aggr_input_data.bind_data->Cast<QuantileBindData>();

		QuantileIncluded included(fmask, dmask);
		const auto n = FrameSize(included, frames);

		// Result is a constant LIST<RESULT_TYPE> with a fixed length
		if (!n) {
			auto &lmask = FlatVector::Validity(list);
			lmask.Set(lidx, false);
			return;
		}

		if (gstate && gstate->HasTrees()) {
			gstate->template WindowList<CHILD_TYPE, DISCRETE>(data, frames, n, list, lidx, bind_data);
		} else {
			//
			state.UpdateSkip(data, frames, included);
			state.template WindowList<CHILD_TYPE, DISCRETE>(data, frames, n, list, lidx, bind_data);
			state.prevs = frames;
		}
	}
};

template <typename INPUT_TYPE, typename SAVE_TYPE>
AggregateFunction GetTypedDiscreteQuantileListAggregateFunction(const LogicalType &type) {
	using STATE = QuantileState<INPUT_TYPE, SAVE_TYPE>;
	using OP = QuantileListOperation<INPUT_TYPE, true>;
	auto fun = QuantileListAggregate<STATE, INPUT_TYPE, list_entry_t, OP>(type, type);
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	fun.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, list_entry_t, OP>;
	fun.window_init = OP::template WindowInit<STATE, INPUT_TYPE>;
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
	case LogicalTypeId::DATE:
		return GetTypedDiscreteQuantileListAggregateFunction<date_t, date_t>(type);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return GetTypedDiscreteQuantileListAggregateFunction<timestamp_t, timestamp_t>(type);
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
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
	using STATE = QuantileState<INPUT_TYPE, INPUT_TYPE>;
	using OP = QuantileScalarOperation<false>;
	auto fun = AggregateFunction::UnaryAggregateDestructor<STATE, INPUT_TYPE, TARGET_TYPE, OP>(input_type, target_type);
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	fun.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, TARGET_TYPE, OP>;
	fun.window_init = OP::template WindowInit<STATE, INPUT_TYPE>;
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
	case LogicalTypeId::DATE:
		return GetTypedContinuousQuantileAggregateFunction<date_t, timestamp_t>(type, LogicalType::TIMESTAMP);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return GetTypedContinuousQuantileAggregateFunction<timestamp_t, timestamp_t>(type, type);
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return GetTypedContinuousQuantileAggregateFunction<dtime_t, dtime_t>(type, type);

	default:
		throw NotImplementedException("Unimplemented continuous quantile aggregate");
	}
}

template <typename INPUT_TYPE, typename CHILD_TYPE>
AggregateFunction GetTypedContinuousQuantileListAggregateFunction(const LogicalType &input_type,
                                                                  const LogicalType &result_type) {
	using STATE = QuantileState<INPUT_TYPE, INPUT_TYPE>;
	using OP = QuantileListOperation<CHILD_TYPE, false>;
	auto fun = QuantileListAggregate<STATE, INPUT_TYPE, list_entry_t, OP>(input_type, result_type);
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	fun.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, list_entry_t, OP>;
	fun.window_init = OP::template WindowInit<STATE, INPUT_TYPE>;
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
	case LogicalTypeId::TIMESTAMP_TZ:
		return GetTypedContinuousQuantileListAggregateFunction<timestamp_t, timestamp_t>(type, type);
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
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
		return TryAbsOperator::Operation<RESULT_TYPE, RESULT_TYPE>(delta);
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
		return TryAbsOperator::Operation<double, double>(delta);
	}
};

// date_t - timestamp_t => interval_t
template <>
struct MadAccessor<date_t, interval_t, timestamp_t> {
	using INPUT_TYPE = date_t;
	using RESULT_TYPE = interval_t;
	using MEDIAN_TYPE = timestamp_t;
	const MEDIAN_TYPE &median;
	explicit MadAccessor(const MEDIAN_TYPE &median_p) : median(median_p) {
	}
	inline RESULT_TYPE operator()(const INPUT_TYPE &input) const {
		const auto dt = Cast::Operation<date_t, timestamp_t>(input);
		const auto delta = dt - median;
		return Interval::FromMicro(TryAbsOperator::Operation<int64_t, int64_t>(delta));
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
		return Interval::FromMicro(TryAbsOperator::Operation<int64_t, int64_t>(delta));
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
		return Interval::FromMicro(TryAbsOperator::Operation<int64_t, int64_t>(delta));
	}
};

template <typename MEDIAN_TYPE>
struct MedianAbsoluteDeviationOperation : public QuantileOperation {

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.v.empty()) {
			finalize_data.ReturnNull();
			return;
		}
		using SAVE_TYPE = typename STATE::SaveType;
		D_ASSERT(finalize_data.input.bind_data);
		auto &bind_data = finalize_data.input.bind_data->Cast<QuantileBindData>();
		D_ASSERT(bind_data.quantiles.size() == 1);
		const auto &q = bind_data.quantiles[0];
		Interpolator<false> interp(q, state.v.size(), false);
		const auto med = interp.template Operation<SAVE_TYPE, MEDIAN_TYPE>(state.v.data(), finalize_data.result);

		MadAccessor<SAVE_TYPE, T, MEDIAN_TYPE> accessor(med);
		target = interp.template Operation<SAVE_TYPE, T>(state.v.data(), finalize_data.result, accessor);
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE>
	static void Window(const INPUT_TYPE *data, const ValidityMask &fmask, const ValidityMask &dmask,
	                   AggregateInputData &aggr_input_data, STATE &state, const SubFrames &frames, Vector &result,
	                   idx_t ridx, const STATE *gstate) {
		auto rdata = FlatVector::GetData<RESULT_TYPE>(result);

		QuantileIncluded included(fmask, dmask);
		const auto n = FrameSize(included, frames);

		if (!n) {
			auto &rmask = FlatVector::Validity(result);
			rmask.Set(ridx, false);
			return;
		}

		//	Compute the median
		D_ASSERT(aggr_input_data.bind_data);
		auto &bind_data = aggr_input_data.bind_data->Cast<QuantileBindData>();

		D_ASSERT(bind_data.quantiles.size() == 1);
		const auto &quantile = bind_data.quantiles[0];
		MEDIAN_TYPE med;
		if (gstate && gstate->HasTrees()) {
			med = gstate->template WindowScalar<MEDIAN_TYPE, false>(data, frames, n, result, quantile);
		} else {
			state.UpdateSkip(data, frames, included);
			med = state.template WindowScalar<MEDIAN_TYPE, false>(data, frames, n, result, quantile);
		}

		//  Lazily initialise frame state
		state.SetCount(frames.back().end - frames.front().start);
		auto index2 = state.m.data();
		D_ASSERT(index2);

		// The replacement trick does not work on the second index because if
		// the median has changed, the previous order is not correct.
		// It is probably close, however, and so reuse is helpful.
		auto &prevs = state.prevs;
		ReuseIndexes(index2, frames, prevs);
		std::partition(index2, index2 + state.count, included);

		Interpolator<false> interp(quantile, n, false);

		// Compute mad from the second index
		using ID = QuantileIndirect<INPUT_TYPE>;
		ID indirect(data);

		using MAD = MadAccessor<INPUT_TYPE, RESULT_TYPE, MEDIAN_TYPE>;
		MAD mad(med);

		using MadIndirect = QuantileComposed<MAD, ID>;
		MadIndirect mad_indirect(mad, indirect);
		rdata[ridx] = interp.template Operation<idx_t, RESULT_TYPE, MadIndirect>(index2, result, mad_indirect);

		//	Prev is used by both skip lists and increments
		prevs = frames;
	}
};

unique_ptr<FunctionData> BindMedian(ClientContext &context, AggregateFunction &function,
                                    vector<unique_ptr<Expression>> &arguments) {
	return make_uniq<QuantileBindData>(Value::DECIMAL(int16_t(5), 2, 1));
}

template <typename INPUT_TYPE, typename MEDIAN_TYPE, typename TARGET_TYPE>
AggregateFunction GetTypedMedianAbsoluteDeviationAggregateFunction(const LogicalType &input_type,
                                                                   const LogicalType &target_type) {
	using STATE = QuantileState<INPUT_TYPE, INPUT_TYPE>;
	using OP = MedianAbsoluteDeviationOperation<MEDIAN_TYPE>;
	auto fun = AggregateFunction::UnaryAggregateDestructor<STATE, INPUT_TYPE, TARGET_TYPE, OP>(input_type, target_type);
	fun.bind = BindMedian;
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	fun.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, TARGET_TYPE, OP>;
	fun.window_init = OP::template WindowInit<STATE, INPUT_TYPE>;
	return fun;
}

AggregateFunction GetMedianAbsoluteDeviationAggregateFunction(const LogicalType &type) {
	switch (type.id()) {
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
	case LogicalTypeId::TIMESTAMP_TZ:
		return GetTypedMedianAbsoluteDeviationAggregateFunction<timestamp_t, timestamp_t, interval_t>(
		    type, LogicalType::INTERVAL);
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return GetTypedMedianAbsoluteDeviationAggregateFunction<dtime_t, dtime_t, interval_t>(type,
		                                                                                      LogicalType::INTERVAL);

	default:
		throw NotImplementedException("Unimplemented Median Absolute Deviation aggregate");
	}
}

unique_ptr<FunctionData> BindMedianDecimal(ClientContext &context, AggregateFunction &function,
                                           vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindMedian(context, function, arguments);

	function = GetDiscreteQuantileAggregateFunction(arguments[0]->return_type);
	function.name = "median";
	function.serialize = QuantileBindData::SerializeDecimalDiscrete;
	function.deserialize = QuantileBindData::Deserialize;
	function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return bind_data;
}

unique_ptr<FunctionData> BindMedianAbsoluteDeviationDecimal(ClientContext &context, AggregateFunction &function,
                                                            vector<unique_ptr<Expression>> &arguments) {
	function = GetMedianAbsoluteDeviationAggregateFunction(arguments[0]->return_type);
	function.name = "mad";
	function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return BindMedian(context, function, arguments);
}

static const Value &CheckQuantile(const Value &quantile_val) {
	if (quantile_val.IsNull()) {
		throw BinderException("QUANTILE parameter cannot be NULL");
	}
	auto quantile = quantile_val.GetValue<double>();
	if (quantile < -1 || quantile > 1) {
		throw BinderException("QUANTILE can only take parameters in the range [-1, 1]");
	}
	if (Value::IsNan(quantile)) {
		throw BinderException("QUANTILE parameter cannot be NaN");
	}

	return quantile_val;
}

unique_ptr<FunctionData> BindQuantile(ClientContext &context, AggregateFunction &function,
                                      vector<unique_ptr<Expression>> &arguments) {
	if (arguments[1]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[1]->IsFoldable()) {
		throw BinderException("QUANTILE can only take constant parameters");
	}
	Value quantile_val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
	vector<Value> quantiles;
	if (quantile_val.type().id() != LogicalTypeId::LIST) {
		quantiles.push_back(CheckQuantile(quantile_val));
	} else {
		for (const auto &element_val : ListValue::GetChildren(quantile_val)) {
			quantiles.push_back(CheckQuantile(element_val));
		}
	}

	Function::EraseArgument(function, arguments, arguments.size() - 1);
	return make_uniq<QuantileBindData>(quantiles);
}

void BindQuantileInner(AggregateFunction &function, const LogicalType &type, QuantileSerializationType quantile_type) {
	switch (quantile_type) {
	case QuantileSerializationType::DECIMAL_DISCRETE:
		function = GetDiscreteQuantileAggregateFunction(type);
		function.serialize = QuantileBindData::SerializeDecimalDiscrete;
		function.name = "quantile_disc";
		break;
	case QuantileSerializationType::DECIMAL_DISCRETE_LIST:
		function = GetDiscreteQuantileListAggregateFunction(type);
		function.serialize = QuantileBindData::SerializeDecimalDiscreteList;
		function.name = "quantile_disc";
		break;
	case QuantileSerializationType::DECIMAL_CONTINUOUS:
		function = GetContinuousQuantileAggregateFunction(type);
		function.serialize = QuantileBindData::SerializeDecimalContinuous;
		function.name = "quantile_cont";
		break;
	case QuantileSerializationType::DECIMAL_CONTINUOUS_LIST:
		function = GetContinuousQuantileListAggregateFunction(type);
		function.serialize = QuantileBindData::SerializeDecimalContinuousList;
		function.name = "quantile_cont";
		break;
	case QuantileSerializationType::NON_DECIMAL:
		throw SerializationException("NON_DECIMAL is not a valid quantile_type for BindQuantileInner");
	}
	function.deserialize = QuantileBindData::Deserialize;
	function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
}

unique_ptr<FunctionData> BindDiscreteQuantileDecimal(ClientContext &context, AggregateFunction &function,
                                                     vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindQuantile(context, function, arguments);
	BindQuantileInner(function, arguments[0]->return_type, QuantileSerializationType::DECIMAL_DISCRETE);
	return bind_data;
}

unique_ptr<FunctionData> BindDiscreteQuantileDecimalList(ClientContext &context, AggregateFunction &function,
                                                         vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindQuantile(context, function, arguments);
	BindQuantileInner(function, arguments[0]->return_type, QuantileSerializationType::DECIMAL_DISCRETE_LIST);
	return bind_data;
}

unique_ptr<FunctionData> BindContinuousQuantileDecimal(ClientContext &context, AggregateFunction &function,
                                                       vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindQuantile(context, function, arguments);
	BindQuantileInner(function, arguments[0]->return_type, QuantileSerializationType::DECIMAL_CONTINUOUS);
	return bind_data;
}

unique_ptr<FunctionData> BindContinuousQuantileDecimalList(ClientContext &context, AggregateFunction &function,
                                                           vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindQuantile(context, function, arguments);
	BindQuantileInner(function, arguments[0]->return_type, QuantileSerializationType::DECIMAL_CONTINUOUS_LIST);
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
	fun.serialize = QuantileBindData::Serialize;
	fun.deserialize = QuantileBindData::Deserialize;
	return fun;
}

AggregateFunction GetDiscreteQuantileAggregate(const LogicalType &type) {
	auto fun = GetDiscreteQuantileAggregateFunction(type);
	fun.bind = BindQuantile;
	fun.serialize = QuantileBindData::Serialize;
	fun.deserialize = QuantileBindData::Deserialize;
	// temporarily push an argument so we can bind the actual quantile
	fun.arguments.emplace_back(LogicalType::DOUBLE);
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return fun;
}

AggregateFunction GetDiscreteQuantileListAggregate(const LogicalType &type) {
	auto fun = GetDiscreteQuantileListAggregateFunction(type);
	fun.bind = BindQuantile;
	fun.serialize = QuantileBindData::Serialize;
	fun.deserialize = QuantileBindData::Deserialize;
	// temporarily push an argument so we can bind the actual quantile
	auto list_of_double = LogicalType::LIST(LogicalType::DOUBLE);
	fun.arguments.push_back(list_of_double);
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return fun;
}

AggregateFunction GetContinuousQuantileAggregate(const LogicalType &type) {
	auto fun = GetContinuousQuantileAggregateFunction(type);
	fun.bind = BindQuantile;
	fun.serialize = QuantileBindData::Serialize;
	fun.deserialize = QuantileBindData::Deserialize;
	// temporarily push an argument so we can bind the actual quantile
	fun.arguments.emplace_back(LogicalType::DOUBLE);
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return fun;
}

AggregateFunction GetContinuousQuantileListAggregate(const LogicalType &type) {
	auto fun = GetContinuousQuantileListAggregateFunction(type);
	fun.bind = BindQuantile;
	fun.serialize = QuantileBindData::Serialize;
	fun.deserialize = QuantileBindData::Deserialize;
	// temporarily push an argument so we can bind the actual quantile
	auto list_of_double = LogicalType::LIST(LogicalType::DOUBLE);
	fun.arguments.push_back(list_of_double);
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return fun;
}

AggregateFunction GetQuantileDecimalAggregate(const vector<LogicalType> &arguments, const LogicalType &return_type,
                                              bind_aggregate_function_t bind) {
	AggregateFunction fun(arguments, return_type, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, bind);
	fun.bind = bind;
	fun.serialize = QuantileBindData::Serialize;
	fun.deserialize = QuantileBindData::Deserialize;
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return fun;
}

vector<LogicalType> GetQuantileTypes() {
	return {LogicalType::TINYINT,   LogicalType::SMALLINT, LogicalType::INTEGER,      LogicalType::BIGINT,
	        LogicalType::HUGEINT,   LogicalType::FLOAT,    LogicalType::DOUBLE,       LogicalType::DATE,
	        LogicalType::TIMESTAMP, LogicalType::TIME,     LogicalType::TIMESTAMP_TZ, LogicalType::TIME_TZ,
	        LogicalType::INTERVAL,  LogicalType::VARCHAR};
}

AggregateFunctionSet MedianFun::GetFunctions() {
	AggregateFunctionSet median("median");
	median.AddFunction(
	    GetQuantileDecimalAggregate({LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL, BindMedianDecimal));
	for (const auto &type : GetQuantileTypes()) {
		median.AddFunction(GetMedianAggregate(type));
	}
	return median;
}

AggregateFunctionSet QuantileDiscFun::GetFunctions() {
	AggregateFunctionSet quantile_disc("quantile_disc");
	quantile_disc.AddFunction(GetQuantileDecimalAggregate({LogicalTypeId::DECIMAL, LogicalType::DOUBLE},
	                                                      LogicalTypeId::DECIMAL, BindDiscreteQuantileDecimal));
	quantile_disc.AddFunction(
	    GetQuantileDecimalAggregate({LogicalTypeId::DECIMAL, LogicalType::LIST(LogicalType::DOUBLE)},
	                                LogicalType::LIST(LogicalTypeId::DECIMAL), BindDiscreteQuantileDecimalList));
	for (const auto &type : GetQuantileTypes()) {
		quantile_disc.AddFunction(GetDiscreteQuantileAggregate(type));
		quantile_disc.AddFunction(GetDiscreteQuantileListAggregate(type));
	}
	return quantile_disc;
	// quantile
}

AggregateFunctionSet QuantileContFun::GetFunctions() {
	AggregateFunctionSet quantile_cont("quantile_cont");
	quantile_cont.AddFunction(GetQuantileDecimalAggregate({LogicalTypeId::DECIMAL, LogicalType::DOUBLE},
	                                                      LogicalTypeId::DECIMAL, BindContinuousQuantileDecimal));
	quantile_cont.AddFunction(
	    GetQuantileDecimalAggregate({LogicalTypeId::DECIMAL, LogicalType::LIST(LogicalType::DOUBLE)},
	                                LogicalType::LIST(LogicalTypeId::DECIMAL), BindContinuousQuantileDecimalList));

	for (const auto &type : GetQuantileTypes()) {
		if (CanInterpolate(type)) {
			quantile_cont.AddFunction(GetContinuousQuantileAggregate(type));
			quantile_cont.AddFunction(GetContinuousQuantileListAggregate(type));
		}
	}
	return quantile_cont;
}

AggregateFunctionSet MadFun::GetFunctions() {
	AggregateFunctionSet mad("mad");
	mad.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL, nullptr, nullptr, nullptr,
	                                  nullptr, nullptr, nullptr, BindMedianAbsoluteDeviationDecimal));

	const vector<LogicalType> MAD_TYPES = {LogicalType::FLOAT,     LogicalType::DOUBLE, LogicalType::DATE,
	                                       LogicalType::TIMESTAMP, LogicalType::TIME,   LogicalType::TIMESTAMP_TZ,
	                                       LogicalType::TIME_TZ};
	for (const auto &type : MAD_TYPES) {
		mad.AddFunction(GetMedianAbsoluteDeviationAggregateFunction(type));
	}
	return mad;
}

} // namespace duckdb

#include "duckdb/execution/expression_executor.hpp"
#include "core_functions/aggregate/holistic_functions.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/abs.hpp"
#include "core_functions/aggregate/quantile_state.hpp"

namespace duckdb {

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

//===--------------------------------------------------------------------===//
// Median Absolute Deviation
//===--------------------------------------------------------------------===//
template <typename T, typename R, typename MEDIAN_TYPE>
struct MadAccessor {
	using INPUT_TYPE = T;
	using RESULT_TYPE = R;
	const MEDIAN_TYPE &median;
	explicit MadAccessor(const MEDIAN_TYPE &median_p) : median(median_p) {
	}

	inline RESULT_TYPE operator()(const INPUT_TYPE &input) const {
		const RESULT_TYPE delta = input - UnsafeNumericCast<RESULT_TYPE>(median);
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
struct MedianAbsoluteDeviationOperation : QuantileOperation {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.v.empty()) {
			finalize_data.ReturnNull();
			return;
		}
		using INPUT_TYPE = typename STATE::InputType;
		D_ASSERT(finalize_data.input.bind_data);
		auto &bind_data = finalize_data.input.bind_data->Cast<QuantileBindData>();
		D_ASSERT(bind_data.quantiles.size() == 1);
		const auto &q = bind_data.quantiles[0];
		Interpolator<false> interp(q, state.v.size(), false);
		const auto med = interp.template Operation<INPUT_TYPE, MEDIAN_TYPE>(state.v.data(), finalize_data.result);

		MadAccessor<INPUT_TYPE, T, MEDIAN_TYPE> accessor(med);
		target = interp.template Operation<INPUT_TYPE, T>(state.v.data(), finalize_data.result, accessor);
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE>
	static void Window(AggregateInputData &aggr_input_data, const WindowPartitionInput &partition,
	                   const_data_ptr_t g_state, data_ptr_t l_state, const SubFrames &frames, Vector &result,
	                   idx_t ridx) {
		auto &state = *reinterpret_cast<STATE *>(l_state);
		auto gstate = reinterpret_cast<const STATE *>(g_state);

		auto &data = state.GetOrCreateWindowCursor(partition);
		const auto &fmask = partition.filter_mask;

		auto rdata = FlatVector::GetData<RESULT_TYPE>(result);

		QuantileIncluded<INPUT_TYPE> included(fmask, data);
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
		auto &window_state = state.GetOrCreateWindowState();
		MEDIAN_TYPE med;
		if (gstate && gstate->HasTrees()) {
			med = gstate->GetWindowState().template WindowScalar<MEDIAN_TYPE, false>(data, frames, n, result, quantile);
		} else {
			window_state.UpdateSkip(data, frames, included);
			med = window_state.template WindowScalar<MEDIAN_TYPE, false>(data, frames, n, result, quantile);
		}

		//  Lazily initialise frame state
		window_state.SetCount(frames.back().end - frames.front().start);
		auto index2 = window_state.m.data();
		D_ASSERT(index2);

		// The replacement trick does not work on the second index because if
		// the median has changed, the previous order is not correct.
		// It is probably close, however, and so reuse is helpful.
		auto &prevs = window_state.prevs;
		ReuseIndexes(index2, frames, prevs);
		std::partition(index2, index2 + window_state.count, included);

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

unique_ptr<FunctionData> BindMAD(ClientContext &context, AggregateFunction &function,
                                 vector<unique_ptr<Expression>> &arguments) {
	return make_uniq<QuantileBindData>(Value::DECIMAL(int16_t(5), 2, 1));
}

template <typename INPUT_TYPE, typename MEDIAN_TYPE, typename TARGET_TYPE>
AggregateFunction GetTypedMedianAbsoluteDeviationAggregateFunction(const LogicalType &input_type,
                                                                   const LogicalType &target_type) {
	using STATE = QuantileState<INPUT_TYPE, QuantileStandardType>;
	using OP = MedianAbsoluteDeviationOperation<MEDIAN_TYPE>;
	auto fun = AggregateFunction::UnaryAggregateDestructor<STATE, INPUT_TYPE, TARGET_TYPE, OP,
	                                                       AggregateDestructorType::LEGACY>(input_type, target_type);
	fun.bind = BindMAD;
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
#ifndef DUCKDB_SMALLER_BINARY
	fun.window = OP::template Window<STATE, INPUT_TYPE, TARGET_TYPE>;
	fun.window_init = OP::template WindowInit<STATE, INPUT_TYPE>;
#endif
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

unique_ptr<FunctionData> BindMedianAbsoluteDeviationDecimal(ClientContext &context, AggregateFunction &function,
                                                            vector<unique_ptr<Expression>> &arguments) {
	function = GetMedianAbsoluteDeviationAggregateFunction(arguments[0]->return_type);
	function.name = "mad";
	function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return BindMAD(context, function, arguments);
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

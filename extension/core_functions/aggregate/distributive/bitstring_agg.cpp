#include "core_functions/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/aggregate_executor.hpp"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

template <class INPUT_TYPE>
struct BitAggState {
	bool is_set;
	string_t value;
	INPUT_TYPE min;
	INPUT_TYPE max;
};

struct BitstringAggBindData : public FunctionData {
	Value min;
	Value max;

	BitstringAggBindData() {
	}

	BitstringAggBindData(Value min, Value max) : min(std::move(min)), max(std::move(max)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<BitstringAggBindData>(*this);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<BitstringAggBindData>();
		if (min.IsNull() && other.min.IsNull() && max.IsNull() && other.max.IsNull()) {
			return true;
		}
		if (Value::NotDistinctFrom(min, other.min) && Value::NotDistinctFrom(max, other.max)) {
			return true;
		}
		return false;
	}

	static void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                      const AggregateFunction &) {
		auto &bind_data = bind_data_p->Cast<BitstringAggBindData>();
		serializer.WriteProperty(100, "min", bind_data.min);
		serializer.WriteProperty(101, "max", bind_data.max);
	}

	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, AggregateFunction &) {
		Value min;
		Value max;
		deserializer.ReadProperty(100, "min", min);
		deserializer.ReadProperty(101, "max", max);
		return make_uniq<BitstringAggBindData>(min, max);
	}
};

struct BitStringAggOperation {
	static constexpr const idx_t MAX_BIT_RANGE = 1000000000; // for now capped at 1 billion bits

	template <class STATE>
	static void Initialize(STATE &state) {
		state.is_set = false;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		auto &bind_agg_data = unary_input.input.bind_data->template Cast<BitstringAggBindData>();
		if (!state.is_set) {
			if (bind_agg_data.min.IsNull() || bind_agg_data.max.IsNull()) {
				throw BinderException(
				    "Could not retrieve required statistics. Alternatively, try by providing the statistics "
				    "explicitly: BITSTRING_AGG(col, min, max) ");
			}
			state.min = bind_agg_data.min.GetValue<INPUT_TYPE>();
			state.max = bind_agg_data.max.GetValue<INPUT_TYPE>();
			if (state.min > state.max) {
				throw InvalidInputException("Invalid explicit bitstring range: Minimum (%s) > maximum (%s)",
				                            NumericHelper::ToString(state.min), NumericHelper::ToString(state.max));
			}
			idx_t bit_range =
			    GetRange(bind_agg_data.min.GetValue<INPUT_TYPE>(), bind_agg_data.max.GetValue<INPUT_TYPE>());
			if (bit_range > MAX_BIT_RANGE) {
				throw OutOfRangeException(
				    "The range between min and max value (%s <-> %s) is too large for bitstring aggregation",
				    NumericHelper::ToString(state.min), NumericHelper::ToString(state.max));
			}
			idx_t len = Bit::ComputeBitstringLen(bit_range);
			auto target = len > string_t::INLINE_LENGTH ? string_t(new char[len], UnsafeNumericCast<uint32_t>(len))
			                                            : string_t(UnsafeNumericCast<uint32_t>(len));
			Bit::SetEmptyBitString(target, bit_range);

			state.value = target;
			state.is_set = true;
		}
		if (input >= state.min && input <= state.max) {
			Execute(state, input, bind_agg_data.min.GetValue<INPUT_TYPE>());
		} else {
			throw OutOfRangeException("Value %s is outside of provided min and max range (%s <-> %s)",
			                          NumericHelper::ToString(input), NumericHelper::ToString(state.min),
			                          NumericHelper::ToString(state.max));
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		OP::template Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
	}

	template <class INPUT_TYPE>
	static idx_t GetRange(INPUT_TYPE min, INPUT_TYPE max) {
		if (min > max) {
			throw InvalidInputException("Invalid explicit bitstring range: Minimum (%d) > maximum (%d)", min, max);
		}
		INPUT_TYPE result;
		if (!TrySubtractOperator::Operation(max, min, result)) {
			return NumericLimits<idx_t>::Maximum();
		}
		auto val = NumericCast<idx_t>(result);
		if (val == NumericLimits<idx_t>::Maximum()) {
			return val;
		}
		return val + 1;
	}

	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input, INPUT_TYPE min) {
		Bit::SetBit(state.value, UnsafeNumericCast<idx_t>(input - min), 1);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.is_set) {
			return;
		}
		if (!target.is_set) {
			Assign(target, source.value);
			target.is_set = true;
			target.min = source.min;
			target.max = source.max;
		} else {
			Bit::BitwiseOr(source.value, target.value, target.value);
		}
	}

	template <class INPUT_TYPE, class STATE>
	static void Assign(STATE &state, INPUT_TYPE input) {
		D_ASSERT(state.is_set == false);
		if (input.IsInlined()) {
			state.value = input;
		} else { // non-inlined string, need to allocate space for it
			auto len = input.GetSize();
			auto ptr = new char[len];
			memcpy(ptr, input.GetData(), len);
			state.value = string_t(ptr, UnsafeNumericCast<uint32_t>(len));
		}
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.is_set) {
			finalize_data.ReturnNull();
		} else {
			target = StringVector::AddStringOrBlob(finalize_data.result, state.value);
		}
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		if (state.is_set && !state.value.IsInlined()) {
			delete[] state.value.GetData();
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

template <>
void BitStringAggOperation::Execute(BitAggState<hugeint_t> &state, hugeint_t input, hugeint_t min) {
	idx_t val;
	if (Hugeint::TryCast(input - min, val)) {
		Bit::SetBit(state.value, val, 1);
	} else {
		throw OutOfRangeException("Range too large for bitstring aggregation");
	}
}

template <>
idx_t BitStringAggOperation::GetRange(hugeint_t min, hugeint_t max) {
	hugeint_t result;
	if (!TrySubtractOperator::Operation(max, min, result)) {
		return NumericLimits<idx_t>::Maximum();
	}
	idx_t range;
	if (!Hugeint::TryCast(result + 1, range) || result == NumericLimits<hugeint_t>::Maximum()) {
		return NumericLimits<idx_t>::Maximum();
	}
	return range;
}

template <>
void BitStringAggOperation::Execute(BitAggState<uhugeint_t> &state, uhugeint_t input, uhugeint_t min) {
	idx_t val;
	if (Uhugeint::TryCast(input - min, val)) {
		Bit::SetBit(state.value, val, 1);
	} else {
		throw OutOfRangeException("Range too large for bitstring aggregation");
	}
}

template <>
idx_t BitStringAggOperation::GetRange(uhugeint_t min, uhugeint_t max) {
	uhugeint_t result;
	if (!TrySubtractOperator::Operation(max, min, result)) {
		return NumericLimits<idx_t>::Maximum();
	}
	idx_t range;
	if (!Uhugeint::TryCast(result + 1, range) || result == NumericLimits<uhugeint_t>::Maximum()) {
		return NumericLimits<idx_t>::Maximum();
	}
	return range;
}

unique_ptr<BaseStatistics> BitstringPropagateStats(ClientContext &context, BoundAggregateExpression &expr,
                                                   AggregateStatisticsInput &input) {

	if (NumericStats::HasMinMax(input.child_stats[0])) {
		auto &bind_agg_data = input.bind_data->Cast<BitstringAggBindData>();
		bind_agg_data.min = NumericStats::Min(input.child_stats[0]);
		bind_agg_data.max = NumericStats::Max(input.child_stats[0]);
	}
	return nullptr;
}

unique_ptr<FunctionData> BindBitstringAgg(ClientContext &context, AggregateFunction &function,
                                          vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() == 3) {
		if (!arguments[1]->IsFoldable() || !arguments[2]->IsFoldable()) {
			throw BinderException("bitstring_agg requires a constant min and max argument");
		}
		auto min = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
		auto max = ExpressionExecutor::EvaluateScalar(context, *arguments[2]);
		Function::EraseArgument(function, arguments, 2);
		Function::EraseArgument(function, arguments, 1);
		return make_uniq<BitstringAggBindData>(min, max);
	}
	return make_uniq<BitstringAggBindData>();
}

template <class TYPE>
static void BindBitString(AggregateFunctionSet &bitstring_agg, const LogicalTypeId &type) {
	auto function =
	    AggregateFunction::UnaryAggregateDestructor<BitAggState<TYPE>, TYPE, string_t, BitStringAggOperation>(
	        type, LogicalType::BIT);
	function.bind = BindBitstringAgg; // create new a 'BitstringAggBindData'
	function.serialize = BitstringAggBindData::Serialize;
	function.deserialize = BitstringAggBindData::Deserialize;
	function.statistics = BitstringPropagateStats; // stores min and max from column stats in BitstringAggBindData
	bitstring_agg.AddFunction(function); // uses the BitstringAggBindData to access statistics for creating bitstring
	function.arguments = {type, type, type};
	function.statistics = nullptr; // min and max are provided as arguments
	bitstring_agg.AddFunction(function);
}

void GetBitStringAggregate(const LogicalType &type, AggregateFunctionSet &bitstring_agg) {
	switch (type.id()) {
	case LogicalType::TINYINT: {
		return BindBitString<int8_t>(bitstring_agg, type.id());
	}
	case LogicalType::SMALLINT: {
		return BindBitString<int16_t>(bitstring_agg, type.id());
	}
	case LogicalType::INTEGER: {
		return BindBitString<int32_t>(bitstring_agg, type.id());
	}
	case LogicalType::BIGINT: {
		return BindBitString<int64_t>(bitstring_agg, type.id());
	}
	case LogicalType::HUGEINT: {
		return BindBitString<hugeint_t>(bitstring_agg, type.id());
	}
	case LogicalType::UTINYINT: {
		return BindBitString<uint8_t>(bitstring_agg, type.id());
	}
	case LogicalType::USMALLINT: {
		return BindBitString<uint16_t>(bitstring_agg, type.id());
	}
	case LogicalType::UINTEGER: {
		return BindBitString<uint32_t>(bitstring_agg, type.id());
	}
	case LogicalType::UBIGINT: {
		return BindBitString<uint64_t>(bitstring_agg, type.id());
	}
	case LogicalType::UHUGEINT: {
		return BindBitString<uhugeint_t>(bitstring_agg, type.id());
	}
	default:
		throw InternalException("Unimplemented bitstring aggregate");
	}
}

AggregateFunctionSet BitstringAggFun::GetFunctions() {
	AggregateFunctionSet bitstring_agg("bitstring_agg");
	for (auto &type : LogicalType::Integral()) {
		GetBitStringAggregate(type, bitstring_agg);
	}
	return bitstring_agg;
}

} // namespace duckdb

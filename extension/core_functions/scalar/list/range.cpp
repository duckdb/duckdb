#include "duckdb/common/vector/flat_vector.hpp"
#include "core_functions/scalar/list_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

namespace {

struct NumericRangeInfo {
	using TYPE = int64_t;
	using INCREMENT_TYPE = int64_t;

	static int64_t DefaultStart() {
		return 0;
	}
	static int64_t DefaultIncrement() {
		return 1;
	}

	static uint64_t ListLength(int64_t start_value, int64_t end_value, int64_t increment_value, bool inclusive_bound) {
		if (increment_value == 0) {
			return 0;
		}
		if (start_value > end_value && increment_value > 0) {
			return 0;
		}
		if (start_value < end_value && increment_value < 0) {
			return 0;
		}
		hugeint_t total_diff = AbsValue(hugeint_t(end_value) - hugeint_t(start_value));
		hugeint_t increment = AbsValue(hugeint_t(increment_value));
		hugeint_t total_values = total_diff / increment;
		if (total_diff % increment == 0) {
			if (inclusive_bound) {
				total_values += 1;
			}
		} else {
			total_values += 1;
		}
		if (total_values > NumericLimits<uint32_t>::Maximum()) {
			throw InvalidInputException("Lists larger than 2^32 elements are not supported");
		}
		return Hugeint::Cast<uint64_t>(total_values);
	}

	static void Increment(int64_t &input, int64_t increment) {
		input += increment;
	}
};
struct TimestampRangeInfo {
	using TYPE = timestamp_t;
	using INCREMENT_TYPE = interval_t;

	static timestamp_t DefaultStart() {
		throw InternalException("Default start not implemented for timestamp range");
	}
	static interval_t DefaultIncrement() {
		throw InternalException("Default increment not implemented for timestamp range");
	}
	static uint64_t ListLength(timestamp_t start_value, timestamp_t end_value, interval_t increment_value,
	                           bool inclusive_bound) {
		bool is_positive = increment_value.months > 0 || increment_value.days > 0 || increment_value.micros > 0;
		bool is_negative = increment_value.months < 0 || increment_value.days < 0 || increment_value.micros < 0;
		if (!is_negative && !is_positive) {
			// interval is 0: no result
			return 0;
		}
		// We don't allow infinite bounds because they generate errors or infinite loops
		if (!Timestamp::IsFinite(start_value) || !Timestamp::IsFinite(end_value)) {
			throw InvalidInputException("Interval infinite bounds not supported");
		}

		if (is_negative && is_positive) {
			// we don't allow a mix of
			throw InvalidInputException("Interval with mix of negative/positive entries not supported");
		}
		if (start_value > end_value && is_positive) {
			return 0;
		}
		if (start_value < end_value && is_negative) {
			return 0;
		}
		uint64_t total_values = 0;
		if (is_negative) {
			// negative interval, start_value is going down
			while (inclusive_bound ? start_value >= end_value : start_value > end_value) {
				start_value = Interval::Add(start_value, increment_value);
				total_values++;
				if (total_values > NumericLimits<uint32_t>::Maximum()) {
					throw InvalidInputException("Lists larger than 2^32 elements are not supported");
				}
			}
		} else {
			// positive interval, start_value is going up
			while (inclusive_bound ? start_value <= end_value : start_value < end_value) {
				start_value = Interval::Add(start_value, increment_value);
				total_values++;
				if (total_values > NumericLimits<uint32_t>::Maximum()) {
					throw InvalidInputException("Lists larger than 2^32 elements are not supported");
				}
			}
		}
		return total_values;
	}

	static void Increment(timestamp_t &input, interval_t increment) {
		input = Interval::Add(input, increment);
	}
};

template <class OP, bool INCLUSIVE_BOUND>
class RangeInfoStruct {
public:
	explicit RangeInfoStruct(DataChunk &args_p) : args(args_p) {
		switch (args.ColumnCount()) {
		case 1:
			args.data[0].ToUnifiedFormat(args.size(), vdata[0]);
			break;
		case 2:
			args.data[0].ToUnifiedFormat(args.size(), vdata[0]);
			args.data[1].ToUnifiedFormat(args.size(), vdata[1]);
			break;
		case 3:
			args.data[0].ToUnifiedFormat(args.size(), vdata[0]);
			args.data[1].ToUnifiedFormat(args.size(), vdata[1]);
			args.data[2].ToUnifiedFormat(args.size(), vdata[2]);
			break;
		default:
			throw InternalException("Unsupported number of parameters for range");
		}
	}

	bool RowIsValid(idx_t row_idx) {
		for (idx_t i = 0; i < args.ColumnCount(); i++) {
			auto idx = vdata[i].sel->get_index(row_idx);
			if (!vdata[i].validity.RowIsValid(idx)) {
				return false;
			}
		}
		return true;
	}

	typename OP::TYPE StartListValue(idx_t row_idx) {
		if (args.ColumnCount() == 1) {
			return OP::DefaultStart();
		} else {
			auto data = (typename OP::TYPE *)vdata[0].data;
			auto idx = vdata[0].sel->get_index(row_idx);
			return data[idx];
		}
	}

	typename OP::TYPE EndListValue(idx_t row_idx) {
		idx_t vdata_idx = args.ColumnCount() == 1 ? 0 : 1;
		auto data = (typename OP::TYPE *)vdata[vdata_idx].data;
		auto idx = vdata[vdata_idx].sel->get_index(row_idx);
		return data[idx];
	}

	typename OP::INCREMENT_TYPE ListIncrementValue(idx_t row_idx) {
		if (args.ColumnCount() < 3) {
			return OP::DefaultIncrement();
		} else {
			auto data = (typename OP::INCREMENT_TYPE *)vdata[2].data;
			auto idx = vdata[2].sel->get_index(row_idx);
			return data[idx];
		}
	}

	void GetListValues(idx_t row_idx, typename OP::TYPE &start_value, typename OP::TYPE &end_value,
	                   typename OP::INCREMENT_TYPE &increment_value) {
		start_value = StartListValue(row_idx);
		end_value = EndListValue(row_idx);
		increment_value = ListIncrementValue(row_idx);
	}

	uint64_t ListLength(idx_t row_idx) {
		typename OP::TYPE start_value;
		typename OP::TYPE end_value;
		typename OP::INCREMENT_TYPE increment_value;
		GetListValues(row_idx, start_value, end_value, increment_value);
		return OP::ListLength(start_value, end_value, increment_value, INCLUSIVE_BOUND);
	}

private:
	DataChunk &args;
	UnifiedVectorFormat vdata[3];
};

template <class OP, bool INCLUSIVE_BOUND>
void ListRangeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);

	RangeInfoStruct<OP, INCLUSIVE_BOUND> info(args);
	idx_t args_size = 1;
	auto result_type = VectorType::CONSTANT_VECTOR;
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			args_size = args.size();
			result_type = VectorType::FLAT_VECTOR;
			break;
		}
	}
	auto list_writer = FlatVector::Writer<VectorListType<typename OP::TYPE>>(result, args_size);
	for (idx_t i = 0; i < args_size; i++) {
		if (!info.RowIsValid(i)) {
			list_writer.WriteNull();
			continue;
		}
		typename OP::TYPE start_value = info.StartListValue(i);
		typename OP::INCREMENT_TYPE increment = info.ListIncrementValue(i);
		const auto length = info.ListLength(i);

		typename OP::TYPE range_value = start_value;
		for (auto &[child_writer, range_idx] : list_writer.WriteList(length)) {
			if (range_idx > 0) {
				OP::Increment(range_value, increment);
			}
			child_writer.WriteValue(range_value);
		}
	}

	result.SetVectorType(result_type);

	result.Verify(args.size());
}

} // namespace

ScalarFunctionSet ListRangeFun::GetFunctions() {
	// the arguments and return types are actually set in the binder function
	ScalarFunctionSet range_set;
	range_set.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::LIST(LogicalType::BIGINT),
	                                     ListRangeFunction<NumericRangeInfo, false>));
	range_set.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT},
	                                     LogicalType::LIST(LogicalType::BIGINT),
	                                     ListRangeFunction<NumericRangeInfo, false>));
	range_set.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	                                     LogicalType::LIST(LogicalType::BIGINT),
	                                     ListRangeFunction<NumericRangeInfo, false>));
	range_set.AddFunction(ScalarFunction({LogicalType::TIMESTAMP, LogicalType::TIMESTAMP, LogicalType::INTERVAL},
	                                     LogicalType::LIST(LogicalType::TIMESTAMP),
	                                     ListRangeFunction<TimestampRangeInfo, false>));
	for (auto &func : range_set.functions) {
		func.SetFallible();
	}
	return range_set;
}

ScalarFunctionSet GenerateSeriesFun::GetFunctions() {
	ScalarFunctionSet generate_series;
	generate_series.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::LIST(LogicalType::BIGINT),
	                                           ListRangeFunction<NumericRangeInfo, true>));
	generate_series.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT},
	                                           LogicalType::LIST(LogicalType::BIGINT),
	                                           ListRangeFunction<NumericRangeInfo, true>));
	generate_series.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	                                           LogicalType::LIST(LogicalType::BIGINT),
	                                           ListRangeFunction<NumericRangeInfo, true>));
	generate_series.AddFunction(ScalarFunction({LogicalType::TIMESTAMP, LogicalType::TIMESTAMP, LogicalType::INTERVAL},
	                                           LogicalType::LIST(LogicalType::TIMESTAMP),
	                                           ListRangeFunction<TimestampRangeInfo, true>));
	for (auto &func : generate_series.functions) {
		func.SetFallible();
	}
	return generate_series;
}

} // namespace duckdb

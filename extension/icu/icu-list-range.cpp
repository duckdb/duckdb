#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "include/icu-datefunc.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

struct ICUListRange : public ICUDateFunc {
	template <bool INCLUSIVE_BOUND>
	class RangeInfoStruct {
	public:
		explicit RangeInfoStruct(DataChunk &args_p)
		    : args(args_p), start_value_data(args.data[0].template Values<timestamp_t>(args.size())),
		      end_value_data(args.data[1].template Values<timestamp_t>(args.size())),
		      increment_value_data(args.data[2].template Values<interval_t>(args.size())) {
		}

		bool RowIsValid(idx_t row_idx) {
			if (!start_value_data[row_idx].IsValid()) {
				return false;
			}
			if (!end_value_data[row_idx].IsValid()) {
				return false;
			}
			if (!increment_value_data[row_idx].IsValid()) {
				return false;
			}
			return true;
		}

		timestamp_t StartListValue(idx_t row_idx) {
			auto start_val = start_value_data[row_idx];
			return start_val.GetValue();
		}

		timestamp_t EndListValue(idx_t row_idx) {
			auto end_val = end_value_data[row_idx];
			return end_val.GetValue();
		}

		interval_t ListIncrementValue(idx_t row_idx) {
			auto increment_val = increment_value_data[row_idx];
			return increment_val.GetValue();
		}

		void GetListValues(idx_t row_idx, timestamp_t &start_value, timestamp_t &end_value,
		                   interval_t &increment_value) {
			start_value = StartListValue(row_idx);
			end_value = EndListValue(row_idx);
			increment_value = ListIncrementValue(row_idx);
		}

		uint64_t ListLength(idx_t row_idx, TZCalendar &calendar) {
			timestamp_t start_value;
			timestamp_t end_value;
			interval_t increment_value;
			GetListValues(row_idx, start_value, end_value, increment_value);
			return ListLength(start_value, end_value, increment_value, INCLUSIVE_BOUND, calendar);
		}

		void Increment(timestamp_t &input, interval_t increment, TZCalendar &calendar) {
			input = Add(calendar, input, increment);
		}

	private:
		DataChunk &args;
		VectorIterator<timestamp_t> start_value_data;
		VectorIterator<timestamp_t> end_value_data;
		VectorIterator<interval_t> increment_value_data;

		uint64_t ListLength(timestamp_t start_value, timestamp_t end_value, interval_t increment_value,
		                    bool inclusive_bound, TZCalendar &calendar) {
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
			int64_t total_values = 0;
			if (is_negative) {
				// negative interval, start_value is going down
				while (inclusive_bound ? start_value >= end_value : start_value > end_value) {
					start_value = Add(calendar, start_value, increment_value);
					total_values++;
					if (total_values > NumericLimits<uint32_t>::Maximum()) {
						throw InvalidInputException("Lists larger than 2^32 elements are not supported");
					}
				}
			} else {
				// positive interval, start_value is going up
				while (inclusive_bound ? start_value <= end_value : start_value < end_value) {
					start_value = Add(calendar, start_value, increment_value);
					total_values++;
					if (total_values > NumericLimits<uint32_t>::Maximum()) {
						throw InvalidInputException("Lists larger than 2^32 elements are not supported");
					}
				}
			}
			return total_values;
		}
	};

	template <bool INCLUSIVE_BOUND>
	static void ICUListRangeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
		D_ASSERT(args.ColumnCount() == 3);

		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		auto &bind_info = func_expr.bind_info->Cast<BindData>();
		TZCalendar calendar(*bind_info.calendar, bind_info.cal_setting);

		RangeInfoStruct<INCLUSIVE_BOUND> info(args);
		idx_t args_size = args.size();
		auto list_writer = FlatVector::Writer<VectorListType<timestamp_t>>(result, args_size);
		for (idx_t i = 0; i < args_size; i++) {
			if (!info.RowIsValid(i)) {
				list_writer.WriteNull();
				continue;
			}
			const auto length = info.ListLength(i, calendar);
			auto list = list_writer.WriteDynamicList();

			timestamp_t range_value = info.StartListValue(i);
			interval_t increment = info.ListIncrementValue(i);
			for (idx_t range_idx = 0; range_idx < NumericCast<idx_t>(length); range_idx++) {
				if (range_idx > 0) {
					info.Increment(range_value, increment, calendar);
				}
				list.WriteElement().WriteValue(range_value);
			}
		}
		result.Verify(args.size());
	}

	static void AddICUListRangeFunction(ExtensionLoader &loader) {
		ScalarFunctionSet range("range");
		range.AddFunction(ScalarFunction({LogicalType::TIMESTAMP_TZ, LogicalType::TIMESTAMP_TZ, LogicalType::INTERVAL},
		                                 LogicalType::LIST(LogicalType::TIMESTAMP_TZ), ICUListRangeFunction<false>,
		                                 Bind));
		loader.RegisterFunction(range);

		// generate_series: similar to range, but inclusive instead of exclusive bounds on the RHS
		ScalarFunctionSet generate_series("generate_series");
		generate_series.AddFunction(
		    ScalarFunction({LogicalType::TIMESTAMP_TZ, LogicalType::TIMESTAMP_TZ, LogicalType::INTERVAL},
		                   LogicalType::LIST(LogicalType::TIMESTAMP_TZ), ICUListRangeFunction<true>, Bind));

		loader.RegisterFunction(generate_series);
	}
};

void RegisterICUListRangeFunctions(ExtensionLoader &loader) {
	ICUListRange::AddICUListRangeFunction(loader);
}

} // namespace duckdb

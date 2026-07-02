#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "include/icu-datefunc.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

struct ICUListRange : public ICUDateFunc {
	template <bool INCLUSIVE_BOUND>
	class RangeInfoStruct {
	public:
		explicit RangeInfoStruct(DataChunk &args_p) : args(args_p) {
			if (args.ColumnCount() == 3) {
				args.data[0].ToUnifiedFormat(args.size(), vdata[0]);
				args.data[1].ToUnifiedFormat(args.size(), vdata[1]);
				args.data[2].ToUnifiedFormat(args.size(), vdata[2]);
			} else {
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

		timestamp_t StartListValue(idx_t row_idx) {
			auto data = (timestamp_t *)vdata[0].data;
			auto idx = vdata[0].sel->get_index(row_idx);
			return data[idx];
		}

		timestamp_t EndListValue(idx_t row_idx) {
			auto data = (timestamp_t *)vdata[1].data;
			auto idx = vdata[1].sel->get_index(row_idx);
			return data[idx];
		}

		interval_t ListIncrementValue(idx_t row_idx) {
			auto data = (interval_t *)vdata[2].data;
			auto idx = vdata[2].sel->get_index(row_idx);
			return data[idx];
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
		UnifiedVectorFormat vdata[3];

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
		idx_t args_size = 1;
		auto result_type = VectorType::CONSTANT_VECTOR;
		for (idx_t i = 0; i < args.ColumnCount(); i++) {
			if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
				args_size = args.size();
				result_type = VectorType::FLAT_VECTOR;
				break;
			}
		}
		auto list_data = FlatVector::GetData<list_entry_t>(result);
		auto &result_validity = FlatVector::Validity(result);
		int64_t total_size = 0;
		for (idx_t i = 0; i < args_size; i++) {
			if (!info.RowIsValid(i)) {
				result_validity.SetInvalid(i);
				list_data[i].offset = total_size;
				list_data[i].length = 0;
			} else {
				list_data[i].offset = total_size;
				list_data[i].length = info.ListLength(i, calendar);
				total_size += list_data[i].length;
			}
		}

		// now construct the child vector of the list
		ListVector::Reserve(result, total_size);
		auto range_data = FlatVector::GetData<timestamp_t>(ListVector::GetEntry(result));
		idx_t total_idx = 0;
		for (idx_t i = 0; i < args_size; i++) {
			timestamp_t start_value = info.StartListValue(i);
			interval_t increment = info.ListIncrementValue(i);

			timestamp_t range_value = start_value;
			for (idx_t range_idx = 0; range_idx < list_data[i].length; range_idx++) {
				if (range_idx > 0) {
					info.Increment(range_value, increment, calendar);
				}
				range_data[total_idx++] = range_value;
			}
		}

		ListVector::SetListSize(result, total_size);
		result.SetVectorType(result_type);

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

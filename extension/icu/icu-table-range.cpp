#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "include/icu-datefunc.hpp"
#include "unicode/calendar.h"
#include "tz_calendar.hpp"

namespace duckdb {

struct ICUTableRange {
	using CalendarPtr = unique_ptr<icu::Calendar>;

	struct ICURangeBindData : public TableFunctionData {
		ICURangeBindData(const ICURangeBindData &other)
		    : TableFunctionData(other), tz_setting(other.tz_setting), cal_setting(other.cal_setting),
		      calendar(other.calendar->clone()), cardinality(other.cardinality) {
		}

		explicit ICURangeBindData(ClientContext &context, const vector<Value> &inputs) {
			Value tz_value;
			if (context.TryGetCurrentSetting("TimeZone", tz_value)) {
				tz_setting = tz_value.ToString();
			}
			auto tz = icu::TimeZone::createTimeZone(icu::UnicodeString::fromUTF8(icu::StringPiece(tz_setting)));

			string cal_id("@calendar=");
			Value cal_value;
			if (context.TryGetCurrentSetting("Calendar", cal_value)) {
				cal_setting = cal_value.ToString();
				cal_id += cal_setting;
			} else {
				cal_id += "gregorian";
			}

			icu::Locale locale(cal_id.c_str());

			UErrorCode success = U_ZERO_ERROR;
			calendar.reset(icu::Calendar::createInstance(tz, locale, success));
			if (U_FAILURE(success)) {
				throw InternalException("Unable to create ICU calendar.");
			}

			timestamp_tz_t bounds[2];
			interval_t step;
			for (idx_t i = 0; i < inputs.size(); i++) {
				if (inputs[i].IsNull()) {
					return;
				}
				if (i >= 2) {
					step = inputs[i].GetValue<interval_t>();
				} else {
					bounds[i] = inputs[i].GetValue<timestamp_tz_t>();
				}
			}
			// Estimate cardinality using micros.
			int64_t increment = 0;
			if (!Interval::TryGetMicro(step, increment) || !increment) {
				return;
			}
			int64_t delta = 0;
			if (!TrySubtractOperator::Operation(bounds[1].value, bounds[0].value, delta)) {
				return;
			}

			cardinality = idx_t(delta / increment);
		}

		string tz_setting;
		string cal_setting;
		CalendarPtr calendar;
		idx_t cardinality;
	};

	struct ICURangeLocalState : public LocalTableFunctionState {
		ICURangeLocalState() {
		}

		bool initialized_row = false;
		idx_t current_input_row = 0;
		timestamp_t current_state;

		timestamp_t start;
		timestamp_t end;
		interval_t increment;
		bool inclusive_bound;
		bool greater_than_check;

		bool empty_range = false;

		bool Finished(timestamp_t current_value) const {
			if (greater_than_check) {
				if (inclusive_bound) {
					return current_value > end;
				} else {
					return current_value >= end;
				}
			} else {
				if (inclusive_bound) {
					return current_value < end;
				} else {
					return current_value <= end;
				}
			}
		}
	};

	template <bool GENERATE_SERIES>
	static void GenerateRangeDateTimeParameters(DataChunk &input, idx_t row_id, ICURangeLocalState &result) {
		input.Flatten();
		for (idx_t c = 0; c < input.ColumnCount(); c++) {
			if (FlatVector::IsNull(input.data[c], row_id)) {
				result.start = timestamp_t(0);
				result.end = timestamp_t(0);
				result.increment = interval_t();
				result.greater_than_check = true;
				result.inclusive_bound = false;
				return;
			}
		}

		result.start = FlatVector::GetValue<timestamp_t>(input.data[0], row_id);
		result.end = FlatVector::GetValue<timestamp_t>(input.data[1], row_id);
		result.increment = FlatVector::GetValue<interval_t>(input.data[2], row_id);

		// Infinities either cause errors or infinite loops, so just ban them
		if (!Timestamp::IsFinite(result.start) || !Timestamp::IsFinite(result.end)) {
			throw BinderException("RANGE with infinite bounds is not supported");
		}

		if (result.increment.months == 0 && result.increment.days == 0 && result.increment.micros == 0) {
			throw BinderException("interval cannot be 0!");
		}
		// all elements should point in the same direction
		if (result.increment.months > 0 || result.increment.days > 0 || result.increment.micros > 0) {
			if (result.increment.months < 0 || result.increment.days < 0 || result.increment.micros < 0) {
				throw BinderException("RANGE with composite interval that has mixed signs is not supported");
			}
			result.greater_than_check = true;
			if (result.start > result.end) {
				result.empty_range = true;
			}
		} else {
			result.greater_than_check = false;
			if (result.start < result.end) {
				result.empty_range = true;
			}
		}
		result.inclusive_bound = GENERATE_SERIES;
	}

	template <bool GENERATE_SERIES>
	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names) {
		auto result = make_uniq<ICURangeBindData>(context, input.inputs);

		return_types.push_back(LogicalType::TIMESTAMP_TZ);
		if (GENERATE_SERIES) {
			names.emplace_back("generate_series");
		} else {
			names.emplace_back("range");
		}
		return std::move(result);
	}

	static unique_ptr<LocalTableFunctionState> RangeDateTimeLocalInit(ExecutionContext &context,
	                                                                  TableFunctionInitInput &input,
	                                                                  GlobalTableFunctionState *global_state) {
		return make_uniq<ICURangeLocalState>();
	}

	static unique_ptr<NodeStatistics> Cardinality(ClientContext &context, const FunctionData *bind_data_p) {
		if (!bind_data_p) {
			return nullptr;
		}
		auto &bind_data = bind_data_p->Cast<ICURangeBindData>();
		return make_uniq<NodeStatistics>(bind_data.cardinality, bind_data.cardinality);
	}

	template <bool GENERATE_SERIES>
	static OperatorResultType ICUTableRangeFunction(ExecutionContext &context, TableFunctionInput &data_p,
	                                                DataChunk &input, DataChunk &output) {
		auto &bind_data = data_p.bind_data->Cast<ICURangeBindData>();
		auto &state = data_p.local_state->Cast<ICURangeLocalState>();
		TZCalendar calendar(*bind_data.calendar, bind_data.cal_setting);
		while (true) {
			if (!state.initialized_row) {
				// initialize for the current input row
				if (state.current_input_row >= input.size()) {
					// ran out of rows
					state.current_input_row = 0;
					state.initialized_row = false;
					return OperatorResultType::NEED_MORE_INPUT;
				}
				GenerateRangeDateTimeParameters<GENERATE_SERIES>(input, state.current_input_row, state);
				state.initialized_row = true;
				state.current_state = state.start;
			}
			if (state.empty_range) {
				// empty range
				output.SetCardinality(0);
				state.current_input_row++;
				state.initialized_row = false;
				return OperatorResultType::HAVE_MORE_OUTPUT;
			}
			idx_t size = 0;
			auto data = FlatVector::GetData<timestamp_t>(output.data[0]);
			while (true) {
				if (state.Finished(state.current_state)) {
					break;
				}
				data[size++] = state.current_state;
				state.current_state = ICUDateFunc::Add(calendar, state.current_state, state.increment);
				if (size >= STANDARD_VECTOR_SIZE) {
					break;
				}
			}
			if (size == 0) {
				// move to next row
				state.current_input_row++;
				state.initialized_row = false;
				continue;
			}
			output.SetCardinality(size);
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
	}

	static void AddICUTableRangeFunction(ExtensionLoader &loader) {
		TableFunctionSet range("range");
		TableFunction range_function({LogicalType::TIMESTAMP_TZ, LogicalType::TIMESTAMP_TZ, LogicalType::INTERVAL},
		                             nullptr, Bind<false>, nullptr, RangeDateTimeLocalInit);
		range_function.in_out_function = ICUTableRangeFunction<false>;
		range_function.cardinality = Cardinality;
		range.AddFunction(range_function);

		loader.RegisterFunction(range);

		// generate_series: similar to range, but inclusive instead of exclusive bounds on the RHS
		TableFunctionSet generate_series("generate_series");
		TableFunction generate_series_function(
		    {LogicalType::TIMESTAMP_TZ, LogicalType::TIMESTAMP_TZ, LogicalType::INTERVAL}, nullptr, Bind<true>, nullptr,
		    RangeDateTimeLocalInit);
		generate_series_function.in_out_function = ICUTableRangeFunction<true>;
		generate_series_function.cardinality = Cardinality;
		generate_series.AddFunction(generate_series_function);

		loader.RegisterFunction(generate_series);
	}
};

void RegisterICUTableRangeFunctions(ExtensionLoader &loader) {
	ICUTableRange::AddICUTableRangeFunction(loader);
}

} // namespace duckdb

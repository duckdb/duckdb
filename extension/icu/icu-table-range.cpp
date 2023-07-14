#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "include/icu-datefunc.hpp"
#include "unicode/calendar.h"

namespace duckdb {

struct ICUTableRange {
	using CalendarPtr = unique_ptr<icu::Calendar>;

	struct BindData : public TableFunctionData {
		BindData(const BindData &other)
		    : TableFunctionData(other), tz_setting(other.tz_setting), cal_setting(other.cal_setting),
		      calendar(other.calendar->clone()), start(other.start), end(other.end), increment(other.increment),
		      inclusive_bound(other.inclusive_bound), greater_than_check(other.greater_than_check) {
		}

		explicit BindData(ClientContext &context) {
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
				throw Exception("Unable to create ICU calendar.");
			}
		}

		string tz_setting;
		string cal_setting;
		CalendarPtr calendar;

		timestamp_t start;
		timestamp_t end;
		interval_t increment;
		bool inclusive_bound;
		bool greater_than_check;

		bool Equals(const FunctionData &other_p) const override {
			auto &other = other_p.Cast<const BindData>();
			return other.start == start && other.end == end && other.increment == increment &&
			       other.inclusive_bound == inclusive_bound && other.greater_than_check == greater_than_check &&
			       *calendar == *other.calendar;
		}

		unique_ptr<FunctionData> Copy() const override {
			return make_uniq<BindData>(*this);
		}

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
	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names) {
		auto result = make_uniq<BindData>(context);

		auto &inputs = input.inputs;
		D_ASSERT(inputs.size() == 3);
		result->start = inputs[0].GetValue<timestamp_t>();
		result->end = inputs[1].GetValue<timestamp_t>();
		result->increment = inputs[2].GetValue<interval_t>();

		// Infinities either cause errors or infinite loops, so just ban them
		if (!Timestamp::IsFinite(result->start) || !Timestamp::IsFinite(result->end)) {
			throw BinderException("RANGE with infinite bounds is not supported");
		}

		if (result->increment.months == 0 && result->increment.days == 0 && result->increment.micros == 0) {
			throw BinderException("interval cannot be 0!");
		}
		// all elements should point in the same direction
		if (result->increment.months > 0 || result->increment.days > 0 || result->increment.micros > 0) {
			if (result->increment.months < 0 || result->increment.days < 0 || result->increment.micros < 0) {
				throw BinderException("RANGE with composite interval that has mixed signs is not supported");
			}
			result->greater_than_check = true;
			if (result->start > result->end) {
				throw BinderException(
				    "start is bigger than end, but increment is positive: cannot generate infinite series");
			}
		} else {
			result->greater_than_check = false;
			if (result->start < result->end) {
				throw BinderException(
				    "start is smaller than end, but increment is negative: cannot generate infinite series");
			}
		}
		return_types.push_back(inputs[0].type());
		if (GENERATE_SERIES) {
			// generate_series has inclusive bounds on the RHS
			result->inclusive_bound = true;
			names.emplace_back("generate_series");
		} else {
			result->inclusive_bound = false;
			names.emplace_back("range");
		}
		return std::move(result);
	}

	struct State : public GlobalTableFunctionState {
		explicit State(timestamp_t start_p) : current_state(start_p) {
		}

		timestamp_t current_state;
		bool finished = false;
	};

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		auto &bind_data = input.bind_data->Cast<BindData>();
		return make_uniq<State>(bind_data.start);
	}

	static void ICUTableRangeFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
		auto &bind_data = data_p.bind_data->Cast<BindData>();
		CalendarPtr calendar_ptr(bind_data.calendar->clone());
		auto calendar = calendar_ptr.get();
		auto &state = data_p.global_state->Cast<State>();
		if (state.finished) {
			return;
		}

		idx_t size = 0;
		auto data = FlatVector::GetData<timestamp_t>(output.data[0]);
		while (true) {
			data[size++] = state.current_state;
			state.current_state = ICUDateFunc::Add(calendar, state.current_state, bind_data.increment);
			if (bind_data.Finished(state.current_state)) {
				state.finished = true;
				break;
			}
			if (size >= STANDARD_VECTOR_SIZE) {
				break;
			}
		}
		output.SetCardinality(size);
	}

	static void AddICUTableRangeFunction(ClientContext &context) {
		auto &catalog = Catalog::GetSystemCatalog(context);

		TableFunctionSet range("range");
		range.AddFunction(TableFunction({LogicalType::TIMESTAMP_TZ, LogicalType::TIMESTAMP_TZ, LogicalType::INTERVAL},
		                                ICUTableRangeFunction, Bind<false>, Init));
		CreateTableFunctionInfo range_func_info(range);
		catalog.AddFunction(context, range_func_info);

		// generate_series: similar to range, but inclusive instead of exclusive bounds on the RHS
		TableFunctionSet generate_series("generate_series");
		generate_series.AddFunction(
		    TableFunction({LogicalType::TIMESTAMP_TZ, LogicalType::TIMESTAMP_TZ, LogicalType::INTERVAL},
		                  ICUTableRangeFunction, Bind<true>, Init));
		CreateTableFunctionInfo generate_series_func_info(generate_series);
		catalog.AddFunction(context, generate_series_func_info);
	}
};

void RegisterICUTableRangeFunctions(ClientContext &context) {
	ICUTableRange::AddICUTableRangeFunction(context);
}

} // namespace duckdb

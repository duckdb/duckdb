#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "include/icu-datefunc.hpp"

namespace duckdb {

struct ICUTimeZoneData : public GlobalTableFunctionState {
	ICUTimeZoneData() : tzs(icu::TimeZone::createEnumeration()) {
		UErrorCode status = U_ZERO_ERROR;
		std::unique_ptr<icu::Calendar> calendar(icu::Calendar::createInstance(status));
		now = calendar->getNow();
	}

	std::unique_ptr<icu::StringEnumeration> tzs;
	UDate now;
};

static unique_ptr<FunctionData> ICUTimeZoneBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("abbrev");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("utc_offset");
	return_types.emplace_back(LogicalType::INTERVAL);
	names.emplace_back("is_dst");
	return_types.emplace_back(LogicalType::BOOLEAN);

	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> ICUTimeZoneInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_unique<ICUTimeZoneData>();
}

static void ICUTimeZoneFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (ICUTimeZoneData &)*data_p.global_state;
	idx_t index = 0;
	while (index < STANDARD_VECTOR_SIZE) {
		UErrorCode status = U_ZERO_ERROR;
		auto long_id = data.tzs->snext(status);
		if (U_FAILURE(status) || !long_id) {
			break;
		}

		//	The LONG name is the one we looked up
		std::string utf8;
		long_id->toUTF8String(utf8);
		output.SetValue(0, index, Value(utf8));

		//	We don't have the zone tree for determining abbreviated names,
		//	so the SHORT name is the shortest, lexicographically first equivalent TZ without a slash.
		std::string short_id;
		long_id->toUTF8String(short_id);
		const auto nIDs = icu::TimeZone::countEquivalentIDs(*long_id);
		for (int32_t idx = 0; idx < nIDs; ++idx) {
			const auto eid = icu::TimeZone::getEquivalentID(*long_id, idx);
			if (eid.indexOf(char16_t('/')) >= 0) {
				continue;
			}
			utf8.clear();
			eid.toUTF8String(utf8);
			if (utf8.size() < short_id.size() || (utf8.size() == short_id.size() && utf8 < short_id)) {
				short_id = utf8;
			}
		}
		output.SetValue(1, index, Value(short_id));

		std::unique_ptr<icu::TimeZone> tz(icu::TimeZone::createTimeZone(*long_id));
		int32_t raw_offset_ms;
		int32_t dst_offset_ms;
		tz->getOffset(data.now, false, raw_offset_ms, dst_offset_ms, status);
		if (U_FAILURE(status)) {
			break;
		}

		output.SetValue(2, index, Value::INTERVAL(Interval::FromMicro(raw_offset_ms * Interval::MICROS_PER_MSEC)));
		output.SetValue(3, index, Value(dst_offset_ms != 0));
		++index;
	}
	output.SetCardinality(index);
}

struct ICUFromLocalTimestamp : public ICUDateFunc {
	static inline timestamp_t Operation(icu::Calendar *calendar, timestamp_t local) {
		// Extract the parts from the "instant"
		date_t local_date;
		dtime_t local_time;
		Timestamp::Convert(local, local_date, local_time);

		int32_t year;
		int32_t mm;
		int32_t dd;
		Date::Convert(local_date, year, mm, dd);

		int32_t hr;
		int32_t mn;
		int32_t secs;
		int32_t frac;
		Time::Convert(local_time, hr, mn, secs, frac);
		int32_t millis = frac / Interval::MICROS_PER_MSEC;
		uint64_t micros = frac % Interval::MICROS_PER_MSEC;

		// Use them to set the time in the time zone
		calendar->set(UCAL_YEAR, year);
		calendar->set(UCAL_MONTH, int32_t(mm - 1));
		calendar->set(UCAL_DATE, dd);
		calendar->set(UCAL_HOUR_OF_DAY, hr);
		calendar->set(UCAL_MINUTE, mn);
		calendar->set(UCAL_SECOND, secs);
		calendar->set(UCAL_MILLISECOND, millis);

		return GetTime(calendar, micros);
	}
};

struct ICUToLocalTimestamp : public ICUDateFunc {
	static inline timestamp_t Operation(icu::Calendar *calendar, timestamp_t instant) {
		// Extract the time zone parts
		auto micros = SetTime(calendar, instant);
		const auto year = ExtractField(calendar, UCAL_YEAR);
		const auto mm = ExtractField(calendar, UCAL_MONTH) + 1;
		const auto dd = ExtractField(calendar, UCAL_DATE);

		date_t local_date;
		if (!Date::TryFromDate(year, mm, dd, local_date)) {
			throw ConversionException("Unable to create local date in TIMEZONE function");
		}

		const auto hr = ExtractField(calendar, UCAL_HOUR_OF_DAY);
		const auto mn = ExtractField(calendar, UCAL_MINUTE);
		const auto secs = ExtractField(calendar, UCAL_SECOND);
		const auto millis = ExtractField(calendar, UCAL_MILLISECOND);

		micros += millis * Interval::MICROS_PER_MSEC;
		dtime_t local_time = Time::FromTime(hr, mn, secs, micros);

		timestamp_t result;
		if (!Timestamp::TryFromDatetime(local_date, local_time, result)) {
			throw ConversionException("Unable to create local timestamp in TIMEZONE function");
		}

		return result;
	}
};

struct ICULocalTimestampFunc : public ICUDateFunc {

	struct BindDataNow : public BindData {
		explicit BindDataNow(ClientContext &context) : BindData(context) {
			now = context.ActiveTransaction().start_timestamp;
		}

		BindDataNow(const BindDataNow &other) : BindData(other), now(other.now) {
		}

		bool Equals(const FunctionData &other_p) const override {
			auto &other = (const BindDataNow &)other_p;
			if (now != other.now) {
				return false;
			}

			return BindData::Equals(other_p);
		}

		unique_ptr<FunctionData> Copy() const override {
			return make_unique<BindDataNow>(*this);
		}

		timestamp_t now;
	};

	static unique_ptr<FunctionData> BindNow(ClientContext &context, ScalarFunction &bound_function,
	                                        vector<unique_ptr<Expression>> &arguments) {
		return make_unique<BindDataNow>(context);
	}

	static timestamp_t GetLocalTimestamp(ExpressionState &state) {
		auto &func_expr = (BoundFunctionExpression &)state.expr;
		auto &info = (BindDataNow &)*func_expr.bind_info;
		CalendarPtr calendar_ptr(info.calendar->clone());
		auto calendar = calendar_ptr.get();

		const auto now = info.now;
		return ICUToLocalTimestamp::Operation(calendar, now);
	}

	static void Execute(DataChunk &input, ExpressionState &state, Vector &result) {
		D_ASSERT(input.ColumnCount() == 0);
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto rdata = ConstantVector::GetData<timestamp_t>(result);
		rdata[0] = GetLocalTimestamp(state);
	}

	static void AddFunction(const string &name, ClientContext &context) {
		ScalarFunctionSet set(name);
		set.AddFunction(ScalarFunction({}, LogicalType::TIMESTAMP, Execute, BindNow));

		CreateScalarFunctionInfo func_info(set);
		auto &catalog = Catalog::GetCatalog(context);
		catalog.AddFunction(context, &func_info);
	}
};

struct ICULocalTimeFunc : public ICUDateFunc {

	static void Execute(DataChunk &input, ExpressionState &state, Vector &result) {
		D_ASSERT(input.ColumnCount() == 0);
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto rdata = ConstantVector::GetData<dtime_t>(result);
		const auto local = ICULocalTimestampFunc::GetLocalTimestamp(state);
		rdata[0] = Timestamp::GetTime(local);
	}

	static void AddFunction(const string &name, ClientContext &context) {
		ScalarFunctionSet set(name);
		set.AddFunction(ScalarFunction({}, LogicalType::TIME, Execute, ICULocalTimestampFunc::BindNow));

		CreateScalarFunctionInfo func_info(set);
		auto &catalog = Catalog::GetCatalog(context);
		catalog.AddFunction(context, &func_info);
	}
};

struct ICUTimeZoneFunc : public ICUDateFunc {
	template <typename OP>
	static void Execute(DataChunk &input, ExpressionState &state, Vector &result) {
		auto &func_expr = (BoundFunctionExpression &)state.expr;
		auto &info = (BindData &)*func_expr.bind_info;
		CalendarPtr calendar_ptr(info.calendar->clone());
		auto calendar = calendar_ptr.get();

		// Two cases: constant TZ, variable TZ
		D_ASSERT(input.ColumnCount() == 2);
		auto &tz_vec = input.data[0];
		auto &ts_vec = input.data[1];
		if (tz_vec.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			if (ConstantVector::IsNull(tz_vec)) {
				result.SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(result, true);
			} else {
				SetTimeZone(calendar, *ConstantVector::GetData<string_t>(tz_vec));
				UnaryExecutor::Execute<timestamp_t, timestamp_t>(ts_vec, result, input.size(), [&](timestamp_t ts) {
					if (Timestamp::IsFinite(ts)) {
						return OP::Operation(calendar, ts);
					} else {
						return ts;
					}
				});
			}
		} else {
			BinaryExecutor::Execute<string_t, timestamp_t, timestamp_t>(tz_vec, ts_vec, result, input.size(),
			                                                            [&](string_t tz_id, timestamp_t ts) {
				                                                            if (Timestamp::IsFinite(ts)) {
					                                                            SetTimeZone(calendar, tz_id);
					                                                            return OP::Operation(calendar, ts);
				                                                            } else {
					                                                            return ts;
				                                                            }
			                                                            });
		}
	}

	static void AddFunction(const string &name, ClientContext &context) {
		ScalarFunctionSet set(name);
		set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::TIMESTAMP}, LogicalType::TIMESTAMP_TZ,
		                               Execute<ICUFromLocalTimestamp>, Bind));
		set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::TIMESTAMP_TZ}, LogicalType::TIMESTAMP,
		                               Execute<ICUToLocalTimestamp>, Bind));

		CreateScalarFunctionInfo func_info(set);
		auto &catalog = Catalog::GetCatalog(context);
		catalog.AddFunction(context, &func_info);
	}
};

void RegisterICUTimeZoneFunctions(ClientContext &context) {
	auto &catalog = Catalog::GetCatalog(context);
	TableFunction tz_names("pg_timezone_names", {}, ICUTimeZoneFunction, ICUTimeZoneBind, ICUTimeZoneInit);
	CreateTableFunctionInfo tz_names_info(move(tz_names));
	catalog.CreateTableFunction(context, &tz_names_info);

	ICUTimeZoneFunc::AddFunction("timezone", context);
	ICULocalTimestampFunc::AddFunction("current_localtimestamp", context);
	ICULocalTimeFunc::AddFunction("current_localtime", context);
}

} // namespace duckdb

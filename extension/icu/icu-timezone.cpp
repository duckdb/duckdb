#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast_rules.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "include/icu-datefunc.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

namespace duckdb {

template <typename T>
static bool ICUIsFinite(const T &t) {
	return true;
}

template <>
bool ICUIsFinite(const timestamp_t &t) {
	return Timestamp::IsFinite(t);
}

struct ICUTimeZoneData : public GlobalTableFunctionState {
	ICUTimeZoneData() : tzs(icu::TimeZone::createEnumeration()) {
		UErrorCode status = U_ZERO_ERROR;
		duckdb::unique_ptr<icu::Calendar> calendar(icu::Calendar::createInstance(status));
		now = calendar->getNow();
	}

	duckdb::unique_ptr<icu::StringEnumeration> tzs;
	UDate now;
};

static duckdb::unique_ptr<FunctionData> ICUTimeZoneBind(ClientContext &context, TableFunctionBindInput &input,
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

static duckdb::unique_ptr<GlobalTableFunctionState> ICUTimeZoneInit(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	return make_uniq<ICUTimeZoneData>();
}

static void ICUTimeZoneFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<ICUTimeZoneData>();
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

		duckdb::unique_ptr<icu::TimeZone> tz(icu::TimeZone::createTimeZone(*long_id));
		int32_t raw_offset_ms;
		int32_t dst_offset_ms;
		tz->getOffset(data.now, false, raw_offset_ms, dst_offset_ms, status);
		if (U_FAILURE(status)) {
			break;
		}

		//	What PG reports is the total offset for today,
		//	which is the ICU total offset (i.e., "raw") plus the DST offset.
		raw_offset_ms += dst_offset_ms;
		output.SetValue(2, index, Value::INTERVAL(Interval::FromMicro(raw_offset_ms * Interval::MICROS_PER_MSEC)));
		output.SetValue(3, index, Value(dst_offset_ms != 0));
		++index;
	}
	output.SetCardinality(index);
}

struct ICUFromNaiveTimestamp : public ICUDateFunc {
	static inline timestamp_t Operation(icu::Calendar *calendar, timestamp_t naive) {
		if (!ICUIsFinite(naive)) {
			return naive;
		}

		// Extract the parts from the "instant"
		date_t local_date;
		dtime_t local_time;
		Timestamp::Convert(naive, local_date, local_time);

		int32_t year;
		int32_t mm;
		int32_t dd;
		Date::Convert(local_date, year, mm, dd);

		int32_t hr;
		int32_t mn;
		int32_t secs;
		int32_t frac;
		Time::Convert(local_time, hr, mn, secs, frac);
		int32_t millis = frac / int32_t(Interval::MICROS_PER_MSEC);
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

	struct CastTimestampUsToUs {
		template <class SRC, class DST>
		static inline DST Operation(SRC input) {
			// no-op
			return input;
		}
	};

	template <class OP>
	static bool CastFromNaive(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		auto &cast_data = parameters.cast_data->Cast<CastData>();
		auto &info = cast_data.info->Cast<BindData>();
		CalendarPtr calendar(info.calendar->clone());

		UnaryExecutor::Execute<timestamp_t, timestamp_t>(source, result, count, [&](timestamp_t input) {
			return Operation(calendar.get(), OP::template Operation<timestamp_t, timestamp_t>(input));
		});
		return true;
	}

	static BoundCastInfo BindCastFromNaive(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
		if (!input.context) {
			throw InternalException("Missing context for TIMESTAMP to TIMESTAMPTZ cast.");
		}

		auto cast_data = make_uniq<CastData>(make_uniq<BindData>(*input.context));
		switch (source.id()) {
		case LogicalTypeId::TIMESTAMP:
			return BoundCastInfo(CastFromNaive<CastTimestampUsToUs>, std::move(cast_data));
		case LogicalTypeId::TIMESTAMP_MS:
			return BoundCastInfo(CastFromNaive<CastTimestampMsToUs>, std::move(cast_data));
		case LogicalTypeId::TIMESTAMP_NS:
			return BoundCastInfo(CastFromNaive<CastTimestampNsToUs>, std::move(cast_data));
		case LogicalTypeId::TIMESTAMP_SEC:
			return BoundCastInfo(CastFromNaive<CastTimestampSecToUs>, std::move(cast_data));
		default:
			throw InternalException("Type %s not handled in BindCastFromNaive", LogicalTypeIdToString(source.id()));
		}
	}

	static void AddCasts(DatabaseInstance &db) {
		auto &config = DBConfig::GetConfig(db);
		auto &casts = config.GetCastFunctions();

		const auto implicit_cost = CastRules::ImplicitCast(LogicalType::TIMESTAMP, LogicalType::TIMESTAMP_TZ);
		casts.RegisterCastFunction(LogicalType::TIMESTAMP, LogicalType::TIMESTAMP_TZ, BindCastFromNaive, implicit_cost);
		casts.RegisterCastFunction(LogicalType::TIMESTAMP_MS, LogicalType::TIMESTAMP_TZ, BindCastFromNaive);
		casts.RegisterCastFunction(LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_TZ, BindCastFromNaive);
		casts.RegisterCastFunction(LogicalType::TIMESTAMP_S, LogicalType::TIMESTAMP_TZ, BindCastFromNaive);
	}
};

struct ICUToNaiveTimestamp : public ICUDateFunc {
	static inline timestamp_t Operation(icu::Calendar *calendar, timestamp_t instant) {
		if (!ICUIsFinite(instant)) {
			return instant;
		}

		// Extract the time zone parts
		auto micros = int32_t(SetTime(calendar, instant));
		const auto era = ExtractField(calendar, UCAL_ERA);
		const auto year = ExtractField(calendar, UCAL_YEAR);
		const auto mm = ExtractField(calendar, UCAL_MONTH) + 1;
		const auto dd = ExtractField(calendar, UCAL_DATE);

		const auto yyyy = era ? year : (-year + 1);
		date_t local_date;
		if (!Date::TryFromDate(yyyy, mm, dd, local_date)) {
			throw ConversionException("Unable to convert TIMESTAMPTZ to local date");
		}

		const auto hr = ExtractField(calendar, UCAL_HOUR_OF_DAY);
		const auto mn = ExtractField(calendar, UCAL_MINUTE);
		const auto secs = ExtractField(calendar, UCAL_SECOND);
		const auto millis = ExtractField(calendar, UCAL_MILLISECOND);

		micros += millis * int32_t(Interval::MICROS_PER_MSEC);
		dtime_t local_time = Time::FromTime(hr, mn, secs, micros);

		timestamp_t naive;
		if (!Timestamp::TryFromDatetime(local_date, local_time, naive)) {
			throw ConversionException("Unable to convert TIMESTAMPTZ to local TIMESTAMP");
		}

		return naive;
	}

	static bool CastToNaive(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		auto &cast_data = parameters.cast_data->Cast<CastData>();
		auto &info = cast_data.info->Cast<BindData>();
		CalendarPtr calendar(info.calendar->clone());

		UnaryExecutor::Execute<timestamp_t, timestamp_t>(
		    source, result, count, [&](timestamp_t input) { return Operation(calendar.get(), input); });
		return true;
	}

	static BoundCastInfo BindCastToNaive(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
		if (!input.context) {
			throw InternalException("Missing context for TIMESTAMPTZ to TIMESTAMP cast.");
		}

		auto cast_data = make_uniq<CastData>(make_uniq<BindData>(*input.context));

		return BoundCastInfo(CastToNaive, std::move(cast_data));
	}

	static void AddCasts(DatabaseInstance &db) {
		auto &config = DBConfig::GetConfig(db);
		auto &casts = config.GetCastFunctions();

		casts.RegisterCastFunction(LogicalType::TIMESTAMP_TZ, LogicalType::TIMESTAMP, BindCastToNaive);
	}
};

struct ICULocalTimestampFunc : public ICUDateFunc {

	struct BindDataNow : public BindData {
		explicit BindDataNow(ClientContext &context) : BindData(context) {
			now = MetaTransaction::Get(context).start_timestamp;
		}

		BindDataNow(const BindDataNow &other) : BindData(other), now(other.now) {
		}

		bool Equals(const FunctionData &other_p) const override {
			auto &other = other_p.Cast<const BindDataNow>();
			if (now != other.now) {
				return false;
			}

			return BindData::Equals(other_p);
		}

		duckdb::unique_ptr<FunctionData> Copy() const override {
			return make_uniq<BindDataNow>(*this);
		}

		timestamp_t now;
	};

	static duckdb::unique_ptr<FunctionData> BindNow(ClientContext &context, ScalarFunction &bound_function,
	                                                vector<duckdb::unique_ptr<Expression>> &arguments) {
		return make_uniq<BindDataNow>(context);
	}

	static timestamp_t GetLocalTimestamp(ExpressionState &state) {
		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		auto &info = func_expr.bind_info->Cast<BindDataNow>();
		CalendarPtr calendar_ptr(info.calendar->clone());
		auto calendar = calendar_ptr.get();

		const auto now = info.now;
		return ICUToNaiveTimestamp::Operation(calendar, now);
	}

	static void Execute(DataChunk &input, ExpressionState &state, Vector &result) {
		D_ASSERT(input.ColumnCount() == 0);
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto rdata = ConstantVector::GetData<timestamp_t>(result);
		rdata[0] = GetLocalTimestamp(state);
	}

	static void AddFunction(const string &name, DatabaseInstance &db) {
		ScalarFunctionSet set(name);
		set.AddFunction(ScalarFunction({}, LogicalType::TIMESTAMP, Execute, BindNow));
		ExtensionUtil::RegisterFunction(db, set);
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

	static void AddFunction(const string &name, DatabaseInstance &db) {
		ScalarFunctionSet set(name);
		set.AddFunction(ScalarFunction({}, LogicalType::TIME, Execute, ICULocalTimestampFunc::BindNow));
		ExtensionUtil::RegisterFunction(db, set);
	}
};

struct ICUToTimeTZ : public ICUDateFunc {
	static inline dtime_tz_t Operation(icu::Calendar *calendar, dtime_tz_t timetz) {
		// Normalise to +00:00, add TZ offset, then set offset to TZ
		auto time = Time::NormalizeTimeTZ(timetz);

		auto offset = ExtractField(calendar, UCAL_ZONE_OFFSET);
		offset += ExtractField(calendar, UCAL_DST_OFFSET);
		offset /= Interval::MSECS_PER_SEC;

		date_t date(0);
		time = Interval::Add(time, {0, 0, offset * Interval::MICROS_PER_SEC}, date);
		return dtime_tz_t(time, offset);
	}
};

struct ICUTimeZoneFunc : public ICUDateFunc {
	template <typename OP, typename T>
	static void Execute(DataChunk &input, ExpressionState &state, Vector &result) {
		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		auto &info = func_expr.bind_info->Cast<BindData>();
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
				UnaryExecutor::Execute<T, T>(ts_vec, result, input.size(),
				                             [&](T ts) { return OP::Operation(calendar, ts); });
			}
		} else {
			BinaryExecutor::Execute<string_t, T, T>(tz_vec, ts_vec, result, input.size(), [&](string_t tz_id, T ts) {
				if (ICUIsFinite(ts)) {
					SetTimeZone(calendar, tz_id);
					return OP::Operation(calendar, ts);
				} else {
					return ts;
				}
			});
		}
	}

	static void AddFunction(const string &name, DatabaseInstance &db) {
		ScalarFunctionSet set(name);
		set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::TIMESTAMP}, LogicalType::TIMESTAMP_TZ,
		                               Execute<ICUFromNaiveTimestamp, timestamp_t>, Bind));
		set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::TIMESTAMP_TZ}, LogicalType::TIMESTAMP,
		                               Execute<ICUToNaiveTimestamp, timestamp_t>, Bind));
		set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::TIME_TZ}, LogicalType::TIME_TZ,
		                               Execute<ICUToTimeTZ, dtime_tz_t>, Bind));
		ExtensionUtil::AddFunctionOverload(db, set);
	}
};

timestamp_t ICUDateFunc::FromNaive(icu::Calendar *calendar, timestamp_t naive) {
	return ICUFromNaiveTimestamp::Operation(calendar, naive);
}

void RegisterICUTimeZoneFunctions(DatabaseInstance &db) {
	//	Table functions
	TableFunction tz_names("pg_timezone_names", {}, ICUTimeZoneFunction, ICUTimeZoneBind, ICUTimeZoneInit);
	ExtensionUtil::RegisterFunction(db, tz_names);

	//	Scalar functions
	ICUTimeZoneFunc::AddFunction("timezone", db);
	ICULocalTimestampFunc::AddFunction("current_localtimestamp", db);
	ICULocalTimeFunc::AddFunction("current_localtime", db);

	// 	Casts
	ICUFromNaiveTimestamp::AddCasts(db);
	ICUToNaiveTimestamp::AddCasts(db);
}

} // namespace duckdb

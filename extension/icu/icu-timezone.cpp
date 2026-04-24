#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast_rules.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "include/icu-casts.hpp"
#include "include/icu-datefunc.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/main/settings.hpp"

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

	// name, VARCHAR
	auto &name_col = output.data[0];
	// abbrev, VARCHAR
	auto &abbrev = output.data[1];
	// utc_offset, INTERVAL
	auto &utc_offset = output.data[2];
	// is_dst, BOOLEAN
	auto &is_dst = output.data[3];

	while (index < STANDARD_VECTOR_SIZE) {
		UErrorCode status = U_ZERO_ERROR;
		auto long_id = data.tzs->snext(status);
		if (U_FAILURE(status) || !long_id) {
			break;
		}

		//	The LONG name is the one we looked up
		std::string utf8;
		long_id->toUTF8String(utf8);

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
			std::string eid_utf8;
			eid.toUTF8String(eid_utf8);
			if (eid_utf8.size() < short_id.size() || (eid_utf8.size() == short_id.size() && eid_utf8 < short_id)) {
				short_id = eid_utf8;
			}
		}

		duckdb::unique_ptr<icu::TimeZone> tz(icu::TimeZone::createTimeZone(*long_id));
		int32_t raw_offset_ms;
		int32_t dst_offset_ms;
		tz->getOffset(data.now, false, raw_offset_ms, dst_offset_ms, status);
		if (U_FAILURE(status)) {
			break;
		}

		name_col.Append(Value(utf8));
		abbrev.Append(Value(short_id));
		//	What PG reports is the total offset for today,
		//	which is the ICU total offset (i.e., "raw") plus the DST offset.
		raw_offset_ms += dst_offset_ms;
		utc_offset.Append(Value::INTERVAL(Interval::FromMicro(raw_offset_ms * Interval::MICROS_PER_MSEC)));
		is_dst.Append(Value::BOOLEAN(dst_offset_ms != 0));
		++index;
	}
	output.SetCardinality(index);
}

//	Wrap the multiply-named and non-type-safe cast utilities.
struct ICUCast {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw NotImplementedException("Naive timezone cast could not be performed!");
	}
};

template <>
timestamp_t ICUCast::Operation(timestamp_t src) {
	return src;
}

template <>
timestamp_ns_t ICUCast::Operation(timestamp_t src) {
	return timestamp_ns_t(CastTimestampUsToNs::Operation<timestamp_t, timestamp_t>(src).value);
}

template <>
timestamp_t ICUCast::Operation(timestamp_ms_t src) {
	return CastTimestampMsToUs::Operation<timestamp_t, timestamp_t>(src);
}

template <>
timestamp_ns_t ICUCast::Operation(timestamp_ms_t src) {
	return timestamp_ns_t(CastTimestampMsToNs::Operation<timestamp_t, timestamp_t>(src).value);
}

template <>
timestamp_t ICUCast::Operation(timestamp_ns_t src) {
	return CastTimestampNsToUs::Operation<timestamp_t, timestamp_t>(timestamp_t(src));
}

template <>
timestamp_ns_t ICUCast::Operation(timestamp_ns_t src) {
	return src;
}

template <>
timestamp_t ICUCast::Operation(timestamp_sec_t src) {
	return CastTimestampSecToUs::Operation<timestamp_t, timestamp_t>(src);
}

template <>
timestamp_ns_t ICUCast::Operation(timestamp_sec_t src) {
	return timestamp_ns_t(CastTimestampSecToNs::Operation<timestamp_t, timestamp_t>(src).value);
}

template <>
timestamp_t ICUCast::Operation(date_t src) {
	return Cast::Operation<date_t, timestamp_t>(src);
}

template <>
timestamp_ns_t ICUCast::Operation(date_t src) {
	return Cast::Operation<date_t, timestamp_ns_t>(src);
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

	static inline timestamp_ns_t Operation(icu::Calendar *calendar, timestamp_ns_t naive) {
		if (!ICUIsFinite(naive)) {
			return naive;
		}

		auto nanos = naive.value % Interval::NANOS_PER_MICRO;
		timestamp_t micros(naive.value / Interval::NANOS_PER_MICRO);
		auto cast = Operation(calendar, micros);

		timestamp_ns_t result;
		if (!Timestamp::TryFromTimestampNanos(cast, nanos, result)) {
			throw ConversionException("ICU date overflows timestamp_ns range");
		}
		return result;
	}

	template <class SRC, class DST>
	static bool CastFromNaive(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		auto &cast_data = parameters.cast_data->Cast<CastData>();
		auto &info = cast_data.info->Cast<BindData>();
		CalendarPtr calendar(info.calendar->clone());

		UnaryExecutor::Execute<SRC, DST>(source, result, count, [&](SRC input) {
			return Operation(calendar.get(), ICUCast::Operation<SRC, DST>(input));
		});
		return true;
	}

	template <typename SRC>
	static BoundCastInfo BindCastFromNaiveType(BindCastInput &input, const LogicalType &target) {
		auto cast_data = make_uniq<CastData>(make_uniq<BindData>(*input.context));
		switch (target.id()) {
		case LogicalTypeId::TIMESTAMP_TZ:
			return BoundCastInfo(CastFromNaive<SRC, timestamp_t>, std::move(cast_data));
		case LogicalTypeId::TIMESTAMP_TZ_NS:
			return BoundCastInfo(CastFromNaive<SRC, timestamp_ns_t>, std::move(cast_data));
		default:
			throw InternalException("Type %s not handled in BindCastFromNaiveType", LogicalTypeIdToString(target.id()));
		}
	}

	static BoundCastInfo BindCastFromNaive(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
		if (!input.context) {
			throw InternalException("Missing context for TIMESTAMP to TIMESTAMPTZ cast.");
		}
		if (Settings::Get<DisableTimestamptzCastsSetting>(*input.context)) {
			throw BinderException("Casting from TIMESTAMP to TIMESTAMP WITH TIME ZONE without an explicit time zone "
			                      "has been disabled  - use \"AT TIME ZONE ...\"");
		}

		switch (source.id()) {
		case LogicalTypeId::TIMESTAMP:
			return BindCastFromNaiveType<timestamp_t>(input, target);
		case LogicalTypeId::TIMESTAMP_MS:
			return BindCastFromNaiveType<timestamp_ms_t>(input, target);
		case LogicalTypeId::TIMESTAMP_NS:
			return BindCastFromNaiveType<timestamp_ns_t>(input, target);
		case LogicalTypeId::TIMESTAMP_SEC:
			return BindCastFromNaiveType<timestamp_sec_t>(input, target);
		case LogicalTypeId::DATE:
			return BindCastFromNaiveType<date_t>(input, target);
		default:
			throw InternalException("Type %s not handled in BindCastFromNaive", LogicalTypeIdToString(source.id()));
		}
	}
	static void AddCast(CastFunctionSet &casts, const LogicalType &source, const LogicalType &target) {
		const auto implicit_cost = CastRules::ImplicitCast(source, target);
		casts.RegisterCastFunction(source, target, BindCastFromNaive, implicit_cost);
	}

	static void AddCasts(ExtensionLoader &loader) {
		auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
		auto &casts = config.GetCastFunctions();

		AddCast(casts, LogicalType::TIMESTAMP, LogicalType::TIMESTAMP_TZ);
		AddCast(casts, LogicalType::TIMESTAMP_MS, LogicalType::TIMESTAMP_TZ);
		AddCast(casts, LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_TZ);
		AddCast(casts, LogicalType::TIMESTAMP_S, LogicalType::TIMESTAMP_TZ);
		AddCast(casts, LogicalType::DATE, LogicalType::TIMESTAMP_TZ);

		AddCast(casts, LogicalType::TIMESTAMP, LogicalType::TIMESTAMP_TZ_NS);
		AddCast(casts, LogicalType::TIMESTAMP_MS, LogicalType::TIMESTAMP_TZ_NS);
		AddCast(casts, LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_TZ_NS);
		AddCast(casts, LogicalType::TIMESTAMP_S, LogicalType::TIMESTAMP_TZ_NS);
		AddCast(casts, LogicalType::DATE, LogicalType::TIMESTAMP_TZ_NS);
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

	static inline timestamp_ns_t Operation(icu::Calendar *calendar, timestamp_ns_t instant) {
		if (!ICUIsFinite(instant)) {
			return instant;
		}

		auto nanos = instant.value % Interval::NANOS_PER_MICRO;
		timestamp_t micros(instant.value / Interval::NANOS_PER_MICRO);
		auto cast = Operation(calendar, instant);

		return timestamp_ns_t(cast.value * Interval::NANOS_PER_MICRO + nanos);
	}

	template <typename T>
	static bool CastToNaive(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		auto &cast_data = parameters.cast_data->Cast<CastData>();
		auto &info = cast_data.info->Cast<BindData>();
		CalendarPtr calendar(info.calendar->clone());

		UnaryExecutor::Execute<T, T>(source, result, count, [&](T input) { return Operation(calendar.get(), input); });
		return true;
	}

	static BoundCastInfo BindCastToNaive(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
		if (!input.context) {
			throw InternalException("Missing context for TIMESTAMPTZ to TIMESTAMP cast.");
		}
		if (Settings::Get<DisableTimestamptzCastsSetting>(*input.context)) {
			throw BinderException("Casting from TIMESTAMP WITH TIME ZONE to TIMESTAMP without an explicit time zone "
			                      "has been disabled  - use \"AT TIME ZONE ...\"");
		}

		auto cast_data = make_uniq<CastData>(make_uniq<BindData>(*input.context));

		switch (source.id()) {
		case LogicalTypeId::TIMESTAMP_TZ:
			return BoundCastInfo(CastToNaive<timestamp_t>, std::move(cast_data));
		case LogicalTypeId::TIMESTAMP_TZ_NS:
			return BoundCastInfo(CastToNaive<timestamp_ns_t>, std::move(cast_data));
		default:
			throw InternalException("Type %s not handled in BindCastToNaive", LogicalTypeIdToString(source.id()));
		}
	}

	static void AddCasts(ExtensionLoader &loader) {
		loader.RegisterCastFunction(LogicalType::TIMESTAMP_TZ, LogicalType::TIMESTAMP, BindCastToNaive);
		loader.RegisterCastFunction(LogicalType::TIMESTAMP_TZ_NS, LogicalType::TIMESTAMP_NS, BindCastToNaive);
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

	static duckdb::unique_ptr<FunctionData> BindNow(BindScalarFunctionInput &input) {
		return make_uniq<BindDataNow>(input.GetClientContext());
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

	static void AddFunction(const string &name, ExtensionLoader &loader) {
		ScalarFunctionSet set(name);
		set.AddFunction(ScalarFunction({}, LogicalType::TIMESTAMP, Execute, BindNow));
		loader.RegisterFunction(set);
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

	static void AddFunction(const string &name, ExtensionLoader &loader) {
		ScalarFunctionSet set(name);
		set.AddFunction(ScalarFunction({}, LogicalType::TIME, Execute, ICULocalTimestampFunc::BindNow));
		loader.RegisterFunction(set);
	}
};

dtime_tz_t ICUToTimeTZ::Operation(icu::Calendar *calendar, dtime_tz_t timetz) {
	// Normalise to +00:00, add TZ offset, then set offset to TZ
	auto time = Time::NormalizeTimeTZ(timetz);

	auto offset = ExtractField(calendar, UCAL_ZONE_OFFSET);
	offset += ExtractField(calendar, UCAL_DST_OFFSET);
	offset /= Interval::MSECS_PER_SEC;

	date_t date(0);
	time = Interval::Add(time, {0, 0, offset * Interval::MICROS_PER_SEC}, date);
	return dtime_tz_t(time, offset);
}

bool ICUToTimeTZ::ToTimeTZ(icu::Calendar *calendar, timestamp_t instant, dtime_tz_t &result) {
	if (!ICUIsFinite(instant)) {
		return false;
	}

	//	Time in current TZ
	auto micros = int32_t(SetTime(calendar, instant));
	const auto hour = ExtractField(calendar, UCAL_HOUR_OF_DAY);
	const auto minute = ExtractField(calendar, UCAL_MINUTE);
	const auto second = ExtractField(calendar, UCAL_SECOND);
	const auto millis = ExtractField(calendar, UCAL_MILLISECOND);
	micros += millis * int32_t(Interval::MICROS_PER_MSEC);
	if (!Time::IsValidTime(hour, minute, second, micros)) {
		return false;
	}
	const auto time = Time::FromTime(hour, minute, second, micros);

	//	Offset in current TZ
	auto offset = ExtractField(calendar, UCAL_ZONE_OFFSET);
	offset += ExtractField(calendar, UCAL_DST_OFFSET);
	offset /= Interval::MSECS_PER_SEC;

	result = dtime_tz_t(time, offset);
	return true;
}

bool ICUToTimeTZ::CastToTimeTZ(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &cast_data = parameters.cast_data->Cast<CastData>();
	auto &info = cast_data.info->Cast<BindData>();
	CalendarPtr calendar(info.calendar->clone());

	UnaryExecutor::ExecuteWithNulls<timestamp_t, dtime_tz_t>(source, result, count,
	                                                         [&](timestamp_t input, ValidityMask &mask, idx_t idx) {
		                                                         dtime_tz_t output;
		                                                         if (ToTimeTZ(calendar.get(), input, output)) {
			                                                         return output;
		                                                         } else {
			                                                         mask.SetInvalid(idx);
			                                                         return dtime_tz_t();
		                                                         }
	                                                         });
	return true;
}

BoundCastInfo ICUToTimeTZ::BindCastToTimeTZ(BindCastInput &input, const LogicalType &source,
                                            const LogicalType &target) {
	if (!input.context) {
		throw InternalException("Missing context for TIMESTAMPTZ to TIMETZ cast.");
	}

	auto cast_data = make_uniq<CastData>(make_uniq<BindData>(*input.context));

	return BoundCastInfo(CastToTimeTZ, std::move(cast_data));
}

void ICUToTimeTZ::AddCasts(ExtensionLoader &loader) {
	const auto implicit_cost = CastRules::ImplicitCast(LogicalType::TIMESTAMP_TZ, LogicalType::TIME_TZ);
	loader.RegisterCastFunction(LogicalType::TIMESTAMP_TZ, LogicalType::TIME_TZ, BindCastToTimeTZ, implicit_cost);
}

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
				throw InternalException("ICUTimeZone called with constant NULL tz");
			}
			SetTimeZone(calendar, *ConstantVector::GetData<string_t>(tz_vec));
			UnaryExecutor::Execute<T, T>(ts_vec, result, input.size(),
			                             [&](T ts) { return OP::Operation(calendar, ts); });
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

	static void AddFunction(const string &name, ExtensionLoader &loader) {
		ScalarFunctionSet set(name);
		set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::TIMESTAMP}, LogicalType::TIMESTAMP_TZ,
		                               Execute<ICUFromNaiveTimestamp, timestamp_t>, Bind));
		set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::TIMESTAMP_TZ}, LogicalType::TIMESTAMP,
		                               Execute<ICUToNaiveTimestamp, timestamp_t>, Bind));
		set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::TIME_TZ}, LogicalType::TIME_TZ,
		                               Execute<ICUToTimeTZ, dtime_tz_t>, Bind));
		for (auto &func : set.functions) {
			func.SetFallible();
		}
		loader.RegisterFunction(set);
	}
};

timestamp_t ICUDateFunc::FromNaive(icu::Calendar *calendar, timestamp_t naive) {
	return ICUFromNaiveTimestamp::Operation(calendar, naive);
}

void RegisterICUTimeZoneFunctions(ExtensionLoader &loader) {
	//	Table functions
	TableFunction tz_names("pg_timezone_names", {}, ICUTimeZoneFunction, ICUTimeZoneBind, ICUTimeZoneInit);
	loader.RegisterFunction(tz_names);

	//	Scalar functions
	ICUTimeZoneFunc::AddFunction("timezone", loader);
	ICULocalTimestampFunc::AddFunction("current_localtimestamp", loader);
	ICULocalTimeFunc::AddFunction("current_localtime", loader);

	// 	Casts
	ICUFromNaiveTimestamp::AddCasts(loader);
	ICUToNaiveTimestamp::AddCasts(loader);
	ICUToTimeTZ::AddCasts(loader);
}

} // namespace duckdb

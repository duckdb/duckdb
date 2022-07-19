#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/operator/string_cast.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Cast Numeric -> String
//===--------------------------------------------------------------------===//
template <>
string_t StringCast::Operation(bool input, Vector &vector) {
	if (input) {
		return StringVector::AddString(vector, "true", 4);
	} else {
		return StringVector::AddString(vector, "false", 5);
	}
}

template <>
string_t StringCast::Operation(int8_t input, Vector &vector) {
	return NumericHelper::FormatSigned<int8_t, uint8_t>(input, vector);
}

template <>
string_t StringCast::Operation(int16_t input, Vector &vector) {
	return NumericHelper::FormatSigned<int16_t, uint16_t>(input, vector);
}
template <>
string_t StringCast::Operation(int32_t input, Vector &vector) {
	return NumericHelper::FormatSigned<int32_t, uint32_t>(input, vector);
}

template <>
string_t StringCast::Operation(int64_t input, Vector &vector) {
	return NumericHelper::FormatSigned<int64_t, uint64_t>(input, vector);
}
template <>
duckdb::string_t StringCast::Operation(uint8_t input, Vector &vector) {
	return NumericHelper::FormatSigned<uint8_t, uint64_t>(input, vector);
}
template <>
duckdb::string_t StringCast::Operation(uint16_t input, Vector &vector) {
	return NumericHelper::FormatSigned<uint16_t, uint64_t>(input, vector);
}
template <>
duckdb::string_t StringCast::Operation(uint32_t input, Vector &vector) {
	return NumericHelper::FormatSigned<uint32_t, uint64_t>(input, vector);
}
template <>
duckdb::string_t StringCast::Operation(uint64_t input, Vector &vector) {
	return NumericHelper::FormatSigned<uint64_t, uint64_t>(input, vector);
}

template <>
string_t StringCast::Operation(float input, Vector &vector) {
	std::string s = duckdb_fmt::format("{}", input);
	return StringVector::AddString(vector, s);
}

template <>
string_t StringCast::Operation(double input, Vector &vector) {
	std::string s = duckdb_fmt::format("{}", input);
	return StringVector::AddString(vector, s);
}

template <>
string_t StringCast::Operation(interval_t input, Vector &vector) {
	char buffer[70];
	idx_t length = IntervalToStringCast::Format(input, buffer);
	return StringVector::AddString(vector, buffer, length);
}

template <>
duckdb::string_t StringCast::Operation(hugeint_t input, Vector &vector) {
	return HugeintToStringCast::FormatSigned(input, vector);
}

template <>
duckdb::string_t StringCast::Operation(date_t input, Vector &vector) {
	if (input == date_t::infinity()) {
		return StringVector::AddString(vector, Date::PINF);
	} else if (input == date_t::ninfinity()) {
		return StringVector::AddString(vector, Date::NINF);
	}
	int32_t date[3];
	Date::Convert(input, date[0], date[1], date[2]);

	idx_t year_length;
	bool add_bc;
	idx_t length = DateToStringCast::Length(date, year_length, add_bc);

	string_t result = StringVector::EmptyString(vector, length);
	auto data = result.GetDataWriteable();

	DateToStringCast::Format(data, date, year_length, add_bc);

	result.Finalize();
	return result;
}

template <>
duckdb::string_t StringCast::Operation(dtime_t input, Vector &vector) {
	int32_t time[4];
	Time::Convert(input, time[0], time[1], time[2], time[3]);

	char micro_buffer[10];
	idx_t length = TimeToStringCast::Length(time, micro_buffer);

	string_t result = StringVector::EmptyString(vector, length);
	auto data = result.GetDataWriteable();

	TimeToStringCast::Format(data, length, time, micro_buffer);

	result.Finalize();
	return result;
}

template <>
duckdb::string_t StringCast::Operation(timestamp_t input, Vector &vector) {
	if (input == timestamp_t::infinity()) {
		return StringVector::AddString(vector, Date::PINF);
	} else if (input == timestamp_t::ninfinity()) {
		return StringVector::AddString(vector, Date::NINF);
	}
	date_t date_entry;
	dtime_t time_entry;
	Timestamp::Convert(input, date_entry, time_entry);

	int32_t date[3], time[4];
	Date::Convert(date_entry, date[0], date[1], date[2]);
	Time::Convert(time_entry, time[0], time[1], time[2], time[3]);

	// format for timestamp is DATE TIME (separated by space)
	idx_t year_length;
	bool add_bc;
	char micro_buffer[6];
	idx_t date_length = DateToStringCast::Length(date, year_length, add_bc);
	idx_t time_length = TimeToStringCast::Length(time, micro_buffer);
	idx_t length = date_length + time_length + 1;

	string_t result = StringVector::EmptyString(vector, length);
	auto data = result.GetDataWriteable();

	DateToStringCast::Format(data, date, year_length, add_bc);
	data[date_length] = ' ';
	TimeToStringCast::Format(data + date_length + 1, time_length, time, micro_buffer);

	result.Finalize();
	return result;
}

template <>
duckdb::string_t StringCast::Operation(duckdb::string_t input, Vector &result) {
	return StringVector::AddStringOrBlob(result, input);
}

template <>
string_t StringCastTZ::Operation(dtime_t input, Vector &vector) {
	int32_t time[4];
	Time::Convert(input, time[0], time[1], time[2], time[3]);

	// format for timetz is TIME+00
	char micro_buffer[10];
	const auto time_length = TimeToStringCast::Length(time, micro_buffer);
	const idx_t length = time_length + 3;

	string_t result = StringVector::EmptyString(vector, length);
	auto data = result.GetDataWriteable();

	idx_t pos = 0;
	TimeToStringCast::Format(data + pos, length, time, micro_buffer);
	pos += time_length;
	data[pos++] = '+';
	data[pos++] = '0';
	data[pos++] = '0';

	result.Finalize();
	return result;
}

template <>
string_t StringCastTZ::Operation(timestamp_t input, Vector &vector) {
	if (input == timestamp_t::infinity()) {
		return StringVector::AddString(vector, Date::PINF);
	} else if (input == timestamp_t::ninfinity()) {
		return StringVector::AddString(vector, Date::NINF);
	}
	date_t date_entry;
	dtime_t time_entry;
	Timestamp::Convert(input, date_entry, time_entry);

	int32_t date[3], time[4];
	Date::Convert(date_entry, date[0], date[1], date[2]);
	Time::Convert(time_entry, time[0], time[1], time[2], time[3]);

	// format for timestamptz is DATE TIME+00 (separated by space)
	idx_t year_length;
	bool add_bc;
	char micro_buffer[6];
	const idx_t date_length = DateToStringCast::Length(date, year_length, add_bc);
	const idx_t time_length = TimeToStringCast::Length(time, micro_buffer);
	const idx_t length = date_length + 1 + time_length + 3;

	string_t result = StringVector::EmptyString(vector, length);
	auto data = result.GetDataWriteable();

	idx_t pos = 0;
	DateToStringCast::Format(data + pos, date, year_length, add_bc);
	pos += date_length;
	data[pos++] = ' ';
	TimeToStringCast::Format(data + pos, time_length, time, micro_buffer);
	pos += time_length;
	data[pos++] = '+';
	data[pos++] = '0';
	data[pos++] = '0';

	result.Finalize();
	return result;
}

} // namespace duckdb

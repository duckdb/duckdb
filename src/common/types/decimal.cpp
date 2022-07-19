#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/cast_helpers.hpp"

namespace duckdb {

template <class SIGNED, class UNSIGNED>
string TemplatedDecimalToString(SIGNED value, uint8_t width, uint8_t scale) {
	auto len = DecimalToString::DecimalLength<SIGNED, UNSIGNED>(value, width, scale);
	auto data = unique_ptr<char[]>(new char[len + 1]);
	DecimalToString::FormatDecimal<SIGNED, UNSIGNED>(value, width, scale, data.get(), len);
	return string(data.get(), len);
}

string Decimal::ToString(int16_t value, uint8_t width, uint8_t scale) {
	return TemplatedDecimalToString<int16_t, uint16_t>(value, width, scale);
}

string Decimal::ToString(int32_t value, uint8_t width, uint8_t scale) {
	return TemplatedDecimalToString<int32_t, uint32_t>(value, width, scale);
}

string Decimal::ToString(int64_t value, uint8_t width, uint8_t scale) {
	return TemplatedDecimalToString<int64_t, uint64_t>(value, width, scale);
}

string Decimal::ToString(hugeint_t value, uint8_t width, uint8_t scale) {
	auto len = HugeintToStringCast::DecimalLength(value, width, scale);
	auto data = unique_ptr<char[]>(new char[len + 1]);
	HugeintToStringCast::FormatDecimal(value, width, scale, data.get(), len);
	return string(data.get(), len);
}

} // namespace duckdb

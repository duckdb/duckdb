#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/cast_helpers.hpp"

namespace duckdb {

template <class SIGNED>
string TemplatedDecimalToString(SIGNED value, uint8_t width, uint8_t scale) {
	auto len = DecimalToString::DecimalLength<SIGNED>(value, width, scale);
	auto data = make_unsafe_uniq_array<char>(UnsafeNumericCast<size_t>(len + 1));
	DecimalToString::FormatDecimal<SIGNED>(value, width, scale, data.get(), UnsafeNumericCast<idx_t>(len));
	return string(data.get(), UnsafeNumericCast<uint32_t>(len));
}

string Decimal::ToString(int16_t value, uint8_t width, uint8_t scale) {
	return TemplatedDecimalToString<int16_t>(value, width, scale);
}

string Decimal::ToString(int32_t value, uint8_t width, uint8_t scale) {
	return TemplatedDecimalToString<int32_t>(value, width, scale);
}

string Decimal::ToString(int64_t value, uint8_t width, uint8_t scale) {
	return TemplatedDecimalToString<int64_t>(value, width, scale);
}

string Decimal::ToString(hugeint_t value, uint8_t width, uint8_t scale) {
	auto len = DecimalToString::DecimalLength(value, width, scale);
	auto data = make_unsafe_uniq_array<char>(UnsafeNumericCast<size_t>(len + 1));
	DecimalToString::FormatDecimal(value, width, scale, data.get(), UnsafeNumericCast<idx_t>(len));
	return string(data.get(), UnsafeNumericCast<uint32_t>(len));
}

} // namespace duckdb

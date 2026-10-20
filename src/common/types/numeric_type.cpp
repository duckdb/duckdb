#include "duckdb/common/types/numeric_type.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

bool NumericType::IsExact(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::DECIMAL:
		return true;
	default:
		return false;
	}
}

LogicalType NumericType::DefaultType() {
	return LogicalType::INTEGER;
}

string NumericType::NotExactMessage(const LogicalType &type) {
	return StringUtil::Format("NUMERIC columns store exact numbers (integers and decimals), not values of type %s - "
	                          "cast the value to a DECIMAL first",
	                          type.ToString());
}

void NumericType::GetDigits(const LogicalType &type, uint8_t &integer_digits, uint8_t &scale) {
	scale = 0;
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::UTINYINT:
		integer_digits = 3;
		break;
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::USMALLINT:
		integer_digits = 5;
		break;
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::UINTEGER:
		integer_digits = 10;
		break;
	case LogicalTypeId::BIGINT:
		integer_digits = 19;
		break;
	case LogicalTypeId::UBIGINT:
		integer_digits = 20;
		break;
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
		integer_digits = 39;
		break;
	case LogicalTypeId::DECIMAL:
		scale = DecimalType::GetScale(type);
		integer_digits = NumericCast<uint8_t>(DecimalType::GetWidth(type) - scale);
		break;
	default:
		throw InternalException("NumericType::GetDigits called on non-exact type %s", type.ToString());
	}
}

//! The rank of a signed integer representation, 0 for types that are not stored as signed integers
static idx_t SignedIntegerRank(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return 1;
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::UTINYINT:
		return 2;
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::USMALLINT:
		return 3;
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UINTEGER:
		return 4;
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UBIGINT:
		return 5;
	default:
		// DECIMAL and UHUGEINT (which does not fit in HUGEINT)
		return 0;
	}
}

static LogicalType SignedIntegerType(idx_t rank) {
	switch (rank) {
	case 1:
		return LogicalType::TINYINT;
	case 2:
		return LogicalType::SMALLINT;
	case 3:
		return LogicalType::INTEGER;
	case 4:
		return LogicalType::BIGINT;
	default:
		return LogicalType::HUGEINT;
	}
}

static LogicalType DecimalForDigits(idx_t integer_digits, uint8_t scale) {
	auto width = integer_digits + scale;
	if (width > Decimal::MAX_WIDTH_DECIMAL) {
		throw OutOfRangeException("NUMERIC value needs %d digits, more than the maximum of %d", width,
		                          Decimal::MAX_WIDTH_DECIMAL);
	}
	if (width < 1) {
		width = 1;
	}
	return LogicalType::DECIMAL(NumericCast<uint8_t>(width), scale);
}

LogicalType NumericType::Widen(const LogicalType &left, const LogicalType &right) {
	if (!IsExact(left)) {
		throw BinderException(NotExactMessage(left));
	}
	if (!IsExact(right)) {
		throw BinderException(NotExactMessage(right));
	}
	auto left_rank = SignedIntegerRank(left);
	auto right_rank = SignedIntegerRank(right);
	if (left_rank > 0 && right_rank > 0) {
		return SignedIntegerType(MaxValue(left_rank, right_rank));
	}
	uint8_t left_digits, left_scale, right_digits, right_scale;
	GetDigits(left, left_digits, left_scale);
	GetDigits(right, right_digits, right_scale);
	return DecimalForDigits(MaxValue(left_digits, right_digits), MaxValue(left_scale, right_scale));
}

uint8_t NumericType::IntegerDigits(const Value &value) {
	if (value.IsNull()) {
		return 0;
	}
	if (!IsExact(value.type())) {
		throw InternalException("NumericType::IntegerDigits called on non-exact value of type %s",
		                        value.type().ToString());
	}
	// exact values print as [-]digits[.digits]
	auto str = value.ToString();
	idx_t pos = 0;
	if (pos < str.size() && str[pos] == '-') {
		pos++;
	}
	// skip leading zeros
	while (pos < str.size() && str[pos] == '0') {
		pos++;
	}
	idx_t digits = 0;
	while (pos < str.size() && StringUtil::CharacterIsDigit(str[pos])) {
		digits++;
		pos++;
	}
	return NumericCast<uint8_t>(digits);
}

LogicalType NumericType::ForRange(const Value &min, const Value &max, uint8_t scale) {
	if (min.IsNull() || max.IsNull()) {
		return DefaultType();
	}
	if (scale == 0) {
		auto min_hugeint = min.DefaultTryCastAs(LogicalType::HUGEINT);
		auto max_hugeint = max.DefaultTryCastAs(LogicalType::HUGEINT);
		if (min_hugeint && max_hugeint) {
			auto lo = min_hugeint->GetValue<hugeint_t>();
			auto hi = max_hugeint->GetValue<hugeint_t>();
			if (lo >= NumericLimits<int8_t>::Minimum() && hi <= NumericLimits<int8_t>::Maximum()) {
				return LogicalType::TINYINT;
			}
			if (lo >= NumericLimits<int16_t>::Minimum() && hi <= NumericLimits<int16_t>::Maximum()) {
				return LogicalType::SMALLINT;
			}
			if (lo >= NumericLimits<int32_t>::Minimum() && hi <= NumericLimits<int32_t>::Maximum()) {
				return LogicalType::INTEGER;
			}
			if (lo >= NumericLimits<int64_t>::Minimum() && hi <= NumericLimits<int64_t>::Maximum()) {
				return LogicalType::BIGINT;
			}
			return LogicalType::HUGEINT;
		}
	}
	auto integer_digits = MaxValue(IntegerDigits(min), IntegerDigits(max));
	return DecimalForDigits(integer_digits, scale);
}

LogicalType NumericType::WidestType(uint8_t scale) {
	return LogicalType::DECIMAL(Decimal::MAX_WIDTH_DECIMAL, scale);
}

} // namespace duckdb

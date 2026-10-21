#include "duckdb/common/types/numeric_type.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
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

idx_t NumericType::Digits(hugeint_t value) {
	if (value == NumericLimits<hugeint_t>::Minimum()) {
		return Hugeint::CACHED_POWERS_OF_TEN;
	}
	if (value < 0) {
		value = -value;
	}
	idx_t digits = 1;
	while (digits < Hugeint::CACHED_POWERS_OF_TEN && value >= Hugeint::POWERS_OF_TEN[digits]) {
		digits++;
	}
	return digits;
}

LogicalType NumericType::ForUnscaledRange(hugeint_t min, hugeint_t max, uint8_t scale) {
	if (scale == 0) {
		if (min >= NumericLimits<int8_t>::Minimum() && max <= NumericLimits<int8_t>::Maximum()) {
			return LogicalType::TINYINT;
		}
		if (min >= NumericLimits<int16_t>::Minimum() && max <= NumericLimits<int16_t>::Maximum()) {
			return LogicalType::SMALLINT;
		}
		if (min >= NumericLimits<int32_t>::Minimum() && max <= NumericLimits<int32_t>::Maximum()) {
			return LogicalType::INTEGER;
		}
		if (min >= NumericLimits<int64_t>::Minimum() && max <= NumericLimits<int64_t>::Maximum()) {
			return LogicalType::BIGINT;
		}
		return LogicalType::HUGEINT;
	}
	auto digits = MaxValue(Digits(min), Digits(max));
	auto width = MaxValue<idx_t>(digits, scale);
	if (width > Decimal::MAX_WIDTH_DECIMAL) {
		throw OutOfRangeException("NUMERIC value needs %d digits, more than the maximum of %d", width,
		                          Decimal::MAX_WIDTH_DECIMAL);
	}
	return LogicalType::DECIMAL(NumericCast<uint8_t>(width), scale);
}

LogicalType NumericType::ForDigits(idx_t integer_digits, uint8_t scale) {
	if (scale == 0) {
		if (integer_digits <= 2) {
			return LogicalType::TINYINT;
		}
		if (integer_digits <= 4) {
			return LogicalType::SMALLINT;
		}
		if (integer_digits <= 9) {
			return LogicalType::INTEGER;
		}
		if (integer_digits <= 18) {
			return LogicalType::BIGINT;
		}
		if (integer_digits <= Decimal::MAX_WIDTH_INT128) {
			return LogicalType::HUGEINT;
		}
	}
	return DecimalForDigits(integer_digits, scale);
}

bool NumericType::TryGetRepresentationRange(const LogicalType &type, hugeint_t &min, hugeint_t &max, uint8_t &scale) {
	scale = 0;
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		min = NumericLimits<int8_t>::Minimum();
		max = NumericLimits<int8_t>::Maximum();
		return true;
	case LogicalTypeId::SMALLINT:
		min = NumericLimits<int16_t>::Minimum();
		max = NumericLimits<int16_t>::Maximum();
		return true;
	case LogicalTypeId::INTEGER:
		min = NumericLimits<int32_t>::Minimum();
		max = NumericLimits<int32_t>::Maximum();
		return true;
	case LogicalTypeId::BIGINT:
		min = NumericLimits<int64_t>::Minimum();
		max = NumericLimits<int64_t>::Maximum();
		return true;
	case LogicalTypeId::HUGEINT:
		min = NumericLimits<hugeint_t>::Minimum();
		max = NumericLimits<hugeint_t>::Maximum();
		return true;
	case LogicalTypeId::UTINYINT:
		min = 0;
		max = NumericLimits<uint8_t>::Maximum();
		return true;
	case LogicalTypeId::USMALLINT:
		min = 0;
		max = NumericLimits<uint16_t>::Maximum();
		return true;
	case LogicalTypeId::UINTEGER:
		min = 0;
		max = NumericLimits<uint32_t>::Maximum();
		return true;
	case LogicalTypeId::UBIGINT:
		min = 0;
		max = hugeint_t(NumericLimits<uint64_t>::Maximum());
		return true;
	case LogicalTypeId::DECIMAL: {
		auto width = DecimalType::GetWidth(type);
		if (width >= Hugeint::CACHED_POWERS_OF_TEN) {
			return false;
		}
		scale = DecimalType::GetScale(type);
		max = Hugeint::POWERS_OF_TEN[width] - 1;
		min = -max;
		return true;
	}
	default:
		return false;
	}
}

bool NumericType::ContainsNumeric(const LogicalType &type) {
	return TypeVisitor::Contains(type, LogicalTypeId::NUMERIC);
}

LogicalType NumericType::WithDefaultRepresentation(const LogicalType &type) {
	if (!ContainsNumeric(type)) {
		return type;
	}
	return TypeVisitor::VisitReplace(
	    type, [](const LogicalType &child) { return child.id() == LogicalTypeId::NUMERIC ? DefaultType() : child; });
}

LogicalType NumericType::ExactLeavesToNumeric(const LogicalType &type) {
	return TypeVisitor::VisitReplace(
	    type, [](const LogicalType &child) { return IsExact(child) ? LogicalType::NUMERIC : child; });
}

LogicalType NumericType::DecimalsAsNumeric(const LogicalType &type) {
	if (!TypeVisitor::Contains(type, LogicalTypeId::DECIMAL)) {
		return type;
	}
	return TypeVisitor::VisitReplace(type, [](const LogicalType &child) {
		return child.id() == LogicalTypeId::DECIMAL ? LogicalType::NUMERIC : child;
	});
}

//! Counts the digits of a number written as [-]digits[.digits] (surrounding spaces allowed); false for other text
static bool CountDigits(const char *text, idx_t size, idx_t &begin, idx_t &end, idx_t &integer_digits,
                        idx_t &fractional_digits) {
	begin = 0;
	end = size;
	while (begin < end && StringUtil::CharacterIsSpace(text[begin])) {
		begin++;
	}
	while (end > begin && StringUtil::CharacterIsSpace(text[end - 1])) {
		end--;
	}
	idx_t pos = begin;
	if (pos < end && (text[pos] == '-' || text[pos] == '+')) {
		pos++;
	}
	idx_t digits = 0;
	integer_digits = 0;
	fractional_digits = 0;
	bool seen_point = false;
	bool leading_zeros = true;
	for (; pos < end; pos++) {
		auto c = text[pos];
		if (StringUtil::CharacterIsDigit(c)) {
			digits++;
			if (seen_point) {
				fractional_digits++;
			} else if (c != '0' || !leading_zeros) {
				leading_zeros = false;
				integer_digits++;
			}
		} else if (c == '.' && !seen_point) {
			seen_point = true;
		} else {
			return false;
		}
	}
	return digits > 0;
}

bool NumericType::TryParseLiteral(const string &text, Value &result) {
	idx_t begin, end, integer_digits, fractional_digits;
	if (!CountDigits(text.c_str(), text.size(), begin, end, integer_digits, fractional_digits)) {
		return false;
	}
	auto width = MaxValue<idx_t>(integer_digits + fractional_digits, 1);
	if (width > Decimal::MAX_WIDTH_DECIMAL) {
		throw OutOfRangeException("NUMERIC value needs %d digits, more than the maximum of %d", width,
		                          Decimal::MAX_WIDTH_DECIMAL);
	}
	auto scale = NumericCast<uint8_t>(fractional_digits);
	auto decimal = Value(text.substr(begin, end - begin))
	                   .DefaultTryCastAs(LogicalType::DECIMAL(NumericCast<uint8_t>(width), scale));
	if (!decimal) {
		return false;
	}
	if (scale > 0) {
		result = *decimal;
		return true;
	}
	// whole numbers take the narrowest integer representation
	result = decimal->DefaultCastAs(ForRange(*decimal, *decimal, 0));
	return true;
}

LogicalType NumericType::AsText(const LogicalType &type) {
	if (!ContainsNumeric(type)) {
		return type;
	}
	return TypeVisitor::VisitReplace(type, [](const LogicalType &child) {
		return child.id() == LogicalTypeId::NUMERIC ? LogicalType::VARCHAR : child;
	});
}

LogicalType NumericType::TextType(const Vector &vector, idx_t count) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::VARCHAR);
	UnifiedVectorFormat vdata;
	vector.ToUnifiedFormat(vdata);
	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
	idx_t integer_digits = 0;
	idx_t fractional_digits = 0;
	bool found = false;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			continue;
		}
		idx_t begin, end, value_integer_digits, value_fractional_digits;
		if (!CountDigits(data[idx].GetData(), data[idx].GetSize(), begin, end, value_integer_digits,
		                 value_fractional_digits)) {
			throw ConversionException("Cannot convert '%s' to NUMERIC: not an exact number", data[idx].GetString());
		}
		found = true;
		integer_digits = MaxValue(integer_digits, value_integer_digits);
		fractional_digits = MaxValue(fractional_digits, value_fractional_digits);
	}
	if (!found) {
		return DefaultType();
	}
	if (fractional_digits > Decimal::MAX_WIDTH_DECIMAL) {
		throw OutOfRangeException("NUMERIC value needs %d digits, more than the maximum of %d",
		                          integer_digits + fractional_digits, Decimal::MAX_WIDTH_DECIMAL);
	}
	return ForDigits(integer_digits, NumericCast<uint8_t>(fractional_digits));
}

static LogicalType ResolveCastTargetInternal(const LogicalType &source, const LogicalType &target) {
	switch (target.id()) {
	case LogicalTypeId::NUMERIC:
		if (source.id() == LogicalTypeId::SQLNULL || source.id() == LogicalTypeId::UNKNOWN) {
			return NumericType::DefaultType();
		}
		if (NumericType::IsExact(source)) {
			return source;
		}
		throw BinderException("Cannot cast %s to NUMERIC: %s", source.ToString(), NumericType::NotExactMessage(source));
	case LogicalTypeId::LIST:
		if (source.id() == LogicalTypeId::LIST) {
			return LogicalType::LIST(
			    ResolveCastTargetInternal(ListType::GetChildType(source), ListType::GetChildType(target)));
		}
		break;
	case LogicalTypeId::ARRAY:
		if (source.id() == LogicalTypeId::ARRAY) {
			return LogicalType::ARRAY(
			    ResolveCastTargetInternal(ArrayType::GetChildType(source), ArrayType::GetChildType(target)),
			    ArrayType::GetSize(target));
		}
		if (source.id() == LogicalTypeId::LIST) {
			return LogicalType::ARRAY(
			    ResolveCastTargetInternal(ListType::GetChildType(source), ArrayType::GetChildType(target)),
			    ArrayType::GetSize(target));
		}
		break;
	case LogicalTypeId::MAP:
		if (source.id() == LogicalTypeId::MAP) {
			return LogicalType::MAP(ResolveCastTargetInternal(MapType::KeyType(source), MapType::KeyType(target)),
			                        ResolveCastTargetInternal(MapType::ValueType(source), MapType::ValueType(target)));
		}
		break;
	case LogicalTypeId::STRUCT:
		if (source.id() == LogicalTypeId::STRUCT) {
			auto &target_children = StructType::GetChildTypes(target);
			auto &source_children = StructType::GetChildTypes(source);
			if (source_children.size() == target_children.size()) {
				child_list_t<LogicalType> children;
				for (idx_t i = 0; i < target_children.size(); i++) {
					children.emplace_back(
					    target_children[i].first,
					    ResolveCastTargetInternal(source_children[i].second, target_children[i].second));
				}
				return LogicalType::STRUCT(std::move(children));
			}
		}
		break;
	default:
		break;
	}
	if (!NumericType::ContainsNumeric(target)) {
		return target;
	}
	// the source does not have the shape of the target - let the regular cast deal with it, using the default
	// representation for the NUMERIC parts
	return NumericType::WithDefaultRepresentation(target);
}

LogicalType NumericType::ResolveCastTarget(const LogicalType &source, const LogicalType &target) {
	return ResolveCastTargetInternal(source, target);
}

LogicalType NumericType::WidestType(uint8_t scale) {
	return LogicalType::DECIMAL(Decimal::MAX_WIDTH_DECIMAL, scale);
}

} // namespace duckdb

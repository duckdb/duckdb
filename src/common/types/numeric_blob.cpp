#include "duckdb/common/types/numeric_blob.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/numeric_type.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

static constexpr data_t NUMERIC_CLASS_NEGATIVE = 0x00;
static constexpr data_t NUMERIC_CLASS_ZERO = 0x40;
static constexpr data_t NUMERIC_CLASS_POSITIVE = 0x80;
static constexpr int64_t NUMERIC_EXPONENT_BIAS = 32768;

bool NumericBlob::TryEncode(const char *text, idx_t size, string &result) {
	idx_t begin = 0;
	idx_t end = size;
	while (begin < end && StringUtil::CharacterIsSpace(text[begin])) {
		begin++;
	}
	while (end > begin && StringUtil::CharacterIsSpace(text[end - 1])) {
		end--;
	}
	bool negative = false;
	if (begin < end && (text[begin] == '-' || text[begin] == '+')) {
		negative = text[begin] == '-';
		begin++;
	}
	string all_digits;
	idx_t integer_digits = 0;
	bool seen_point = false;
	for (idx_t i = begin; i < end; i++) {
		auto c = text[i];
		if (StringUtil::CharacterIsDigit(c)) {
			all_digits.push_back(c);
			if (!seen_point) {
				integer_digits++;
			}
		} else if (c == '.' && !seen_point) {
			seen_point = true;
		} else {
			return false;
		}
	}
	if (all_digits.empty()) {
		return false;
	}
	idx_t first = 0;
	while (first < all_digits.size() && all_digits[first] == '0') {
		first++;
	}
	if (first == all_digits.size()) {
		result.assign(1, char(NUMERIC_CLASS_ZERO));
		return true;
	}
	idx_t last = all_digits.size();
	while (last > first && all_digits[last - 1] == '0') {
		last--;
	}
	// the decimal exponent of the first significant digit: "123.45" -> 2, "0.0012" -> -3
	int64_t adjusted = int64_t(integer_digits) - 1 - int64_t(first);
	if (adjusted < -NUMERIC_EXPONENT_BIAS || adjusted >= NUMERIC_EXPONENT_BIAS) {
		return false;
	}
	result.clear();
	result.reserve(4 + (last - first));
	result.push_back(char(negative ? NUMERIC_CLASS_NEGATIVE : NUMERIC_CLASS_POSITIVE));
	auto emit = [&](data_t b) {
		result.push_back(char(negative ? data_t(~b) : b));
	};
	auto biased = uint16_t(adjusted + NUMERIC_EXPONENT_BIAS);
	emit(data_t(biased >> 8));
	emit(data_t(biased & 0xFF));
	for (idx_t i = first; i < last; i++) {
		emit(data_t(all_digits[i] - '0' + 1));
	}
	emit(0);
	return true;
}

string NumericBlob::Encode(const string &text) {
	string result;
	if (!TryEncode(text.c_str(), text.size(), result)) {
		throw ConversionException("Cannot convert '%s' to NUMERIC: not an exact number", text);
	}
	return result;
}

namespace {

struct DecodedBlob {
	bool zero = false;
	bool negative = false;
	int64_t adjusted = 0;
	string digits;
};

DecodedBlob DecodeBlob(const string_t &blob) {
	DecodedBlob decoded;
	auto data = const_data_ptr_cast(blob.GetData());
	auto size = blob.GetSize();
	if (size == 0) {
		throw InternalException("Empty NUMERIC blob");
	}
	if (data[0] == NUMERIC_CLASS_ZERO) {
		decoded.zero = true;
		return decoded;
	}
	decoded.negative = data[0] == NUMERIC_CLASS_NEGATIVE;
	if (size < 4) {
		throw InternalException("Truncated NUMERIC blob");
	}
	auto get = [&](idx_t i) {
		return decoded.negative ? data_t(~data[i]) : data[i];
	};
	auto biased = uint16_t((uint16_t(get(1)) << 8) | get(2));
	decoded.adjusted = int64_t(biased) - NUMERIC_EXPONENT_BIAS;
	for (idx_t i = 3; i < size; i++) {
		auto b = get(i);
		if (b == 0) {
			break;
		}
		decoded.digits.push_back(char('0' + b - 1));
	}
	return decoded;
}

} // namespace

string NumericBlob::ToText(const string_t &blob) {
	auto decoded = DecodeBlob(blob);
	if (decoded.zero) {
		return "0";
	}
	string text;
	if (decoded.negative) {
		text.push_back('-');
	}
	auto &digits = decoded.digits;
	if (decoded.adjusted >= 0) {
		auto integer_length = idx_t(decoded.adjusted) + 1;
		if (digits.size() <= integer_length) {
			text += digits;
			text.append(integer_length - digits.size(), '0');
		} else {
			text += digits.substr(0, integer_length);
			text.push_back('.');
			text += digits.substr(integer_length);
		}
	} else {
		text += "0.";
		text.append(idx_t(-decoded.adjusted) - 1, '0');
		text += digits;
	}
	return text;
}

void NumericBlob::Digits(const string_t &blob, idx_t &integer_digits, idx_t &fractional_digits) {
	auto decoded = DecodeBlob(blob);
	if (decoded.zero) {
		integer_digits = 0;
		fractional_digits = 0;
		return;
	}
	auto digit_count = decoded.digits.size();
	if (decoded.adjusted >= 0) {
		integer_digits = idx_t(decoded.adjusted) + 1;
		fractional_digits = digit_count > integer_digits ? digit_count - integer_digits : 0;
	} else {
		integer_digits = 0;
		fractional_digits = idx_t(-decoded.adjusted) - 1 + digit_count;
	}
}

LogicalType NumericBlob::RangeType(Vector &canonical, idx_t count) {
	UnifiedVectorFormat vdata;
	canonical.ToUnifiedFormat(count, vdata);
	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
	idx_t integer_digits = 0;
	idx_t fractional_digits = 0;
	bool found = false;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			continue;
		}
		idx_t value_integer_digits, value_fractional_digits;
		Digits(data[idx], value_integer_digits, value_fractional_digits);
		integer_digits = MaxValue(integer_digits, value_integer_digits);
		fractional_digits = MaxValue(fractional_digits, value_fractional_digits);
		found = true;
	}
	if (!found) {
		return NumericType::DefaultType();
	}
	if (fractional_digits > Decimal::MAX_WIDTH_DECIMAL) {
		throw OutOfRangeException("NUMERIC value needs %d digits, more than the maximum of %d",
		                          integer_digits + fractional_digits, Decimal::MAX_WIDTH_DECIMAL);
	}
	return NumericType::ForDigits(integer_digits, NumericCast<uint8_t>(fractional_digits));
}

string NumericBlob::NormalizeText(const string &text) {
	auto point = text.find('.');
	if (point == string::npos) {
		return text;
	}
	idx_t end = text.size();
	while (end > point + 1 && text[end - 1] == '0') {
		end--;
	}
	if (end == point + 1) {
		end = point;
	}
	auto result = text.substr(0, end);
	if (result == "-0") {
		return "0";
	}
	return result;
}

void NumericBlob::Encode(Vector &typed, idx_t count, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::NUMERIC);
	D_ASSERT(NumericType::IsExact(typed.GetType()));
	Vector text(LogicalType::VARCHAR, count);
	VectorOperations::DefaultCast(typed, text, count, true);
	UnifiedVectorFormat vdata;
	text.ToUnifiedFormat(count, vdata);
	auto text_data = UnifiedVectorFormat::GetData<string_t>(vdata);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto writer = FlatVector::Writer<string_t>(result, count);
	string blob;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			writer.WriteNull();
			continue;
		}
		if (!TryEncode(text_data[idx].GetData(), text_data[idx].GetSize(), blob)) {
			throw InternalException("Exact value '%s' does not encode as NUMERIC", text_data[idx].GetString());
		}
		writer.WriteValue(StringVector::AddStringOrBlob(result, blob.c_str(), blob.size()));
	}
}

void NumericBlob::Decode(Vector &canonical, idx_t count, Vector &typed) {
	D_ASSERT(canonical.GetType().id() == LogicalTypeId::NUMERIC);
	UnifiedVectorFormat vdata;
	canonical.ToUnifiedFormat(count, vdata);
	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
	Vector text(LogicalType::VARCHAR, count);
	{
		auto writer = FlatVector::Writer<string_t>(text, count);
		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			if (!vdata.validity.RowIsValid(idx)) {
				writer.WriteNull();
				continue;
			}
			auto value_text = ToText(data[idx]);
			writer.WriteValue(StringVector::AddString(text, value_text.c_str(), value_text.size()));
		}
	}
	VectorOperations::DefaultCast(text, typed, count, false);
}

} // namespace duckdb

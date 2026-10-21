#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/numeric_blob.hpp"
#include "duckdb/common/types/numeric_type.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/common/vector/numeric_vector.hpp"
#include "duckdb/common/vector/shredded_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/cast/default_casts.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// to NUMERIC
//===--------------------------------------------------------------------===//
//! An exact value is a NUMERIC value: the result references the source as its representation
static bool CastExactToNumeric(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	result.Shred(source, count);
	return true;
}

//! Text is parsed into the representation that holds every value of the vector
static bool CastTextToNumeric(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	if (count == 0) {
		return true;
	}
	LogicalType type;
	try {
		type = NumericType::TextType(source, count);
	} catch (ConversionException &ex) {
		if (!parameters.error_message) {
			throw;
		}
		HandleCastError::AssignError(ex.what(), parameters);
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, count_t(count));
		return false;
	}
	Vector typed(type, count);
	VectorOperations::DefaultCast(source, typed, count, false);
	result.Shred(typed, count);
	return true;
}

//! Floating point values take their shortest round-trip decimal text (NaN and infinities are not exact numbers)
static bool CastFloatingToNumeric(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	Vector text(LogicalType::VARCHAR, count);
	VectorOperations::DefaultCast(source, text, count, true);
	return CastTextToNumeric(text, result, count, parameters);
}

BoundCastInfo DefaultCasts::ToNumericCastSwitch(BindCastInput &input, const LogicalType &source,
                                                const LogicalType &target) {
	D_ASSERT(target.id() == LogicalTypeId::NUMERIC);
	if (NumericType::IsExact(source)) {
		return CastExactToNumeric;
	}
	switch (source.id()) {
	case LogicalTypeId::VARCHAR:
		return CastTextToNumeric;
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return CastFloatingToNumeric;
	default:
		return TryVectorNullCast;
	}
}

//===--------------------------------------------------------------------===//
// from NUMERIC
//===--------------------------------------------------------------------===//
//! The decimal text of every value, the same whatever its representation: no exponent, no trailing zeros
//! Writes the decimal text of a fixed-size DECIMAL representation without the trailing zeros of the fraction
template <class T>
static void WriteDecimalText(const Vector &typed, idx_t count, uint8_t width, uint8_t scale, Vector &result) {
	UnifiedVectorFormat vdata;
	typed.ToUnifiedFormat(vdata);
	auto data = UnifiedVectorFormat::GetData<T>(vdata);
	auto writer = FlatVector::Writer<string_t>(result, count);
	char buffer[64];
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			writer.WriteNull();
			continue;
		}
		auto value = data[idx];
		auto length = idx_t(DecimalToString::DecimalLength<T>(value, width, scale));
		DecimalToString::FormatDecimal<T>(value, width, scale, buffer, length);
		while (buffer[length - 1] == '0') {
			length--;
		}
		if (buffer[length - 1] == '.') {
			length--;
		}
		if (length == 2 && buffer[0] == '-' && buffer[1] == '0') {
			// a negative zero after trimming
			length = 1;
			buffer[0] = '0';
		}
		if (length <= string_t::INLINE_LENGTH) {
			writer.WriteStringRef(string_t(buffer, UnsafeNumericCast<uint32_t>(length)));
		} else {
			auto &text = writer.WriteEmptyString(length);
			memcpy(text.GetDataWriteable(), buffer, length);
			text.Finalize();
		}
	}
}

static bool CastNumericToVarchar(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	if (source.GetVectorType() == VectorType::SHREDDED_VECTOR) {
		auto &typed = ShreddedVector::GetShreddedVector(source);
		auto &type = typed.GetType();
		if (type.id() != LogicalTypeId::DECIMAL || DecimalType::GetScale(type) == 0) {
			// whole numbers print as they are
			VectorOperations::DefaultCast(typed, result, count, true);
			return true;
		}
		auto width = DecimalType::GetWidth(type);
		auto scale = DecimalType::GetScale(type);
		result.SetVectorType(VectorType::FLAT_VECTOR);
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			WriteDecimalText<int16_t>(typed, count, width, scale, result);
			return true;
		case PhysicalType::INT32:
			WriteDecimalText<int32_t>(typed, count, width, scale, result);
			return true;
		case PhysicalType::INT64:
			WriteDecimalText<int64_t>(typed, count, width, scale, result);
			return true;
		case PhysicalType::INT128:
			WriteDecimalText<hugeint_t>(typed, count, width, scale, result);
			return true;
		default:
			break;
		}
		// wide decimals: the text of the representation without the trailing zeros of the fraction, in place
		Vector text(LogicalType::VARCHAR, count);
		VectorOperations::DefaultCast(typed, text, count, true);
		UnifiedVectorFormat vdata;
		text.ToUnifiedFormat(vdata);
		auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
		StringVector::AddHeapReference(result, text);
		auto writer = FlatVector::Writer<string_t>(result, count);
		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			if (!vdata.validity.RowIsValid(idx)) {
				writer.WriteNull();
				continue;
			}
			auto ptr = data[idx].GetData();
			idx_t end = data[idx].GetSize();
			while (end > 0 && ptr[end - 1] == '0') {
				end--;
			}
			if (end > 0 && ptr[end - 1] == '.') {
				end--;
			}
			if (end == 2 && ptr[0] == '-' && ptr[1] == '0') {
				// a negative zero after trimming
				writer.WriteValue(StringVector::AddString(result, "0"));
				continue;
			}
			writer.WriteStringRef(string_t(ptr, UnsafeNumericCast<uint32_t>(end)));
		}
		return true;
	}
	UnifiedVectorFormat vdata;
	source.ToUnifiedFormat(vdata);
	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto writer = FlatVector::Writer<string_t>(result, count);
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			writer.WriteNull();
			continue;
		}
		auto value_text = NumericBlob::ToText(data[idx]);
		writer.WriteValue(StringVector::AddString(result, value_text.c_str(), value_text.size()));
	}
	return true;
}

//! Whether the values of a typed NUMERIC vector are the same bits as values of the target: the same physical type
//! and scale, and a value range the target holds (a DECIMAL(12,2) representation read as DECIMAL(10,2))
static bool SameBits(Vector &source, const Vector &typed, const LogicalType &target, idx_t count) {
	auto &type = typed.GetType();
	if (type.InternalType() != target.InternalType() || !NumericType::IsExact(target)) {
		return false;
	}
	uint8_t source_scale = type.id() == LogicalTypeId::DECIMAL ? DecimalType::GetScale(type) : 0;
	hugeint_t min, max;
	uint8_t target_scale;
	if (!NumericType::TryGetRepresentationRange(target, min, max, target_scale) || target_scale != source_scale) {
		return false;
	}
	ShreddedNumericRange range;
	if (!NumericVector::TryGetRange(source, count, range)) {
		return false;
	}
	return min <= range.min && range.max <= max;
}

static bool CastNumericToTyped(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	if (source.GetVectorType() == VectorType::SHREDDED_VECTOR) {
		auto &typed = ShreddedVector::GetShreddedVector(source);
		if (typed.GetType() == result.GetType()) {
			result.Reference(typed);
			return true;
		}
		if (SameBits(source, typed, result.GetType(), count)) {
			result.Reinterpret(typed);
			return true;
		}
		return VectorOperations::DefaultTryCast(typed, result, count, parameters.error_message, parameters.strict);
	}
	// canonical values: through their decimal text
	Vector text(LogicalType::VARCHAR, count);
	CastNumericToVarchar(source, text, count, parameters);
	return VectorOperations::DefaultTryCast(text, result, count, parameters.error_message, parameters.strict);
}

BoundCastInfo DefaultCasts::FromNumericCastSwitch(BindCastInput &input, const LogicalType &source,
                                                  const LogicalType &target) {
	D_ASSERT(source.id() == LogicalTypeId::NUMERIC);
	if (NumericType::IsExact(target)) {
		return CastNumericToTyped;
	}
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		return CastNumericToVarchar;
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::BOOLEAN:
		return CastNumericToTyped;
	default:
		return TryVectorNullCast;
	}
}

} // namespace duckdb

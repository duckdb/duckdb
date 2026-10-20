#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/numeric_blob.hpp"
#include "duckdb/common/types/numeric_type.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
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
static bool CastNumericToVarchar(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	if (source.GetVectorType() == VectorType::SHREDDED_VECTOR) {
		auto &typed = ShreddedVector::GetShreddedVector(source);
		Vector text(LogicalType::VARCHAR, count);
		VectorOperations::DefaultCast(typed, text, count, true);
		UnifiedVectorFormat vdata;
		text.ToUnifiedFormat(count, vdata);
		auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto writer = FlatVector::Writer<string_t>(result, count);
		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			if (!vdata.validity.RowIsValid(idx)) {
				writer.WriteNull();
				continue;
			}
			auto value_text = NumericBlob::NormalizeText(data[idx].GetString());
			writer.WriteValue(StringVector::AddString(result, value_text.c_str(), value_text.size()));
		}
		return true;
	}
	UnifiedVectorFormat vdata;
	source.ToUnifiedFormat(count, vdata);
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

//! A typed target: the representation is referenced when it matches, cast otherwise
static bool CastNumericToTyped(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	if (source.GetVectorType() == VectorType::SHREDDED_VECTOR) {
		auto &typed = ShreddedVector::GetShreddedVector(source);
		if (typed.GetType() == result.GetType()) {
			result.Reference(typed);
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

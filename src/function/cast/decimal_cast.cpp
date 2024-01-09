#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"

#include "duckdb/common/vector_operations/general_cast.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/types/cast_helpers.hpp"

namespace duckdb {

template <class T>
static bool FromDecimalCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &source_type = source.GetType();
	auto width = DecimalType::GetWidth(source_type);
	auto scale = DecimalType::GetScale(source_type);
	switch (source_type.InternalType()) {
	case PhysicalType::INT16:
		return VectorCastHelpers::TemplatedDecimalCast<int16_t, T, TryCastFromDecimal>(
		    source, result, count, parameters.error_message, width, scale);
	case PhysicalType::INT32:
		return VectorCastHelpers::TemplatedDecimalCast<int32_t, T, TryCastFromDecimal>(
		    source, result, count, parameters.error_message, width, scale);
	case PhysicalType::INT64:
		return VectorCastHelpers::TemplatedDecimalCast<int64_t, T, TryCastFromDecimal>(
		    source, result, count, parameters.error_message, width, scale);
	case PhysicalType::INT128:
		return VectorCastHelpers::TemplatedDecimalCast<hugeint_t, T, TryCastFromDecimal>(
		    source, result, count, parameters.error_message, width, scale);
	default:
		throw InternalException("Unimplemented internal type for decimal");
	}
}

template <class LIMIT_TYPE, class FACTOR_TYPE = LIMIT_TYPE>
struct DecimalScaleInput {
	DecimalScaleInput(Vector &result_p, FACTOR_TYPE factor_p) : result(result_p), factor(factor_p) {
	}
	DecimalScaleInput(Vector &result_p, LIMIT_TYPE limit_p, FACTOR_TYPE factor_p, string *error_message_p,
	                  uint8_t source_width_p, uint8_t source_scale_p)
	    : result(result_p), limit(limit_p), factor(factor_p), error_message(error_message_p),
	      source_width(source_width_p), source_scale(source_scale_p) {
	}

	Vector &result;
	LIMIT_TYPE limit;
	FACTOR_TYPE factor;
	bool all_converted = true;
	string *error_message;
	uint8_t source_width;
	uint8_t source_scale;
};

struct DecimalScaleUpOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto data = (DecimalScaleInput<INPUT_TYPE, RESULT_TYPE> *)dataptr;
		return Cast::Operation<INPUT_TYPE, RESULT_TYPE>(input) * data->factor;
	}
};

struct DecimalScaleUpCheckOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto data = (DecimalScaleInput<INPUT_TYPE, RESULT_TYPE> *)dataptr;
		if (input >= data->limit || input <= -data->limit) {
			auto error = StringUtil::Format("Casting value \"%s\" to type %s failed: value is out of range!",
			                                Decimal::ToString(input, data->source_width, data->source_scale),
			                                data->result.GetType().ToString());
			return HandleVectorCastError::Operation<RESULT_TYPE>(std::move(error), mask, idx, data->error_message,
			                                                     data->all_converted);
		}
		return Cast::Operation<INPUT_TYPE, RESULT_TYPE>(input) * data->factor;
	}
};

template <class SOURCE, class DEST, class POWERS_SOURCE, class POWERS_DEST>
bool TemplatedDecimalScaleUp(Vector &source, Vector &result, idx_t count, string *error_message) {
	auto source_scale = DecimalType::GetScale(source.GetType());
	auto source_width = DecimalType::GetWidth(source.GetType());
	auto result_scale = DecimalType::GetScale(result.GetType());
	auto result_width = DecimalType::GetWidth(result.GetType());
	D_ASSERT(result_scale >= source_scale);
	idx_t scale_difference = result_scale - source_scale;
	DEST multiply_factor = POWERS_DEST::POWERS_OF_TEN[scale_difference];
	idx_t target_width = result_width - scale_difference;
	if (source_width < target_width) {
		DecimalScaleInput<SOURCE, DEST> input(result, multiply_factor);
		// type will always fit: no need to check limit
		UnaryExecutor::GenericExecute<SOURCE, DEST, DecimalScaleUpOperator>(source, result, count, &input);
		return true;
	} else {
		// type might not fit: check limit
		auto limit = POWERS_SOURCE::POWERS_OF_TEN[target_width];
		DecimalScaleInput<SOURCE, DEST> input(result, limit, multiply_factor, error_message, source_width,
		                                      source_scale);
		UnaryExecutor::GenericExecute<SOURCE, DEST, DecimalScaleUpCheckOperator>(source, result, count, &input,
		                                                                         error_message);
		return input.all_converted;
	}
}

struct DecimalScaleDownOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto data = (DecimalScaleInput<INPUT_TYPE> *)dataptr;
		return Cast::Operation<INPUT_TYPE, RESULT_TYPE>(input / data->factor);
	}
};

struct DecimalScaleDownCheckOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto data = (DecimalScaleInput<INPUT_TYPE> *)dataptr;
		if (input >= data->limit || input <= -data->limit) {
			auto error = StringUtil::Format("Casting value \"%s\" to type %s failed: value is out of range!",
			                                Decimal::ToString(input, data->source_width, data->source_scale),
			                                data->result.GetType().ToString());
			return HandleVectorCastError::Operation<RESULT_TYPE>(std::move(error), mask, idx, data->error_message,
			                                                     data->all_converted);
		}
		return Cast::Operation<INPUT_TYPE, RESULT_TYPE>(input / data->factor);
	}
};

template <class SOURCE, class DEST, class POWERS_SOURCE>
bool TemplatedDecimalScaleDown(Vector &source, Vector &result, idx_t count, string *error_message) {
	auto source_scale = DecimalType::GetScale(source.GetType());
	auto source_width = DecimalType::GetWidth(source.GetType());
	auto result_scale = DecimalType::GetScale(result.GetType());
	auto result_width = DecimalType::GetWidth(result.GetType());
	D_ASSERT(result_scale < source_scale);
	idx_t scale_difference = source_scale - result_scale;
	idx_t target_width = result_width + scale_difference;
	SOURCE divide_factor = POWERS_SOURCE::POWERS_OF_TEN[scale_difference];
	if (source_width < target_width) {
		DecimalScaleInput<SOURCE> input(result, divide_factor);
		// type will always fit: no need to check limit
		UnaryExecutor::GenericExecute<SOURCE, DEST, DecimalScaleDownOperator>(source, result, count, &input);
		return true;
	} else {
		// type might not fit: check limit
		auto limit = POWERS_SOURCE::POWERS_OF_TEN[target_width];
		DecimalScaleInput<SOURCE> input(result, limit, divide_factor, error_message, source_width, source_scale);
		UnaryExecutor::GenericExecute<SOURCE, DEST, DecimalScaleDownCheckOperator>(source, result, count, &input,
		                                                                           error_message);
		return input.all_converted;
	}
}

template <class SOURCE, class POWERS_SOURCE>
static bool DecimalDecimalCastSwitch(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto source_scale = DecimalType::GetScale(source.GetType());
	auto result_scale = DecimalType::GetScale(result.GetType());
	source.GetType().Verify();
	result.GetType().Verify();

	// we need to either multiply or divide by the difference in scales
	if (result_scale >= source_scale) {
		// multiply
		switch (result.GetType().InternalType()) {
		case PhysicalType::INT16:
			return TemplatedDecimalScaleUp<SOURCE, int16_t, POWERS_SOURCE, NumericHelper>(source, result, count,
			                                                                              parameters.error_message);
		case PhysicalType::INT32:
			return TemplatedDecimalScaleUp<SOURCE, int32_t, POWERS_SOURCE, NumericHelper>(source, result, count,
			                                                                              parameters.error_message);
		case PhysicalType::INT64:
			return TemplatedDecimalScaleUp<SOURCE, int64_t, POWERS_SOURCE, NumericHelper>(source, result, count,
			                                                                              parameters.error_message);
		case PhysicalType::INT128:
			return TemplatedDecimalScaleUp<SOURCE, hugeint_t, POWERS_SOURCE, Hugeint>(source, result, count,
			                                                                          parameters.error_message);
		default:
			throw NotImplementedException("Unimplemented internal type for decimal");
		}
	} else {
		// divide
		switch (result.GetType().InternalType()) {
		case PhysicalType::INT16:
			return TemplatedDecimalScaleDown<SOURCE, int16_t, POWERS_SOURCE>(source, result, count,
			                                                                 parameters.error_message);
		case PhysicalType::INT32:
			return TemplatedDecimalScaleDown<SOURCE, int32_t, POWERS_SOURCE>(source, result, count,
			                                                                 parameters.error_message);
		case PhysicalType::INT64:
			return TemplatedDecimalScaleDown<SOURCE, int64_t, POWERS_SOURCE>(source, result, count,
			                                                                 parameters.error_message);
		case PhysicalType::INT128:
			return TemplatedDecimalScaleDown<SOURCE, hugeint_t, POWERS_SOURCE>(source, result, count,
			                                                                   parameters.error_message);
		default:
			throw NotImplementedException("Unimplemented internal type for decimal");
		}
	}
}

struct DecimalCastInput {
	DecimalCastInput(Vector &result_p, uint8_t width_p, uint8_t scale_p)
	    : result(result_p), width(width_p), scale(scale_p) {
	}

	Vector &result;
	uint8_t width;
	uint8_t scale;
};

struct StringCastFromDecimalOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto data = reinterpret_cast<DecimalCastInput *>(dataptr);
		return StringCastFromDecimal::Operation<INPUT_TYPE>(input, data->width, data->scale, data->result);
	}
};

template <class SRC>
static bool DecimalToStringCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &source_type = source.GetType();
	auto width = DecimalType::GetWidth(source_type);
	auto scale = DecimalType::GetScale(source_type);
	DecimalCastInput input(result, width, scale);

	UnaryExecutor::GenericExecute<SRC, string_t, StringCastFromDecimalOperator>(source, result, count, (void *)&input);
	return true;
}

BoundCastInfo DefaultCasts::DecimalCastSwitch(BindCastInput &input, const LogicalType &source,
                                              const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::BOOLEAN:
		return FromDecimalCast<bool>;
	case LogicalTypeId::TINYINT:
		return FromDecimalCast<int8_t>;
	case LogicalTypeId::SMALLINT:
		return FromDecimalCast<int16_t>;
	case LogicalTypeId::INTEGER:
		return FromDecimalCast<int32_t>;
	case LogicalTypeId::BIGINT:
		return FromDecimalCast<int64_t>;
	case LogicalTypeId::UTINYINT:
		return FromDecimalCast<uint8_t>;
	case LogicalTypeId::USMALLINT:
		return FromDecimalCast<uint16_t>;
	case LogicalTypeId::UINTEGER:
		return FromDecimalCast<uint32_t>;
	case LogicalTypeId::UBIGINT:
		return FromDecimalCast<uint64_t>;
	case LogicalTypeId::HUGEINT:
		return FromDecimalCast<hugeint_t>;
	case LogicalTypeId::DECIMAL: {
		// decimal to decimal cast
		// first we need to figure out the source and target internal types
		switch (source.InternalType()) {
		case PhysicalType::INT16:
			return DecimalDecimalCastSwitch<int16_t, NumericHelper>;
		case PhysicalType::INT32:
			return DecimalDecimalCastSwitch<int32_t, NumericHelper>;
		case PhysicalType::INT64:
			return DecimalDecimalCastSwitch<int64_t, NumericHelper>;
		case PhysicalType::INT128:
			return DecimalDecimalCastSwitch<hugeint_t, Hugeint>;
		default:
			throw NotImplementedException("Unimplemented internal type for decimal in decimal_decimal cast");
		}
	}
	case LogicalTypeId::FLOAT:
		return FromDecimalCast<float>;
	case LogicalTypeId::DOUBLE:
		return FromDecimalCast<double>;
	case LogicalTypeId::VARCHAR: {
		switch (source.InternalType()) {
		case PhysicalType::INT16:
			return DecimalToStringCast<int16_t>;
		case PhysicalType::INT32:
			return DecimalToStringCast<int32_t>;
		case PhysicalType::INT64:
			return DecimalToStringCast<int64_t>;
		case PhysicalType::INT128:
			return DecimalToStringCast<hugeint_t>;
		default:
			throw InternalException("Unimplemented internal decimal type");
		}
	}
	default:
		return DefaultCasts::TryVectorNullCast;
	}
}

} // namespace duckdb

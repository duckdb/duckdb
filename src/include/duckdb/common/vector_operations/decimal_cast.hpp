//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/decimal_cast.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector_operations/general_cast.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/operator/decimal_cast_operators.hpp"

namespace duckdb {

struct VectorDecimalCastData {
	VectorDecimalCastData(string *error_message_p, uint8_t width_p, uint8_t scale_p)
	    : error_message(error_message_p), width(width_p), scale(scale_p) {
	}

	string *error_message;
	uint8_t width;
	uint8_t scale;
	bool all_converted = true;
};

template <class OP>
struct VectorDecimalCastOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto data = (VectorDecimalCastData *)dataptr;
		RESULT_TYPE result_value;
		if (!OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, result_value, data->error_message, data->width,
		                                                     data->scale)) {
			return HandleVectorCastError::Operation<RESULT_TYPE>("Failed to cast decimal value", mask, idx,
			                                                     data->error_message, data->all_converted);
		}
		return result_value;
	}
};

template <class SRC, class T, class OP>
bool TemplatedVectorDecimalCast(Vector &source, Vector &result, idx_t count, string *error_message, uint8_t width,
                                uint8_t scale) {
	VectorDecimalCastData input(error_message, width, scale);
	UnaryExecutor::GenericExecute<SRC, T, VectorDecimalCastOperator<OP>>(source, result, count, (void *)&input,
	                                                                     error_message);
	return input.all_converted;
}

template <class T>
static bool ToDecimalCast(Vector &source, Vector &result, idx_t count, string *error_message) {
	auto &result_type = result.GetType();
	auto width = DecimalType::GetWidth(result_type);
	auto scale = DecimalType::GetScale(result_type);
	switch (result_type.InternalType()) {
	case PhysicalType::INT16:
		return TemplatedVectorDecimalCast<T, int16_t, TryCastToDecimal>(source, result, count, error_message, width,
		                                                                scale);
	case PhysicalType::INT32:
		return TemplatedVectorDecimalCast<T, int32_t, TryCastToDecimal>(source, result, count, error_message, width,
		                                                                scale);
	case PhysicalType::INT64:
		return TemplatedVectorDecimalCast<T, int64_t, TryCastToDecimal>(source, result, count, error_message, width,
		                                                                scale);
	case PhysicalType::INT128:
		return TemplatedVectorDecimalCast<T, hugeint_t, TryCastToDecimal>(source, result, count, error_message, width,
		                                                                  scale);
	default:
		throw InternalException("Unimplemented internal type for decimal");
	}
}

template <class T>
static bool FromDecimalCast(Vector &source, Vector &result, idx_t count, string *error_message) {
	auto &source_type = source.GetType();
	auto width = DecimalType::GetWidth(source_type);
	auto scale = DecimalType::GetScale(source_type);
	switch (source_type.InternalType()) {
	case PhysicalType::INT16:
		return TemplatedVectorDecimalCast<int16_t, T, TryCastFromDecimal>(source, result, count, error_message, width,
		                                                                  scale);
	case PhysicalType::INT32:
		return TemplatedVectorDecimalCast<int32_t, T, TryCastFromDecimal>(source, result, count, error_message, width,
		                                                                  scale);
	case PhysicalType::INT64:
		return TemplatedVectorDecimalCast<int64_t, T, TryCastFromDecimal>(source, result, count, error_message, width,
		                                                                  scale);
	case PhysicalType::INT128:
		return TemplatedVectorDecimalCast<hugeint_t, T, TryCastFromDecimal>(source, result, count, error_message, width,
		                                                                    scale);
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
			return HandleVectorCastError::Operation<RESULT_TYPE>(move(error), mask, idx, data->error_message,
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
			return HandleVectorCastError::Operation<RESULT_TYPE>(move(error), mask, idx, data->error_message,
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
static bool DecimalDecimalCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
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
			                                                                              error_message);
		case PhysicalType::INT32:
			return TemplatedDecimalScaleUp<SOURCE, int32_t, POWERS_SOURCE, NumericHelper>(source, result, count,
			                                                                              error_message);
		case PhysicalType::INT64:
			return TemplatedDecimalScaleUp<SOURCE, int64_t, POWERS_SOURCE, NumericHelper>(source, result, count,
			                                                                              error_message);
		case PhysicalType::INT128:
			return TemplatedDecimalScaleUp<SOURCE, hugeint_t, POWERS_SOURCE, Hugeint>(source, result, count,
			                                                                          error_message);
		default:
			throw NotImplementedException("Unimplemented internal type for decimal");
		}
	} else {
		// divide
		switch (result.GetType().InternalType()) {
		case PhysicalType::INT16:
			return TemplatedDecimalScaleDown<SOURCE, int16_t, POWERS_SOURCE>(source, result, count, error_message);
		case PhysicalType::INT32:
			return TemplatedDecimalScaleDown<SOURCE, int32_t, POWERS_SOURCE>(source, result, count, error_message);
		case PhysicalType::INT64:
			return TemplatedDecimalScaleDown<SOURCE, int64_t, POWERS_SOURCE>(source, result, count, error_message);
		case PhysicalType::INT128:
			return TemplatedDecimalScaleDown<SOURCE, hugeint_t, POWERS_SOURCE>(source, result, count, error_message);
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
		auto data = (DecimalCastInput *)dataptr;
		return StringCastFromDecimal::Operation<INPUT_TYPE>(input, data->width, data->scale, data->result);
	}
};

static bool DecimalCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::BOOLEAN:
		return FromDecimalCast<bool>(source, result, count, error_message);
	case LogicalTypeId::TINYINT:
		return FromDecimalCast<int8_t>(source, result, count, error_message);
	case LogicalTypeId::SMALLINT:
		return FromDecimalCast<int16_t>(source, result, count, error_message);
	case LogicalTypeId::INTEGER:
		return FromDecimalCast<int32_t>(source, result, count, error_message);
	case LogicalTypeId::BIGINT:
		return FromDecimalCast<int64_t>(source, result, count, error_message);
	case LogicalTypeId::UTINYINT:
		return FromDecimalCast<uint8_t>(source, result, count, error_message);
	case LogicalTypeId::USMALLINT:
		return FromDecimalCast<uint16_t>(source, result, count, error_message);
	case LogicalTypeId::UINTEGER:
		return FromDecimalCast<uint32_t>(source, result, count, error_message);
	case LogicalTypeId::UBIGINT:
		return FromDecimalCast<uint64_t>(source, result, count, error_message);
	case LogicalTypeId::HUGEINT:
		return FromDecimalCast<hugeint_t>(source, result, count, error_message);
	case LogicalTypeId::DECIMAL: {
		// decimal to decimal cast
		// first we need to figure out the source and target internal types
		switch (source.GetType().InternalType()) {
		case PhysicalType::INT16:
			return DecimalDecimalCastSwitch<int16_t, NumericHelper>(source, result, count, error_message);
		case PhysicalType::INT32:
			return DecimalDecimalCastSwitch<int32_t, NumericHelper>(source, result, count, error_message);
		case PhysicalType::INT64:
			return DecimalDecimalCastSwitch<int64_t, NumericHelper>(source, result, count, error_message);
		case PhysicalType::INT128:
			return DecimalDecimalCastSwitch<hugeint_t, Hugeint>(source, result, count, error_message);
		default:
			throw NotImplementedException("Unimplemented internal type for decimal in decimal_decimal cast");
		}
		break;
	}
	case LogicalTypeId::FLOAT:
		return FromDecimalCast<float>(source, result, count, error_message);
	case LogicalTypeId::DOUBLE:
		return FromDecimalCast<double>(source, result, count, error_message);
	case LogicalTypeId::VARCHAR: {
		auto &source_type = source.GetType();
		auto width = DecimalType::GetWidth(source_type);
		auto scale = DecimalType::GetScale(source_type);
		DecimalCastInput input(result, width, scale);
		switch (source_type.InternalType()) {
		case PhysicalType::INT16:
			UnaryExecutor::GenericExecute<int16_t, string_t, StringCastFromDecimalOperator>(source, result, count,
			                                                                                (void *)&input);
			break;
		case PhysicalType::INT32:
			UnaryExecutor::GenericExecute<int32_t, string_t, StringCastFromDecimalOperator>(source, result, count,
			                                                                                (void *)&input);
			break;
		case PhysicalType::INT64:
			UnaryExecutor::GenericExecute<int64_t, string_t, StringCastFromDecimalOperator>(source, result, count,
			                                                                                (void *)&input);
			break;
		case PhysicalType::INT128:
			UnaryExecutor::GenericExecute<hugeint_t, string_t, StringCastFromDecimalOperator>(source, result, count,
			                                                                                  (void *)&input);
			break;
		default:
			throw InternalException("Unimplemented internal decimal type");
		}
		return true;
	}
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
}

} // namespace duckdb

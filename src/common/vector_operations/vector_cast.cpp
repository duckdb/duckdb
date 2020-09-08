//===--------------------------------------------------------------------===//
// cast_operators.cpp
// Description: This file contains the implementation of the different casts
//===--------------------------------------------------------------------===//
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/numeric_helper.hpp"

namespace duckdb {
using namespace std;

template <class SRC, class OP> static void string_cast(Vector &source, Vector &result, idx_t count) {
	assert(result.type.InternalType() == PhysicalType::VARCHAR);
	UnaryExecutor::Execute<SRC, string_t, true>(source, result, count,
	                                            [&](SRC input) { return OP::template Operation<SRC>(input, result); });
}

static NotImplementedException UnimplementedCast(LogicalType source_type, LogicalType target_type) {
	return NotImplementedException("Unimplemented type for cast (%s -> %s)", source_type.ToString(),
	                               target_type.ToString());
}

// NULL cast only works if all values in source are NULL, otherwise an unimplemented cast exception is thrown
static void null_cast(Vector &source, Vector &result, idx_t count) {
	if (VectorOperations::HasNotNull(source, count)) {
		throw UnimplementedCast(source.type, result.type);
	}
	if (source.vector_type == VectorType::CONSTANT_VECTOR) {
		result.vector_type = VectorType::CONSTANT_VECTOR;
		ConstantVector::SetNull(result, true);
	} else {
		result.vector_type = VectorType::FLAT_VECTOR;
		FlatVector::Nullmask(result).set();
	}
}

template <class T> static void to_decimal_cast(Vector &source, Vector &result, idx_t count) {
	switch (result.type.InternalType()) {
	case PhysicalType::INT16:
		UnaryExecutor::Execute<T, int16_t, true>(source, result, count, [&](T input) {
			return CastToDecimal::Operation<T, int16_t>(input, result.type.width(), result.type.scale());
		});
		break;
	case PhysicalType::INT32:
		UnaryExecutor::Execute<T, int32_t, true>(source, result, count, [&](T input) {
			return CastToDecimal::Operation<T, int32_t>(input, result.type.width(), result.type.scale());
		});
		break;
	case PhysicalType::INT64:
		UnaryExecutor::Execute<T, int64_t, true>(source, result, count, [&](T input) {
			return CastToDecimal::Operation<T, int64_t>(input, result.type.width(), result.type.scale());
		});
		break;
	case PhysicalType::INT128:
		UnaryExecutor::Execute<T, hugeint_t, true>(source, result, count, [&](T input) {
			return CastToDecimal::Operation<T, hugeint_t>(input, result.type.width(), result.type.scale());
		});
		break;
	default:
		throw NotImplementedException("Unimplemented internal type for decimal");
	}
}

template <class T> static void from_decimal_cast(Vector &source, Vector &result, idx_t count) {
	switch (source.type.InternalType()) {
	case PhysicalType::INT16:
		UnaryExecutor::Execute<int16_t, T, true>(source, result, count, [&](int16_t input) {
			return CastFromDecimal::Operation<int16_t, T>(input, source.type.width(), source.type.scale());
		});
		break;
	case PhysicalType::INT32:
		UnaryExecutor::Execute<int32_t, T, true>(source, result, count, [&](int32_t input) {
			return CastFromDecimal::Operation<int32_t, T>(input, source.type.width(), source.type.scale());
		});
		break;
	case PhysicalType::INT64:
		UnaryExecutor::Execute<int64_t, T, true>(source, result, count, [&](int64_t input) {
			return CastFromDecimal::Operation<int64_t, T>(input, source.type.width(), source.type.scale());
		});
		break;
	case PhysicalType::INT128:
		UnaryExecutor::Execute<hugeint_t, T, true>(source, result, count, [&](hugeint_t input) {
			return CastFromDecimal::Operation<hugeint_t, T>(input, source.type.width(), source.type.scale());
		});
		break;
	default:
		throw NotImplementedException("Unimplemented internal type for decimal");
	}
}

template <class SOURCE, class DEST, class POWERS_SOURCE, class POWERS_DEST>
void decimal_scale_up_loop(Vector &source, Vector &result, idx_t count) {
	assert(result.type.scale() >= source.type.scale());
	idx_t scale_difference = result.type.scale() - source.type.scale();
	auto multiply_factor = POWERS_DEST::PowersOfTen[scale_difference];
	idx_t target_width = result.type.width() - scale_difference;
	if (source.type.width() < target_width) {
		// type will always fit: no need to check limit
		UnaryExecutor::Execute<SOURCE, DEST, true>(source, result, count, [&](SOURCE input) {
			return Cast::Operation<SOURCE, DEST>(input) * multiply_factor;
		});
	} else {
		// type might not fit: check limit
		auto limit = POWERS_SOURCE::PowersOfTen[target_width];
		UnaryExecutor::Execute<SOURCE, DEST, true>(source, result, count, [&](SOURCE input) {
			if (input >= limit || input <= -limit) {
				throw OutOfRangeException("Casting to %s failed", result.type.ToString());
			}
			return Cast::Operation<SOURCE, DEST>(input) * multiply_factor;
		});
	}
}

template <class SOURCE, class DEST, class POWERS_SOURCE>
void decimal_scale_down_loop(Vector &source, Vector &result, idx_t count) {
	assert(result.type.scale() < source.type.scale());
	idx_t scale_difference = source.type.scale() - result.type.scale();
	idx_t target_width = result.type.width() + scale_difference;
	auto divide_factor = POWERS_SOURCE::PowersOfTen[scale_difference];
	if (source.type.width() < target_width) {
		// type will always fit: no need to check limit
		UnaryExecutor::Execute<SOURCE, DEST, true>(source, result, count, [&](SOURCE input) {
			return Cast::Operation<SOURCE, DEST>(input / divide_factor);
		});
	} else {
		// type might not fit: check limit
		auto limit = POWERS_SOURCE::PowersOfTen[target_width];
		UnaryExecutor::Execute<SOURCE, DEST, true>(source, result, count, [&](SOURCE input) {
			if (input >= limit || input <= -limit) {
				throw OutOfRangeException("Casting to %s failed", result.type.ToString());
			}
			return Cast::Operation<SOURCE, DEST>(input / divide_factor);
		});
	}
}

template <class SOURCE, class POWERS_SOURCE>
static void decimal_decimal_cast_switch(Vector &source, Vector &result, idx_t count) {
	source.type.Verify();
	result.type.Verify();

	// we need to either multiply or divide by the difference in scales
	if (result.type.scale() >= source.type.scale()) {
		// multiply
		switch (result.type.InternalType()) {
		case PhysicalType::INT16:
			decimal_scale_up_loop<SOURCE, int16_t, POWERS_SOURCE, NumericHelper>(source, result, count);
			break;
		case PhysicalType::INT32:
			decimal_scale_up_loop<SOURCE, int32_t, POWERS_SOURCE, NumericHelper>(source, result, count);
			break;
		case PhysicalType::INT64:
			decimal_scale_up_loop<SOURCE, int64_t, POWERS_SOURCE, NumericHelper>(source, result, count);
			break;
		case PhysicalType::INT128:
			decimal_scale_up_loop<SOURCE, hugeint_t, POWERS_SOURCE, Hugeint>(source, result, count);
			break;
		default:
			throw NotImplementedException("Unimplemented internal type for decimal");
		}
	} else {
		// divide
		switch (result.type.InternalType()) {
		case PhysicalType::INT16:
			decimal_scale_down_loop<SOURCE, int16_t, POWERS_SOURCE>(source, result, count);
			break;
		case PhysicalType::INT32:
			decimal_scale_down_loop<SOURCE, int32_t, POWERS_SOURCE>(source, result, count);
			break;
		case PhysicalType::INT64:
			decimal_scale_down_loop<SOURCE, int64_t, POWERS_SOURCE>(source, result, count);
			break;
		case PhysicalType::INT128:
			decimal_scale_down_loop<SOURCE, hugeint_t, POWERS_SOURCE>(source, result, count);
			break;
		default:
			throw NotImplementedException("Unimplemented internal type for decimal");
		}
	}
}

static void decimal_cast_switch(Vector &source, Vector &result, idx_t count) {
	// now switch on the result type
	switch (result.type.id()) {
	case LogicalTypeId::BOOLEAN:
		from_decimal_cast<bool>(source, result, count);
		break;
	case LogicalTypeId::TINYINT:
		from_decimal_cast<int8_t>(source, result, count);
		break;
	case LogicalTypeId::SMALLINT:
		from_decimal_cast<int16_t>(source, result, count);
		break;
	case LogicalTypeId::INTEGER:
		from_decimal_cast<int32_t>(source, result, count);
		break;
	case LogicalTypeId::BIGINT:
		from_decimal_cast<int64_t>(source, result, count);
		break;
	case LogicalTypeId::HUGEINT:
		from_decimal_cast<hugeint_t>(source, result, count);
		break;
	case LogicalTypeId::DECIMAL: {
		// decimal to decimal cast
		// first we need to figure out the source and target internal types
		switch (source.type.InternalType()) {
		case PhysicalType::INT16:
			decimal_decimal_cast_switch<int16_t, NumericHelper>(source, result, count);
			break;
		case PhysicalType::INT32:
			decimal_decimal_cast_switch<int32_t, NumericHelper>(source, result, count);
			break;
		case PhysicalType::INT64:
			decimal_decimal_cast_switch<int64_t, NumericHelper>(source, result, count);
			break;
		case PhysicalType::INT128:
			decimal_decimal_cast_switch<hugeint_t, Hugeint>(source, result, count);
			break;
		default:
			throw NotImplementedException("Unimplemented internal type for decimal in decimal_decimal cast");
		}
		break;
	}
	case LogicalTypeId::FLOAT:
		from_decimal_cast<float>(source, result, count);
		break;
	case LogicalTypeId::DOUBLE:
		from_decimal_cast<double>(source, result, count);
		break;
	case LogicalTypeId::VARCHAR: {
		switch (source.type.InternalType()) {
		case PhysicalType::INT16:
			UnaryExecutor::Execute<int16_t, string_t, true>(source, result, count, [&](int16_t input) {
				return StringCastFromDecimal::Operation<int16_t>(input, source.type.width(), source.type.scale(),
				                                                 result);
			});
			break;
		case PhysicalType::INT32:
			UnaryExecutor::Execute<int32_t, string_t, true>(source, result, count, [&](int32_t input) {
				return StringCastFromDecimal::Operation<int32_t>(input, source.type.width(), source.type.scale(),
				                                                 result);
			});
			break;
		case PhysicalType::INT64:
			UnaryExecutor::Execute<int64_t, string_t, true>(source, result, count, [&](int64_t input) {
				return StringCastFromDecimal::Operation<int64_t>(input, source.type.width(), source.type.scale(),
				                                                 result);
			});
			break;
		case PhysicalType::INT128:
			UnaryExecutor::Execute<hugeint_t, string_t, true>(source, result, count, [&](hugeint_t input) {
				return StringCastFromDecimal::Operation<hugeint_t>(input, source.type.width(), source.type.scale(),
				                                                   result);
			});
			break;
		default:
			throw NotImplementedException("Unimplemented internal decimal type");
		}
		break;
	}
	default:
		null_cast(source, result, count);
		break;
	}
}

template <class SRC> static void numeric_cast_switch(Vector &source, Vector &result, idx_t count) {
	// now switch on the result type
	switch (result.type.id()) {
	case LogicalTypeId::BOOLEAN:
		UnaryExecutor::Execute<SRC, bool, duckdb::Cast, true>(source, result, count);
		break;
	case LogicalTypeId::TINYINT:
		UnaryExecutor::Execute<SRC, int8_t, duckdb::Cast, true>(source, result, count);
		break;
	case LogicalTypeId::SMALLINT:
		UnaryExecutor::Execute<SRC, int16_t, duckdb::Cast, true>(source, result, count);
		break;
	case LogicalTypeId::INTEGER:
		UnaryExecutor::Execute<SRC, int32_t, duckdb::Cast, true>(source, result, count);
		break;
	case LogicalTypeId::BIGINT:
		UnaryExecutor::Execute<SRC, int64_t, duckdb::Cast, true>(source, result, count);
		break;
	case LogicalTypeId::HUGEINT:
		UnaryExecutor::Execute<SRC, hugeint_t, duckdb::Cast, true>(source, result, count);
		break;
	case LogicalTypeId::FLOAT:
		UnaryExecutor::Execute<SRC, float, duckdb::Cast, true>(source, result, count);
		break;
	case LogicalTypeId::DOUBLE:
		UnaryExecutor::Execute<SRC, double, duckdb::Cast, true>(source, result, count);
		break;
	case LogicalTypeId::DECIMAL:
		to_decimal_cast<SRC>(source, result, count);
		break;
	case LogicalTypeId::VARCHAR: {
		string_cast<SRC, duckdb::StringCast>(source, result, count);
		break;
	}
	case LogicalTypeId::LIST: {
		auto list_child = make_unique<ChunkCollection>();
		ListVector::SetEntry(result, move(list_child));
		null_cast(source, result, count);
		break;
	}
	default:
		null_cast(source, result, count);
		break;
	}
}

template <class OP> static void string_cast_numeric_switch(Vector &source, Vector &result, idx_t count) {
	// now switch on the result type
	switch (result.type.id()) {
	case LogicalTypeId::BOOLEAN:
		UnaryExecutor::Execute<string_t, bool, OP, true>(source, result, count);
		break;
	case LogicalTypeId::TINYINT:
		UnaryExecutor::Execute<string_t, int8_t, OP, true>(source, result, count);
		break;
	case LogicalTypeId::SMALLINT:
		UnaryExecutor::Execute<string_t, int16_t, OP, true>(source, result, count);
		break;
	case LogicalTypeId::INTEGER:
		UnaryExecutor::Execute<string_t, int32_t, OP, true>(source, result, count);
		break;
	case LogicalTypeId::BIGINT:
		UnaryExecutor::Execute<string_t, int64_t, OP, true>(source, result, count);
		break;
	case LogicalTypeId::HUGEINT:
		UnaryExecutor::Execute<string_t, hugeint_t, OP, true>(source, result, count);
		break;
	case LogicalTypeId::FLOAT:
		UnaryExecutor::Execute<string_t, float, OP, true>(source, result, count);
		break;
	case LogicalTypeId::DOUBLE:
		UnaryExecutor::Execute<string_t, double, OP, true>(source, result, count);
		break;
	case LogicalTypeId::INTERVAL:
		UnaryExecutor::Execute<string_t, interval_t, OP, true>(source, result, count);
		break;
	case LogicalTypeId::DECIMAL:
		to_decimal_cast<string_t>(source, result, count);
		break;
	default:
		null_cast(source, result, count);
		break;
	}
}

static void string_cast_switch(Vector &source, Vector &result, idx_t count, bool strict = false) {
	// now switch on the result type
	switch (result.type.id()) {
	case LogicalTypeId::DATE:
		if (strict) {
			UnaryExecutor::Execute<string_t, date_t, duckdb::StrictCastToDate, true>(source, result, count);
		} else {
			UnaryExecutor::Execute<string_t, date_t, duckdb::CastToDate, true>(source, result, count);
		}
		break;
	case LogicalTypeId::TIME:
		if (strict) {
			UnaryExecutor::Execute<string_t, dtime_t, duckdb::StrictCastToTime, true>(source, result, count);
		} else {
			UnaryExecutor::Execute<string_t, dtime_t, duckdb::CastToTime, true>(source, result, count);
		}
		break;
	case LogicalTypeId::TIMESTAMP:
		UnaryExecutor::Execute<string_t, timestamp_t, duckdb::CastToTimestamp, true>(source, result, count);
		break;
	case LogicalTypeId::BLOB:
		string_cast<string_t, duckdb::CastToBlob>(source, result, count);
		break;
	default:
		if (strict) {
			string_cast_numeric_switch<duckdb::StrictCast>(source, result, count);
		} else {
			string_cast_numeric_switch<duckdb::Cast>(source, result, count);
		}
		break;
	}
}

static void date_cast_switch(Vector &source, Vector &result, idx_t count) {
	// now switch on the result type
	switch (result.type.id()) {
	case LogicalTypeId::VARCHAR:
		// date to varchar
		string_cast<date_t, duckdb::CastFromDate>(source, result, count);
		break;
	case LogicalTypeId::TIMESTAMP:
		// date to timestamp
		UnaryExecutor::Execute<date_t, timestamp_t, duckdb::CastDateToTimestamp, true>(source, result, count);
		break;
	default:
		null_cast(source, result, count);
		break;
	}
}

static void time_cast_switch(Vector &source, Vector &result, idx_t count) {
	// now switch on the result type
	switch (result.type.id()) {
	case LogicalTypeId::VARCHAR:
		// time to varchar
		string_cast<dtime_t, duckdb::CastFromTime>(source, result, count);
		break;
	default:
		null_cast(source, result, count);
		break;
	}
}

static void timestamp_cast_switch(Vector &source, Vector &result, idx_t count) {
	// now switch on the result type
	switch (result.type.id()) {
	case LogicalTypeId::VARCHAR:
		// timestamp to varchar
		string_cast<timestamp_t, duckdb::CastFromTimestamp>(source, result, count);
		break;
	case LogicalTypeId::DATE:
		// timestamp to date
		UnaryExecutor::Execute<timestamp_t, date_t, duckdb::CastTimestampToDate, true>(source, result, count);
		break;
	case LogicalTypeId::TIME:
		// timestamp to time
		UnaryExecutor::Execute<timestamp_t, dtime_t, duckdb::CastTimestampToTime, true>(source, result, count);
		break;
	default:
		null_cast(source, result, count);
		break;
	}
}

static void interval_cast_switch(Vector &source, Vector &result, idx_t count) {
	// now switch on the result type
	switch (result.type.id()) {
	case LogicalTypeId::VARCHAR:
		// time to varchar
		string_cast<interval_t, duckdb::StringCast>(source, result, count);
		break;
	default:
		null_cast(source, result, count);
		break;
	}
}

static void blob_cast_switch(Vector &source, Vector &result, idx_t count) {
	// now switch on the result type
	switch (result.type.id()) {
	case LogicalTypeId::VARCHAR:
		// blob to varchar
		string_cast<string_t, duckdb::CastFromBlob>(source, result, count);
		break;
	default:
		null_cast(source, result, count);
		break;
	}
}

static void value_string_cast_switch(Vector &source, Vector &result, idx_t count) {
	switch(result.type.id()) {
	case LogicalTypeId::VARCHAR:
		if (source.vector_type == VectorType::CONSTANT_VECTOR) {
			result.vector_type = source.vector_type;
		} else {
			result.vector_type = VectorType::FLAT_VECTOR;
		}
		for(idx_t i = 0; i < count; i++) {
			auto src_val = source.GetValue(i);
			auto str_val = src_val.ToString();
			result.SetValue(i, Value(str_val));
			break;
		}
		break;
	default:
		null_cast(source, result, count);
		break;
	}
}

void VectorOperations::Cast(Vector &source, Vector &result, idx_t count, bool strict) {
	assert(source.type != result.type);
	// first switch on source type
	switch (source.type.id()) {
	case LogicalTypeId::BOOLEAN:
		numeric_cast_switch<bool>(source, result, count);
		break;
	case LogicalTypeId::TINYINT:
		numeric_cast_switch<int8_t>(source, result, count);
		break;
	case LogicalTypeId::SMALLINT:
		numeric_cast_switch<int16_t>(source, result, count);
		break;
	case LogicalTypeId::INTEGER:
		numeric_cast_switch<int32_t>(source, result, count);
		break;
	case LogicalTypeId::BIGINT:
		numeric_cast_switch<int64_t>(source, result, count);
		break;
	case LogicalTypeId::HUGEINT:
		numeric_cast_switch<hugeint_t>(source, result, count);
		break;
	case LogicalTypeId::DECIMAL:
		decimal_cast_switch(source, result, count);
		break;
	case LogicalTypeId::FLOAT:
		numeric_cast_switch<float>(source, result, count);
		break;
	case LogicalTypeId::DOUBLE:
		numeric_cast_switch<double>(source, result, count);
		break;
	case LogicalTypeId::DATE:
		date_cast_switch(source, result, count);
		break;
	case LogicalTypeId::TIME:
		time_cast_switch(source, result, count);
		break;
	case LogicalTypeId::TIMESTAMP:
		timestamp_cast_switch(source, result, count);
		break;
	case LogicalTypeId::INTERVAL:
		interval_cast_switch(source, result, count);
		break;
	case LogicalTypeId::VARCHAR:
		string_cast_switch(source, result, count, strict);
		break;
	case LogicalTypeId::BLOB:
		blob_cast_switch(source, result, count);
		break;
	case LogicalTypeId::SQLNULL: {
		// cast a NULL to another type, just copy the properties and change the type
		result.vector_type = VectorType::CONSTANT_VECTOR;
		ConstantVector::SetNull(result, true);
		break;
	}
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::LIST:
		value_string_cast_switch(source, result, count);
		break;
	default:
		throw UnimplementedCast(source.type, result.type);
	}
}

} // namespace duckdb

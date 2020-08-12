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

using namespace std;

namespace duckdb {

template <class SRC, class OP> static void string_cast(Vector &source, Vector &result, idx_t count) {
	assert(result.type == TypeId::VARCHAR);
	UnaryExecutor::Execute<SRC, string_t, true>(source, result, count,
	                                            [&](SRC input) { return OP::template Operation<SRC>(input, result); });
}

static NotImplementedException UnimplementedCast(SQLType source_type, SQLType target_type) {
	return NotImplementedException("Unimplemented type for cast (%s -> %s)", SQLTypeToString(source_type).c_str(),
	                               SQLTypeToString(target_type).c_str());
}

// NULL cast only works if all values in source are NULL, otherwise an unimplemented cast exception is thrown
static void null_cast(Vector &source, Vector &result, SQLType source_type, SQLType target_type, idx_t count) {
	if (VectorOperations::HasNotNull(source, count)) {
		throw UnimplementedCast(source_type, target_type);
	}
	if (source.vector_type == VectorType::CONSTANT_VECTOR) {
		result.vector_type = VectorType::CONSTANT_VECTOR;
		ConstantVector::SetNull(result, true);
	} else {
		result.vector_type = VectorType::FLAT_VECTOR;
		FlatVector::Nullmask(result).set();
	}
}

template <class SRC>
static void numeric_cast_switch(Vector &source, Vector &result, SQLType source_type, SQLType target_type, idx_t count) {
	// now switch on the result type
	switch (target_type.id) {
	case SQLTypeId::BOOLEAN:
		assert(result.type == TypeId::BOOL);
		UnaryExecutor::Execute<SRC, bool, duckdb::Cast, true>(source, result, count);
		break;
	case SQLTypeId::TINYINT:
		assert(result.type == TypeId::INT8);
		UnaryExecutor::Execute<SRC, int8_t, duckdb::Cast, true>(source, result, count);
		break;
	case SQLTypeId::SMALLINT:
		assert(result.type == TypeId::INT16);
		UnaryExecutor::Execute<SRC, int16_t, duckdb::Cast, true>(source, result, count);
		break;
	case SQLTypeId::INTEGER:
		assert(result.type == TypeId::INT32);
		UnaryExecutor::Execute<SRC, int32_t, duckdb::Cast, true>(source, result, count);
		break;
	case SQLTypeId::BIGINT:
		assert(result.type == TypeId::INT64);
		UnaryExecutor::Execute<SRC, int64_t, duckdb::Cast, true>(source, result, count);
		break;
	case SQLTypeId::HUGEINT:
		assert(result.type == TypeId::INT128);
		UnaryExecutor::Execute<SRC, hugeint_t, duckdb::Cast, true>(source, result, count);
		break;
	case SQLTypeId::FLOAT:
		assert(result.type == TypeId::FLOAT);
		UnaryExecutor::Execute<SRC, float, duckdb::Cast, true>(source, result, count);
		break;
	case SQLTypeId::DOUBLE:
		assert(result.type == TypeId::DOUBLE);
		UnaryExecutor::Execute<SRC, double, duckdb::Cast, true>(source, result, count);
		break;
	case SQLTypeId::VARCHAR: {
		string_cast<SRC, duckdb::StringCast>(source, result, count);
		break;
	}
	case SQLTypeId::LIST: {
		assert(result.type == TypeId::LIST);
		auto list_child = make_unique<ChunkCollection>();
		ListVector::SetEntry(result, move(list_child));
		null_cast(source, result, source_type, target_type, count);
		break;
	}
	default:
		null_cast(source, result, source_type, target_type, count);
		break;
	}
}

template<class T>
static void to_decimal_cast(Vector &source, Vector &result, SQLType decimal_type, idx_t count) {
	TypeId internal_type = GetInternalType(decimal_type);
	switch(internal_type) {
	case TypeId::INT16:
		UnaryExecutor::Execute<T, int16_t, true>(source, result, count, [&](T input) {
			return CastToDecimal::Operation<T, int16_t>(input, decimal_type.width, decimal_type.scale);
		});
		break;
	case TypeId::INT32:
		UnaryExecutor::Execute<T, int32_t, true>(source, result, count, [&](T input) {
			return CastToDecimal::Operation<T, int32_t>(input, decimal_type.width, decimal_type.scale);
		});
		break;
	case TypeId::INT64:
		UnaryExecutor::Execute<T, int64_t, true>(source, result, count, [&](T input) {
			return CastToDecimal::Operation<T, int64_t>(input, decimal_type.width, decimal_type.scale);
		});
		break;
	case TypeId::INT128:
		UnaryExecutor::Execute<T, hugeint_t, true>(source, result, count, [&](T input) {
			return CastToDecimal::Operation<T, hugeint_t>(input, decimal_type.width, decimal_type.scale);
		});
		break;
	default:
		throw NotImplementedException("Unimplemented internal type for decimal");
	}
}

template<class T>
static void from_decimal_cast(Vector &source, Vector &result, SQLType decimal_type, idx_t count) {
	TypeId internal_type = GetInternalType(decimal_type);
	switch(internal_type) {
	case TypeId::INT16:
		UnaryExecutor::Execute<int16_t, T, true>(source, result, count, [&](int16_t input) {
			return CastFromDecimal::Operation<int16_t, T>(input, decimal_type.width, decimal_type.scale);
		});
		break;
	case TypeId::INT32:
		UnaryExecutor::Execute<int32_t, T, true>(source, result, count, [&](int32_t input) {
			return CastFromDecimal::Operation<int32_t, T>(input, decimal_type.width, decimal_type.scale);
		});
		break;
	case TypeId::INT64:
		UnaryExecutor::Execute<int64_t, T, true>(source, result, count, [&](int64_t input) {
			return CastFromDecimal::Operation<int64_t, T>(input, decimal_type.width, decimal_type.scale);
		});
		break;
	case TypeId::INT128:
		UnaryExecutor::Execute<hugeint_t, T, true>(source, result, count, [&](hugeint_t input) {
			return CastFromDecimal::Operation<hugeint_t, T>(input, decimal_type.width, decimal_type.scale);
		});
		break;
	default:
		throw NotImplementedException("Unimplemented internal type for decimal");
	}
}

template<class T>
static void decimal_decimal_cast_switch(Vector &source, Vector &result, SQLType source_type, SQLType target_type, idx_t count) {
	TypeId internal_target_type = GetInternalType(target_type);

	// we need to either multiply or divide by the difference in scales
	if (target_type.scale > source_type.scale) {
		// multiply
		int64_t multiply_factor = NumericHelper::PowersOfTen[target_type.scale - source_type.scale];
		switch(internal_target_type) {
		case TypeId::INT16:
			UnaryExecutor::Execute<T, int16_t>(source, result, count, [&](T input) {
				return Cast::Operation<T, int16_t>(input * multiply_factor);
			});
			break;
		case TypeId::INT32:
			UnaryExecutor::Execute<T, int32_t>(source, result, count, [&](T input) {
				return Cast::Operation<T, int32_t>(input * multiply_factor);
			});
			break;
		case TypeId::INT64:
			UnaryExecutor::Execute<T, int64_t>(source, result, count, [&](T input) {
				return Cast::Operation<T, int64_t>(input * multiply_factor);
			});
			break;
		case TypeId::INT128:
			UnaryExecutor::Execute<T, hugeint_t>(source, result, count, [&](T input) {
				return Cast::Operation<T, hugeint_t>(input * multiply_factor);
			});
			break;
		default:
			throw NotImplementedException("Unimplemented internal type for decimal");
		}
	} else if (target_type.scale < source_type.scale) {
		// divide
		int64_t divide_factor = NumericHelper::PowersOfTen[source_type.scale - target_type.scale];
		switch(internal_target_type) {
		case TypeId::INT16:
			UnaryExecutor::Execute<T, int16_t>(source, result, count, [&](T input) {
				return Cast::Operation<T, int16_t>(input / divide_factor);
			});
			break;
		case TypeId::INT32:
			UnaryExecutor::Execute<T, int32_t>(source, result, count, [&](T input) {
				return Cast::Operation<T, int32_t>(input / divide_factor);
			});
			break;
		case TypeId::INT64:
			UnaryExecutor::Execute<T, int64_t>(source, result, count, [&](T input) {
				return Cast::Operation<T, int64_t>(input / divide_factor);
			});
			break;
		case TypeId::INT128:
			UnaryExecutor::Execute<T, hugeint_t>(source, result, count, [&](T input) {
				return Cast::Operation<T, hugeint_t>(input / divide_factor);
			});
			break;
		default:
			throw NotImplementedException("Unimplemented internal type for decimal");
		}
	} else {
		// scale is the same: only need to cast
		VectorOperations::Cast(source, result, SQLTypeFromInternalType(GetInternalType(source_type)), SQLTypeFromInternalType(internal_target_type), count);
	}
}

static void decimal_cast_switch(Vector &source, Vector &result, SQLType source_type, SQLType target_type, idx_t count) {
	// now switch on the result type
	switch (target_type.id) {
	case SQLTypeId::BOOLEAN:
		assert(result.type == TypeId::BOOL);
		from_decimal_cast<bool>(source, result, source_type, count);
		break;
	case SQLTypeId::TINYINT:
		assert(result.type == TypeId::INT8);
		from_decimal_cast<int8_t>(source, result, source_type, count);
		break;
	case SQLTypeId::SMALLINT:
		assert(result.type == TypeId::INT16);
		from_decimal_cast<int16_t>(source, result, source_type, count);
		break;
	case SQLTypeId::INTEGER:
		assert(result.type == TypeId::INT32);
		from_decimal_cast<int32_t>(source, result, source_type, count);
		break;
	case SQLTypeId::BIGINT:
		assert(result.type == TypeId::INT64);
		from_decimal_cast<int64_t>(source, result, source_type, count);
		break;
	case SQLTypeId::HUGEINT:
		assert(result.type == TypeId::INT128);
		from_decimal_cast<hugeint_t>(source, result, source_type, count);
		break;
	case SQLTypeId::DECIMAL: {
		// decimal to decimal cast
		// first we need to figure out the source and target internal types
		TypeId internal_source_type = GetInternalType(source_type);
		switch(internal_source_type) {
		case TypeId::INT16:
			decimal_decimal_cast_switch<int16_t>(source, result, source_type, target_type, count);
			break;
		case TypeId::INT32:
			decimal_decimal_cast_switch<int32_t>(source, result, source_type, target_type, count);
			break;
		case TypeId::INT64:
			decimal_decimal_cast_switch<int64_t>(source, result, source_type, target_type, count);
			break;
		case TypeId::INT128:
			decimal_decimal_cast_switch<hugeint_t>(source, result, source_type, target_type, count);
			break;
		default:
			throw NotImplementedException("Unimplemented internal type for decimal in decimal_decimal cast");
		}
		break;
	}
	case SQLTypeId::FLOAT:
		assert(result.type == TypeId::FLOAT);
		from_decimal_cast<float>(source, result, source_type, count);
		break;
	case SQLTypeId::DOUBLE:
		assert(result.type == TypeId::DOUBLE);
		from_decimal_cast<double>(source, result, source_type, count);
		break;
	case SQLTypeId::VARCHAR: {
		TypeId internal_source_type = GetInternalType(source_type);
		switch(internal_source_type) {
		case TypeId::INT16:
			UnaryExecutor::Execute<int16_t, string_t, true>(source, result, count, [&](int16_t input) {
				return StringCastFromDecimal::Operation<int16_t>(input, source_type.width, source_type.scale, result);
			});
			break;
		case TypeId::INT32:
			UnaryExecutor::Execute<int32_t, string_t, true>(source, result, count, [&](int32_t input) {
				return StringCastFromDecimal::Operation<int32_t>(input, source_type.width, source_type.scale, result);
			});
			break;
		case TypeId::INT64:
			UnaryExecutor::Execute<int64_t, string_t, true>(source, result, count, [&](int64_t input) {
				return StringCastFromDecimal::Operation<int64_t>(input, source_type.width, source_type.scale, result);
			});
			break;
		case TypeId::INT128:
			UnaryExecutor::Execute<hugeint_t, string_t, true>(source, result, count, [&](hugeint_t input) {
				return StringCastFromDecimal::Operation<hugeint_t>(input, source_type.width, source_type.scale, result);
			});
			break;
		default:
			throw NotImplementedException("Unimplemented internal decimal type");
		}
		break;
	}
	default:
		null_cast(source, result, source_type, target_type, count);
		break;
	}
}

template <class OP>
static void string_cast_numeric_switch(Vector &source, Vector &result, SQLType source_type, SQLType target_type,
                                      idx_t count) {
	// now switch on the result type
	switch (target_type.id) {
	case SQLTypeId::BOOLEAN:
		assert(result.type == TypeId::BOOL);
		UnaryExecutor::Execute<string_t, bool, OP, true>(source, result, count);
		break;
	case SQLTypeId::TINYINT:
		assert(result.type == TypeId::INT8);
		UnaryExecutor::Execute<string_t, int8_t, OP, true>(source, result, count);
		break;
	case SQLTypeId::SMALLINT:
		assert(result.type == TypeId::INT16);
		UnaryExecutor::Execute<string_t, int16_t, OP, true>(source, result, count);
		break;
	case SQLTypeId::INTEGER:
		assert(result.type == TypeId::INT32);
		UnaryExecutor::Execute<string_t, int32_t, OP, true>(source, result, count);
		break;
	case SQLTypeId::BIGINT:
		assert(result.type == TypeId::INT64);
		UnaryExecutor::Execute<string_t, int64_t, OP, true>(source, result, count);
		break;
	case SQLTypeId::HUGEINT:
		assert(result.type == TypeId::INT128);
		UnaryExecutor::Execute<string_t, hugeint_t, OP, true>(source, result, count);
		break;
	case SQLTypeId::FLOAT:
		assert(result.type == TypeId::FLOAT);
		UnaryExecutor::Execute<string_t, float, OP, true>(source, result, count);
		break;
	case SQLTypeId::DOUBLE:
		assert(result.type == TypeId::DOUBLE);
		UnaryExecutor::Execute<string_t, double, OP, true>(source, result, count);
		break;
	case SQLTypeId::INTERVAL:
		assert(result.type == TypeId::INTERVAL);
		UnaryExecutor::Execute<string_t, interval_t, OP, true>(source, result, count);
		break;
	case SQLTypeId::DECIMAL:
		to_decimal_cast<string_t>(source, result, target_type, count);
		break;
	default:
		null_cast(source, result, source_type, target_type, count);
		break;
	}
}

static void string_cast_switch(Vector &source, Vector &result, SQLType source_type, SQLType target_type, idx_t count,
                               bool strict = false) {
	// now switch on the result type
	switch (target_type.id) {
	case SQLTypeId::DATE:
		assert(result.type == TypeId::INT32);
		if (strict) {
			UnaryExecutor::Execute<string_t, date_t, duckdb::StrictCastToDate, true>(source, result, count);
		} else {
			UnaryExecutor::Execute<string_t, date_t, duckdb::CastToDate, true>(source, result, count);
		}
		break;
	case SQLTypeId::TIME:
		assert(result.type == TypeId::INT32);
		if (strict) {
			UnaryExecutor::Execute<string_t, dtime_t, duckdb::StrictCastToTime, true>(source, result, count);
		} else {
			UnaryExecutor::Execute<string_t, dtime_t, duckdb::CastToTime, true>(source, result, count);
		}
		break;
	case SQLTypeId::TIMESTAMP:
		assert(result.type == TypeId::INT64);
		UnaryExecutor::Execute<string_t, timestamp_t, duckdb::CastToTimestamp, true>(source, result, count);
		break;
	case SQLTypeId::BLOB:
		assert(result.type == TypeId::VARCHAR);
		string_cast<string_t, duckdb::CastToBlob>(source, result, count);
		break;
	default:
		if (strict) {
			string_cast_numeric_switch<duckdb::StrictCast>(source, result, source_type, target_type, count);
		} else {
			string_cast_numeric_switch<duckdb::Cast>(source, result, source_type, target_type, count);
		}
		break;
	}
}

static void date_cast_switch(Vector &source, Vector &result, SQLType source_type, SQLType target_type, idx_t count) {
	// now switch on the result type
	switch (target_type.id) {
	case SQLTypeId::VARCHAR:
		// date to varchar
		string_cast<date_t, duckdb::CastFromDate>(source, result, count);
		break;
	case SQLTypeId::TIMESTAMP:
		// date to timestamp
		UnaryExecutor::Execute<date_t, timestamp_t, duckdb::CastDateToTimestamp, true>(source, result, count);
		break;
	default:
		null_cast(source, result, source_type, target_type, count);
		break;
	}
}

static void time_cast_switch(Vector &source, Vector &result, SQLType source_type, SQLType target_type, idx_t count) {
	// now switch on the result type
	switch (target_type.id) {
	case SQLTypeId::VARCHAR:
		// time to varchar
		string_cast<dtime_t, duckdb::CastFromTime>(source, result, count);
		break;
	default:
		null_cast(source, result, source_type, target_type, count);
		break;
	}
}

static void timestamp_cast_switch(Vector &source, Vector &result, SQLType source_type, SQLType target_type,
                                  idx_t count) {
	// now switch on the result type
	switch (target_type.id) {
	case SQLTypeId::VARCHAR:
		// timestamp to varchar
		string_cast<timestamp_t, duckdb::CastFromTimestamp>(source, result, count);
		break;
	case SQLTypeId::DATE:
		// timestamp to date
		UnaryExecutor::Execute<timestamp_t, date_t, duckdb::CastTimestampToDate, true>(source, result, count);
		break;
	case SQLTypeId::TIME:
		// timestamp to time
		UnaryExecutor::Execute<timestamp_t, dtime_t, duckdb::CastTimestampToTime, true>(source, result, count);
		break;
	default:
		null_cast(source, result, source_type, target_type, count);
		break;
	}
}

static void interval_cast_switch(Vector &source, Vector &result, SQLType source_type, SQLType target_type, idx_t count) {
	// now switch on the result type
	switch (target_type.id) {
	case SQLTypeId::VARCHAR:
		// time to varchar
		string_cast<interval_t, duckdb::StringCast>(source, result, count);
		break;
	default:
		null_cast(source, result, source_type, target_type, count);
		break;
	}
}

static void blob_cast_switch(Vector &source, Vector &result, SQLType source_type, SQLType target_type,
                                  idx_t count) {
	// now switch on the result type
	switch (target_type.id) {
	case SQLTypeId::VARCHAR:
		// blob to varchar
		string_cast<string_t, duckdb::CastFromBlob>(source, result, count);
		break;
	default:
		null_cast(source, result, source_type, target_type, count);
		break;
	}
}

void VectorOperations::Cast(Vector &source, Vector &result, SQLType source_type, SQLType target_type, idx_t count,
                            bool strict) {
	assert(source_type != target_type);
	// first switch on source type
	switch (source_type.id) {
	case SQLTypeId::BOOLEAN:
		assert(source.type == TypeId::BOOL);
		numeric_cast_switch<bool>(source, result, source_type, target_type, count);
		break;
	case SQLTypeId::TINYINT:
		assert(source.type == TypeId::INT8);
		numeric_cast_switch<int8_t>(source, result, source_type, target_type, count);
		break;
	case SQLTypeId::SMALLINT:
		assert(source.type == TypeId::INT16);
		numeric_cast_switch<int16_t>(source, result, source_type, target_type, count);
		break;
	case SQLTypeId::INTEGER:
		assert(source.type == TypeId::INT32);
		numeric_cast_switch<int32_t>(source, result, source_type, target_type, count);
		break;
	case SQLTypeId::BIGINT:
		assert(source.type == TypeId::INT64);
		numeric_cast_switch<int64_t>(source, result, source_type, target_type, count);
		break;
	case SQLTypeId::HUGEINT:
		assert(source.type == TypeId::INT128);
		numeric_cast_switch<hugeint_t>(source, result, source_type, target_type, count);
		break;
	case SQLTypeId::DECIMAL:
		decimal_cast_switch(source, result, source_type, target_type, count);
		break;
	case SQLTypeId::FLOAT:
		assert(source.type == TypeId::FLOAT);
		numeric_cast_switch<float>(source, result, source_type, target_type, count);
		break;
	case SQLTypeId::DOUBLE:
		assert(source.type == TypeId::DOUBLE);
		numeric_cast_switch<double>(source, result, source_type, target_type, count);
		break;
	case SQLTypeId::DATE:
		assert(source.type == TypeId::INT32);
		date_cast_switch(source, result, source_type, target_type, count);
		break;
	case SQLTypeId::TIME:
		assert(source.type == TypeId::INT32);
		time_cast_switch(source, result, source_type, target_type, count);
		break;
	case SQLTypeId::TIMESTAMP:
		assert(source.type == TypeId::INT64);
		timestamp_cast_switch(source, result, source_type, target_type, count);
		break;
	case SQLTypeId::INTERVAL:
		assert(source.type == TypeId::INTERVAL);
		interval_cast_switch(source, result, source_type, target_type, count);
		break;
	case SQLTypeId::VARCHAR:
		assert(source.type == TypeId::VARCHAR);
		string_cast_switch(source, result, source_type, target_type, count, strict);
		break;
	case SQLTypeId::BLOB:
		assert(source.type == TypeId::VARCHAR);
		blob_cast_switch(source, result, source_type, target_type, count);
		break;
	case SQLTypeId::SQLNULL: {
		// cast a NULL to another type, just copy the properties and change the type
		result.vector_type = VectorType::CONSTANT_VECTOR;
		ConstantVector::SetNull(result, true);
		break;
	}
	default:
		throw UnimplementedCast(source_type, target_type);
	}
}

void VectorOperations::Cast(Vector &source, Vector &result, idx_t count, bool strict) {
	return VectorOperations::Cast(source, result, SQLTypeFromInternalType(source.type),
	                              SQLTypeFromInternalType(result.type), count, strict);
}

}

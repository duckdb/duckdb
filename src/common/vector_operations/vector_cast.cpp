#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/operator/string_cast.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/decimal_cast.hpp"
#include "duckdb/common/operator/numeric_cast.hpp"
#include "duckdb/common/likely.hpp"

namespace duckdb {

template <class OP>
struct VectorStringCastOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto result = (Vector *)dataptr;
		return OP::template Operation<INPUT_TYPE>(input, *result);
	}
};

struct VectorTryCastData {
	VectorTryCastData(Vector &result_p, string *error_message_p, bool strict_p)
	    : result(result_p), error_message(error_message_p), strict(strict_p) {
	}

	Vector &result;
	string *error_message;
	bool strict;
	bool all_converted = true;
};

template <class OP>
struct VectorTryCastOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		RESULT_TYPE output;
		if (DUCKDB_LIKELY(OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, output))) {
			return output;
		}
		auto data = (VectorTryCastData *)dataptr;
		return HandleVectorCastError::Operation<RESULT_TYPE>(CastExceptionText<INPUT_TYPE, RESULT_TYPE>(input), mask,
		                                                     idx, data->error_message, data->all_converted);
	}
};

template <class OP>
struct VectorTryCastStrictOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto data = (VectorTryCastData *)dataptr;
		RESULT_TYPE output;
		if (DUCKDB_LIKELY(OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, output, data->strict))) {
			return output;
		}
		return HandleVectorCastError::Operation<RESULT_TYPE>(CastExceptionText<INPUT_TYPE, RESULT_TYPE>(input), mask,
		                                                     idx, data->error_message, data->all_converted);
	}
};

template <class OP>
struct VectorTryCastErrorOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto data = (VectorTryCastData *)dataptr;
		RESULT_TYPE output;
		if (DUCKDB_LIKELY(
		        OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, output, data->error_message, data->strict))) {
			return output;
		}
		bool has_error = data->error_message && !data->error_message->empty();
		return HandleVectorCastError::Operation<RESULT_TYPE>(
		    has_error ? *data->error_message : CastExceptionText<INPUT_TYPE, RESULT_TYPE>(input), mask, idx,
		    data->error_message, data->all_converted);
	}
};

template <class OP>
struct VectorTryCastStringOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto data = (VectorTryCastData *)dataptr;
		RESULT_TYPE output;
		if (DUCKDB_LIKELY(OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, output, data->result,
		                                                                  data->error_message, data->strict))) {
			return output;
		}
		return HandleVectorCastError::Operation<RESULT_TYPE>(CastExceptionText<INPUT_TYPE, RESULT_TYPE>(input), mask,
		                                                     idx, data->error_message, data->all_converted);
	}
};

template <class SRC, class DST, class OP>
static bool TemplatedVectorTryCastLoop(Vector &source, Vector &result, idx_t count, bool strict,
                                       string *error_message) {
	VectorTryCastData input(result, error_message, strict);
	UnaryExecutor::GenericExecute<SRC, DST, OP>(source, result, count, &input, error_message);
	return input.all_converted;
}

template <class SRC, class DST, class OP>
static bool VectorTryCastLoop(Vector &source, Vector &result, idx_t count, string *error_message) {
	return TemplatedVectorTryCastLoop<SRC, DST, VectorTryCastOperator<OP>>(source, result, count, false, error_message);
}

template <class SRC, class DST, class OP>
static bool VectorTryCastStrictLoop(Vector &source, Vector &result, idx_t count, bool strict, string *error_message) {
	return TemplatedVectorTryCastLoop<SRC, DST, VectorTryCastStrictOperator<OP>>(source, result, count, strict,
	                                                                             error_message);
}

template <class SRC, class DST, class OP>
static bool VectorTryCastErrorLoop(Vector &source, Vector &result, idx_t count, bool strict, string *error_message) {
	return TemplatedVectorTryCastLoop<SRC, DST, VectorTryCastErrorOperator<OP>>(source, result, count, strict,
	                                                                            error_message);
}

template <class SRC, class DST, class OP>
static bool VectorTryCastStringLoop(Vector &source, Vector &result, idx_t count, bool strict, string *error_message) {
	return TemplatedVectorTryCastLoop<SRC, DST, VectorTryCastStringOperator<OP>>(source, result, count, strict,
	                                                                             error_message);
}

template <class SRC, class OP>
static void VectorStringCast(Vector &source, Vector &result, idx_t count) {
	D_ASSERT(result.GetType().InternalType() == PhysicalType::VARCHAR);
	UnaryExecutor::GenericExecute<SRC, string_t, VectorStringCastOperator<OP>>(source, result, count, (void *)&result);
}

template <class SRC>
static bool NumericCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::BOOLEAN:
		return VectorTryCastLoop<SRC, bool, duckdb::NumericTryCast>(source, result, count, error_message);
	case LogicalTypeId::TINYINT:
		return VectorTryCastLoop<SRC, int8_t, duckdb::NumericTryCast>(source, result, count, error_message);
	case LogicalTypeId::SMALLINT:
		return VectorTryCastLoop<SRC, int16_t, duckdb::NumericTryCast>(source, result, count, error_message);
	case LogicalTypeId::INTEGER:
		return VectorTryCastLoop<SRC, int32_t, duckdb::NumericTryCast>(source, result, count, error_message);
	case LogicalTypeId::BIGINT:
		return VectorTryCastLoop<SRC, int64_t, duckdb::NumericTryCast>(source, result, count, error_message);
	case LogicalTypeId::UTINYINT:
		return VectorTryCastLoop<SRC, uint8_t, duckdb::NumericTryCast>(source, result, count, error_message);
	case LogicalTypeId::USMALLINT:
		return VectorTryCastLoop<SRC, uint16_t, duckdb::NumericTryCast>(source, result, count, error_message);
	case LogicalTypeId::UINTEGER:
		return VectorTryCastLoop<SRC, uint32_t, duckdb::NumericTryCast>(source, result, count, error_message);
	case LogicalTypeId::UBIGINT:
		return VectorTryCastLoop<SRC, uint64_t, duckdb::NumericTryCast>(source, result, count, error_message);
	case LogicalTypeId::HUGEINT:
		return VectorTryCastLoop<SRC, hugeint_t, duckdb::NumericTryCast>(source, result, count, error_message);
	case LogicalTypeId::FLOAT:
		return VectorTryCastLoop<SRC, float, duckdb::NumericTryCast>(source, result, count, error_message);
	case LogicalTypeId::DOUBLE:
		return VectorTryCastLoop<SRC, double, duckdb::NumericTryCast>(source, result, count, error_message);
	case LogicalTypeId::DECIMAL:
		return ToDecimalCast<SRC>(source, result, count, error_message);
	case LogicalTypeId::VARCHAR: {
		VectorStringCast<SRC, duckdb::StringCast>(source, result, count);
		return true;
	}
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
}

static bool VectorStringCastNumericSwitch(Vector &source, Vector &result, idx_t count, bool strict,
                                          string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::BOOLEAN:
		return VectorTryCastStrictLoop<string_t, bool, duckdb::TryCast>(source, result, count, strict, error_message);
	case LogicalTypeId::TINYINT:
		return VectorTryCastStrictLoop<string_t, int8_t, duckdb::TryCast>(source, result, count, strict, error_message);
	case LogicalTypeId::SMALLINT:
		return VectorTryCastStrictLoop<string_t, int16_t, duckdb::TryCast>(source, result, count, strict,
		                                                                   error_message);
	case LogicalTypeId::INTEGER:
		return VectorTryCastStrictLoop<string_t, int32_t, duckdb::TryCast>(source, result, count, strict,
		                                                                   error_message);
	case LogicalTypeId::BIGINT:
		return VectorTryCastStrictLoop<string_t, int64_t, duckdb::TryCast>(source, result, count, strict,
		                                                                   error_message);
	case LogicalTypeId::UTINYINT:
		return VectorTryCastStrictLoop<string_t, uint8_t, duckdb::TryCast>(source, result, count, strict,
		                                                                   error_message);
	case LogicalTypeId::USMALLINT:
		return VectorTryCastStrictLoop<string_t, uint16_t, duckdb::TryCast>(source, result, count, strict,
		                                                                    error_message);
	case LogicalTypeId::UINTEGER:
		return VectorTryCastStrictLoop<string_t, uint32_t, duckdb::TryCast>(source, result, count, strict,
		                                                                    error_message);
	case LogicalTypeId::UBIGINT:
		return VectorTryCastStrictLoop<string_t, uint64_t, duckdb::TryCast>(source, result, count, strict,
		                                                                    error_message);
	case LogicalTypeId::HUGEINT:
		return VectorTryCastStrictLoop<string_t, hugeint_t, duckdb::TryCast>(source, result, count, strict,
		                                                                     error_message);
	case LogicalTypeId::FLOAT:
		return VectorTryCastStrictLoop<string_t, float, duckdb::TryCast>(source, result, count, strict, error_message);
	case LogicalTypeId::DOUBLE:
		return VectorTryCastStrictLoop<string_t, double, duckdb::TryCast>(source, result, count, strict, error_message);
	case LogicalTypeId::INTERVAL:
		return VectorTryCastErrorLoop<string_t, interval_t, duckdb::TryCastErrorMessage>(source, result, count, strict,
		                                                                                 error_message);
	case LogicalTypeId::DECIMAL:
		return ToDecimalCast<string_t>(source, result, count, error_message);
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
}

static bool StringCastSwitch(Vector &source, Vector &result, idx_t count, bool strict, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::DATE:
		return VectorTryCastErrorLoop<string_t, date_t, duckdb::TryCastErrorMessage>(source, result, count, strict,
		                                                                             error_message);
	case LogicalTypeId::TIME:
		return VectorTryCastErrorLoop<string_t, dtime_t, duckdb::TryCastErrorMessage>(source, result, count, strict,
		                                                                              error_message);
	case LogicalTypeId::TIMESTAMP:
		return VectorTryCastErrorLoop<string_t, timestamp_t, duckdb::TryCastErrorMessage>(source, result, count, strict,
		                                                                                  error_message);
	case LogicalTypeId::TIMESTAMP_NS:
		return VectorTryCastStrictLoop<string_t, timestamp_t, duckdb::TryCastToTimestampNS>(source, result, count,
		                                                                                    strict, error_message);
	case LogicalTypeId::TIMESTAMP_SEC:
		return VectorTryCastStrictLoop<string_t, timestamp_t, duckdb::TryCastToTimestampSec>(source, result, count,
		                                                                                     strict, error_message);
	case LogicalTypeId::TIMESTAMP_MS:
		return VectorTryCastStrictLoop<string_t, timestamp_t, duckdb::TryCastToTimestampMS>(source, result, count,
		                                                                                    strict, error_message);
	case LogicalTypeId::BLOB:
		return VectorTryCastStringLoop<string_t, string_t, duckdb::TryCastToBlob>(source, result, count, strict,
		                                                                          error_message);
	case LogicalTypeId::UUID:
		return VectorTryCastStringLoop<string_t, hugeint_t, duckdb::TryCastToUUID>(source, result, count, strict,
		                                                                           error_message);
	case LogicalTypeId::SQLNULL:
		return TryVectorNullCast(source, result, count, error_message);
	default:
		return VectorStringCastNumericSwitch(source, result, count, strict, error_message);
	}
}

static bool DateCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
		// date to varchar
		VectorStringCast<date_t, duckdb::StringCast>(source, result, count);
		return true;
	case LogicalTypeId::TIMESTAMP:
		// date to timestamp
		return VectorTryCastLoop<date_t, timestamp_t, duckdb::TryCast>(source, result, count, error_message);
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
}

static bool TimeCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
		// time to varchar
		VectorStringCast<dtime_t, duckdb::StringCast>(source, result, count);
		return true;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
}

static bool TimestampCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
		// timestamp to varchar
		VectorStringCast<timestamp_t, duckdb::StringCast>(source, result, count);
		break;
	case LogicalTypeId::DATE:
		// timestamp to date
		UnaryExecutor::Execute<timestamp_t, date_t, duckdb::Cast>(source, result, count);
		break;
	case LogicalTypeId::TIME:
		// timestamp to time
		UnaryExecutor::Execute<timestamp_t, dtime_t, duckdb::Cast>(source, result, count);
		break;
	case LogicalTypeId::TIMESTAMP_NS:
		// timestamp (us) to timestamp (ns)
		UnaryExecutor::Execute<timestamp_t, timestamp_t, duckdb::CastTimestampUsToNs>(source, result, count);
		break;
	case LogicalTypeId::TIMESTAMP_MS:
		// timestamp (us) to timestamp (ms)
		UnaryExecutor::Execute<timestamp_t, timestamp_t, duckdb::CastTimestampUsToMs>(source, result, count);
		break;
	case LogicalTypeId::TIMESTAMP_SEC:
		// timestamp (us) to timestamp (s)
		UnaryExecutor::Execute<timestamp_t, timestamp_t, duckdb::CastTimestampUsToSec>(source, result, count);
		break;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
	return true;
}

static bool TimestampNsCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
		// timestamp (ns) to varchar
		VectorStringCast<timestamp_t, duckdb::CastFromTimestampNS>(source, result, count);
		break;
	case LogicalTypeId::TIMESTAMP:
		// timestamp (ns) to timestamp (us)
		UnaryExecutor::Execute<timestamp_t, timestamp_t, duckdb::CastTimestampNsToUs>(source, result, count);
		break;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
	return true;
}

static bool TimestampMsCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
		// timestamp (ms) to varchar
		VectorStringCast<timestamp_t, duckdb::CastFromTimestampMS>(source, result, count);
		break;
	case LogicalTypeId::TIMESTAMP:
		// timestamp (ms) to timestamp (us)
		UnaryExecutor::Execute<timestamp_t, timestamp_t, duckdb::CastTimestampMsToUs>(source, result, count);
		break;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
	return true;
}

static bool TimestampSecCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
		// timestamp (sec) to varchar
		VectorStringCast<timestamp_t, duckdb::CastFromTimestampSec>(source, result, count);
		break;
	case LogicalTypeId::TIMESTAMP:
		// timestamp (s) to timestamp (us)
		UnaryExecutor::Execute<timestamp_t, timestamp_t, duckdb::CastTimestampSecToUs>(source, result, count);
		break;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
	return true;
}

static bool IntervalCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
		// time to varchar
		VectorStringCast<interval_t, duckdb::StringCast>(source, result, count);
		break;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
	return true;
}

static bool UUIDCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
		// uuid to varchar
		VectorStringCast<hugeint_t, duckdb::CastFromUUID>(source, result, count);
		break;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
	return true;
}

static bool BlobCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
		// blob to varchar
		VectorStringCast<string_t, duckdb::CastFromBlob>(source, result, count);
		break;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
	return true;
}

static bool ValueStringCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
		if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(source.GetVectorType());
		} else {
			result.SetVectorType(VectorType::FLAT_VECTOR);
		}
		for (idx_t i = 0; i < count; i++) {
			auto src_val = source.GetValue(i);
			auto str_val = src_val.ToString();
			result.SetValue(i, Value(str_val));
		}
		return true;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
}

static bool ListCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	switch (result.GetType().id()) {
	case LogicalTypeId::LIST: {
		// only handle constant and flat vectors here for now
		if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(source.GetVectorType());
			ConstantVector::SetNull(result, ConstantVector::IsNull(source));

			auto ldata = ConstantVector::GetData<list_entry_t>(source);
			auto tdata = ConstantVector::GetData<list_entry_t>(result);
			*tdata = *ldata;
		} else {
			source.Normalify(count);
			result.SetVectorType(VectorType::FLAT_VECTOR);
			FlatVector::SetValidity(result, FlatVector::Validity(source));

			auto ldata = FlatVector::GetData<list_entry_t>(source);
			auto tdata = FlatVector::GetData<list_entry_t>(result);
			for (idx_t i = 0; i < count; i++) {
				tdata[i] = ldata[i];
			}
		}
		auto &source_cc = ListVector::GetEntry(source);
		auto source_size = ListVector::GetListSize(source);

		ListVector::Reserve(result, source_size);
		auto &append_vector = ListVector::GetEntry(result);

		VectorOperations::Cast(source_cc, append_vector, source_size);
		ListVector::SetListSize(result, source_size);
		D_ASSERT(ListVector::GetListSize(result) == source_size);
		return true;
	}
	default:
		return ValueStringCastSwitch(source, result, count, error_message);
	}
}

static bool StructCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	switch (result.GetType().id()) {
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP: {
		auto &source_child_types = StructType::GetChildTypes(source.GetType());
		auto &result_child_types = StructType::GetChildTypes(result.GetType());
		if (source_child_types.size() != result_child_types.size()) {
			throw TypeMismatchException(source.GetType(), result.GetType(), "Cannot cast STRUCTs of different size");
		}
		auto &source_children = StructVector::GetEntries(source);
		D_ASSERT(source_children.size() == source_child_types.size());

		auto &result_children = StructVector::GetEntries(result);
		for (idx_t c_idx = 0; c_idx < result_child_types.size(); c_idx++) {
			auto &result_child_vector = result_children[c_idx];
			auto &source_child_vector = *source_children[c_idx];
			if (result_child_vector->GetType() != source_child_vector.GetType()) {
				VectorOperations::Cast(source_child_vector, *result_child_vector, count, false);
			} else {
				result_child_vector->Reference(source_child_vector);
			}
		}
		if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, ConstantVector::IsNull(source));
		} else {
			source.Normalify(count);
			FlatVector::Validity(result) = FlatVector::Validity(source);
		}
		return true;
	}
	case LogicalTypeId::VARCHAR:
		if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(source.GetVectorType());
		} else {
			result.SetVectorType(VectorType::FLAT_VECTOR);
		}
		for (idx_t i = 0; i < count; i++) {
			auto src_val = source.GetValue(i);
			auto str_val = src_val.ToString();
			result.SetValue(i, Value(str_val));
		}
		return true;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
}

bool VectorOperations::TryCast(Vector &source, Vector &result, idx_t count, string *error_message, bool strict) {
	D_ASSERT(source.GetType() != result.GetType());
	// first switch on source type
	switch (source.GetType().id()) {
	case LogicalTypeId::BOOLEAN:
		return NumericCastSwitch<bool>(source, result, count, error_message);
	case LogicalTypeId::TINYINT:
		return NumericCastSwitch<int8_t>(source, result, count, error_message);
	case LogicalTypeId::SMALLINT:
		return NumericCastSwitch<int16_t>(source, result, count, error_message);
	case LogicalTypeId::INTEGER:
		return NumericCastSwitch<int32_t>(source, result, count, error_message);
	case LogicalTypeId::BIGINT:
		return NumericCastSwitch<int64_t>(source, result, count, error_message);
	case LogicalTypeId::UTINYINT:
		return NumericCastSwitch<uint8_t>(source, result, count, error_message);
	case LogicalTypeId::USMALLINT:
		return NumericCastSwitch<uint16_t>(source, result, count, error_message);
	case LogicalTypeId::UINTEGER:
		return NumericCastSwitch<uint32_t>(source, result, count, error_message);
	case LogicalTypeId::UBIGINT:
		return NumericCastSwitch<uint64_t>(source, result, count, error_message);
	case LogicalTypeId::HUGEINT:
		return NumericCastSwitch<hugeint_t>(source, result, count, error_message);
	case LogicalTypeId::UUID:
		return UUIDCastSwitch(source, result, count, error_message);
	case LogicalTypeId::DECIMAL:
		return DecimalCastSwitch(source, result, count, error_message);
	case LogicalTypeId::FLOAT:
		return NumericCastSwitch<float>(source, result, count, error_message);
	case LogicalTypeId::DOUBLE:
		return NumericCastSwitch<double>(source, result, count, error_message);
	case LogicalTypeId::DATE:
		return DateCastSwitch(source, result, count, error_message);
	case LogicalTypeId::TIME:
		return TimeCastSwitch(source, result, count, error_message);
	case LogicalTypeId::TIMESTAMP:
		return TimestampCastSwitch(source, result, count, error_message);
	case LogicalTypeId::TIMESTAMP_NS:
		return TimestampNsCastSwitch(source, result, count, error_message);
	case LogicalTypeId::TIMESTAMP_MS:
		return TimestampMsCastSwitch(source, result, count, error_message);
	case LogicalTypeId::TIMESTAMP_SEC:
		return TimestampSecCastSwitch(source, result, count, error_message);
	case LogicalTypeId::INTERVAL:
		return IntervalCastSwitch(source, result, count, error_message);
	case LogicalTypeId::VARCHAR:
		return StringCastSwitch(source, result, count, strict, error_message);
	case LogicalTypeId::BLOB:
		return BlobCastSwitch(source, result, count, error_message);
	case LogicalTypeId::SQLNULL: {
		// cast a NULL to another type, just copy the properties and change the type
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
		return true;
	}
	case LogicalTypeId::MAP:
	case LogicalTypeId::STRUCT:
		return StructCastSwitch(source, result, count, error_message);
	case LogicalTypeId::LIST:
		return ListCastSwitch(source, result, count, error_message);
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
	return true;
}

void VectorOperations::Cast(Vector &source, Vector &result, idx_t count, bool strict) {
	VectorOperations::TryCast(source, result, count, nullptr, strict);
}

} // namespace duckdb

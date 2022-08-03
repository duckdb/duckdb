#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/likely.hpp"
#include "duckdb/common/limits.hpp"

namespace duckdb {

BindCastInfo::~BindCastInfo() {}

BoundCastData::~BoundCastData() {}

BoundCastInfo::BoundCastInfo(cast_function_t function_p, unique_ptr<BoundCastData> cast_data_p) :
    function(function_p), cast_data(move(cast_data_p)) {}

BoundCastInfo BoundCastInfo::Copy() {
	return BoundCastInfo(function, cast_data->Copy());
}

// NULL cast only works if all values in source are NULL, otherwise an unimplemented cast exception is thrown
bool DefaultCasts::TryVectorNullCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	bool success = true;
	if (VectorOperations::HasNotNull(source, count)) {
		HandleCastError::AssignError(UnimplementedCastMessage(source.GetType(), result.GetType()), parameters.error_message);
		success = false;
	}
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::SetNull(result, true);
	return success;
}

static cast_function_t VectorStringCastNumericSwitch(Vector &source, Vector &result, idx_t count, bool strict,
                                          string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::ENUM: {
		switch (result.GetType().InternalType()) {
		case PhysicalType::UINT8:
			return TransformEnum<uint8_t>(source, result, count, error_message);
		case PhysicalType::UINT16:
			return TransformEnum<uint16_t>(source, result, count, error_message);
		case PhysicalType::UINT32:
			return TransformEnum<uint32_t>(source, result, count, error_message);
		default:
			throw InternalException("ENUM can only have unsigned integers (except UINT64) as physical types");
		}
	}
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

static cast_function_t StringCastSwitch(Vector &source, Vector &result, idx_t count, bool strict, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::DATE:
		return VectorTryCastErrorLoop<string_t, date_t, duckdb::TryCastErrorMessage>(source, result, count, strict,
		                                                                             error_message);
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return VectorTryCastErrorLoop<string_t, dtime_t, duckdb::TryCastErrorMessage>(source, result, count, strict,
		                                                                              error_message);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
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
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		result.Reinterpret(source);
		return true;
	default:
		return VectorStringCastNumericSwitch(source, result, count, strict, error_message);
	}
}

static cast_function_t UUIDCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// uuid to varchar
		VectorStringCast<hugeint_t, duckdb::CastFromUUID>(source, result, count);
		break;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
}

static cast_function_t BlobCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// blob to varchar
		VectorStringCast<string_t, duckdb::CastFromBlob>(source, result, count);
		break;
	case LogicalTypeId::AGGREGATE_STATE:
		result.Reinterpret(source);
		break;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
}

static cast_function_t ValueStringCastSwitch(const LogicalType &source, const LogicalType &target) {
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(source.GetVectorType());
		} else {
			result.SetVectorType(VectorType::FLAT_VECTOR);
		}
		for (idx_t i = 0; i < count; i++) {
			auto src_val = source.GetValue(i);
			if (src_val.IsNull()) {
				result.SetValue(i, Value(result.GetType()));
			} else {
				auto str_val = src_val.ToString();
				result.SetValue(i, Value(str_val));
			}
		}
		return true;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
}

static cast_function_t ListCastSwitch(const LogicalType &source, const LogicalType &target) {
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
			source.Flatten(count);
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

template <class SRC_TYPE, class RES_TYPE>
bool FillEnum(Vector &source, Vector &result, idx_t count, string *error_message) {
	bool all_converted = true;
	result.SetVectorType(VectorType::FLAT_VECTOR);

	auto &str_vec = EnumType::GetValuesInsertOrder(source.GetType());
	auto str_vec_ptr = FlatVector::GetData<string_t>(str_vec);

	auto res_enum_type = result.GetType();

	UnifiedVectorFormat vdata;
	source.ToUnifiedFormat(count, vdata);

	auto source_data = (SRC_TYPE *)vdata.data;
	auto source_sel = vdata.sel;
	auto source_mask = vdata.validity;

	auto result_data = FlatVector::GetData<RES_TYPE>(result);
	auto &result_mask = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto src_idx = source_sel->get_index(i);
		if (!source_mask.RowIsValid(src_idx)) {
			result_mask.SetInvalid(i);
			continue;
		}
		auto str = str_vec_ptr[source_data[src_idx]].GetString();
		auto key = EnumType::GetPos(res_enum_type, str);
		if (key == -1) {
			// key doesn't exist on result enum
			if (!error_message) {
				result_data[i] = HandleVectorCastError::Operation<RES_TYPE>(
				    CastExceptionText<SRC_TYPE, RES_TYPE>(source_data[src_idx]), result_mask, i, error_message,
				    all_converted);
			} else {
				result_mask.SetInvalid(i);
			}
			continue;
		}
		result_data[i] = key;
	}
	return all_converted;
}

template <class SRC_TYPE>
cast_function_t FillEnumResultTemplate(Vector &source, Vector &result, idx_t count, string *error_message) {
	switch (source.GetType().InternalType()) {
	case PhysicalType::UINT8:
		return FillEnum<SRC_TYPE, uint8_t>(source, result, count, error_message);
	case PhysicalType::UINT16:
		return FillEnum<SRC_TYPE, uint16_t>(source, result, count, error_message);
	case PhysicalType::UINT32:
		return FillEnum<SRC_TYPE, uint32_t>(source, result, count, error_message);
	default:
		throw InternalException("ENUM can only have unsigned integers (except UINT64) as physical types");
	}
}

void EnumToVarchar(Vector &source, Vector &result, idx_t count, PhysicalType enum_physical_type) {
	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(source.GetVectorType());
	} else {
		result.SetVectorType(VectorType::FLAT_VECTOR);
	}
	auto &str_vec = EnumType::GetValuesInsertOrder(source.GetType());
	auto str_vec_ptr = FlatVector::GetData<string_t>(str_vec);
	auto res_vec_ptr = FlatVector::GetData<string_t>(result);

	// TODO remove value api from this loop
	for (idx_t i = 0; i < count; i++) {
		auto src_val = source.GetValue(i);
		if (src_val.IsNull()) {
			result.SetValue(i, Value());
			continue;
		}

		uint64_t enum_idx;
		switch (enum_physical_type) {
		case PhysicalType::UINT8:
			enum_idx = UTinyIntValue::Get(src_val);
			break;
		case PhysicalType::UINT16:
			enum_idx = USmallIntValue::Get(src_val);
			break;
		case PhysicalType::UINT32:
			enum_idx = UIntegerValue::Get(src_val);
			break;
		case PhysicalType::UINT64: //  DEDUP_POINTER_ENUM
		{
			res_vec_ptr[i] = (const char *)UBigIntValue::Get(src_val);
			continue;
		}

		default:
			throw InternalException("ENUM can only have unsigned integers as physical types");
		}
		res_vec_ptr[i] = str_vec_ptr[enum_idx];
	}
}

static cast_function_t EnumCastSwitch(const LogicalType &source, const LogicalType &target) {
	auto enum_physical_type = source.GetType().InternalType();
	switch (result.GetType().id()) {
	case LogicalTypeId::ENUM: {
		// This means they are both ENUMs, but of different types.
		switch (enum_physical_type) {
		case PhysicalType::UINT8:
			return FillEnumResultTemplate<uint8_t>(source, result, count, error_message);
		case PhysicalType::UINT16:
			return FillEnumResultTemplate<uint16_t>(source, result, count, error_message);
		case PhysicalType::UINT32:
			return FillEnumResultTemplate<uint32_t>(source, result, count, error_message);
		default:
			throw InternalException("ENUM can only have unsigned integers (except UINT64) as physical types");
		}
	}
	case LogicalTypeId::JSON:
	case LogicalTypeId::VARCHAR: {
		EnumToVarchar(source, result, count, enum_physical_type);
		break;
	}
	default: {
		// Cast to varchar
		Vector varchar_cast(LogicalType::VARCHAR, count);
		EnumToVarchar(source, varchar_cast, count, enum_physical_type);
		// Try to cast from varchar to whatever we wanted before
		VectorOperations::TryCast(varchar_cast, result, count, error_message, strict);
		break;
	}
	}
	return true;
}

static bool AggregateStateToBlobCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	if (result.GetType().id() != LogicalTypeId::BLOB) {
		throw TypeMismatchException(source.GetType(), result.GetType(),
		                            "Cannot cast AGGREGATE_STATE to anything but BLOB");
	}
	result.Reinterpret(source);
	return true;
}

static bool NullTypeCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	// cast a NULL to another type, just copy the properties and change the type
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::SetNull(result, true);
	return true;
}

BoundCastInfo DefaultCasts::GetDefaultCastFunction(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	D_ASSERT(source != target);
	// first switch on source type
	switch (source.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return NumericCastSwitch(input, source, target);
	case LogicalTypeId::UUID:
		return UUIDCastSwitch(source, result, count, error_message);
	case LogicalTypeId::DECIMAL:
		return DecimalCastSwitch(source, result, count, error_message);
	case LogicalTypeId::DATE:
		return DateCastSwitch(source, result, count, error_message);
	case LogicalTypeId::TIME:
		return TimeCastSwitch(source, result, count, error_message);
	case LogicalTypeId::TIME_TZ:
		return TimeTzCastSwitch(source, result, count, error_message);
	case LogicalTypeId::TIMESTAMP:
		return TimestampCastSwitch(source, result, count, error_message);
	case LogicalTypeId::TIMESTAMP_TZ:
		return TimestampTzCastSwitch(source, result, count, error_message);
	case LogicalTypeId::TIMESTAMP_NS:
		return TimestampNsCastSwitch(source, result, count, error_message);
	case LogicalTypeId::TIMESTAMP_MS:
		return TimestampMsCastSwitch(source, result, count, error_message);
	case LogicalTypeId::TIMESTAMP_SEC:
		return TimestampSecCastSwitch(source, result, count, error_message);
	case LogicalTypeId::INTERVAL:
		return IntervalCastSwitch(source, result, count, error_message);
	case LogicalTypeId::JSON:
	case LogicalTypeId::VARCHAR:
		return StringCastSwitch(source, result, count, strict, error_message);
	case LogicalTypeId::BLOB:
		return BlobCastSwitch(source, result, count, error_message);
	case LogicalTypeId::SQLNULL:
		return NullTypeCast;
	case LogicalTypeId::MAP:
	case LogicalTypeId::STRUCT:
		return StructCastSwitch(source, result, count, error_message);
	case LogicalTypeId::LIST:
		return ListCastSwitch(source, result, count, error_message);
	case LogicalTypeId::ENUM:
		return EnumCastSwitch(source, result, count, error_message, strict);
	case LogicalTypeId::AGGREGATE_STATE:
		return AggregateStateToBlobCast(source, result, count, error_message, strict);
	default:
		return nullptr;
	}
}

}

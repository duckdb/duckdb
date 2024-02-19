#include "duckdb/function/cast/default_casts.hpp"

#include "duckdb/common/likely.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/cast_helpers.hpp"

#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"

namespace duckdb {

BindCastInfo::~BindCastInfo() {
}

BoundCastData::~BoundCastData() {
}

BoundCastInfo::BoundCastInfo(cast_function_t function_p, unique_ptr<BoundCastData> cast_data_p,
                             init_cast_local_state_t init_local_state_p)
    : function(function_p), init_local_state(init_local_state_p), cast_data(std::move(cast_data_p)) {
}

BoundCastInfo BoundCastInfo::Copy() const {
	return BoundCastInfo(function, cast_data ? cast_data->Copy() : nullptr, init_local_state);
}

bool DefaultCasts::NopCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	result.Reference(source);
	return true;
}

void HandleCastError::AssignError(const string &error_message, CastParameters &parameters) {
	AssignError(error_message, parameters.error_message, parameters.query_location);
}

static string UnimplementedCastMessage(const LogicalType &source_type, const LogicalType &target_type) {
	return StringUtil::Format("Unimplemented type for cast (%s -> %s)", source_type.ToString(), target_type.ToString());
}

// NULL cast only works if all values in source are NULL, otherwise an unimplemented cast exception is thrown
bool DefaultCasts::TryVectorNullCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	bool success = true;
	if (VectorOperations::HasNotNull(source, count)) {
		HandleCastError::AssignError(UnimplementedCastMessage(source.GetType(), result.GetType()), parameters);
		success = false;
	}
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::SetNull(result, true);
	return success;
}

bool DefaultCasts::ReinterpretCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	result.Reinterpret(source);
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

BoundCastInfo DefaultCasts::GetDefaultCastFunction(BindCastInput &input, const LogicalType &source,
                                                   const LogicalType &target) {
	D_ASSERT(source != target);

	// first check if were casting to a union
	if (source.id() != LogicalTypeId::UNION && source.id() != LogicalTypeId::SQLNULL &&
	    target.id() == LogicalTypeId::UNION) {
		return ImplicitToUnionCast(input, source, target);
	}

	// else, switch on source type
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
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return NumericCastSwitch(input, source, target);
	case LogicalTypeId::POINTER:
		return PointerCastSwitch(input, source, target);
	case LogicalTypeId::UUID:
		return UUIDCastSwitch(input, source, target);
	case LogicalTypeId::DECIMAL:
		return DecimalCastSwitch(input, source, target);
	case LogicalTypeId::DATE:
		return DateCastSwitch(input, source, target);
	case LogicalTypeId::TIME:
		return TimeCastSwitch(input, source, target);
	case LogicalTypeId::TIME_TZ:
		return TimeTzCastSwitch(input, source, target);
	case LogicalTypeId::TIMESTAMP:
		return TimestampCastSwitch(input, source, target);
	case LogicalTypeId::TIMESTAMP_TZ:
		return TimestampTzCastSwitch(input, source, target);
	case LogicalTypeId::TIMESTAMP_NS:
		return TimestampNsCastSwitch(input, source, target);
	case LogicalTypeId::TIMESTAMP_MS:
		return TimestampMsCastSwitch(input, source, target);
	case LogicalTypeId::TIMESTAMP_SEC:
		return TimestampSecCastSwitch(input, source, target);
	case LogicalTypeId::INTERVAL:
		return IntervalCastSwitch(input, source, target);
	case LogicalTypeId::VARCHAR:
		return StringCastSwitch(input, source, target);
	case LogicalTypeId::BLOB:
		return BlobCastSwitch(input, source, target);
	case LogicalTypeId::BIT:
		return BitCastSwitch(input, source, target);
	case LogicalTypeId::SQLNULL:
		return NullTypeCast;
	case LogicalTypeId::MAP:
		return MapCastSwitch(input, source, target);
	case LogicalTypeId::STRUCT:
		return StructCastSwitch(input, source, target);
	case LogicalTypeId::LIST:
		return ListCastSwitch(input, source, target);
	case LogicalTypeId::UNION:
		return UnionCastSwitch(input, source, target);
	case LogicalTypeId::ENUM:
		return EnumCastSwitch(input, source, target);
	case LogicalTypeId::ARRAY:
		return ArrayCastSwitch(input, source, target);
	case LogicalTypeId::AGGREGATE_STATE:
		return AggregateStateToBlobCast;
	default:
		return nullptr;
	}
}

} // namespace duckdb

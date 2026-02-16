#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"

namespace duckdb {

BoundCastInfo DefaultCasts::UUIDCastSwitch(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		// uuid to varchar
		return BoundCastInfo(&VectorCastHelpers::StringCast<hugeint_t, duckdb::CastFromUUID>);
	case LogicalTypeId::BLOB:
		// uuid to blob
		return BoundCastInfo(&VectorCastHelpers::StringCast<hugeint_t, duckdb::CastFromUUIDToBlob>);
	case LogicalTypeId::UHUGEINT:
		// uuid to uhugeint
		return BoundCastInfo(
		    &VectorCastHelpers::TemplatedCastLoop<hugeint_t, uhugeint_t, duckdb::CastFromUUIDToUHugeint>);
	default:
		return TryVectorNullCast;
	}
}

} // namespace duckdb

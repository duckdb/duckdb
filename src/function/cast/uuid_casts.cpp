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
	default:
		return TryVectorNullCast;
	}
}

} // namespace duckdb

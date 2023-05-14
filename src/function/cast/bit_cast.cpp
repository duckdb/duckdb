#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"

namespace duckdb {

BoundCastInfo DefaultCasts::BitCastSwitch(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		// bit to varchar
		return BoundCastInfo(&VectorCastHelpers::StringCast<string_t, duckdb::CastFromBit>);
	default:
		return DefaultCasts::TryVectorNullCast;
	}
}

} // namespace duckdb

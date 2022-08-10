#include "duckdb/function/cast/default_casts.hpp"

namespace duckdb {

BoundCastInfo DefaultCasts::UUIDCastSwitch(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// uuid to varchar
		VectorStringCast<hugeint_t, duckdb::CastFromUUID>(source, result, count);
		break;
	default:
		return nullptr;
	}
}

}

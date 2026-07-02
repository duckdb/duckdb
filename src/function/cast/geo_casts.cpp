#include "duckdb/common/types/geometry.hpp"
#include "duckdb/common/types/geometry_crs.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/exception/binder_exception.hpp"

namespace duckdb {

static bool GeometryToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	UnaryExecutor::Execute<string_t, string_t>(
	    source, result, count, [&](const string_t &input) -> string_t { return Geometry::ToString(result, input); });
	return true;
}

BoundCastInfo DefaultCasts::GeoCastSwitch(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		return GeometryToVarcharCast;
	case LogicalTypeId::GEOMETRY: {
		// If the coordinate reference systems differ, we may not be able to cast
		if (GeoType::HasCRS(source) && GeoType::HasCRS(target)) {
			auto &target_crs = GeoType::GetCRS(target);
			auto &source_crs = GeoType::GetCRS(source);

			// TODO: Use client context (and allow extensions) to determine if the cast is okay (crs's are equivalent)

			if (!source_crs.Equals(target_crs)) {
				throw BinderException("Cannot cast GEOMETRY with CRS '" + source_crs.GetIdentifier() +
				                      "' to GEOMETRY with different CRS '" + target_crs.GetIdentifier() + "'");
			}
		}
		// The actual data representation stays the same, so we can do a reinterpret cast
		return DefaultCasts::ReinterpretCast;
	}
	default:
		return TryVectorNullCast;
	}
}

} // namespace duckdb

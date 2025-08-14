#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/types/variant.hpp"

namespace duckdb {

BoundCastInfo DefaultCasts::VariantCastSwitch(BindCastInput &input, const LogicalType &source,
                                              const LogicalType &target) {
	D_ASSERT(source.id() == LogicalTypeId::VARIANT);
	switch (target.id()) {
	//! TODO: add cast logical from VARIANT -> ANY logical type
	default:
		return TryVectorNullCast;
	}
}

} // namespace duckdb

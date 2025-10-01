#include "duckdb/function/scalar/geometry_functions.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

namespace duckdb {

static void IntersectsExtentFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, bool>(
	    input.data[0], input.data[1], result, input.size(), [](const string_t &lhs_geom, const string_t &rhs_geom) {
		    auto lhs_extent = GeometryExtent::Empty();
		    auto rhs_extent = GeometryExtent::Empty();

		    const auto lhs_is_empty = Geometry::GetExtent(lhs_geom, lhs_extent) == 0;
		    const auto rhs_is_empty = Geometry::GetExtent(rhs_geom, rhs_extent) == 0;

		    if (lhs_is_empty || rhs_is_empty) {
			    // One of the geometries is empty
			    return false;
		    }

		    // Don't take Z and M into account for intersection test
		    return lhs_extent.IntersectsXY(rhs_extent);
	    });
}

ScalarFunction StIntersectsExtentFun::GetFunction() {
	ScalarFunction function({LogicalType::GEOMETRY(), LogicalType::GEOMETRY()}, LogicalType::BOOLEAN,
	                        IntersectsExtentFunction);
	return function;
}

} // namespace duckdb

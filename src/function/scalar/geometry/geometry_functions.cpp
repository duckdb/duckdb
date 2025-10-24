#include "duckdb/function/scalar/geometry_functions.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

namespace duckdb {

static void FromWKBFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	Geometry::FromBinary(input.data[0], result, input.size(), true);
}

ScalarFunction StGeomfromwkbFun::GetFunction() {
	ScalarFunction function({LogicalType::BLOB}, LogicalType::GEOMETRY(), FromWKBFunction);
	return function;
}

static void ToWKBFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(input.data[0], result, input.size(), [&](const string_t &geom) {
		// TODO: convert to internal representation
		return geom;
	});
	// Add a heap reference to the input WKB to prevent it from being freed
	StringVector::AddHeapReference(input.data[0], result);
}

ScalarFunction StAswkbFun::GetFunction() {
	ScalarFunction function({LogicalType::GEOMETRY()}, LogicalType::BLOB, ToWKBFunction);
	return function;
}

static void ToWKTFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(input.data[0], result, input.size(),
	                                           [&](const string_t &geom) { return Geometry::ToString(result, geom); });
}

ScalarFunction StAstextFun::GetFunction() {
	ScalarFunction function({LogicalType::GEOMETRY()}, LogicalType::VARCHAR, ToWKTFunction);
	return function;
}

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

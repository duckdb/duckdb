#include "duckdb/function/scalar/geometry_functions.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "postgis.hpp"

namespace duckdb{

struct GeometryDistanceBinaryOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA geom1, TB geom2) {
        Postgis postgis;
        if (geom1.GetSize() == 0 || geom2.GetSize() == 0) {
            return NULL;
        }
        double dis = 0.00;
        auto gser1 = Geometry::GetGserialized(geom1);
        auto gser2 = Geometry::GetGserialized(geom2);
        if (!gser1 || !gser2) {
            throw ConversionException(
			    "Failure in geometry distance: could not calculate distance from geometries");
        }
        dis = postgis.ST_distance(gser1, gser2);
        Geometry::DestroyGeometry(gser1);
        Geometry::DestroyGeometry(gser2);
		return dis;
	}
};

struct GeometryDistanceTernaryOperator {
	template <class TA, class TB, class TC, class TR>
	static inline TR Operation(TA geom1, TB geom2, TC use_spheroid) {
        Postgis postgis;
        if (geom1.GetSize() == 0 || geom2.GetSize() == 0) {
            return NULL;
        }
        double dis = 0.00;
        auto gser1 = Geometry::GetGserialized(geom1);
        auto gser2 = Geometry::GetGserialized(geom2);
        if (!gser1 || !gser2) {
            throw ConversionException(
			    "Failure in geometry distance: could not calculate distance from geometries");
        }
        dis = postgis.geography_distance(gser1, gser2, use_spheroid);
        Geometry::DestroyGeometry(gser1);
        Geometry::DestroyGeometry(gser2);
		return dis;
	}
};

template <typename TA, typename TB, typename TR>
static void GeometryDistanceBinaryExecutor(Vector &geom1, Vector &geom2, Vector &result, idx_t count) {
    BinaryExecutor::ExecuteStandard<TA, TB, TR, GeometryDistanceBinaryOperator>(geom1, geom2, result, count);
}

template <typename TA, typename TB, typename TC, typename TR>
static void GeometryDistanceTernaryExecutor(Vector &geom1, Vector &geom2, Vector &use_spheroid, Vector &result, idx_t count) {
    TernaryExecutor::Execute<TA, TB, TC, TR>(geom1, geom2, use_spheroid, result, count, GeometryDistanceTernaryOperator::Operation<TA, TB, TC, TR>);
}


static void GeometryDistanceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &geom1_arg = args.data[0];
    auto &geom2_arg = args.data[1];
    if (args.data.size() == 2) {
        GeometryDistanceBinaryExecutor<string_t, string_t, double>(geom1_arg, geom2_arg, result, args.size());
    } else if (args.data.size() == 3) {
        auto &use_spheroid_arg = args.data[2];
        GeometryDistanceTernaryExecutor<string_t, string_t, bool, double>(geom1_arg, geom2_arg, use_spheroid_arg, result, args.size());
    }
}

void GeometryDistance::RegisterFunction(BuiltinFunctions &set) {
    ScalarFunctionSet distance("st_distance");
    distance.AddFunction(ScalarFunction({LogicalType::GEOMETRY, LogicalType::GEOMETRY}, LogicalType::DOUBLE,
                                    GeometryDistanceFunction));
    distance.AddFunction(ScalarFunction({LogicalType::GEOMETRY, LogicalType::GEOMETRY, LogicalType::BOOLEAN}, LogicalType::DOUBLE,
                                    GeometryDistanceFunction));
    set.AddFunction(distance);
}

} // duckdb
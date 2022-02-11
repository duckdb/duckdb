#include "duckdb/function/scalar/geometry_functions.hpp"
#include "postgis.hpp"

namespace duckdb {

struct CentroidUnaryOperator {
	template <class TA, class TR>
	static inline TR Operation(TA geom) {
        // Postgis postgis;
        // auto gser = postgis.LWGEOM_from_text(&text.GetString()[0]);
        // auto base = postgis.LWGEOM_base(gser);
        // auto size = postgis.LWGEOM_size(gser);
        // postgis.LWGEOM_free(gser);
		// return string_t(base, size);
        return string_t(geom.GetDataUnsafe(), geom.GetSize());
	}
};

struct CentroidBinaryOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA geom, TB use_spheroid) {
        // Postgis postgis;
        // auto gser = postgis.LWGEOM_from_text(&text.GetString()[0], srid);
        // auto base = postgis.LWGEOM_base(gser);
        // auto size = postgis.LWGEOM_size(gser);
        // postgis.LWGEOM_free(gser);
		// return string_t(base, size);
        return string_t(geom.GetDataUnsafe(), geom.GetSize());
	}
};

template <typename TA, typename TR>
static void GeometryCentroidUnaryExecutor(Vector &geom, Vector &result, idx_t count) {
    UnaryExecutor::Execute<TA, TR, CentroidUnaryOperator>(geom, result, count);
}

template <typename TA, typename TB, typename TR>
static void GeometryCentroidBinaryExecutor(Vector &geom, Vector &use_spheroid, Vector &result, idx_t count) {
    BinaryExecutor::ExecuteStandard<TA, TB, TR, CentroidBinaryOperator>(geom, use_spheroid, result, count);
}


static void GeometryCentroidFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &geom_arg = args.data[0];
    if (args.data.size() == 1) {
        GeometryCentroidUnaryExecutor<string_t, string_t>(geom_arg, result, args.size());
    } else if (args.data.size() == 2) {
        auto &use_spheroid_arg = args.data[1];
        GeometryCentroidBinaryExecutor<string_t, bool, string_t>(geom_arg, use_spheroid_arg, result, args.size());
    }
}

void GeometryCentroid::RegisterFunction(BuiltinFunctions &set) {
    ScalarFunctionSet centroid("st_centroid");
    centroid.AddFunction(ScalarFunction({LogicalType::GEOMETRY}, LogicalType::GEOMETRY,
                                    GeometryCentroidFunction));
    centroid.AddFunction(ScalarFunction({LogicalType::GEOMETRY, LogicalType::BOOLEAN}, LogicalType::GEOMETRY,
                                    GeometryCentroidFunction));
    set.AddFunction(centroid);
}

} // duckdb
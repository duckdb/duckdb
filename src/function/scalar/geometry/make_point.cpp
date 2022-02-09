#include "duckdb/function/scalar/geometry_functions.hpp"
#include "postgis.hpp"

namespace duckdb{

struct MakePointBinaryOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA pointX, TB pointY) {
        Postgis postgis;
        auto gser = postgis.LWGEOM_makepoint(pointX, pointY);
        auto base = postgis.LWGEOM_base(gser);
        auto size = postgis.LWGEOM_size(gser);
        postgis.LWGEOM_free(gser);
		return string_t(base, size);
	}
};

struct MakePointTernaryOperator {
	template <class TA, class TB, class TC, class TR>
	static inline TR Operation(TA pointX, TB pointY, TC pointZ) {
        Postgis postgis;
        auto gser = postgis.LWGEOM_makepoint(pointX, pointY, pointZ);
        auto base = postgis.LWGEOM_base(gser);
        auto size = postgis.LWGEOM_size(gser);
        postgis.LWGEOM_free(gser);
		return string_t(base, size);
	}
};

template <typename TA, typename TB, typename TR>
static void MakePointBinaryExecutor(Vector &pointX, Vector &pointY, Vector &result, idx_t count) {
    BinaryExecutor::ExecuteStandard<TA, TB, TR, MakePointBinaryOperator>(pointX, pointY, result, count);
}

template <typename TA, typename TB, typename TC, typename TR>
static void MakePointTernaryExecutor(Vector &pointX, Vector &pointY, Vector &pointZ, Vector &result, idx_t count) {
    TernaryExecutor::Execute<TA, TB, TC, TR>(pointX, pointY, pointZ, result, count, MakePointTernaryOperator::Operation<TA, TB, TC, TR>);
}

static void MakePointFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &pointX_arg = args.data[0];
	auto &pointY_arg = args.data[1];
    if (args.data.size() == 2) {
        MakePointBinaryExecutor<double, double, string_t>(pointX_arg, pointY_arg, result, args.size());
    } else if (args.data.size() == 3) {
        auto &pointZ_arg = args.data[2];
        MakePointTernaryExecutor<double, double, double, string_t>(pointX_arg, pointY_arg, pointZ_arg, result, args.size());
    }
}

void MakePointFun::RegisterFunction(BuiltinFunctions &set) {
    ScalarFunctionSet make_point("st_makepoint");
    make_point.AddFunction(ScalarFunction({LogicalType::DOUBLE, LogicalType::DOUBLE}, LogicalType::GEOMETRY,
                                    MakePointFunction));
    make_point.AddFunction(ScalarFunction({LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE}, LogicalType::GEOMETRY,
                                    MakePointFunction));
    set.AddFunction(make_point);
}

}
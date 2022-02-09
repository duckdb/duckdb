#include "duckdb/function/scalar/geometry_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterGeometryFunctions() {
    Register<MakePointFun>();
    Register<GeometryFromText>();
    Register<GeometryFromWKB>();
    Register<GeometryAsText>();
    Register<GeometryGetX>();
    Register<GeometryCentroid>();
    Register<GeometryDistance>();
};

}
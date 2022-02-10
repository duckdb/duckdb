#include "duckdb/function/scalar/geometry_functions.hpp"
#include "postgis.hpp"

namespace duckdb{

struct GetXUnaryOperator {
	template <class TA, class TR>
	static inline TR Operation(TA text) {
        Postgis postgis;
        if (text.GetSize() == 0) {
            // throw ConversionException(
			//     "Failure in geometry get X: could not get coordinate X from geometry");
            return 0.00;
        }
        double x_val = postgis.LWGEOM_x_point(text.GetDataUnsafe(), text.GetSize());
		return x_val;
	}
};

template <typename TA, typename TR>
static void GeometryGetXUnaryExecutor(Vector &text, Vector &result, idx_t count) {
    UnaryExecutor::Execute<TA, TR, GetXUnaryOperator>(text, result, count);
}

static void GeometryGetXFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &text_arg = args.data[0];
    GeometryGetXUnaryExecutor<string_t, double>(text_arg, result, args.size());
}

void GeometryGetX::RegisterFunction(BuiltinFunctions &set) {
    ScalarFunctionSet get_x("st_x");
    get_x.AddFunction(ScalarFunction({LogicalType::GEOMETRY}, LogicalType::DOUBLE,
                                    GeometryGetXFunction));
    set.AddFunction(get_x);
}

} // duckdb
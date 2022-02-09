#include "duckdb/function/scalar/geometry_functions.hpp"
#include "postgis.hpp"

namespace duckdb{

struct FromTextUnaryOperator {
	template <class TA, class TR>
	static inline TR Operation(TA text) {
        Postgis postgis;
        if (text.GetSize() == 0) {
            return text;
        }
        auto gser = postgis.LWGEOM_from_text(&text.GetString()[0]);
        if (!gser) {
            throw ConversionException(
			    "Failure in geometry from text: could not convert text to geometry");
        }
        auto base = postgis.LWGEOM_base(gser);
        auto size = postgis.LWGEOM_size(gser);
        postgis.LWGEOM_free(gser);
		return string_t(base, size);
	}
};

struct FromTextBinaryOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA text, TB srid) {
        Postgis postgis;
        if (text.GetSize() == 0) {
            return text;
        }
        auto gser = postgis.LWGEOM_from_text(&text.GetString()[0], srid);
        if (!gser) {
            throw ConversionException(
			    "Failure in geometry from text: could not convert text to geometry");
        }
        auto base = postgis.LWGEOM_base(gser);
        auto size = postgis.LWGEOM_size(gser);
        postgis.LWGEOM_free(gser);
		return string_t(base, size);
	}
};

template <typename TA, typename TR>
static void GeometryFromTextUnaryExecutor(Vector &text, Vector &result, idx_t count) {
    UnaryExecutor::Execute<TA, TR, FromTextUnaryOperator>(text, result, count);
}

template <typename TA, typename TB, typename TR>
static void GeometryFromTextBinaryExecutor(Vector &text, Vector &srid, Vector &result, idx_t count) {
    BinaryExecutor::ExecuteStandard<TA, TB, TR, FromTextBinaryOperator>(text, srid, result, count);
}


static void GeometryFromTextFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &text_arg = args.data[0];
    if (args.data.size() == 1) {
        GeometryFromTextUnaryExecutor<string_t, string_t>(text_arg, result, args.size());
    } else if (args.data.size() == 2) {
        auto &srid_arg = args.data[1];
        GeometryFromTextBinaryExecutor<string_t, int32_t, string_t>(text_arg, srid_arg, result, args.size());
    }
}

void GeometryFromText::RegisterFunction(BuiltinFunctions &set) {
    ScalarFunctionSet from_text("st_geomfromtext");
    from_text.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::GEOMETRY,
                                    GeometryFromTextFunction));
    from_text.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::INTEGER}, LogicalType::GEOMETRY,
                                    GeometryFromTextFunction));
    set.AddFunction(from_text);
}

} // duckdb
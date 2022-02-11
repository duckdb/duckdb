#include "duckdb/function/scalar/geometry_functions.hpp"
#include "postgis.hpp"

namespace duckdb {

struct AsTextUnaryOperator {
	template <class TA, class TR>
	static inline TR Operation(TA text, Vector &result) {
        Postgis postgis;
        if (text.GetSize() == 0) {
            return text;
        }
        string str = postgis.LWGEOM_asText(text.GetDataUnsafe(), text.GetSize());
        auto result_str = StringVector::EmptyString(result, str.size());
		memcpy(result_str.GetDataWriteable(), str.c_str(), str.size());
		result_str.Finalize();
		return result_str;
	}
};

template <typename TA, typename TR>
static void GeometryAsTextUnaryExecutor(Vector &text, Vector &result, idx_t count) {
    UnaryExecutor::ExecuteString<TA, TR, AsTextUnaryOperator>(text, result, count);
}

static void GeometryAsTextFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &text_arg = args.data[0];
    GeometryAsTextUnaryExecutor<string_t, string_t>(text_arg, result, args.size());
}

void GeometryAsText::RegisterFunction(BuiltinFunctions &set) {
    ScalarFunctionSet as_text("st_astext");
    as_text.AddFunction(ScalarFunction({LogicalType::GEOMETRY}, LogicalType::VARCHAR,
                                    GeometryAsTextFunction));
    set.AddFunction(as_text);
}

} // duckdb
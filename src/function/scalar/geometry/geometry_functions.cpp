#include "duckdb/function/scalar/geometry_functions.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/common/types/geometry_crs.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

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

static LogicalType GetCRSLogicalType() {
	return LogicalType::STRUCT({
	    {"type", LogicalType::VARCHAR},
	    {"name", LogicalType::VARCHAR},
	    {"value", LogicalType::VARCHAR},
	});
}

static Value GetCRSValue(const LogicalType &logical_type) {
	if (!GeoType::HasCRS(logical_type)) {
		// Return null
		return Value(GetCRSLogicalType());
	}

	auto &crs = GeoType::GetCRS(logical_type);

	const char *type_str;
	switch (crs.GetType()) {
	case CoordinateReferenceSystemType::PROJJSON:
		type_str = "projjson";
		break;
	case CoordinateReferenceSystemType::WKT2_2019:
		type_str = "wkt2:2019";
		break;
	case CoordinateReferenceSystemType::AUTH_CODE:
		type_str = "authority_code";
		break;
	case CoordinateReferenceSystemType::SRID:
		type_str = "srid";
		break;
	case CoordinateReferenceSystemType::INVALID:
	default:
		type_str = "unknown";
		break;
	}

	auto type_value = Value(type_str);
	auto name_value = crs.GetName().empty() ? Value(LogicalTypeId::VARCHAR) : Value(crs.GetName());
	auto text_value = Value(crs.GetDefinition());

	auto crs_value =
	    Value::STRUCT(GetCRSLogicalType(), {std::move(type_value), std::move(name_value), std::move(text_value)});

	return crs_value;
}

static void CRSFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &type = args.data[0].GetType();
	result.Reference(GetCRSValue(type));
}

static unique_ptr<Expression> BindCRSFunctionExpression(FunctionBindExpressionInput &input) {
	const auto &return_type = input.children[0]->return_type;
	if (return_type.id() == LogicalTypeId::UNKNOWN || return_type.id() == LogicalTypeId::SQLNULL) {
		// parameter - unknown return type
		return nullptr;
	}

	return make_uniq<BoundConstantExpression>(GetCRSValue(return_type));
}

static unique_ptr<FunctionData> BindCRSFunction(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	// Check if the CRS is set in the first argument
	bound_function.arguments[0] = arguments[0]->return_type;
	return nullptr;
}

ScalarFunction StCrsFun::GetFunction() {
	const auto crs_type = GetCRSLogicalType();
	ScalarFunction geom_func({LogicalType::GEOMETRY()}, crs_type, CRSFunction, BindCRSFunction);
	geom_func.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	geom_func.bind_expression = BindCRSFunctionExpression;
	return geom_func;
}

} // namespace duckdb

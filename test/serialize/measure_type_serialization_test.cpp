#include "catch.hpp"

#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

TEST_CASE("Test measure type serialization", "[serialization]") {
	auto measure_type_in =
	    LogicalType::MEASURE_TYPE(LogicalType::INTEGER, "m", make_uniq<BoundConstantExpression>(Value::INTEGER(7)));
	auto different_measure_type =
	    LogicalType::MEASURE_TYPE(LogicalType::INTEGER, "m", make_uniq<BoundConstantExpression>(Value::INTEGER(8)));

	Allocator allocator;
	MemoryStream stream(allocator);
	SerializationOptions options;
	options.serialize_default_values = false;

	// Serialize the measure type
	BinarySerializer::Serialize(measure_type_in, stream, options);
	stream.Rewind();

	// Deserialize the measure type
	BinaryDeserializer deserializer(stream);
	auto measure_type_out = LogicalType::Deserialize(deserializer);
	REQUIRE(measure_type_out == measure_type_in);
	REQUIRE(measure_type_in == measure_type_out);
	REQUIRE(measure_type_in != different_measure_type);
	REQUIRE(measure_type_out != different_measure_type);

	// Verify the deserialized type matches the original
	REQUIRE(measure_type_in.id() == measure_type_out.id());
	REQUIRE(MeasureTypeInfo::MeasureAlias(measure_type_in) == "m");
	REQUIRE(MeasureTypeInfo::MeasureAlias(measure_type_out) == "m");

	// Verify the output type is preserved
	auto &measure_info_in = measure_type_in.GetAuxInfoShrPtr()->Cast<MeasureTypeInfo>();
	auto &measure_info_out = measure_type_out.GetAuxInfoShrPtr()->Cast<MeasureTypeInfo>();

	REQUIRE(measure_info_in.measure_output_type == LogicalType::INTEGER);
	REQUIRE(measure_info_out.measure_output_type == LogicalType::INTEGER);
}

TEST_CASE("Test measure type value serialization", "[serialization]") {
	auto measure_type =
	    LogicalType::MEASURE_TYPE(LogicalType::INTEGER, "m", make_uniq<BoundConstantExpression>(Value::INTEGER(7)));

	Value measure_value(measure_type);
	REQUIRE(measure_value.IsNull());

	Allocator allocator;
	MemoryStream stream(allocator);
	SerializationOptions options;
	options.serialize_default_values = false;

	BinarySerializer::Serialize(measure_value, stream, options);

	stream.Rewind();

	BinaryDeserializer deserializer(stream);
	Value deserialized_value = Value::Deserialize(deserializer);

	REQUIRE(deserialized_value.IsNull());
	REQUIRE(deserialized_value.type() == measure_value.type());
}

} // namespace duckdb

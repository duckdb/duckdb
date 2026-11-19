#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

// Extension type metadata only exists on programmatically built types, and the raw type constructor is C++ only, so SQL
// cannot construct a parameterless info carrying metadata.

static LogicalType RawConstructedType(LogicalTypeId id, int32_t modifier) {
	auto type_info = make_uniq<LogicalTypeInfo>(LogicalTypeInfoType::INVALID_TYPE_INFO);
	type_info->extension_info = make_uniq<ExtensionTypeInfo>();
	type_info->extension_info->modifiers.emplace_back(Value::INTEGER(modifier));
	return LogicalType(id, std::move(type_info));
}

static LogicalType RoundTrip(const LogicalType &type) {
	MemoryStream stream;
	SerializationOptions options;
	options.storage_compatibility = StorageCompatibility::Latest();
	BinarySerializer::Serialize(type, stream, options);
	stream.Rewind();
	BinaryDeserializer deserializer(stream);
	deserializer.Begin();
	auto result = LogicalType::Deserialize(deserializer);
	deserializer.End();
	return result;
}

TEST_CASE("The raw type constructor normalizes a parameterless info carrying metadata", "[api]") {
	// the serializer drops a parameterless info entirely, so the constructor normalizes it to the GENERIC info the
	// factories produce, and its metadata survives a serialization round trip
	auto type = RawConstructedType(LogicalTypeId::INTEGER, 1);
	REQUIRE(type.GetTypeInfo().type == LogicalTypeInfoType::GENERIC_TYPE_INFO);
	REQUIRE(type.GetTypeInfo().extension_info->modifiers[0].value == Value::INTEGER(1));
	auto restored = RoundTrip(type);
	REQUIRE(restored.id() == LogicalTypeId::INTEGER);
	REQUIRE(restored.GetTypeInfo().extension_info);
	REQUIRE(restored.GetTypeInfo().extension_info->modifiers[0].value == Value::INTEGER(1));

	auto aliased = LogicalType(LogicalTypeId::VARCHAR,
	                           make_uniq<LogicalTypeInfo>(LogicalTypeInfoType::INVALID_TYPE_INFO, string("my_alias")));
	REQUIRE(RoundTrip(aliased).GetAlias() == "my_alias");
}

#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

// Extension type metadata only exists on programmatically built types, the raw type constructor is C++ only, and
// TypeVisitor is a C++ utility, so SQL cannot construct these types or observe the identity of a rebuilt container.

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

TEST_CASE("TypeVisitor keeps the identity of subtrees it does not change", "[api]") {
	auto metadata = make_uniq<ExtensionTypeInfo>();
	metadata->modifiers.emplace_back(Value::INTEGER(7));
	auto payload = LogicalType::LIST(LogicalType::INTEGER).WithAlias("payload").WithExtensionInfo(std::move(metadata));
	auto container = LogicalType::STRUCT({{"payload", payload}, {"n", LogicalType::BIGINT}});
	auto nested = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::ARRAY(container, 2));

	// nothing replaced: every node, including the containers, is the original type
	auto identity = TypeVisitor::VisitReplace(nested, [](const LogicalType &type) { return type; });
	REQUIRE(&identity.GetTypeInfo() == &nested.GetTypeInfo());

	// a replaced sibling rebuilds its containers, the untouched payload keeps its alias and metadata
	auto replaced = TypeVisitor::VisitReplace(container, [](const LogicalType &type) {
		return type.id() == LogicalTypeId::BIGINT ? LogicalType::HUGEINT : type;
	});
	auto &children = StructType::GetChildTypes(replaced);
	REQUIRE(children[1].second.id() == LogicalTypeId::HUGEINT);
	REQUIRE(&children[0].second.GetTypeInfo() == &payload.GetTypeInfo());
	REQUIRE(children[0].second.GetAlias() == "payload");
	REQUIRE(children[0].second.GetTypeInfo().extension_info->modifiers[0].value == Value::INTEGER(7));
	REQUIRE(RoundTrip(replaced) == replaced);

	// an ARRAY of any size stays any-sized when its child is replaced
	auto any_array = LogicalType::ARRAY(LogicalType::BIGINT, optional_idx());
	auto any_replaced = TypeVisitor::VisitReplace(any_array, [](const LogicalType &type) {
		return type.id() == LogicalTypeId::BIGINT ? LogicalType::HUGEINT : type;
	});
	REQUIRE(ArrayType::IsAnySize(any_replaced));
	REQUIRE(ArrayType::GetChildType(any_replaced).id() == LogicalTypeId::HUGEINT);
}

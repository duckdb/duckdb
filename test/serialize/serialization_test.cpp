#include "catch.hpp"

#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

namespace duckdb {

struct Bar {
	uint32_t b;
	void Serialize(Serializer &serializer) const {
		serializer.WriteProperty<uint32_t>(1, "b", b);
	}

	static unique_ptr<Bar> Deserialize(Deserializer &deserializer) {
		auto result = make_uniq<Bar>();
		deserializer.ReadProperty<uint32_t>(1, "b", result->b);
		return result;
	}
};

struct Foo {
	int32_t a;
	unique_ptr<Bar> bar;
	int32_t c;

	void Serialize(Serializer &serializer) const {
		serializer.WriteProperty<int32_t>(1, "a", a);
		serializer.WritePropertyWithDefault<unique_ptr<Bar>>(2, "bar", bar, unique_ptr<Bar>());
		serializer.WriteProperty<int32_t>(3, "c", c);
	}

	static unique_ptr<Foo> Deserialize(Deserializer &deserializer) {
		auto result = make_uniq<Foo>();
		deserializer.ReadProperty<int32_t>(1, "a", result->a);
		deserializer.ReadPropertyWithDefault<unique_ptr<Bar>>(2, "bar", result->bar, unique_ptr<Bar>());
		deserializer.ReadProperty<int32_t>(3, "c", result->c);
		return result;
	}
};

TEST_CASE("Test default values", "[serialization]") {

	Foo foo_in;
	foo_in.a = 42;
	foo_in.bar = make_uniq<Bar>();
	foo_in.bar->b = 43;
	foo_in.c = 44;

	MemoryStream stream;
	BinarySerializer::Serialize(foo_in, stream, false);
	auto pos1 = stream.GetPosition();
	stream.Rewind();
	auto foo_out_ptr = BinaryDeserializer::Deserialize<Foo>(stream);
	auto &foo_out = *foo_out_ptr.get();

	REQUIRE(foo_in.a == foo_out.a);
	REQUIRE(foo_in.bar->b == foo_out.bar->b);
	REQUIRE(foo_in.c == foo_out.c);

	// Now try with a default value
	foo_in.bar = nullptr;

	stream.Rewind();

	BinarySerializer::Serialize(foo_in, stream, false);
	auto pos2 = stream.GetPosition();
	stream.Rewind();

	foo_out_ptr = BinaryDeserializer::Deserialize<Foo>(stream);
	auto &foo_out2 = *foo_out_ptr.get();

	REQUIRE(foo_in.a == foo_out2.a);
	REQUIRE(foo_out2.bar == nullptr);
	REQUIRE(foo_in.c == foo_out2.c);

	// We should not have written the default value
	REQUIRE(pos1 > pos2);
}

} // namespace duckdb

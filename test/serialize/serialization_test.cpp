#include "catch.hpp"

#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/message_serializer.hpp"

#include "test_message_writer.hpp"

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
		deserializer.ReadPropertyWithExplicitDefault<unique_ptr<Bar>>(2, "bar", result->bar, unique_ptr<Bar>());
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

	Allocator allocator;
	MemoryStream stream(allocator);
	SerializationOptions options;
	options.serialize_default_values = false;
	BinarySerializer::Serialize(foo_in, stream, options);
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

	options.serialize_default_values = false;
	BinarySerializer::Serialize(foo_in, stream, options);
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

//------------------------------------------------------
// Test deleted properties
//------------------------------------------------------

struct Complex {
	int c1;
	string c2;
	Complex(int c1, string c2) : c1(c1), c2(c2) {
	}
	Complex() : c1(0), c2("") {
	}

	void Serialize(Serializer &serializer) const {
		serializer.WriteProperty<int>(1, "c1", c1);
		serializer.WriteProperty<string>(2, "c2", c2);
	}

	static unique_ptr<Complex> Deserialize(Deserializer &deserializer) {
		auto result = make_uniq<Complex>();
		deserializer.ReadProperty<int>(1, "c1", result->c1);
		deserializer.ReadProperty<string>(2, "c2", result->c2);
		return result;
	}
};

struct FooV1 {
	int p1;
	vector<unique_ptr<Complex>> p2;
	int p3;
	unique_ptr<Complex> p4;

	void Serialize(Serializer &serializer) const {
		serializer.WriteProperty<int>(1, "p1", p1);
		serializer.WritePropertyWithDefault<vector<unique_ptr<Complex>>>(2, "p2", p2);
		serializer.WriteProperty<int>(3, "p3", p3);
		serializer.WriteProperty<unique_ptr<Complex>>(4, "p4", p4);
	}

	static unique_ptr<FooV1> Deserialize(Deserializer &deserializer) {
		auto result = make_uniq<FooV1>();
		deserializer.ReadProperty<int>(1, "p1", result->p1);
		deserializer.ReadPropertyWithDefault<vector<unique_ptr<Complex>>>(2, "p2", result->p2);
		deserializer.ReadProperty<int>(3, "p3", result->p3);
		deserializer.ReadProperty<unique_ptr<Complex>>(4, "p4", result->p4);
		return result;
	}
};

struct FooV2 {
	int p1;
	/*vector<unique_ptr<Complex>> p2;*/ // In v2, this is deleted
	int p3;
	unique_ptr<Complex> p4;
	unique_ptr<Complex> p5; // In v2, this is added

	void Serialize(Serializer &serializer) const {
		serializer.WriteProperty<int>(1, "p1", p1);
		// This field is deleted!
		/* serializer.WriteDeletedProperty<vector<unique_ptr<Complex>>>(2, "p2"); */
		serializer.WriteProperty<int>(3, "p3", p3);
		serializer.WriteProperty<unique_ptr<Complex>>(4, "p4", p4);

		// Because this is a new field, we have to provide a default value
		// to try to preserve backwards compatibility (in best case)
		serializer.WritePropertyWithDefault<unique_ptr<Complex>>(5, "p5", p5);
	}

	static unique_ptr<FooV2> Deserialize(Deserializer &deserializer) {
		auto result = make_uniq<FooV2>();
		deserializer.ReadProperty(1, "p1", result->p1);
		deserializer.ReadDeletedProperty<vector<unique_ptr<Complex>>>(2, "p2");
		deserializer.ReadProperty(3, "p3", result->p3);
		deserializer.ReadProperty(4, "p4", result->p4);
		deserializer.ReadPropertyWithDefault<unique_ptr<Complex>>(5, "p5", result->p5);
		return result;
	}
};

TEST_CASE("Test deleted values", "[serialization]") {
	FooV1 v1_in = {1, {}, 6, make_uniq<Complex>(1, "foo")};
	v1_in.p2.push_back(make_uniq<Complex>(2, "3"));
	v1_in.p2.push_back(make_uniq<Complex>(4, "5"));

	FooV2 v2_in = {1, 3, make_uniq<Complex>(1, "foo"), nullptr};

	Allocator allocator;
	MemoryStream stream(allocator);
	SerializationOptions options;
	options.serialize_default_values = false;
	// First of, sanity check that foov1 <-> foov1 works
	BinarySerializer::Serialize(v1_in, stream, options);
	{
		stream.Rewind();
		auto v1_out_ptr = BinaryDeserializer::Deserialize<FooV1>(stream);
		auto &v1_out = *v1_out_ptr.get();
		REQUIRE(v1_in.p1 == v1_out.p1);
		REQUIRE(v1_in.p2.size() == v1_out.p2.size());
		REQUIRE(v1_in.p2[0]->c1 == v1_out.p2[0]->c1);
		REQUIRE(v1_in.p2[0]->c2 == v1_out.p2[0]->c2);
		REQUIRE(v1_in.p2[1]->c1 == v1_out.p2[1]->c1);
		REQUIRE(v1_in.p2[1]->c2 == v1_out.p2[1]->c2);
		REQUIRE(v1_in.p3 == v1_out.p3);
		REQUIRE(v1_in.p4->c1 == v1_out.p4->c1);
		REQUIRE(v1_in.p4->c2 == v1_out.p4->c2);
	}

	stream.Rewind();

	// Also check that foov2 <-> foov2 works
	options.serialize_default_values = false;
	BinarySerializer::Serialize(v2_in, stream, options);
	{
		stream.Rewind();
		auto v2_out_ptr = BinaryDeserializer::Deserialize<FooV2>(stream);
		auto &v2_out = *v2_out_ptr.get();
		REQUIRE(v2_in.p1 == v2_out.p1);
		REQUIRE(v2_in.p3 == v2_out.p3);
		REQUIRE(v2_in.p4->c1 == v2_out.p4->c1);
		REQUIRE(v2_in.p4->c2 == v2_out.p4->c2);
		REQUIRE(v2_in.p5 == v2_out.p5);
	}

	// Check that foov1 -> foov2 works (backwards compatible)
	stream.Rewind();
	options.serialize_default_values = false;
	BinarySerializer::Serialize(v1_in, stream, options);
	{
		stream.Rewind();
		auto v2_out_ptr = BinaryDeserializer::Deserialize<FooV2>(stream);
		auto &v2_out = *v2_out_ptr.get();
		REQUIRE(v1_in.p1 == v2_out.p1);
		REQUIRE(v1_in.p3 == v2_out.p3);
		REQUIRE(v1_in.p4->c1 == v2_out.p4->c1);
		REQUIRE(v1_in.p4->c2 == v2_out.p4->c2);
		REQUIRE(v2_out.p5 == nullptr);
	}

	// Check that foov2 -> foov1 works (forwards compatible)
	// This should be ok, since the property we deleted was optional (had a default value)
	stream.Rewind();
	options.serialize_default_values = false;
	BinarySerializer::Serialize(v2_in, stream, options);
	{
		stream.Rewind();
		auto v1_out_ptr = BinaryDeserializer::Deserialize<FooV1>(stream);
		auto &v1_out = *v1_out_ptr.get();
		REQUIRE(v2_in.p1 == v1_out.p1);
		REQUIRE(v2_in.p3 == v1_out.p3);
		REQUIRE(v2_in.p4->c1 == v1_out.p4->c1);
		REQUIRE(v2_in.p4->c2 == v1_out.p4->c2);
		REQUIRE(v1_out.p2.empty());
	}

	// If we change the new value in foov2 to something thats not the default, we break forwards compatibility.
	// But thats life. Tough shit.
	stream.Rewind();
	v2_in.p5 = make_uniq<Complex>(2, "foo");
	options.serialize_default_values = false;
	BinarySerializer::Serialize(v2_in, stream, options);
	{
		stream.Rewind();
		REQUIRE_THROWS(BinaryDeserializer::Deserialize<FooV1>(stream));
	}

	// However, the new value should be read correctly!
	stream.Rewind();
	options.serialize_default_values = false;
	BinarySerializer::Serialize(v2_in, stream, options);
	{
		stream.Rewind();
		auto v2_out_ptr = BinaryDeserializer::Deserialize<FooV2>(stream);
		auto &v2_out = *v2_out_ptr.get();
		REQUIRE(v2_in.p1 == v2_out.p1);
		REQUIRE(v2_in.p3 == v2_out.p3);
		REQUIRE(v2_in.p4->c1 == v2_out.p4->c1);
		REQUIRE(v2_in.p4->c2 == v2_out.p4->c2);
		REQUIRE(v2_out.p5->c1 == 2);
		REQUIRE(v2_out.p5->c2 == "foo");
	}
}

TEST_CASE("Test padded LEB128 encoding", "[serialization]") {
	REQUIRE(EncodingUtil::MaxLEB128Width<uint8_t>() == 2);
	REQUIRE(EncodingUtil::MaxLEB128Width<uint16_t>() == 3);
	REQUIRE(EncodingUtil::MaxLEB128Width<uint32_t>() == 5);
	REQUIRE(EncodingUtil::MaxLEB128Width<uint64_t>() == 10);

	// Every legal width decodes to the same value, so a decoder needs no knowledge of slots
	uint64_t values[] = {0, 1, 127, 128, 300, 16384, DConstants::INVALID_INDEX};
	for (auto value : values) {
		for (idx_t width = EncodingUtil::MinimalLEB128Width(value); width <= EncodingUtil::MaxLEB128Width<uint64_t>();
		     width++) {
			data_t buffer[16] = {};
			EncodingUtil::EncodePaddedLEB128<uint64_t>(buffer, value, width);
			uint64_t result;
			REQUIRE(EncodingUtil::DecodeUnsignedLEB128<uint64_t>(buffer, result) == width);
			REQUIRE(result == value);
		}
	}

	data_t buffer[16] = {};
	REQUIRE_THROWS(EncodingUtil::EncodePaddedLEB128<uint64_t>(buffer, 128, 1));
	// Six bytes is more than the decoder accepts for a 32-bit value
	REQUIRE_THROWS(EncodingUtil::EncodePaddedLEB128<uint32_t>(buffer, 1, 6));
}

TEST_CASE("Test reserved slots and deferred lists", "[serialization]") {
	Allocator allocator;
	MemoryStream stream(allocator);
	MessageSerializer serializer(stream);

	serializer.Begin();
	serializer.WritePropertyWithDefault<string>(1, "name", string("incremental"));
	auto items = serializer.BeginDeferredList(2, "items");
	for (idx_t i = 0; i < 5; i++) {
		TestMessageItem item;
		item.value = static_cast<int32_t>(i);
		item.label = "item" + to_string(i);
		serializer.AppendElement(items, &item);
	}
	serializer.EndDeferredList(items);
	auto total = serializer.ReserveProperty<idx_t>(3, "total_items");
	serializer.ReserveProperty<idx_t>(4, "next_index", DConstants::INVALID_INDEX);
	serializer.End();

	serializer.PatchReserved(total, items.count);

	stream.Rewind();
	auto message = BinaryDeserializer::Deserialize<TestMessage>(stream);
	REQUIRE(message->name == "incremental");
	REQUIRE(message->total_items == 5);
	REQUIRE(message->items.size() == 5);
	for (idx_t i = 0; i < 5; i++) {
		REQUIRE(message->items[i]->value == static_cast<int32_t>(i));
		REQUIRE(message->items[i]->label == "item" + to_string(i));
	}
}

TEST_CASE("Test patching a slot after the stream reallocates", "[serialization]") {
	Allocator allocator;
	// A small capacity, so the buffer moves between the reserve and the patch
	MemoryStream stream(allocator, 16);
	MessageSerializer serializer(stream);

	serializer.Begin();
	auto items = serializer.BeginDeferredList(2, "items");
	auto initial_data = stream.GetData();
	for (idx_t i = 0; i < 200; i++) {
		TestMessageItem item;
		item.value = static_cast<int32_t>(i);
		item.label = string(32, 'x');
		serializer.AppendElement(items, &item);
	}
	serializer.EndDeferredList(items);
	serializer.ReserveProperty<idx_t>(4, "next_index", DConstants::INVALID_INDEX);
	serializer.End();
	REQUIRE(stream.GetData() != initial_data);

	stream.Rewind();
	auto message = BinaryDeserializer::Deserialize<TestMessage>(stream);
	REQUIRE(message->items.size() == 200);
}

TEST_CASE("Test reserved slot errors", "[serialization]") {
	Allocator allocator;
	MemoryStream stream(allocator);
	MessageSerializer serializer(stream);

	serializer.Begin();
	auto narrow = serializer.ReserveProperty<uint16_t>(1, "narrow");
	serializer.End();

	// A three byte slot holds a uint16_t, and nothing wider
	REQUIRE_NOTHROW(serializer.PatchReserved(narrow, 65535));
	REQUIRE_THROWS(serializer.PatchReserved(narrow, 1ULL << 30));

	REQUIRE_THROWS(MessageSerializer::PatchSlot(stream.GetData(), 1, narrow, 1));

	MessageSerializer::ReservedSlot invalid;
	REQUIRE(!invalid.IsValid());
	REQUIRE_THROWS(serializer.PatchReserved(invalid, 1));
}

TEST_CASE("Test the generated incremental writer", "[serialization]") {
	Allocator allocator;
	MemoryStream stream(allocator);
	MessageSerializer serializer(stream);
	TestMessageWriter writer(serializer);

	serializer.Begin();
	writer.WriteName("generated");
	writer.BeginItems();
	for (idx_t i = 0; i < 3; i++) {
		TestMessageItem item;
		item.value = static_cast<int32_t>(i * 10);
		item.label = "label" + to_string(i);
		writer.AppendItems(&item);
	}
	writer.EndItems();
	auto total = writer.ReserveTotalItems();
	auto next = writer.ReserveNextIndex();
	serializer.End();

	// The totals are known only after the payload closes, and the patch needs no live serializer
	MessageSerializer::PatchSlot(stream.GetData(), stream.GetPosition(), total, 3);
	MessageSerializer::PatchSlot(stream.GetData(), stream.GetPosition(), next, 99);

	stream.Rewind();
	auto message = BinaryDeserializer::Deserialize<TestMessage>(stream);
	REQUIRE(message->name == "generated");
	REQUIRE(message->total_items == 3);
	REQUIRE(message->next_index.IsValid());
	REQUIRE(message->next_index.GetIndex() == 99);
	REQUIRE(message->items.size() == 3);
	for (idx_t i = 0; i < 3; i++) {
		REQUIRE(message->items[i]->value == static_cast<int32_t>(i * 10));
		REQUIRE(message->items[i]->label == "label" + to_string(i));
	}
}

TEST_CASE("Test the incremental writer agrees with the one-shot codec", "[serialization]") {
	TestMessage expected;
	expected.name = "both";
	expected.total_items = 2;
	for (idx_t i = 0; i < 2; i++) {
		auto item = make_uniq<TestMessageItem>();
		item->value = static_cast<int32_t>(i);
		item->label = "e" + to_string(i);
		expected.items.push_back(std::move(item));
	}

	Allocator allocator;
	MemoryStream one_shot(allocator);
	BinarySerializer::Serialize(expected, one_shot);

	MemoryStream incremental(allocator);
	MessageSerializer serializer(incremental);
	TestMessageWriter writer(serializer);
	serializer.Begin();
	writer.WriteName(expected.name);
	writer.BeginItems();
	for (auto &item : expected.items) {
		writer.AppendItems(item.get());
	}
	writer.EndItems();
	auto total = writer.ReserveTotalItems();
	writer.ReserveNextIndex();
	serializer.End();
	serializer.PatchReserved(total, expected.items.size());

	// No byte comparison: a slot is a padded varint, and it is written even at its default value.
	// The two payloads must decode to the same message.
	one_shot.Rewind();
	incremental.Rewind();
	auto from_one_shot = BinaryDeserializer::Deserialize<TestMessage>(one_shot);
	auto from_writer = BinaryDeserializer::Deserialize<TestMessage>(incremental);

	REQUIRE(from_one_shot->name == from_writer->name);
	REQUIRE(from_one_shot->total_items == from_writer->total_items);
	REQUIRE(from_one_shot->next_index.IsValid() == from_writer->next_index.IsValid());
	REQUIRE(from_one_shot->items.size() == from_writer->items.size());
	for (idx_t i = 0; i < from_writer->items.size(); i++) {
		REQUIRE(from_one_shot->items[i]->value == from_writer->items[i]->value);
		REQUIRE(from_one_shot->items[i]->label == from_writer->items[i]->label);
	}
}

TEST_CASE("Test an unpatched slot reads back as its placeholder", "[serialization]") {
	Allocator allocator;
	MemoryStream stream(allocator);
	MessageSerializer serializer(stream);
	TestMessageWriter writer(serializer);

	serializer.Begin();
	writer.WriteName("unpatched");
	writer.BeginItems();
	writer.EndItems();
	writer.ReserveTotalItems();
	writer.ReserveNextIndex();
	serializer.End();

	stream.Rewind();
	auto message = BinaryDeserializer::Deserialize<TestMessage>(stream);
	REQUIRE(message->items.empty());
	// Zero is a real index, so an optional_idx slot starts at its own sentinel. An unpatched slot
	// must read back as absent, not as index zero.
	REQUIRE(message->total_items == 0);
	REQUIRE(!message->next_index.IsValid());
}

#ifdef DEBUG
TEST_CASE("Test the generated writer rejects out of order fields", "[serialization]") {
	Allocator allocator;
	MemoryStream stream(allocator);
	MessageSerializer serializer(stream);
	TestMessageWriter writer(serializer);

	serializer.Begin();
	writer.ReserveTotalItems();
	REQUIRE_THROWS(writer.WriteName("out of order"));
}
#endif

} // namespace duckdb

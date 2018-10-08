
#include "catch.hpp"

#include "common/serializer.hpp"
#include "common/types/data_chunk.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Basic serializer test", "[serializer]") {
	Serializer serializer(4);
	serializer.Write<sel_t>(33);
	serializer.Write<uint64_t>(42);
	auto data = serializer.GetData();

	Deserializer source(data.data.get(), data.size);
	bool failed = false;
	REQUIRE(source.Read<sel_t>(failed) == 33);
	REQUIRE(source.Read<uint64_t>(failed) == 42);
	REQUIRE(!failed);
}

TEST_CASE("Data Chunk serialization", "[serializer]") {
	Value a = Value::INTEGER(33);
	Value b = Value::INTEGER(42);
	Value c = Value("hello");
	Value d = Value("world");
	// test serializing of DataChunk
	DataChunk chunk;
	vector<TypeId> types = {TypeId::INTEGER, TypeId::VARCHAR};
	chunk.Initialize(types);
	chunk.data[0].count = 2;
	chunk.data[0].SetValue(0, a);
	chunk.data[0].SetValue(1, b);
	chunk.data[1].count = 2;
	chunk.data[1].SetValue(0, c);
	chunk.data[1].SetValue(1, d);
	chunk.count = 2;

	Serializer serializer;
	chunk.Serialize(serializer);

	auto data = serializer.GetData();
	Deserializer source(data.data.get(), data.size);

	DataChunk other_chunk;
	REQUIRE(other_chunk.Deserialize(source));
	REQUIRE(other_chunk.count == 2);
	REQUIRE(other_chunk.column_count == 2);
	REQUIRE(other_chunk.data[0].count == 2);
	REQUIRE(Value::Equals(other_chunk.data[0].GetValue(0), a));
	REQUIRE(Value::Equals(other_chunk.data[0].GetValue(1), b));
	REQUIRE(other_chunk.data[1].count == 2);
	REQUIRE(Value::Equals(other_chunk.data[1].GetValue(0), c));
	REQUIRE(Value::Equals(other_chunk.data[1].GetValue(1), d));
}

#include "catch.hpp"
#include "duckdb/common/serializer/buffered_deserializer.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "expression_helper.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Basic serializer test", "[serializer]") {
	BufferedSerializer serializer(4);
	serializer.Write<int32_t>(33);
	serializer.Write<uint64_t>(42);
	auto data = serializer.GetData();

	Deserializer source(data.data.get(), data.size);
	REQUIRE_NOTHROW(source.Read<int32_t>() == 33);
	REQUIRE_NOTHROW(source.Read<uint64_t>() == 42);
}

TEST_CASE("Data Chunk serialization", "[serializer]") {
	Value a = Value::INTEGER(33);
	Value b = Value::INTEGER(42);
	Value c = Value("hello");
	Value d = Value("world");
	// test serializing of DataChunk
	DataChunk chunk;
	vector<PhysicalType> types = {PhysicalType::INT32, PhysicalType::VARCHAR};
	chunk.Initialize(types);
	chunk.SetCardinality(2);
	chunk.SetValue(0, 0, a);
	chunk.SetValue(0, 1, b);
	chunk.SetValue(1, 0, c);
	chunk.SetValue(1, 1, d);

	BufferedSerializer serializer;
	chunk.Serialize(serializer);

	auto data = serializer.GetData();
	BufferedDeserializer source(data.data.get(), data.size);

	DataChunk other_chunk;
	REQUIRE_NOTHROW(other_chunk.Deserialize(source));
	REQUIRE(other_chunk.size() == 2);
	REQUIRE(other_chunk.column_count() == 2);
	REQUIRE(other_chunk.data[0].count == 2);
	REQUIRE(ValueOperations::Equals(other_chunk.GetValue(0, 0), a));
	REQUIRE(ValueOperations::Equals(other_chunk.GetValue(0, 1), b));
	REQUIRE(other_chunk.data[1].count == 2);
	REQUIRE(ValueOperations::Equals(other_chunk.GetValue(1, 0), c));
	REQUIRE(ValueOperations::Equals(other_chunk.GetValue(1, 1), d));
}

TEST_CASE("Value serialization", "[serializer]") {
	Value a = Value::BOOLEAN(12);
	Value b = Value::TINYINT(12);
	Value c = Value::SMALLINT(12);
	Value d = Value::INTEGER(12);
	Value e = Value::BIGINT(12);
	Value f = Value::POINTER(12);
	Value g = Value::DATE(12);
	Value h = Value();
	Value i = Value("hello world");

	BufferedSerializer serializer(4);
	a.Serialize(serializer);
	b.Serialize(serializer);
	c.Serialize(serializer);
	d.Serialize(serializer);
	e.Serialize(serializer);
	f.Serialize(serializer);
	g.Serialize(serializer);
	h.Serialize(serializer);
	i.Serialize(serializer);

	auto data = serializer.GetData();
	BufferedDeserializer source(data.data.get(), data.size);

	Value a1, b1, c1, d1, e1, f1, g1, h1, i1, j1;

	REQUIRE_NOTHROW(a1 = Value::Deserialize(source));
	REQUIRE(ValueOperations::Equals(a, a1));
	REQUIRE_NOTHROW(b1 = Value::Deserialize(source));
	REQUIRE(ValueOperations::Equals(b, b1));
	REQUIRE_NOTHROW(c1 = Value::Deserialize(source));
	REQUIRE(ValueOperations::Equals(c, c1));
	REQUIRE_NOTHROW(d1 = Value::Deserialize(source));
	REQUIRE(ValueOperations::Equals(d, d1));
	REQUIRE_NOTHROW(e1 = Value::Deserialize(source));
	REQUIRE(ValueOperations::Equals(e, e1));
	REQUIRE_NOTHROW(f1 = Value::Deserialize(source));
	REQUIRE(ValueOperations::Equals(f, f1));
	REQUIRE_NOTHROW(g1 = Value::Deserialize(source));
	REQUIRE(ValueOperations::Equals(g, g1));
	REQUIRE_NOTHROW(h1 = Value::Deserialize(source));
	REQUIRE(ValueOperations::Equals(h, h1));
	REQUIRE_NOTHROW(i1 = Value::Deserialize(source));
	REQUIRE(ValueOperations::Equals(i, i1));
	// try to deserialize too much, should throw a serialization exception
	REQUIRE_THROWS(j1 = Value::Deserialize(source));
}

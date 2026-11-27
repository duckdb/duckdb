#include "catch.hpp"

#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/parser/literal.hpp"

namespace duckdb {

//! Deserializes a Literal from data written by write
template <class FUNC>
static Literal DeserializeLiteral(FUNC write) {
	Allocator allocator;
	MemoryStream stream(allocator);
	BinarySerializer serializer(stream);
	serializer.Begin();
	write(serializer);
	serializer.End();
	stream.Rewind();

	BinaryDeserializer deserializer(stream);
	deserializer.Begin();
	auto result = Literal::Deserialize(deserializer);
	deserializer.End();
	return result;
}

static void WriteLiteral(Serializer &s, LiteralKind kind, const string &text) {
	s.WriteProperty<LiteralKind>(100, "kind", kind);
	s.WritePropertyWithDefault<string>(101, "text", text);
}

TEST_CASE("Deserialize Literal validates text against its kind", "[serialization]") {
	REQUIRE_NOTHROW(DeserializeLiteral([&](Serializer &s) { WriteLiteral(s, LiteralKind::BOOLEAN, "true"); }));
	REQUIRE_NOTHROW(DeserializeLiteral([&](Serializer &s) { WriteLiteral(s, LiteralKind::INTEGER, "123"); }));
	REQUIRE_NOTHROW(DeserializeLiteral([&](Serializer &s) { WriteLiteral(s, LiteralKind::HEX, "0a1b"); }));

	// a tampered BOOLEAN literal that would otherwise render raw SQL text verbatim is rejected
	REQUIRE_THROWS_WITH(DeserializeLiteral([&](Serializer &s) {
		                    WriteLiteral(s, LiteralKind::BOOLEAN, "1);CREATE TABLE pwned(x INT");
	                    }),
	                    Catch::Contains("Invalid BOOLEAN literal text"));
	REQUIRE_THROWS_WITH(DeserializeLiteral([&](Serializer &s) { WriteLiteral(s, LiteralKind::INTEGER, "1 OR 1=1"); }),
	                    Catch::Contains("Invalid INTEGER literal text"));
	REQUIRE_THROWS_WITH(DeserializeLiteral([&](Serializer &s) { WriteLiteral(s, LiteralKind::HEX, "0g"); }),
	                    Catch::Contains("Invalid HEX literal text"));
	REQUIRE_THROWS_WITH(DeserializeLiteral([&](Serializer &s) { WriteLiteral(s, LiteralKind::NULL_LITERAL, "x"); }),
	                    Catch::Contains("Invalid NULL literal"));
}

} // namespace duckdb

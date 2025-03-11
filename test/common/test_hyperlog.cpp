#include "catch.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/hyperloglog.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test that hyperloglog works", "[hyperloglog]") {
	HyperLogLog log;
	// add a million elements of the same value
	int x = 4;
	for (size_t i = 0; i < 1000000; i++) {
		log.InsertElement(Hash(x));
	}
	REQUIRE(log.Count() == 1);

	// now add a million different values
	HyperLogLog log2;
	for (size_t i = 0; i < 1000000; i++) {
		x = i;
		log2.InsertElement(Hash(x));
	}
	// the count is approximate, but should be pretty close to a million
	size_t count = log2.Count();
	REQUIRE(count > 950000LL);
	REQUIRE(count < 1050000LL);

	// now we can merge the HLLs
	log.Merge(log2);
	// the count should be pretty much the same
	count = log.Count();
	REQUIRE(count > 950000LL);
	REQUIRE(count < 1050000LL);

	// now test composability of the merge
	// add everything to one big_hll one
	// add chunks to small_hll ones and then merge them
	// the result should be the same
	HyperLogLog big_hll;
	HyperLogLog small_hll[16];
	for (size_t i = 0; i < 1000000; i++) {
		x = ((2 * i) + 3) % (i + 3 / 2);
		big_hll.InsertElement(Hash(x));
		small_hll[i % 16].InsertElement(Hash(x));
	}
	// now merge them into one big_hll HyperLogLog
	for (idx_t i = 1; i < 16; i++) {
		small_hll[0].Merge(small_hll[i]);
	}
	// the result should be identical to the big_hll one
	REQUIRE(small_hll[0].Count() == big_hll.Count());
}

TEST_CASE("Test different hyperloglog version serialization", "[hyperloglog]") {
	Allocator allocator;
	MemoryStream stream(allocator);
	SerializationOptions options;
	options.serialization_compatibility = SerializationCompatibility::FromString("v1.0.0");

	// Add 100M values to a NEW HyperLogLog
	HyperLogLog original_log;
	for (size_t i = 0; i < 100000000; i++) {
		original_log.InsertElement(Hash(i));

		switch (i + 1) {
		case 1:
		case 10:
		case 100:
		case 1000:
		case 10000:
		case 100000:
		case 1000000:
		case 10000000:
		case 100000000:
			break; // We roundtrip the serialization every order of magnitude
		default:
			continue;
		}

		// Grab the count
		const auto original_count = original_log.Count();

		// Serialize it as an OLD HyperLogLog
		stream.Rewind();
		BinarySerializer::Serialize(original_log, stream, options);

		// Deserialize it, creating a NEW HyperLogLog from the OLD one
		stream.Rewind();
		auto deserialized_log = BinaryDeserializer::Deserialize<HyperLogLog>(stream);

		// Verify that the deserialized count is equal
		const auto deserialized_count = deserialized_log->Count();
		REQUIRE(original_count == deserialized_count);
	}
}

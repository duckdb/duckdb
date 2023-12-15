#include "catch.hpp"
#include "duckdb/common/radix.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/execution/index/art/art_key.hpp"

#include <cstring>
#include <iostream>

using namespace duckdb;
using namespace std;

static void TestKeyEqual(ARTKey &left, ARTKey &right) {
	REQUIRE(left == right);
	REQUIRE(left >= right);
	REQUIRE(!(left > right));

	REQUIRE(right == left);
	REQUIRE(right >= left);
	REQUIRE(!(right > left));
}

static void TestKeyBigger(ARTKey &big_key, ARTKey &small_key) {
	REQUIRE(!(big_key == small_key));
	if (!(big_key >= small_key)) {
		REQUIRE(0);
	}
	REQUIRE(big_key > small_key);

	REQUIRE(!(small_key == big_key));
	REQUIRE(!(small_key >= big_key));
	REQUIRE(!(small_key > big_key));
}

static void TestKeys(duckdb::vector<ARTKey> &keys) {
	for (idx_t outer = 0; outer < keys.size(); outer++) {
		for (idx_t inner = 0; inner < keys.size(); inner++) {
			if (inner == outer) {
				TestKeyEqual(keys[inner], keys[outer]);
			} else if (inner > outer) {
				TestKeyBigger(keys[inner], keys[outer]);
			} else {
				TestKeyBigger(keys[outer], keys[inner]);
			}
		}
	}
}

static ARTKey CreateCompoundKey(ArenaAllocator &arena_allocator, string str_val, int32_t int_val) {

	auto key_left = ARTKey::CreateARTKey<string_t>(arena_allocator, LogicalType::VARCHAR,
	                                               string_t(str_val.c_str(), str_val.size()));
	auto key_right = ARTKey::CreateARTKey<int32_t>(arena_allocator, LogicalType::VARCHAR, int_val);

	auto data = arena_allocator.Allocate(key_left.len + key_right.len);
	memcpy(data, key_left.data, key_left.len);
	memcpy(data + key_left.len, key_right.data, key_right.len);
	return ARTKey(data, key_left.len + key_right.len);
}

TEST_CASE("Test correct functioning of art keys", "[art]") {

	ArenaAllocator arena_allocator(Allocator::DefaultAllocator());

	// Test tiny int
	duckdb::vector<ARTKey> keys;
	keys.push_back(ARTKey::CreateARTKey<int8_t>(arena_allocator, LogicalType::TINYINT, -127));
	keys.push_back(ARTKey::CreateARTKey<int8_t>(arena_allocator, LogicalType::TINYINT, -55));
	keys.push_back(ARTKey::CreateARTKey<int8_t>(arena_allocator, LogicalType::TINYINT, -1));
	keys.push_back(ARTKey::CreateARTKey<int8_t>(arena_allocator, LogicalType::TINYINT, 0));
	keys.push_back(ARTKey::CreateARTKey<int8_t>(arena_allocator, LogicalType::TINYINT, 1));
	keys.push_back(ARTKey::CreateARTKey<int8_t>(arena_allocator, LogicalType::TINYINT, 55));
	keys.push_back(ARTKey::CreateARTKey<int8_t>(arena_allocator, LogicalType::TINYINT, 127));
	TestKeys(keys);

	keys.clear();

	// Test small int
	keys.push_back(ARTKey::CreateARTKey<int16_t>(arena_allocator, LogicalType::SMALLINT, -32767));
	keys.push_back(ARTKey::CreateARTKey<int16_t>(arena_allocator, LogicalType::SMALLINT, -127));
	keys.push_back(ARTKey::CreateARTKey<int16_t>(arena_allocator, LogicalType::SMALLINT, -55));
	keys.push_back(ARTKey::CreateARTKey<int16_t>(arena_allocator, LogicalType::SMALLINT, -1));
	keys.push_back(ARTKey::CreateARTKey<int16_t>(arena_allocator, LogicalType::SMALLINT, 0));
	keys.push_back(ARTKey::CreateARTKey<int16_t>(arena_allocator, LogicalType::SMALLINT, 1));
	keys.push_back(ARTKey::CreateARTKey<int16_t>(arena_allocator, LogicalType::SMALLINT, 55));
	keys.push_back(ARTKey::CreateARTKey<int16_t>(arena_allocator, LogicalType::SMALLINT, 127));
	keys.push_back(ARTKey::CreateARTKey<int16_t>(arena_allocator, LogicalType::SMALLINT, 32767));
	TestKeys(keys);

	keys.clear();

	// Test int
	keys.push_back(ARTKey::CreateARTKey<int32_t>(arena_allocator, LogicalType::INTEGER, -2147483647));
	keys.push_back(ARTKey::CreateARTKey<int32_t>(arena_allocator, LogicalType::INTEGER, -8388608));
	keys.push_back(ARTKey::CreateARTKey<int32_t>(arena_allocator, LogicalType::INTEGER, -32767));
	keys.push_back(ARTKey::CreateARTKey<int32_t>(arena_allocator, LogicalType::INTEGER, -1));
	keys.push_back(ARTKey::CreateARTKey<int32_t>(arena_allocator, LogicalType::INTEGER, 0));
	keys.push_back(ARTKey::CreateARTKey<int32_t>(arena_allocator, LogicalType::INTEGER, 1));
	keys.push_back(ARTKey::CreateARTKey<int32_t>(arena_allocator, LogicalType::INTEGER, 32767));
	keys.push_back(ARTKey::CreateARTKey<int32_t>(arena_allocator, LogicalType::INTEGER, 8388608));
	keys.push_back(ARTKey::CreateARTKey<int32_t>(arena_allocator, LogicalType::INTEGER, 2147483647));
	TestKeys(keys);

	keys.clear();

	// Test big int
	keys.push_back(ARTKey::CreateARTKey<int64_t>(arena_allocator, LogicalType::BIGINT, -9223372036854775807));
	keys.push_back(ARTKey::CreateARTKey<int64_t>(arena_allocator, LogicalType::BIGINT, -72057594037927936));
	keys.push_back(ARTKey::CreateARTKey<int64_t>(arena_allocator, LogicalType::BIGINT, -281474976710656));
	keys.push_back(ARTKey::CreateARTKey<int64_t>(arena_allocator, LogicalType::BIGINT, -1099511627776));
	keys.push_back(ARTKey::CreateARTKey<int64_t>(arena_allocator, LogicalType::BIGINT, -2147483647));
	keys.push_back(ARTKey::CreateARTKey<int64_t>(arena_allocator, LogicalType::BIGINT, -8388608));
	keys.push_back(ARTKey::CreateARTKey<int64_t>(arena_allocator, LogicalType::BIGINT, -32767));
	keys.push_back(ARTKey::CreateARTKey<int64_t>(arena_allocator, LogicalType::BIGINT, -1));
	keys.push_back(ARTKey::CreateARTKey<int64_t>(arena_allocator, LogicalType::BIGINT, 0));
	keys.push_back(ARTKey::CreateARTKey<int64_t>(arena_allocator, LogicalType::BIGINT, 1));
	keys.push_back(ARTKey::CreateARTKey<int64_t>(arena_allocator, LogicalType::BIGINT, 32767));
	keys.push_back(ARTKey::CreateARTKey<int64_t>(arena_allocator, LogicalType::BIGINT, 8388608));
	keys.push_back(ARTKey::CreateARTKey<int64_t>(arena_allocator, LogicalType::BIGINT, 2147483647));
	keys.push_back(ARTKey::CreateARTKey<int64_t>(arena_allocator, LogicalType::BIGINT, 1099511627776));
	keys.push_back(ARTKey::CreateARTKey<int64_t>(arena_allocator, LogicalType::BIGINT, 281474976710656));
	keys.push_back(ARTKey::CreateARTKey<int64_t>(arena_allocator, LogicalType::BIGINT, 72057594037927936));
	keys.push_back(ARTKey::CreateARTKey<int64_t>(arena_allocator, LogicalType::BIGINT, 9223372036854775807));
	TestKeys(keys);

	keys.clear();

	// Test utiny int
	keys.push_back(ARTKey::CreateARTKey<uint8_t>(arena_allocator, LogicalType::UTINYINT, 0));
	keys.push_back(ARTKey::CreateARTKey<uint8_t>(arena_allocator, LogicalType::UTINYINT, 1));
	keys.push_back(ARTKey::CreateARTKey<uint8_t>(arena_allocator, LogicalType::UTINYINT, 55));
	keys.push_back(ARTKey::CreateARTKey<uint8_t>(arena_allocator, LogicalType::UTINYINT, 127));
	keys.push_back(ARTKey::CreateARTKey<uint8_t>(arena_allocator, LogicalType::UTINYINT, 200));
	keys.push_back(ARTKey::CreateARTKey<uint8_t>(arena_allocator, LogicalType::UTINYINT, 250));
	TestKeys(keys);

	keys.clear();

	// Test usmall int
	keys.push_back(ARTKey::CreateARTKey<uint16_t>(arena_allocator, LogicalType::USMALLINT, 0));
	keys.push_back(ARTKey::CreateARTKey<uint16_t>(arena_allocator, LogicalType::USMALLINT, 1));
	keys.push_back(ARTKey::CreateARTKey<uint16_t>(arena_allocator, LogicalType::USMALLINT, 55));
	keys.push_back(ARTKey::CreateARTKey<uint16_t>(arena_allocator, LogicalType::USMALLINT, 127));
	keys.push_back(ARTKey::CreateARTKey<uint16_t>(arena_allocator, LogicalType::USMALLINT, 32767));
	keys.push_back(ARTKey::CreateARTKey<uint16_t>(arena_allocator, LogicalType::USMALLINT, 40000));
	keys.push_back(ARTKey::CreateARTKey<uint16_t>(arena_allocator, LogicalType::USMALLINT, 60000));

	TestKeys(keys);

	keys.clear();

	// Test uint
	keys.push_back(ARTKey::CreateARTKey<uint32_t>(arena_allocator, LogicalType::UINTEGER, 0));
	keys.push_back(ARTKey::CreateARTKey<uint32_t>(arena_allocator, LogicalType::UINTEGER, 1));
	keys.push_back(ARTKey::CreateARTKey<uint32_t>(arena_allocator, LogicalType::UINTEGER, 32767));
	keys.push_back(ARTKey::CreateARTKey<uint32_t>(arena_allocator, LogicalType::UINTEGER, 8388608));
	keys.push_back(ARTKey::CreateARTKey<uint32_t>(arena_allocator, LogicalType::UINTEGER, 2147483647));
	keys.push_back(ARTKey::CreateARTKey<uint32_t>(arena_allocator, LogicalType::UINTEGER, 3047483647));
	keys.push_back(ARTKey::CreateARTKey<uint32_t>(arena_allocator, LogicalType::UINTEGER, 4047483647));
	TestKeys(keys);

	keys.clear();

	// Test ubig int
	keys.push_back(ARTKey::CreateARTKey<uint64_t>(arena_allocator, LogicalType::UBIGINT, 0));
	keys.push_back(ARTKey::CreateARTKey<uint64_t>(arena_allocator, LogicalType::UBIGINT, 1));
	keys.push_back(ARTKey::CreateARTKey<uint64_t>(arena_allocator, LogicalType::UBIGINT, 32767));
	keys.push_back(ARTKey::CreateARTKey<uint64_t>(arena_allocator, LogicalType::UBIGINT, 8388608));
	keys.push_back(ARTKey::CreateARTKey<uint64_t>(arena_allocator, LogicalType::UBIGINT, 2147483647));
	keys.push_back(ARTKey::CreateARTKey<uint64_t>(arena_allocator, LogicalType::UBIGINT, 1099511627776));
	keys.push_back(ARTKey::CreateARTKey<uint64_t>(arena_allocator, LogicalType::UBIGINT, 281474976710656));
	keys.push_back(ARTKey::CreateARTKey<uint64_t>(arena_allocator, LogicalType::UBIGINT, 72057594037927936));
	TestKeys(keys);

	keys.clear();

	// Test strings
	keys.push_back(ARTKey::CreateARTKey<const char *>(arena_allocator, LogicalType::VARCHAR, "abc"));
	keys.push_back(ARTKey::CreateARTKey<const char *>(arena_allocator, LogicalType::VARCHAR, "babababa"));
	keys.push_back(ARTKey::CreateARTKey<const char *>(arena_allocator, LogicalType::VARCHAR, "hello"));
	keys.push_back(ARTKey::CreateARTKey<const char *>(arena_allocator, LogicalType::VARCHAR, "hellow"));
	keys.push_back(ARTKey::CreateARTKey<const char *>(arena_allocator, LogicalType::VARCHAR, "torororororo"));
	keys.push_back(ARTKey::CreateARTKey<const char *>(arena_allocator, LogicalType::VARCHAR, "torororororp"));
	keys.push_back(ARTKey::CreateARTKey<const char *>(arena_allocator, LogicalType::VARCHAR, "z"));

	TestKeys(keys);

	keys.clear();

	// test compound keys
	keys.push_back(CreateCompoundKey(arena_allocator, "abc", -100));
	keys.push_back(CreateCompoundKey(arena_allocator, "abc", 1000));
	keys.push_back(CreateCompoundKey(arena_allocator, "abcd", -100000));
	keys.push_back(CreateCompoundKey(arena_allocator, "hello", -100000));
	keys.push_back(CreateCompoundKey(arena_allocator, "hello", -1));
	keys.push_back(CreateCompoundKey(arena_allocator, "hello", 0));
	keys.push_back(CreateCompoundKey(arena_allocator, "hello", 1));
	keys.push_back(CreateCompoundKey(arena_allocator, "hellow", -10000));
	keys.push_back(CreateCompoundKey(arena_allocator, "z", 30));

	TestKeys(keys);

	keys.clear();

	keys.push_back(ARTKey::CreateARTKey<double>(arena_allocator, LogicalType::DOUBLE, 0));
	keys.push_back(ARTKey::CreateARTKey<double>(arena_allocator, LogicalType::DOUBLE, 0.1));
	keys.push_back(ARTKey::CreateARTKey<double>(arena_allocator, LogicalType::DOUBLE, 488566));
	keys.push_back(ARTKey::CreateARTKey<double>(arena_allocator, LogicalType::DOUBLE, 1163404482));

	TestKeys(keys);

	keys.clear();
}

TEST_CASE("Test correct functioning of art EncodeFloat/EncodeDouble", "[art-enc]") {
	{
		// EncodeFloat
		// positive values
		duckdb::vector<float> values;
		float current_value = 0.00001f;
		while (isfinite(current_value)) {
			values.push_back(current_value);
			current_value *= 2;
		}
		// negative values
		current_value = -0.00001f;
		while (isfinite(current_value)) {
			values.push_back(current_value);
			current_value *= 2;
		}
		std::sort(values.begin(), values.end());
		uint32_t current_encoded = Radix::EncodeFloat(values[0]);
		for (idx_t i = 1; i < values.size(); i++) {
			uint32_t next_encoded = Radix::EncodeFloat(values[i]);
			if (next_encoded <= current_encoded) {
				printf("Failure in Key::EncodeFloat!\n");
				printf(
				    "Generated value for key %f (=> %u) is bigger or equal to the generated value for key %f (=> %u)\n",
				    values[i - 1], current_encoded, values[i], next_encoded);
			}
			REQUIRE(next_encoded > current_encoded);
			current_encoded = next_encoded;
		}
	}
	{
		// EncodeDouble
		// positive values
		duckdb::vector<double> values;
		double current_value = 0.0000001;
		while (isfinite(current_value)) {
			values.push_back(current_value);
			current_value *= 2;
		}
		// negative values
		current_value = -0.0000001;
		while (isfinite(current_value)) {
			values.push_back(current_value);
			current_value *= 2;
		}
		std::sort(values.begin(), values.end());
		uint64_t current_encoded = Radix::EncodeDouble(values[0]);
		for (idx_t i = 1; i < values.size(); i++) {
			uint64_t next_encoded = Radix::EncodeDouble(values[i]);
			if (next_encoded <= current_encoded) {
				cout << "Failure in Key::EncodeDouble!" << std::endl;
				cout << "Generated value for key " << values[i - 1] << " (=> %" << current_encoded
				     << ") is bigger or equal to the generated value for key " << values[i] << "(=> %" << next_encoded
				     << ")" << std::endl;
			}
			REQUIRE(next_encoded > current_encoded);
			current_encoded = next_encoded;
		}
	}
}

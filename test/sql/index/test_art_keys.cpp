#include "catch.hpp"
#include "duckdb/common/radix.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/execution/index/art/art_key.hpp"

#include <cstring>
#include <iostream>

using namespace duckdb;
using namespace std;

static void TestKeyEqual(Key &left, Key &right) {
	REQUIRE(left == right);
	REQUIRE(left >= right);
	REQUIRE(!(left > right));

	REQUIRE(right == left);
	REQUIRE(right >= left);
	REQUIRE(!(right > left));
}

static void TestKeyBigger(Key &big_key, Key &small_key) {
	REQUIRE(!(big_key == small_key));
	if (!(big_key >= small_key)) {
		REQUIRE(0);
	}
	REQUIRE(big_key > small_key);

	REQUIRE(!(small_key == big_key));
	REQUIRE(!(small_key >= big_key));
	REQUIRE(!(small_key > big_key));
}

static void TestKeys(vector<unique_ptr<Key>> &keys) {
	for (idx_t outer = 0; outer < keys.size(); outer++) {
		for (idx_t inner = 0; inner < keys.size(); inner++) {
			if (inner == outer) {
				TestKeyEqual(*keys[inner], *keys[outer]);
			} else if (inner > outer) {
				TestKeyBigger(*keys[inner], *keys[outer]);
			} else {
				TestKeyBigger(*keys[outer], *keys[inner]);
			}
		}
	}
}

static unique_ptr<Key> CreateCompoundKey(string str_val, int32_t int_val) {
	auto key_left = Key::CreateKey<string_t>(string_t(str_val.c_str(), str_val.size()));
	auto key_right = Key::CreateKey<int32_t>(int_val);
	unique_ptr<data_t[]> data = unique_ptr<data_t[]>(new data_t[key_left->len + key_right->len]);
	memcpy(data.get(), key_left->data.get(), key_left->len);
	memcpy(data.get() + key_left->len, key_right->data.get(), key_right->len);
	return make_unique<Key>(move(data), key_left->len + key_right->len);
}

TEST_CASE("Test correct functioning of art keys", "[art]") {
	// Test tiny int
	vector<unique_ptr<Key>> keys;
	keys.push_back(Key::CreateKey<int8_t>(-127));
	keys.push_back(Key::CreateKey<int8_t>(-55));
	keys.push_back(Key::CreateKey<int8_t>(-1));
	keys.push_back(Key::CreateKey<int8_t>(0));
	keys.push_back(Key::CreateKey<int8_t>(1));
	keys.push_back(Key::CreateKey<int8_t>(55));
	keys.push_back(Key::CreateKey<int8_t>(127));
	TestKeys(keys);

	keys.clear();

	// Test small int
	keys.push_back(Key::CreateKey<int16_t>(-32767));
	keys.push_back(Key::CreateKey<int16_t>(-127));
	keys.push_back(Key::CreateKey<int16_t>(-55));
	keys.push_back(Key::CreateKey<int16_t>(-1));
	keys.push_back(Key::CreateKey<int16_t>(0));
	keys.push_back(Key::CreateKey<int16_t>(1));
	keys.push_back(Key::CreateKey<int16_t>(55));
	keys.push_back(Key::CreateKey<int16_t>(127));
	keys.push_back(Key::CreateKey<int16_t>(32767));
	TestKeys(keys);

	keys.clear();

	// Test int
	keys.push_back(Key::CreateKey<int32_t>(-2147483647));
	keys.push_back(Key::CreateKey<int32_t>(-8388608));
	keys.push_back(Key::CreateKey<int32_t>(-32767));
	keys.push_back(Key::CreateKey<int32_t>(-1));
	keys.push_back(Key::CreateKey<int32_t>(0));
	keys.push_back(Key::CreateKey<int32_t>(1));
	keys.push_back(Key::CreateKey<int32_t>(32767));
	keys.push_back(Key::CreateKey<int32_t>(8388608));
	keys.push_back(Key::CreateKey<int32_t>(2147483647));
	TestKeys(keys);

	keys.clear();

	// Test big int
	keys.push_back(Key::CreateKey<int64_t>(-9223372036854775807));
	keys.push_back(Key::CreateKey<int64_t>(-72057594037927936));
	keys.push_back(Key::CreateKey<int64_t>(-281474976710656));
	keys.push_back(Key::CreateKey<int64_t>(-1099511627776));
	keys.push_back(Key::CreateKey<int64_t>(-2147483647));
	keys.push_back(Key::CreateKey<int64_t>(-8388608));
	keys.push_back(Key::CreateKey<int64_t>(-32767));
	keys.push_back(Key::CreateKey<int64_t>(-1));
	keys.push_back(Key::CreateKey<int64_t>(0));
	keys.push_back(Key::CreateKey<int64_t>(1));
	keys.push_back(Key::CreateKey<int64_t>(32767));
	keys.push_back(Key::CreateKey<int64_t>(8388608));
	keys.push_back(Key::CreateKey<int64_t>(2147483647));
	keys.push_back(Key::CreateKey<int64_t>(1099511627776));
	keys.push_back(Key::CreateKey<int64_t>(281474976710656));
	keys.push_back(Key::CreateKey<int64_t>(72057594037927936));
	keys.push_back(Key::CreateKey<int64_t>(9223372036854775807));
	TestKeys(keys);

	keys.clear();

	// Test utiny int
	keys.push_back(Key::CreateKey<uint8_t>(0));
	keys.push_back(Key::CreateKey<uint8_t>(1));
	keys.push_back(Key::CreateKey<uint8_t>(55));
	keys.push_back(Key::CreateKey<uint8_t>(127));
	keys.push_back(Key::CreateKey<uint8_t>(200));
	keys.push_back(Key::CreateKey<uint8_t>(250));
	TestKeys(keys);

	keys.clear();

	// Test small int
	keys.push_back(Key::CreateKey<uint16_t>(0));
	keys.push_back(Key::CreateKey<uint16_t>(1));
	keys.push_back(Key::CreateKey<uint16_t>(55));
	keys.push_back(Key::CreateKey<uint16_t>(127));
	keys.push_back(Key::CreateKey<uint16_t>(32767));
	keys.push_back(Key::CreateKey<uint16_t>(40000));
	keys.push_back(Key::CreateKey<uint16_t>(60000));

	TestKeys(keys);

	keys.clear();

	// Test int
	keys.push_back(Key::CreateKey<uint32_t>(0));
	keys.push_back(Key::CreateKey<uint32_t>(1));
	keys.push_back(Key::CreateKey<uint32_t>(32767));
	keys.push_back(Key::CreateKey<uint32_t>(8388608));
	keys.push_back(Key::CreateKey<uint32_t>(2147483647));
	keys.push_back(Key::CreateKey<uint32_t>(3047483647));
	keys.push_back(Key::CreateKey<uint32_t>(4047483647));
	TestKeys(keys);

	keys.clear();

	// Test big int
	keys.push_back(Key::CreateKey<uint64_t>(0));
	keys.push_back(Key::CreateKey<uint64_t>(1));
	keys.push_back(Key::CreateKey<uint64_t>(32767));
	keys.push_back(Key::CreateKey<uint64_t>(8388608));
	keys.push_back(Key::CreateKey<uint64_t>(2147483647));
	keys.push_back(Key::CreateKey<uint64_t>(1099511627776));
	keys.push_back(Key::CreateKey<uint64_t>(281474976710656));
	keys.push_back(Key::CreateKey<uint64_t>(72057594037927936));
	TestKeys(keys);

	keys.clear();

	// Test strings
	keys.push_back(Key::CreateKey<const char *>("abc"));
	keys.push_back(Key::CreateKey<const char *>("babababa"));
	keys.push_back(Key::CreateKey<const char *>("hello"));
	keys.push_back(Key::CreateKey<const char *>("hellow"));
	keys.push_back(Key::CreateKey<const char *>("torororororo"));
	keys.push_back(Key::CreateKey<const char *>("torororororp"));
	keys.push_back(Key::CreateKey<const char *>("z"));

	TestKeys(keys);

	keys.clear();

	// test compound keys
	keys.push_back(CreateCompoundKey("abc", -100));
	keys.push_back(CreateCompoundKey("abc", 1000));
	keys.push_back(CreateCompoundKey("abcd", -100000));
	keys.push_back(CreateCompoundKey("hello", -100000));
	keys.push_back(CreateCompoundKey("hello", -1));
	keys.push_back(CreateCompoundKey("hello", 0));
	keys.push_back(CreateCompoundKey("hello", 1));
	keys.push_back(CreateCompoundKey("hellow", -10000));
	keys.push_back(CreateCompoundKey("z", 30));

	TestKeys(keys);

	keys.clear();

	keys.push_back(Key::CreateKey<double>(0));
	keys.push_back(Key::CreateKey<double>(0.1));
	keys.push_back(Key::CreateKey<double>(488566));
	keys.push_back(Key::CreateKey<double>(1163404482));

	TestKeys(keys);

	keys.clear();
}

TEST_CASE("Test correct functioning of art EncodeFloat/EncodeDouble", "[art-enc]") {
	{
		// EncodeFloat
		// positive values
		vector<float> values;
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
		vector<double> values;
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

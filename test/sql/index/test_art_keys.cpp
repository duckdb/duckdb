#include "catch.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/common/types/string_type.hpp"
#include <iostream>
#include <cstring>

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

static void TestKeyBigger(Key &big, Key &small) {
	REQUIRE(!(big == small));
	if (!(big >= small)) {
		REQUIRE(0);
	}
	REQUIRE(big > small);

	REQUIRE(!(small == big));
	REQUIRE(!(small >= big));
	REQUIRE(!(small > big));
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

static unique_ptr<Key> CreateCompoundKey(string str_val, int32_t int_val, bool is_little_endian) {
	auto key_left = Key::CreateKey<string_t>(string_t(str_val.c_str(), str_val.size()), is_little_endian);
	auto key_right = Key::CreateKey<int32_t>(int_val, is_little_endian);
	unique_ptr<data_t[]> data = unique_ptr<data_t[]>(new data_t[key_left->len + key_right->len]);
	memcpy(data.get(), key_left->data.get(), key_left->len);
	memcpy(data.get() + key_left->len, key_right->data.get(), key_right->len);
	return make_unique<Key>(move(data), key_left->len + key_right->len);
}

TEST_CASE("Test correct functioning of art keys", "[art]") {
	bool is_little_endian;
	int n = 1;
	if (*(char *)&n == 1) {
		is_little_endian = true;
	} else {
		is_little_endian = false;
	}
	// Test tiny int
	vector<unique_ptr<Key>> keys;
	keys.push_back(Key::CreateKey<int8_t>(-127, is_little_endian));
	keys.push_back(Key::CreateKey<int8_t>(-55, is_little_endian));
	keys.push_back(Key::CreateKey<int8_t>(-1, is_little_endian));
	keys.push_back(Key::CreateKey<int8_t>(0, is_little_endian));
	keys.push_back(Key::CreateKey<int8_t>(1, is_little_endian));
	keys.push_back(Key::CreateKey<int8_t>(55, is_little_endian));
	keys.push_back(Key::CreateKey<int8_t>(127, is_little_endian));
	TestKeys(keys);

	keys.clear();

	// Test small int
	keys.push_back(Key::CreateKey<int16_t>(-32767, is_little_endian));
	keys.push_back(Key::CreateKey<int16_t>(-127, is_little_endian));
	keys.push_back(Key::CreateKey<int16_t>(-55, is_little_endian));
	keys.push_back(Key::CreateKey<int16_t>(-1, is_little_endian));
	keys.push_back(Key::CreateKey<int16_t>(0, is_little_endian));
	keys.push_back(Key::CreateKey<int16_t>(1, is_little_endian));
	keys.push_back(Key::CreateKey<int16_t>(55, is_little_endian));
	keys.push_back(Key::CreateKey<int16_t>(127, is_little_endian));
	keys.push_back(Key::CreateKey<int16_t>(32767, is_little_endian));
	TestKeys(keys);

	keys.clear();

	// Test int
	keys.push_back(Key::CreateKey<int32_t>(-2147483647, is_little_endian));
	keys.push_back(Key::CreateKey<int32_t>(-8388608, is_little_endian));
	keys.push_back(Key::CreateKey<int32_t>(-32767, is_little_endian));
	keys.push_back(Key::CreateKey<int32_t>(-1, is_little_endian));
	keys.push_back(Key::CreateKey<int32_t>(0, is_little_endian));
	keys.push_back(Key::CreateKey<int32_t>(1, is_little_endian));
	keys.push_back(Key::CreateKey<int32_t>(32767, is_little_endian));
	keys.push_back(Key::CreateKey<int32_t>(8388608, is_little_endian));
	keys.push_back(Key::CreateKey<int32_t>(2147483647, is_little_endian));
	TestKeys(keys);

	keys.clear();

	// Test big int
	keys.push_back(Key::CreateKey<int64_t>(-9223372036854775807, is_little_endian));
	keys.push_back(Key::CreateKey<int64_t>(-72057594037927936, is_little_endian));
	keys.push_back(Key::CreateKey<int64_t>(-281474976710656, is_little_endian));
	keys.push_back(Key::CreateKey<int64_t>(-1099511627776, is_little_endian));
	keys.push_back(Key::CreateKey<int64_t>(-2147483647, is_little_endian));
	keys.push_back(Key::CreateKey<int64_t>(-8388608, is_little_endian));
	keys.push_back(Key::CreateKey<int64_t>(-32767, is_little_endian));
	keys.push_back(Key::CreateKey<int64_t>(-1, is_little_endian));
	keys.push_back(Key::CreateKey<int64_t>(0, is_little_endian));
	keys.push_back(Key::CreateKey<int64_t>(1, is_little_endian));
	keys.push_back(Key::CreateKey<int64_t>(32767, is_little_endian));
	keys.push_back(Key::CreateKey<int64_t>(8388608, is_little_endian));
	keys.push_back(Key::CreateKey<int64_t>(2147483647, is_little_endian));
	keys.push_back(Key::CreateKey<int64_t>(1099511627776, is_little_endian));
	keys.push_back(Key::CreateKey<int64_t>(281474976710656, is_little_endian));
	keys.push_back(Key::CreateKey<int64_t>(72057594037927936, is_little_endian));
	keys.push_back(Key::CreateKey<int64_t>(9223372036854775807, is_little_endian));
	TestKeys(keys);

	keys.clear();

	// Test strings
	keys.push_back(Key::CreateKey<const char *>("abc", is_little_endian));
	keys.push_back(Key::CreateKey<const char *>("babababa", is_little_endian));
	keys.push_back(Key::CreateKey<const char *>("hello", is_little_endian));
	keys.push_back(Key::CreateKey<const char *>("hellow", is_little_endian));
	keys.push_back(Key::CreateKey<const char *>("torororororo", is_little_endian));
	keys.push_back(Key::CreateKey<const char *>("torororororp", is_little_endian));
	keys.push_back(Key::CreateKey<const char *>("z", is_little_endian));

	TestKeys(keys);

	keys.clear();

	// test compound keys
	keys.push_back(CreateCompoundKey("abc", -100, is_little_endian));
	keys.push_back(CreateCompoundKey("abc", 1000, is_little_endian));
	keys.push_back(CreateCompoundKey("abcd", -100000, is_little_endian));
	keys.push_back(CreateCompoundKey("hello", -100000, is_little_endian));
	keys.push_back(CreateCompoundKey("hello", -1, is_little_endian));
	keys.push_back(CreateCompoundKey("hello", 0, is_little_endian));
	keys.push_back(CreateCompoundKey("hello", 1, is_little_endian));
	keys.push_back(CreateCompoundKey("hellow", -10000, is_little_endian));
	keys.push_back(CreateCompoundKey("z", 30, is_little_endian));

	TestKeys(keys);

	keys.clear();

	keys.push_back(Key::CreateKey<double>(0, is_little_endian));
	keys.push_back(Key::CreateKey<double>(0.1, is_little_endian));
	keys.push_back(Key::CreateKey<double>(488566, is_little_endian));
	keys.push_back(Key::CreateKey<double>(1163404482, is_little_endian));

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
		uint32_t current_encoded = Key::EncodeFloat(values[0]);
		for (idx_t i = 1; i < values.size(); i++) {
			uint32_t next_encoded = Key::EncodeFloat(values[i]);
			if (!(next_encoded > current_encoded)) {
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
		uint64_t current_encoded = Key::EncodeDouble(values[0]);
		for (idx_t i = 1; i < values.size(); i++) {
			uint64_t next_encoded = Key::EncodeDouble(values[i]);
			if (!(next_encoded > current_encoded)) {
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

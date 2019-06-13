#include "catch.hpp"
#include "common/operator/cast_operators.hpp"
#include "common/string_util.hpp"
#include <vector>

using namespace duckdb;
using namespace std;

template<class SRC, class DST>
static void TestNumericCast(vector<SRC> &working_values, vector<SRC> &broken_values) {
	DST result;
	for(auto value : working_values) {
		REQUIRE_NOTHROW(Cast::Operation<SRC, DST>(value) == (DST) value);
		REQUIRE(TryCast::Operation<SRC, DST>(value, result));
		REQUIRE(result == (DST) value);
	}
	for(auto value : broken_values) {
		REQUIRE_THROWS(Cast::Operation<SRC, DST>(value));
		REQUIRE(!TryCast::Operation<SRC, DST>(value, result));
	}
}

template<class DST>
static void TestStringCast(vector<string> &working_values, vector<DST> &expected_values, vector<string> &broken_values) {
	DST result;
	for(index_t i = 0; i < working_values.size(); i++) {
		auto &value = working_values[i];
		auto expected_value = expected_values[i];
		REQUIRE_NOTHROW(Cast::Operation<const char*, DST>(value.c_str()) == expected_value);
		REQUIRE(TryCast::Operation<const char*, DST>(value.c_str(), result));
		REQUIRE(result == expected_value);

		auto splits = StringUtil::Split(value, '.');
		REQUIRE(Cast::Operation<DST, string>(result) == splits[0]);
	}
	for(auto &value : broken_values) {
		REQUIRE_THROWS(Cast::Operation<const char*, DST>(value.c_str()));
		REQUIRE(!TryCast::Operation<const char*, DST>(value.c_str(), result));
	}
}

TEST_CASE("Test casting to boolean", "[cast]") {
	vector<string> working_values     = { "true", "false", "TRUE", "FALSE", "T", "F" };
	vector<int8_t> expected_values    = { true, false, true, false, true, false };
	vector<string> broken_values = { "1", "blabla", "", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaa" };

	bool result;
	for(index_t i = 0; i < working_values.size(); i++) {
		auto &value = working_values[i];
		auto expected_value = expected_values[i];
		REQUIRE_NOTHROW(Cast::Operation<const char*, bool>(value.c_str()) == expected_value);
		REQUIRE(TryCast::Operation<const char*, bool>(value.c_str(), result));
		REQUIRE(result == expected_value);
	}
	for(auto &value : broken_values) {
		REQUIRE_THROWS(Cast::Operation<const char*, bool>(value.c_str()));
		REQUIRE(!TryCast::Operation<const char*, bool>(value.c_str(), result));
	}
}

TEST_CASE("Test casting to int8_t", "[cast]") {
	// int16_t -> int8_t
	vector<int16_t> working_values_int16     = { 10, -10, 127, -127 };
	vector<int16_t> broken_values_int16 = { 128, -128, 1000, -1000 };
	TestNumericCast<int16_t, int8_t>(working_values_int16, broken_values_int16);
	// int32_t -> int8_t
	vector<int32_t> working_values_int32     = { 10, -10, 127, -127 };
	vector<int32_t> broken_values_int32 = { 128, -128, 1000000, -1000000 };
	TestNumericCast<int32_t, int8_t>(working_values_int32, broken_values_int32);
	// int64_t -> int8_t
	vector<int64_t> working_values_int64     = { 10, -10, 127, -127 };
	vector<int64_t> broken_values_int64 = { 128, -128, 10000000000, -10000000000 };
	TestNumericCast<int64_t, int8_t>(working_values_int64, broken_values_int64);
	// float -> int8_t
	vector<float> working_values_float = { 10, -10, 127, -127, 1.3, -2.7 };
	vector<float> broken_values_float = { 128, -128, 10000000000, -10000000000, 1e30f, -1e30f };
	TestNumericCast<float, int8_t>(working_values_float, broken_values_float);
	// double -> int8_t
	vector<double> working_values_double = { 10, -10, 127, -127, 1.3, -2.7 };
	vector<double> broken_values_double = { 128, -128, 10000000000, -10000000000, 1e100, -1e100 };
	TestNumericCast<double, int8_t>(working_values_double, broken_values_double);
	// string -> int8_t
	vector<string> working_values_str     = { "10", "-10", "127", "-127", "1.3" };
	vector<int8_t> expected_values_str    = { 10, -10, 127, -127, 1 };
	vector<string> broken_values_str = { "128", "-128", "10000000000000000000000000000000000000000000000000000000000000", "1.", "aaaa", "19A", "" };
	TestStringCast<int8_t>(working_values_str, expected_values_str, broken_values_str);
}

TEST_CASE("Test casting to int16_t", "[cast]") {
	// int32_t -> int16_t
	vector<int32_t> working_values_int32     = { 10, -10, 127, -127, 32767, -32767 };
	vector<int32_t> broken_values_int32 = { 32768, -32768, 1000000, -1000000 };
	TestNumericCast<int32_t, int16_t>(working_values_int32, broken_values_int32);
	// int64_t -> int16_t
	vector<int64_t> working_values_int64     = { 10, -10, 127, -127,  32767, -32767 };
	vector<int64_t> broken_values_int64 = { 32768, -32768, 10000000000, -10000000000 };
	TestNumericCast<int64_t, int16_t>(working_values_int64, broken_values_int64);
	// float -> int16_t
	vector<float> working_values_float     = { 10.0f, -10.0f, 32767.0f, -32767.0f, 1.3f, -2.7f };
	vector<float> broken_values_float = { 32768.0f, -32768.0f, 10000000000.0f, -10000000000.0f, 1e30f, -1e30f };
	TestNumericCast<float, int16_t>(working_values_float, broken_values_float);
	// double -> int16_t
	vector<double> working_values_double     = { 10, -10, 32767, -32767, 1.3, -2.7 };
	vector<double> broken_values_double = { 32768, -32768, 10000000000, -10000000000, 1e100, -1e100 };
	TestNumericCast<double, int16_t>(working_values_double, broken_values_double);
	// string -> int16_t
	vector<string> working_values_str     = { "10", "-10", "32767", "-32767", "1.3" };
	vector<int16_t> expected_values_str    = { 10, -10, 32767, -32767, 1 };
	vector<string> broken_values_str = { "32768", "-32768", "10000000000000000000000000000000000000000000000000000000000000", "1.", "aaaa", "19A", "", "1.A" };
	TestStringCast<int16_t>(working_values_str, expected_values_str, broken_values_str);
}

TEST_CASE("Test casting to int32_t", "[cast]") {
	// int64_t -> int32_t
	vector<int64_t> working_values_int64     = { 10, -10, 127, -127,  32767, -32767, 2147483647, -2147483647 };
	vector<int64_t> broken_values_int64 = { 2147483648, -2147483648, 10000000000, -10000000000 };
	TestNumericCast<int64_t, int32_t>(working_values_int64, broken_values_int64);
	// float -> int32_t
	vector<float> working_values_float     = { 10.0f, -10.0f, 2000000000.0f, -2000000000.0f, 1.3f, -2.7f };
	vector<float> broken_values_float = { 3000000000.0f, -3000000000.0f, 10000000000.0f, -10000000000.0f, 1e30f, -1e30f };
	TestNumericCast<float, int32_t>(working_values_float, broken_values_float);
	// double -> int32_t
	vector<double> working_values_double     = { 10, -10, 32767, -32767, 1.3, -2.7, 2147483647, -2147483647 };
	vector<double> broken_values_double = { 2147483648, -2147483648, 10000000000, -10000000000, 1e100, -1e100 };
	TestNumericCast<double, int32_t>(working_values_double, broken_values_double);
	// string -> int32_t
	vector<string> working_values_str     = { "10", "-10", "2147483647", "-2147483647", "1.3" };
	vector<int32_t> expected_values_str    = { 10, -10, 2147483647, -2147483647, 1 };
	vector<string> broken_values_str = { "2147483648", "-2147483648", "10000000000000000000000000000000000000000000000000000000000000", "1.", "aaaa", "19A", "", "1.A" };
	TestStringCast<int32_t>(working_values_str, expected_values_str, broken_values_str);
}

TEST_CASE("Test casting to int64_t", "[cast]") {
	// float -> int64_t
	vector<float> working_values_float     = { 10.0f, -10.0f, 32767.0f, -32767.0f, 1.3, -2.7, 2000000000.0f, -2000000000.0f, 4000000000000000000.0f, -4000000000000000000.0f };
	vector<float> broken_values_float = { 20000000000000000000.0f, -20000000000000000000.0f, 1e30f, -1e30f };
	TestNumericCast<float, int64_t>(working_values_float, broken_values_float);
	// double -> int64_t
	vector<double> working_values_double     = { 10, -10, 32767, -32767, 1.3, -2.7, 2147483647, -2147483647, 4611686018427387904, -4611686018427387904 };
	vector<double> broken_values_double = { 18446744073709551616.0, -18446744073709551616.0, 1e100, -1e100 };
	TestNumericCast<double, int64_t>(working_values_double, broken_values_double);
	// string -> int64_t
	vector<string> working_values_str     = { "10", "-10", "9223372036854775807", "-9223372036854775807", "1.3" };
	vector<int64_t> expected_values_str    = { 10, -10, 9223372036854775807LL, -9223372036854775807LL, 1 };
	vector<string> broken_values_str = { "9223372036854775808", "-9223372036854775808", "10000000000000000000000000000000000000000000000000000000000000", "1.", "aaaa", "19A", "", "1.A", "1.2382398723A"};
	TestStringCast<int64_t>(working_values_str, expected_values_str, broken_values_str);
}
